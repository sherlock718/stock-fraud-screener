"""
Train a LightGBM regression model to predict excess_return_local_3y.

Uses the frozen ICIR-selected features from models/feature_sets_3y.json (the same 45
features that passed the binary 3y classification's ICIR gate), so no new feature
selection is performed on the regression target — this prevents overfitting to the
magnitude target while reusing already-validated predictors.

Target: excess_return_local_3y winsorized at 1st/99th pct on the train split only.
Algorithm: LightGBM Huber regression (robust to the rare +100% outlier years).
Primary metric: Spearman IC (rank correlation of predicted vs actual excess return).

Temporal split (PIT-safe, same as binary model):
  train : fiscal_year <= TRAIN_CUTOFF AND filed_date < Jan 1 of (TRAIN_CUTOFF+1)
  val   : TRAIN_CUTOFF < fiscal_year <= VAL_END
  test  : fiscal_year > VAL_END

Outputs:
  models/model_3y_regression.joblib     Huber regressor
  models/model_3y_regression_meta.json  Feature list, IC stats, train medians
  reports/regression_ic_3y.csv          Walk-forward Spearman IC per fold

Usage:
    python3 scripts/train_regression_model.py
    python3 scripts/train_regression_model.py --walk-forward
    python3 scripts/train_regression_model.py --train-cutoff 2020
    python3 scripts/train_regression_model.py --no-walk-forward  # skip WF CV (faster)
"""
from __future__ import annotations

import argparse
import json
import warnings
warnings.filterwarnings('ignore')

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats

BASE       = Path(__file__).parent.parent
MODELS_DIR = BASE / 'models'
REPORTS    = BASE / 'reports'
MODELS_DIR.mkdir(exist_ok=True)
REPORTS.mkdir(exist_ok=True)

import sys
sys.path.insert(0, str(BASE))

TRAIN_CUTOFF = 2022
VAL_END      = 2023
TARGET_COL   = 'excess_return_local_3y'
WINSOR_LO    = 0.01
WINSOR_HI    = 0.99


def _load_data() -> pd.DataFrame:
    """Reuse the same loader as train_models.py to ensure identical preprocessing."""
    from scripts.train_models import load_data
    return load_data()


def _load_features() -> list[str]:
    """Load the frozen 3y feature set (45 features from binary ICIR selection)."""
    feat_path = MODELS_DIR / 'feature_sets_3y.json'
    if not feat_path.exists():
        raise FileNotFoundError(
            f'{feat_path} not found — run train_models.py first to generate feature sets'
        )
    d = json.loads(feat_path.read_text())
    return d.get('features', d.get('selected_features', []))


def _temporal_split(df: pd.DataFrame, train_cutoff: int, val_end: int):
    filed = pd.to_datetime(df.get('filed_date', pd.NaT), errors='coerce')
    cutoff_date = pd.Timestamp(f'{train_cutoff + 1}-01-01')
    df_train = df[
        (df['fiscal_year'] <= train_cutoff) &
        (filed.isna() | (filed < cutoff_date))
    ].copy()
    df_val  = df[(df['fiscal_year'] > train_cutoff) & (df['fiscal_year'] <= val_end)].copy()
    df_test = df[df['fiscal_year'] > val_end].copy()
    return df_train, df_val, df_test


def _winsorize_target(train_series: pd.Series, apply_series: pd.Series) -> tuple[pd.Series, float, float]:
    """Winsorize apply_series at train-derived percentiles (PIT-safe)."""
    lo = train_series.quantile(WINSOR_LO)
    hi = train_series.quantile(WINSOR_HI)
    return apply_series.clip(lo, hi), lo, hi


def _spearman_ic(y_true: pd.Series, y_pred: np.ndarray) -> float:
    """Spearman rank IC between predicted and actual excess return."""
    mask = y_true.notna()
    if mask.sum() < 20:
        return float('nan')
    corr, _ = stats.spearmanr(y_pred[mask], y_true[mask])
    return float(corr)


def _prep(df: pd.DataFrame, features: list[str], train_medians: dict,
          lo: float, hi: float) -> tuple[pd.DataFrame, pd.Series]:
    sub = df[df[TARGET_COL].notna()].copy()
    feats = [f for f in features if f in sub.columns]
    X = sub[feats].fillna(pd.Series(train_medians))
    y = sub[TARGET_COL].clip(lo, hi)
    return X, y


def train_regression(df_train: pd.DataFrame, features: list[str]) -> tuple:
    """Train Huber LightGBM regressor. Returns (model, used_features, train_medians, lo, hi)."""
    sub = df_train[df_train[TARGET_COL].notna()].copy()
    feats = [f for f in features if f in sub.columns]
    train_medians = sub[feats].median().to_dict()

    y_raw = sub[TARGET_COL]
    lo = y_raw.quantile(WINSOR_LO)
    hi = y_raw.quantile(WINSOR_HI)
    y = y_raw.clip(lo, hi)
    X = sub[feats].fillna(pd.Series(train_medians))

    model = lgb.LGBMRegressor(
        objective='huber',
        alpha=0.9,             # 90th pct quantile for the Huber transition point
        n_estimators=600,
        max_depth=6,
        learning_rate=0.03,
        num_leaves=63,
        subsample=0.8,
        colsample_bytree=0.7,
        min_child_samples=20,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    model.fit(X, y)
    return model, feats, train_medians, lo, hi


def walk_forward_cv(df: pd.DataFrame, features: list[str],
                    train_cutoff: int, min_train_years: int = 6) -> list[dict]:
    """Expanding-window walk-forward CV. Each fold trains on ≤t, evaluates on t+1.

    Uses PIT-safe filed_date cutoff per fold to prevent look-ahead from late SEC filings.
    Excludes folds where the 3y return window hasn't fully elapsed yet.
    """
    filed = pd.to_datetime(df.get('filed_date', pd.NaT), errors='coerce')
    first_year = int(df['fiscal_year'].min())
    max_year   = int(df['fiscal_year'].max())
    max_test_year = max_year - 3 + 1  # 3y return needs 3 years to elapse
    fold_years = range(first_year + min_train_years, train_cutoff + 1)

    records = []
    print(f'  Walk-forward regression CV: folds up to test_year≤{max_test_year}', end=' ')

    for t in fold_years:
        test_year = t + 1
        if test_year > max_test_year:
            continue
        cutoff_date = pd.Timestamp(f'{test_year}-01-01')
        train_mask = (
            (df['fiscal_year'] <= t) &
            (filed.isna() | (filed < cutoff_date))
        )
        tr = df[train_mask].copy()
        te = df[df['fiscal_year'] == test_year].copy()
        te = te[te[TARGET_COL].notna()]

        if len(tr[tr[TARGET_COL].notna()]) < 100 or len(te) < 20:
            continue

        try:
            fold_model, fold_feats, medians, lo, hi = train_regression(tr, features)
            fa = [f for f in fold_feats if f in te.columns]
            X_te = te[fa].fillna(pd.Series(medians))
            preds = fold_model.predict(X_te)
            ic = _spearman_ic(te[TARGET_COL], preds)
            records.append({'test_year': test_year, 'n_train': len(tr), 'n_test': len(te),
                             'spearman_ic': round(ic, 4)})
            print('.', end='', flush=True)
        except Exception as exc:
            print(f'  fold {test_year} failed: {exc}')

    print()
    return records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--train-cutoff', type=int, default=TRAIN_CUTOFF)
    parser.add_argument('--val-end',      type=int, default=VAL_END)
    parser.add_argument('--walk-forward', action='store_true', default=True,
                        help='Run walk-forward CV (default: on)')
    parser.add_argument('--no-walk-forward', dest='walk_forward', action='store_false',
                        help='Skip walk-forward CV (faster)')
    args = parser.parse_args()

    print('Loading data...')
    df = _load_data()
    features = _load_features()
    print(f'  Dataset: {len(df):,} rows | {TARGET_COL} available: {df[TARGET_COL].notna().sum():,} rows')
    print(f'  Features loaded from feature_sets_3y.json: {len(features)} features')

    df_train, df_val, df_test = _temporal_split(df, args.train_cutoff, args.val_end)
    print(f'  Train: {len(df_train):,} | Val: {len(df_val):,} | Test: {len(df_test):,}')

    # ── Train final model ────────────────────────────────────────────────────
    print(f'\nTraining Huber regression model (target: {TARGET_COL})...')
    model, used_feats, train_medians, lo, hi = train_regression(df_train, features)
    print(f'  Features used: {len(used_feats)} | Winsor range: [{lo:.3f}, {hi:.3f}]')

    # ── Evaluate ─────────────────────────────────────────────────────────────
    def _eval_split(split_df: pd.DataFrame, label: str) -> float:
        sub = split_df[split_df[TARGET_COL].notna()]
        if len(sub) < 20:
            print(f'  {label}: insufficient data ({len(sub)} rows)')
            return float('nan')
        fa = [f for f in used_feats if f in sub.columns]
        X  = sub[fa].fillna(pd.Series(train_medians))
        ic = _spearman_ic(sub[TARGET_COL], model.predict(X))
        print(f'  {label:6s} Spearman IC = {ic:.4f}  (n={len(sub):,})')
        return ic

    train_ic = _eval_split(df_train, 'Train')
    val_ic   = _eval_split(df_val,   'Val')
    test_ic  = _eval_split(df_test,  'Test')

    # ── Walk-forward CV ──────────────────────────────────────────────────────
    wf_mean_ic = float('nan')
    if args.walk_forward:
        print('\nRunning walk-forward CV...')
        wf_records = walk_forward_cv(df, features, args.train_cutoff)
        if wf_records:
            wf_df = pd.DataFrame(wf_records)
            wf_df.to_csv(REPORTS / 'regression_ic_3y.csv', index=False)
            wf_mean_ic = float(wf_df['spearman_ic'].mean())
            print(f'  WF mean Spearman IC = {wf_mean_ic:.4f} ({len(wf_df)} folds)')
            print(f'  Saved: reports/regression_ic_3y.csv')

    # ── Save model + meta ────────────────────────────────────────────────────
    model_path = MODELS_DIR / 'model_3y_regression.joblib'
    meta_path  = MODELS_DIR / 'model_3y_regression_meta.json'

    joblib.dump(model, model_path)

    meta = {
        'horizon':      '3y',
        'target':       TARGET_COL,
        'objective':    'huber',
        'features':     used_feats,
        'n_features':   len(used_feats),
        'train_cutoff': args.train_cutoff,
        'val_end':      args.val_end,
        'winsor_lo':    round(lo, 6),
        'winsor_hi':    round(hi, 6),
        'train_medians': train_medians,
        'train_ic':     round(train_ic, 4) if not np.isnan(train_ic) else None,
        'val_ic':       round(val_ic, 4)   if not np.isnan(val_ic)   else None,
        'test_ic':      round(test_ic, 4)  if not np.isnan(test_ic)  else None,
        'wf_mean_ic':   round(wf_mean_ic, 4) if not np.isnan(wf_mean_ic) else None,
    }
    meta_path.write_text(json.dumps(meta, indent=2))

    print(f'\nSaved: {model_path}')
    print(f'Saved: {meta_path}')
    print('\n── Summary ─────────────────────────────────────────────')
    print(f'  Spearman IC — Train: {train_ic:.4f} | Val: {val_ic:.4f} | '
          f'Test: {test_ic:.4f} | WF mean: {wf_mean_ic:.4f}')
    print('  Interpret: IC > 0.05 useful, > 0.10 strong for annual rebalance')


if __name__ == '__main__':
    main()
