"""
Train LightGBM Huber regression models to predict continuous excess returns for all horizons.

For each horizon (6m/1y/2y/3y/5y), selects features only on the eligible training
population. Walk-forward folds repeat selection locally. Predicts
excess_return_local_{h} or falls back to forward_return_{h} if absent.

Algorithm: LightGBM Huber regression (robust to the rare +100%/−80% outlier years).
Primary metric: Spearman IC (rank correlation of predicted vs actual excess return).

Temporal split (PIT-safe, same as binary model):
  train : fiscal_year <= TRAIN_CUTOFF AND filed_date < Jan 1 of (TRAIN_CUTOFF+1)
  val   : TRAIN_CUTOFF < fiscal_year <= VAL_END
  test  : fiscal_year > VAL_END

Outputs per horizon:
  models/model_{h}_regression.joblib      Huber regressor
  models/model_{h}_regression_meta.json   Feature list, IC stats, train medians, target col
  reports/regression_ic_{h}.csv           Walk-forward Spearman IC per fold

Usage:
    python3 scripts/train_regression_model.py
    python3 scripts/train_regression_model.py --horizons 1y 3y 5y
    python3 scripts/train_regression_model.py --horizons 3y --walk-forward
    python3 scripts/train_regression_model.py --train-cutoff 2020 --no-walk-forward
"""
from __future__ import annotations

import argparse
import json
import math
import warnings
from _root import ROOT

BASE = ROOT
warnings.filterwarnings('ignore')

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats
from modeling.label_eligibility import LABEL_POLICIES, OBSERVED_ONLY, training_label_eligible
from modeling.fold_lineage import (
    LineageError,
    SelectorConfig,
    make_lineage,
    select_fold_features,
    validate_lineage,
)

MODELS_DIR = BASE / 'models'
REPORTS    = BASE / 'reports'
MODELS_DIR.mkdir(exist_ok=True)
REPORTS.mkdir(exist_ok=True)

import sys
TRAIN_CUTOFF = 2022
VAL_END      = 2023
WINSOR_LO    = 0.01
WINSOR_HI    = 0.99

ALL_HORIZONS = ['6m', '1y', '2y', '3y', '5y']

# Horizon → fractional years (used to exclude incomplete WF folds)
HORIZON_YEARS: dict[str, float] = {'6m': 0.5, '1y': 1.0, '2y': 2.0, '3y': 3.0, '5y': 5.0}

# Preferred continuous target column per horizon; fallback to forward_return_{h}
TARGET_CANDIDATES: dict[str, list[str]] = {
    '6m': ['excess_return_local_6m', 'forward_return_6m'],
    '1y': ['excess_return_local_1y', 'forward_return_1y'],
    '2y': ['excess_return_local_2y', 'forward_return_2y'],
    '3y': ['excess_return_local_3y', 'forward_return_3y'],
    '5y': ['excess_return_local_5y', 'forward_return_5y'],
}


def _resolve_target(df: pd.DataFrame, horizon: str) -> str:
    for col in TARGET_CANDIDATES[horizon]:
        if col in df.columns and df[col].notna().sum() >= 100:
            return col
    raise ValueError(
        f'No continuous return column found for horizon={horizon}. '
        f'Tried: {TARGET_CANDIDATES[horizon]}'
    )


def _load_data() -> pd.DataFrame:
    from modeling.train import load_data
    return load_data()


def _load_features(horizon: str, expected_lineage: dict) -> list[str]:
    feat_path = MODELS_DIR / f'feature_sets_{horizon}.json'
    if not feat_path.exists():
        raise FileNotFoundError(
            f'{feat_path} not found — run train_models.py first to generate feature sets'
        )
    d = json.loads(feat_path.read_text())
    validate_lineage(d.get('lineage'), expected_lineage)
    features = d.get('features', d.get('selected_features', []))
    if not features:
        raise LineageError(f'{feat_path} has no selected features')
    return features


def _temporal_split(df: pd.DataFrame, train_cutoff: int, val_end: int,
                    target_col: str | None = None,
                    label_policy: str = OBSERVED_ONLY):
    filed = pd.to_datetime(df.get('filed_date', pd.NaT), errors='coerce')
    cutoff_date = pd.Timestamp(f'{train_cutoff + 1}-01-01')
    df_train = df[
        (df['fiscal_year'] <= train_cutoff) &
        (filed.isna() | (filed < cutoff_date)) &
        training_label_eligible(df, target_col or '', cutoff_date, label_policy)
    ].copy()
    df_val  = df[(df['fiscal_year'] > train_cutoff) & (df['fiscal_year'] <= val_end)].copy()
    df_test = df[df['fiscal_year'] > val_end].copy()
    return df_train, df_val, df_test


def _spearman_ic(y_true: pd.Series, y_pred: np.ndarray) -> float:
    mask = y_true.notna()
    if mask.sum() < 20:
        return float('nan')
    corr, _ = stats.spearmanr(y_pred[mask], y_true[mask])
    return float(corr)


def train_regression(df_train: pd.DataFrame, features: list[str],
                     target_col: str) -> tuple:
    """Train Huber LightGBM regressor. Returns (model, used_features, train_medians, lo, hi)."""
    sub = df_train[df_train[target_col].notna()].copy()
    feats = [f for f in features if f in sub.columns]
    train_medians = sub[feats].median().to_dict()

    y_raw = sub[target_col]
    lo = y_raw.quantile(WINSOR_LO)
    hi = y_raw.quantile(WINSOR_HI)
    y = y_raw.clip(lo, hi)
    X = sub[feats].fillna(pd.Series(train_medians))

    model = lgb.LGBMRegressor(
        objective='huber',
        alpha=0.9,
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
                    target_col: str, horizon: str,
                    train_cutoff: int, min_train_years: int = 6,
                    label_policy: str = OBSERVED_ONLY) -> list[dict]:
    """Expanding-window walk-forward CV with PIT-safe filed_date cutoff."""
    filed = pd.to_datetime(df.get('filed_date', pd.NaT), errors='coerce')
    first_year    = int(df['fiscal_year'].min())
    max_year      = int(df['fiscal_year'].max())
    horizon_ceil  = math.ceil(HORIZON_YEARS[horizon])
    max_test_year = max_year - horizon_ceil + 1
    fold_years    = range(first_year + min_train_years, train_cutoff + 1)

    records = []
    print(f'  WF regression CV [{horizon}]: folds up to test_year≤{max_test_year}', end=' ')

    for t in fold_years:
        test_year = t + 1
        if test_year > max_test_year:
            continue
        cutoff_date = pd.Timestamp(f'{test_year}-01-01')
        train_mask = (
            (df['fiscal_year'] <= t) &
            (filed.isna() | (filed < cutoff_date)) &
            training_label_eligible(df, target_col, cutoff_date, label_policy)
        )
        tr = df[train_mask].copy()
        te = df[df['fiscal_year'] == test_year].copy()
        te = te[te[target_col].notna()]

        if len(tr[tr[target_col].notna()]) < 100 or len(te) < 20:
            continue

        try:
            fold_features = select_fold_features(
                tr, target_col, SelectorConfig(top_n=40)
            )
            if not fold_features:
                continue
            fold_model, fold_feats, medians, lo, hi = train_regression(
                tr, fold_features, target_col
            )
            fa = [f for f in fold_feats if f in te.columns]
            X_te = te[fa].fillna(pd.Series(medians))
            preds = fold_model.predict(X_te)
            ic = _spearman_ic(te[target_col], preds)
            records.append({
                'test_year':   test_year,
                'n_train':     len(tr),
                'n_test':      len(te),
                'spearman_ic': round(ic, 4),
            })
            print('.', end='', flush=True)
        except Exception as exc:
            print(f'  fold {test_year} failed: {exc}')

    print()
    return records


def train_one_horizon(df: pd.DataFrame, horizon: str,
                      args: argparse.Namespace) -> dict | None:
    """Full train + eval + save for one horizon. Returns summary dict."""
    print(f'\n── Horizon: {horizon} ──────────────────────────────────────────────')

    try:
        target_col = _resolve_target(df, horizon)
    except ValueError as exc:
        print(f'  SKIP: {exc}')
        return None

    available = df[target_col].notna().sum()
    print(f'  Target: {target_col}  ({available:,} non-null rows)')
    df_train, df_val, df_test = _temporal_split(
        df, args.train_cutoff, args.val_end, target_col, args.label_policy
    )
    print(f'  Train: {len(df_train):,} | Val: {len(df_val):,} | Test: {len(df_test):,}')

    features = select_fold_features(df_train, target_col, SelectorConfig(top_n=40))
    if not features:
        print('  SKIP: fold-local feature selection returned no features')
        return None
    print(f'  Fold-local features: {len(features)}')

    # Train final model
    model, used_feats, train_medians, lo, hi = train_regression(df_train, features, target_col)
    print(f'  Features used: {len(used_feats)} | Winsor [{lo:.3f}, {hi:.3f}]')

    # Eval splits
    def _eval(split_df: pd.DataFrame, label: str) -> float:
        sub = split_df[split_df[target_col].notna()]
        if len(sub) < 20:
            return float('nan')
        fa = [f for f in used_feats if f in sub.columns]
        X = sub[fa].fillna(pd.Series(train_medians))
        ic = _spearman_ic(sub[target_col], model.predict(X))
        print(f'  {label:6s} Spearman IC = {ic:.4f}  (n={len(sub):,})')
        return ic

    train_ic = _eval(df_train, 'Train')
    val_ic   = _eval(df_val,   'Val')
    test_ic  = _eval(df_test,  'Test')

    # Walk-forward CV
    wf_mean_ic = float('nan')
    if args.walk_forward:
        print('  Running walk-forward CV...')
        wf_records = walk_forward_cv(
            df, used_feats, target_col, horizon, args.train_cutoff,
            label_policy=args.label_policy,
        )
        if wf_records:
            wf_df = pd.DataFrame(wf_records)
            report_path = REPORTS / f'regression_ic_{horizon}.csv'
            wf_df.to_csv(report_path, index=False)
            wf_mean_ic = float(wf_df['spearman_ic'].mean())
            print(f'  WF mean Spearman IC = {wf_mean_ic:.4f} ({len(wf_df)} folds)')
            print(f'  Saved: {report_path}')

    # Save model + meta
    model_path = MODELS_DIR / f'model_{horizon}_regression.joblib'
    meta_path  = MODELS_DIR / f'model_{horizon}_regression_meta.json'
    joblib.dump(model, model_path)

    meta = {
        'horizon':       horizon,
        'target':        target_col,
        'objective':     'huber',
        'features':      used_feats,
        'n_features':    len(used_feats),
        'train_cutoff':  args.train_cutoff,
        'label_policy':  args.label_policy,
        'val_end':       args.val_end,
        'winsor_lo':     round(lo, 6),
        'winsor_hi':     round(hi, 6),
        'train_medians': train_medians,
        'train_ic':      round(train_ic, 4) if not np.isnan(train_ic) else None,
        'val_ic':        round(val_ic,   4) if not np.isnan(val_ic)   else None,
        'test_ic':       round(test_ic,  4) if not np.isnan(test_ic)  else None,
        'wf_mean_ic':    round(wf_mean_ic, 4) if not np.isnan(wf_mean_ic) else None,
    }
    cutoff_date = pd.Timestamp(f'{args.train_cutoff + 1}-01-01')
    meta['lineage'] = make_lineage(
        dataset=df,
        training_population=df_train,
        development_population=df_val,
        horizon=horizon,
        target_col=target_col,
        label_policy=args.label_policy,
        cutoff=cutoff_date.isoformat(),
        selector_config={
            'method': 'fold_local_icir',
            **SelectorConfig(top_n=40).__dict__,
            'psi_population': None,
        },
        features=used_feats,
    )
    meta_path.write_text(json.dumps(meta, indent=2))
    print(f'  Saved: {model_path}')
    print(f'  Saved: {meta_path}')

    return {
        'horizon':    horizon,
        'target':     target_col,
        'n_features': len(used_feats),
        'train_ic':   round(train_ic,  4) if not np.isnan(train_ic)  else None,
        'val_ic':     round(val_ic,    4) if not np.isnan(val_ic)    else None,
        'test_ic':    round(test_ic,   4) if not np.isnan(test_ic)   else None,
        'wf_mean_ic': round(wf_mean_ic,4) if not np.isnan(wf_mean_ic) else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Train LightGBM Huber regression (continuous CAGR) for all horizons')
    parser.add_argument('--horizons', nargs='+', default=ALL_HORIZONS,
                        choices=ALL_HORIZONS, metavar='H',
                        help='Horizons to train (default: all 5)')
    parser.add_argument('--train-cutoff', type=int, default=TRAIN_CUTOFF,
                        dest='train_cutoff')
    parser.add_argument('--val-end',      type=int, default=VAL_END,
                        dest='val_end')
    parser.add_argument('--walk-forward', action='store_true', default=True,
                        help='Run walk-forward Spearman IC CV (default: on)')
    parser.add_argument('--no-walk-forward', dest='walk_forward', action='store_false',
                        help='Skip walk-forward CV (faster)')
    parser.add_argument('--label-policy', choices=LABEL_POLICIES, default=OBSERVED_ONLY,
                        help='Observed-only primary labels or explicit policy-imputed sensitivity')
    args = parser.parse_args()

    print('Loading data...')
    df = _load_data()
    print(f'  Dataset: {len(df):,} rows × {len(df.columns)} columns')
    print(f'  Horizons to train: {args.horizons}')

    summaries = []
    for h in args.horizons:
        result = train_one_horizon(df, h, args)
        if result:
            summaries.append(result)

    # Summary table
    print('\n' + '═' * 68)
    print(f'  {"Horizon":<8} {"Target":<30} {"Val IC":>7} {"WF IC":>7}')
    print('─' * 68)
    for s in summaries:
        val  = f"{s['val_ic']:.4f}"  if s['val_ic']  is not None else 'n/a'
        wf   = f"{s['wf_mean_ic']:.4f}" if s['wf_mean_ic'] is not None else 'n/a'
        print(f"  {s['horizon']:<8} {s['target']:<30} {val:>7} {wf:>7}")
    print('═' * 68)
    print('  Interpret: IC > 0.05 useful · > 0.10 strong · > 0.20 exceptional')
    print('  Models output: annualised excess return (decimal, e.g. 0.12 = +12%)')


if __name__ == '__main__':
    main()
