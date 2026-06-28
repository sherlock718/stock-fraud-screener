"""
Train and save ML models (1y / 3y / 5y) from historical_dataset_clean.parquet.

Temporal split (no data leakage):
  train  : fiscal_year <= TRAIN_CUTOFF AND filed_date < Jan 1 of (TRAIN_CUTOFF+1)
  val    : TRAIN_CUTOFF < fiscal_year <= VAL_END  (default 2021-2023, 3 years)
  test   : fiscal_year > VAL_END  (default 2024+)

Feature selection pipeline (on TRAIN split only):
  1. IC analysis (Spearman rank correlation with forward returns, year-by-year)
  2. Select top-N by |ICIR| (IC / StdIC) — keeps consistent, stable predictors
  3. Drop near-duplicate features (Spearman corr > 0.90 within the top set)
  4. PSI filter (default 0.25) — drops distribution-shifted features
  5. Train LightGBM classifier to predict top-quartile forward returns

Model: LightGBM  n_estimators=600, max_depth=6, num_leaves=63, lr=0.03

Output: models/model_{h}.joblib + models/model_meta.json
         reports/feature_importance_{h}.csv
         reports/shap_importance_{h}.csv  (mean |SHAP| per feature)

Usage:
    python3 scripts/train_models.py
    python3 scripts/train_models.py --top-n 50    # allow more features
    python3 scripts/train_models.py --no-dedup    # skip correlation pruning
    python3 scripts/train_models.py --train-cutoff 2020  # earlier cutoff
    python3 scripts/train_models.py --no-shap     # skip SHAP computation
    python3 scripts/train_models.py --walk-forward  # run WF CV after training
    python3 scripts/train_models.py --oot-eval      # OOT diagnostic: retrain 3y with cutoff=2019, test on FY2022
"""
from __future__ import annotations

import argparse
import json
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
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    import shap as _shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

try:
    import xgboost as _xgb
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False

from modeling.constants import EXCLUDE_COLS, EXCLUDE_PATTERNS, load_data, get_feature_candidates

DATA_PATH  = BASE / 'data' / 'historical_dataset_clean.parquet'
MODELS_DIR = BASE / 'models'
REPORTS    = BASE / 'reports'
MODELS_DIR.mkdir(exist_ok=True)
REPORTS.mkdir(exist_ok=True)

import sys
from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext

TRAIN_CUTOFF = 2020   # last fiscal year included in training
VAL_END      = 2023   # val = (TRAIN_CUTOFF, VAL_END]; test = > VAL_END

HORIZONS = {
    '6m': ('forward_return_6m', 'beat_local_market_6m'),
    '1y': ('forward_return_1y', 'beat_local_market_1y'),
    '2y': ('forward_return_2y', 'beat_local_market_2y'),
    '3y': ('forward_return_3y', 'beat_local_market_3y'),
    '5y': ('forward_return_5y', 'beat_local_market_5y'),
}

# Price/momentum features to force-include per horizon.
# ICIR selection on short-horizon targets (6m, 1y) naturally ranks fundamental/value features
# first, systematically under-selecting momentum features that stabilise the 3y/5y models.
# Force-includes bypass the ICIR ranking for these specific features.
# Only features that actually exist in the dataset are force-included at runtime.
# Note: 'price_to_52w_high' tested for 1y — hurt the 2019 fold (IC=−0.40 in COVID reversal year). Excluded.
FORCE_INCLUDE_6M = ['vol_rank_12m', 'quality_x_momentum',
                    'sales_to_price', 'ohlson_roe', 'value_x_quality', 'piotroski_f_score',
                    'macro_regime', 'yield_curve', 'credit_spread_baa']
FORCE_INCLUDE_1Y = ['vol_rank_12m', 'quality_x_momentum',
                    'sales_to_price', 'ohlson_roe', 'value_x_quality', 'piotroski_f_score',
                    'macro_regime', 'yield_curve', 'credit_spread_baa']
FORCE_INCLUDE_2Y = ['vol_rank_12m', 'macro_regime', 'yield_curve', 'credit_spread_baa']

EXCLUDE = EXCLUDE_COLS


get_candidates = get_feature_candidates


def compute_ic_table(df: pd.DataFrame, features: list[str], return_col: str,
                     sector_neutral: bool = False) -> pd.DataFrame:
    years = sorted(df['fiscal_year'].unique())
    sub_all = df[df[return_col].notna()]
    records = []
    for feat in features:
        sub = sub_all[sub_all[feat].notna()]
        ics = []
        for yr in years:
            grp = sub[sub['fiscal_year'] == yr]
            if len(grp) < 30:
                continue
            if sector_neutral and 'sic_code' in grp.columns:
                ic_vals = []
                for _, sec_grp in grp.groupby('sic_code'):
                    if len(sec_grp) < 5:
                        continue
                    c, _ = stats.spearmanr(sec_grp[feat], sec_grp[return_col])
                    if not np.isnan(c):
                        ic_vals.append(c)
                if not ic_vals:
                    continue
                corr = float(np.mean(ic_vals))
            else:
                corr, _ = stats.spearmanr(grp[feat], grp[return_col])
            if not np.isnan(corr):
                ics.append(corr)
        if not ics:
            continue
        mean_ic = np.mean(ics)
        std_ic  = np.std(ics) + 1e-8
        records.append({
            'feature':         feat,
            'mean_ic':         mean_ic,
            'std_ic':          std_ic,
            'icir':            mean_ic / std_ic,
            'n_years':         len(ics),
            'pct_positive_ic': np.mean([ic > 0 for ic in ics]),
        })
    return (pd.DataFrame(records).set_index('feature')
              .sort_values('icir', key=abs, ascending=False))


def deduplicate_features(df: pd.DataFrame, features: list[str], corr_threshold: float = 0.85) -> list[str]:
    """Drop features that are near-duplicates (|Spearman corr| > threshold).
    Keeps the feature that appears earlier in `features` (i.e. higher ICIR rank).
    """
    sub = df[features].copy()
    kept = []
    dropped_by = {}
    def _corr(a: pd.Series, b: pd.Series) -> float:
        common = a.notna() & b.notna()
        if common.sum() < 50:
            return 0.0
        return abs(stats.spearmanr(a[common], b[common])[0])

    for feat in features:
        if any(_corr(sub[feat], sub[k]) > corr_threshold for k in kept):
            dropped_by[feat] = True
        else:
            kept.append(feat)
    print(f'    Dedup: {len(features)} → {len(kept)} (removed {len(features)-len(kept)} near-duplicates)')
    return kept


def compute_psi(train: pd.Series, test: pd.Series, buckets: int = 10) -> float:
    """Population Stability Index between train and test distributions."""
    combined = pd.concat([train, test]).dropna()
    if len(combined) < 20:
        return 0.0
    bins = np.percentile(combined, np.linspace(0, 100, buckets + 1))
    bins = np.unique(bins)
    if len(bins) < 2:
        return 0.0
    def _dist(s: pd.Series) -> np.ndarray:
        counts, _ = np.histogram(s.dropna(), bins=bins)
        pct = counts / max(counts.sum(), 1)
        return np.where(pct == 0, 1e-4, pct)
    e, a = _dist(train), _dist(test)
    return float(np.sum((a - e) * np.log(a / e)))


def log_psi_report(df_train: pd.DataFrame, df_test: pd.DataFrame,
                   features: list[str]) -> None:
    """Print top-drifting features by PSI (train vs test)."""
    records = []
    for f in features:
        if f not in df_train.columns or f not in df_test.columns:
            continue
        psi = compute_psi(df_train[f], df_test[f])
        records.append({'feature': f, 'psi': round(psi, 4)})
    if not records:
        return
    psi_df = pd.DataFrame(records).sort_values('psi', ascending=False)
    psi_df.to_csv(REPORTS / 'feature_psi_train_vs_test.csv', index=False)
    print('\n  Top 10 features by distribution shift (PSI train→test):')
    for _, row in psi_df.head(10).iterrows():
        flag = ' ⚠' if row['psi'] > 0.20 else ''
        print(f'    {row["feature"]:<45} PSI={row["psi"]:.4f}{flag}')





def sector_zscore_normalize(df: pd.DataFrame, features: list[str],
                             sic_col: str = 'sic_code') -> pd.DataFrame:
    """Within-sector z-score normalization per (fiscal_year, sector) group.

    Removes cross-sector valuation level differences so IC measures
    within-sector stock selection ability rather than between-sector tilts.
    Groups with fewer than 5 members are left unnormalized to avoid
    inflated z-scores from tiny groups.
    """
    df_out = df.copy()
    if sic_col not in df.columns:
        return df_out
    for feat in features:
        if feat not in df.columns:
            continue
        grouped = df_out.groupby(['fiscal_year', sic_col])[feat]
        mu = grouped.transform('mean')
        sigma = grouped.transform('std').clip(lower=1e-8)
        group_sizes = grouped.transform('count')
        normalized = (df_out[feat] - mu) / sigma
        df_out[feat] = np.where(group_sizes >= 5, normalized, df_out[feat])
    return df_out


def train_model(df_train: pd.DataFrame, features: list[str], beat_col: str,
                override_params: dict | None = None) -> tuple:
    sub = df_train[df_train[beat_col].notna()].copy()
    feats = [f for f in features if f in sub.columns]
    train_medians = sub[feats].median().to_dict()
    X = sub[feats].fillna(pd.Series(train_medians))
    y = sub[beat_col].astype(int)

    pos = int((y == 1).sum())
    neg = int((y == 0).sum())
    base = dict(
        n_estimators=600,
        max_depth=6,
        learning_rate=0.03,
        num_leaves=63,
        subsample=0.8,
        colsample_bytree=0.7,
        min_child_samples=20,
        reg_alpha=0.1,
        reg_lambda=1.0,
    )
    if override_params:
        base.update(override_params)
    clf = lgb.LGBMClassifier(
        **base,
        scale_pos_weight=neg / max(pos, 1),
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    clf.fit(X, y)
    return clf, feats, y, train_medians


def train_xgb_model(df_train: pd.DataFrame, features: list[str], beat_col: str,
                    override_params: dict | None = None) -> tuple:
    """Train XGBoost classifier — used for ensemble blending with LightGBM."""
    if not XGB_AVAILABLE:
        raise RuntimeError('xgboost is not installed. Run: pip install xgboost>=2.0.0')
    sub = df_train[df_train[beat_col].notna()].copy()
    feats = [f for f in features if f in sub.columns]
    train_medians = sub[feats].median().to_dict()
    X = sub[feats].fillna(pd.Series(train_medians))
    y = sub[beat_col].astype(int)
    pos = int((y == 1).sum())
    neg = int((y == 0).sum())
    base = dict(
        n_estimators=500,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.7,
        min_child_weight=20,
        reg_alpha=0.1,
        reg_lambda=1.0,
        scale_pos_weight=neg / max(pos, 1),
        random_state=42,
        n_jobs=-1,
        eval_metric='auc',
        verbosity=0,
    )
    if override_params:
        base.update(override_params)
    clf = _xgb.XGBClassifier(**base)
    clf.fit(X, y)
    return clf, feats, y, train_medians


def feature_importance_df(clf: lgb.LGBMClassifier, feats: list[str]) -> pd.DataFrame:
    imp = clf.feature_importances_
    return (pd.DataFrame({'feature': feats, 'importance': imp})
              .sort_values('importance', ascending=False)
              .reset_index(drop=True))


def compute_shap_importance(clf: lgb.LGBMClassifier, X: pd.DataFrame,
                             max_rows: int = 5_000) -> pd.DataFrame | None:
    """Compute mean absolute SHAP value per feature using a sample of training data.

    Returns a DataFrame with columns [feature, shap_mean_abs] sorted descending,
    or None if shap is not installed.
    """
    if not SHAP_AVAILABLE:
        return None
    sample = X if len(X) <= max_rows else X.sample(max_rows, random_state=42)
    explainer = _shap.TreeExplainer(clf)
    shap_values = explainer.shap_values(sample)
    # shap_values is list[ndarray] for multi-class or ndarray for binary
    if isinstance(shap_values, list):
        # Take positive class (index 1) for binary classification
        sv = shap_values[1] if len(shap_values) == 2 else shap_values[0]
    else:
        sv = shap_values
    mean_abs = np.abs(sv).mean(axis=0)
    return (pd.DataFrame({'feature': sample.columns.tolist(), 'shap_mean_abs': mean_abs})
              .sort_values('shap_mean_abs', ascending=False)
              .reset_index(drop=True))


def train_baseline(df_train: pd.DataFrame, features: list[str], beat_col: str,
                   train_medians: dict) -> Pipeline:
    sub = df_train[df_train[beat_col].notna()].copy()
    feats = [f for f in features if f in sub.columns]
    X = sub[feats].fillna(pd.Series(train_medians))
    y = sub[beat_col].astype(int)
    pipe = Pipeline([
        ('scaler', StandardScaler()),
        ('lr', LogisticRegression(max_iter=1000, class_weight='balanced',
                                  solver='lbfgs', random_state=42, C=0.1)),
    ])
    pipe.fit(X, y)
    return pipe


def walk_forward_cv(df: pd.DataFrame, features_per_horizon: dict[str, list[str]],
                    train_cutoff: int, min_train_years: int = 6,
                    override_params_per_horizon: dict | None = None,
                    embargo_years: int = 0,
                    ensemble: bool = False) -> dict[str, float]:
    """Expanding-window walk-forward validation.

    For each fold year t in [first_year + min_train_years, train_cutoff]:
      - Train on fiscal_year <= (t - embargo_years) AND filed_date < Jan 1 of test_year (PIT-safe)
        embargo_years=1 excludes the most recent training year, preventing adjacent-year leakage.
      - Evaluate on fiscal_year == t+1
    Folds where the forward-return horizon hasn't fully elapsed by the dataset
    end year are excluded to avoid survivorship bias in partially realised returns.
    Saves per-fold AUC to reports/walk_forward_auc_{h}.csv.
    Returns {h: wf_mean_auc} for the caller to persist in model_meta.json.
    """
    # Years needed for each horizon's forward return to fully elapse.
    _horizon_years = {'6m': 1, '1y': 1, '2y': 2, '3y': 3, '5y': 5}
    max_fiscal_year = int(df['fiscal_year'].max())

    filed = pd.to_datetime(df.get('filed_date', pd.NaT), errors='coerce')
    first_year = int(df['fiscal_year'].min())
    fold_years = range(first_year + min_train_years, train_cutoff + 1)

    wf_aucs: dict[str, float] = {}

    for h, (ret_col, beat_col) in HORIZONS.items():
        feats = features_per_horizon.get(h, [])
        if not feats:
            continue
        # Exclude folds whose test_year is too recent for the horizon to have
        # fully realised returns.  test_year = t+1; we require test_year + H - 1 <= max_fiscal_year.
        h_years = _horizon_years.get(h, 1)
        max_test_year = max_fiscal_year - h_years + 1
        records = []
        print(f'  Walk-forward {h}: folds up to test_year≤{max_test_year}', end=' ')
        for t in fold_years:
            test_year = t + 1
            if test_year > max_test_year:
                continue
            cutoff_date = pd.Timestamp(f'{test_year}-01-01')
            max_train_year = t - embargo_years  # purged embargo: exclude most recent years
            train_mask = (
                (df['fiscal_year'] <= max_train_year) &
                (filed.isna() | (filed < cutoff_date))
            )
            tr = df[train_mask].copy()
            te = df[df['fiscal_year'] == test_year].copy()
            te = te[te[beat_col].notna()]
            if len(tr[tr[beat_col].notna()]) < 100 or len(te) < 20 or te[beat_col].nunique() < 2:
                continue
            try:
                op = (override_params_per_horizon or {}).get(h)
                clf, fold_feats, _, medians = train_model(tr, feats, beat_col, override_params=op)
                fa = [f for f in fold_feats if f in te.columns]
                X_te = te[fa].fillna(pd.Series(medians))
                y_te = te[beat_col].astype(int)
                lgbm_proba = clf.predict_proba(X_te)[:, 1]
                if ensemble and XGB_AVAILABLE:
                    try:
                        xclf, xfeats, _, xmedians = train_xgb_model(tr, feats, beat_col)
                        xfa = [f for f in xfeats if f in te.columns]
                        X_te_x = te[xfa].fillna(pd.Series(xmedians))
                        xgb_proba = xclf.predict_proba(X_te_x)[:, 1]
                        proba = 0.5 * lgbm_proba + 0.5 * xgb_proba
                    except Exception:
                        proba = lgbm_proba
                else:
                    proba = lgbm_proba
                auc = roc_auc_score(y_te, proba)
                records.append({'fold_year': t, 'test_year': test_year, 'auc': round(auc, 4),
                                 'n_train': len(tr), 'n_test': len(te)})
                print('.', end='', flush=True)
            except Exception:
                pass
        print()
        if records:
            wf_df = pd.DataFrame(records)
            wf_df.to_csv(REPORTS / f'walk_forward_auc_{h}.csv', index=False)
            mean_auc = round(float(wf_df['auc'].mean()), 4)
            wf_aucs[h] = mean_auc
            print(f'    mean AUC={mean_auc:.4f}  '
                  f'min={wf_df["auc"].min():.4f}  max={wf_df["auc"].max():.4f}')

    return wf_aucs


def run_oot_diagnostic(df: pd.DataFrame) -> None:
    """OOT diagnostic for the 3y model only.

    Retrains a fresh 3y model with TRAIN_CUTOFF=2019 (diagnostic only —
    production model is NOT overwritten). Tests on FY2022, where
    beat_local_market_3y is fully known (2022+3=2025 prices exist).
    Saves AUC + sample sizes to reports/oot_auc_diagnostic.json.
    """
    OOT_CUTOFF  = 2019   # diagnostic train cutoff
    OOT_TEST_YR = 2022   # test year: 3y returns fully elapsed by 2025
    HORIZON     = '3y'
    ret_col, beat_col = HORIZONS[HORIZON]

    print(f'\n── OOT Diagnostic (3y) ──────────────────────────────')
    print(f'  Train: fiscal_year <= {OOT_CUTOFF}  |  Test: fiscal_year == {OOT_TEST_YR}')

    filed = pd.to_datetime(df.get('filed_date', pd.NaT), errors='coerce')
    cutoff_date = pd.Timestamp(f'{OOT_CUTOFF + 1}-01-01')

    df_tr = df[
        (df['fiscal_year'] <= OOT_CUTOFF) &
        (filed.isna() | (filed < cutoff_date))
    ].copy()
    df_oot = df[df['fiscal_year'] == OOT_TEST_YR].copy()
    df_oot = df_oot[df_oot[beat_col].notna()]

    n_train_labeled = int(df_tr[beat_col].notna().sum())
    n_test          = len(df_oot)
    n_classes       = int(df_oot[beat_col].nunique()) if n_test > 0 else 0

    print(f'  Train labeled: {n_train_labeled:,}  |  Test: {n_test:,}  |  Classes: {n_classes}')

    if n_train_labeled < 100 or n_test < 30 or n_classes < 2:
        msg = (f'Insufficient data for OOT diagnostic '
               f'(train_labeled={n_train_labeled}, test={n_test}, classes={n_classes})')
        print(f'  ⚠  {msg}')
        result = {'error': msg, 'oot_cutoff': OOT_CUTOFF, 'oot_test_year': OOT_TEST_YR}
        (REPORTS / 'oot_auc_diagnostic.json').write_text(json.dumps(result, indent=2))
        return

    all_features = get_candidates(df_tr)
    # Quick IC pass (no sector-neutral for speed)
    ic_tbl = compute_ic_table(df_tr, all_features, ret_col, sector_neutral=False)
    mask = ic_tbl['mean_ic'].abs() > 0.02
    candidates = ic_tbl[mask].index.tolist()[:40]
    candidates = deduplicate_features(df_tr, candidates, corr_threshold=0.90)
    print(f'  Features selected: {len(candidates)}')

    clf, feats, _, medians = train_model(df_tr, candidates, beat_col)
    fa = [f for f in feats if f in df_oot.columns]
    X_oot = df_oot[fa].fillna(pd.Series(medians))
    y_oot = df_oot[beat_col].astype(int)
    oot_auc = roc_auc_score(y_oot, clf.predict_proba(X_oot)[:, 1])

    result = {
        'horizon':       HORIZON,
        'oot_cutoff':    OOT_CUTOFF,
        'oot_test_year': OOT_TEST_YR,
        'oot_auc':       round(float(oot_auc), 4),
        'n_train':       n_train_labeled,
        'n_test':        n_test,
        'n_features':    len(feats),
        'note':          'Diagnostic only — production model unchanged',
    }
    (REPORTS / 'oot_auc_diagnostic.json').write_text(json.dumps(result, indent=2))
    print(f'  OOT AUC (3y): {oot_auc:.4f}  (target ≥ 0.62)')
    print(f'  Saved → reports/oot_auc_diagnostic.json')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--top-n', type=int, default=40,
                        help='Max features per horizon after IC ranking (default: 40)')
    parser.add_argument('--no-dedup', action='store_true',
                        help='Skip correlation-based deduplication')
    parser.add_argument('--min-ic', type=float, default=0.02,
                        help='Minimum |mean IC| to enter candidate set (default: 0.02)')
    parser.add_argument('--sector-neutral', action=argparse.BooleanOptionalAction, default=True,
                        help='Compute IC within SIC sectors (sector-neutral IC, default: on). Use --no-sector-neutral to disable.')
    parser.add_argument('--train-cutoff', type=int, default=TRAIN_CUTOFF,
                        help=f'Last fiscal_year in training set (default: {TRAIN_CUTOFF})')
    parser.add_argument('--val-end', type=int, default=VAL_END,
                        help=f'Last fiscal_year in validation set (default: {VAL_END})')
    parser.add_argument('--no-shap', action='store_true',
                        help='Skip SHAP computation (faster run)')
    parser.add_argument('--walk-forward', action='store_true',
                        help='Run expanding-window walk-forward CV after main training')
    parser.add_argument('--use-tuned-params', action='store_true',
                        help='Load best_params from model_meta.json and use them for WF CV')
    parser.add_argument('--oot-eval', action='store_true',
                        help='OOT diagnostic: retrain 3y model with cutoff=2019, test on FY2022 '
                             '(3y returns fully elapsed as of 2025). Does NOT overwrite production '
                             'models. Saves result to reports/oot_auc_diagnostic.json.')
    parser.add_argument('--max-psi', type=float, default=0.25,
                        help='Drop features with PSI > threshold before IC analysis (default: 0.25)')
    parser.add_argument('--min-ic-stability', type=float, default=0.6,
                        help='Minimum fraction of years IC must have the correct sign to keep feature '
                             '(default: 0.6). Features whose IC direction is inconsistent across years are dropped.')
    parser.add_argument('--min-ic-years', type=int, default=1,
                        help='Minimum number of years with valid IC data to keep a feature '
                             '(default: 1 = off). Set to e.g. 5 to prevent spurious ICIR inflation '
                             'from features with very few historical observations (e.g. fraud_label).')
    parser.add_argument('--embargo-years', type=int, default=0, dest='embargo_years',
                        help='Purged walk-forward embargo: exclude most recent N training years from '
                             'each fold to prevent adjacent-year autocorrelation leakage (default: 0). '
                             'Use 1 for standard purged CV.')
    parser.add_argument('--ensemble', action='store_true',
                        help='Blend LightGBM + XGBoost predictions in walk-forward CV '
                             '(requires xgboost>=2.0.0). Default: LightGBM only.')
    parser.add_argument('--sector-zscore', action='store_true', dest='sector_zscore',
                        help='Apply within-sector z-score normalization to features before training '
                             '(removes cross-sector valuation level differences).')
    parser.add_argument('--clean-training', action='store_true',
                        help='Filter training data to clean stocks only: '
                             'fraud_suspect==0, piotroski_roa_pos==1, beneish_m_score<-1.78. '
                             'Removes value trap bias — model learns what honest/profitable companies do.')
    args = parser.parse_args()

    train_cutoff = args.train_cutoff
    val_end      = args.val_end

    print('Loading data...')
    df = load_data()
    print(f'  {len(df):,} annual rows | {df["ticker"].nunique():,} companies')

    _filed = pd.to_datetime(df.get('filed_date', pd.NaT), errors='coerce')
    _cutoff_date = pd.Timestamp(f'{train_cutoff + 1}-01-01')
    df_train = df[
        (df['fiscal_year'] <= train_cutoff) &
        (_filed.isna() | (_filed < _cutoff_date))
    ].copy()
    df_val   = df[(df['fiscal_year'] > train_cutoff) & (df['fiscal_year'] <= val_end)].copy()
    df_test  = df[df['fiscal_year'] > val_end].copy()
    pit_excluded = (df['fiscal_year'] <= train_cutoff).sum() - len(df_train)

    if args.clean_training:
        n_before = len(df_train)
        mask = pd.Series(True, index=df_train.index)
        if 'fraud_suspect' in df_train.columns:
            mask &= (df_train['fraud_suspect'] == 0)
        if 'piotroski_roa_pos' in df_train.columns:
            mask &= (df_train['piotroski_roa_pos'] == 1)
        if 'beneish_m_score' in df_train.columns:
            mask &= (df_train['beneish_m_score'] < -1.78)
        df_train = df_train[mask].copy()
        print(f'  Clean training filter: {n_before:,} → {len(df_train):,} '
              f'(removed {n_before - len(df_train):,} fraud/distress/unprofitable)')

    print(f'  Train : fiscal_year <= {train_cutoff}, filed < {_cutoff_date.date()} → {len(df_train):,} rows ({pit_excluded:,} PIT-excluded)')
    print(f'  Val   : {train_cutoff+1}–{val_end} → {len(df_val):,} rows')
    print(f'  Test  : > {val_end} → {len(df_test):,} rows')

    print('Computing feature candidates...')
    all_features = get_candidates(df_train)
    print(f'  {len(all_features)} candidates')

    print('\nPSI distribution audit (train vs test)...')
    log_psi_report(df_train, df_test, all_features)

    # Drop high-drift features before IC analysis so macro regime features
    # don't inflate IC on train data and then fail on shifted test distributions.
    if df_test.empty:
        psi_filtered = all_features
    else:
        psi_scores = {f: compute_psi(df_train[f], df_test[f])
                      for f in all_features
                      if f in df_train.columns and f in df_test.columns}
        psi_filtered = [f for f in all_features if psi_scores.get(f, 0.0) <= args.max_psi]
        n_dropped = len(all_features) - len(psi_filtered)
        print(f'  PSI filter (>{args.max_psi}): dropped {n_dropped}, {len(psi_filtered)} remain')
    all_features = psi_filtered

    print(f'\nRunning IC analysis on train split (2–4 min)...')
    ic_tables = {}
    for h, (ret_col, _) in HORIZONS.items():
        print(f'  {h}...', end=' ', flush=True)
        ic_tables[h] = compute_ic_table(df_train, all_features, ret_col,
                                         sector_neutral=args.sector_neutral)
        ic_tables[h].to_csv(REPORTS / f'ic_table_{h}.csv')
        n_sig = (ic_tables[h]['mean_ic'].abs() > args.min_ic).sum()
        print(f'{len(ic_tables[h])} computed | {n_sig} with |IC|>{args.min_ic}')

    selected_features = {}
    for h in HORIZONS:
        tbl = ic_tables[h]
        # Filter 1: minimum |mean IC|
        mask = tbl['mean_ic'].abs() > args.min_ic
        # Filter 2: IC stability — fraction of years IC has the correct sign must be >= threshold.
        #   For positive-IC features: pct_positive_ic >= min_ic_stability
        #   For negative-IC features: (1 - pct_positive_ic) >= min_ic_stability
        #   This evicts features that are high |ICIR| only because IC is consistently opposite to mean sign.
        stability_ok = pd.Series(np.where(
            tbl['mean_ic'] >= 0,
            tbl['pct_positive_ic'] >= args.min_ic_stability,
            (1 - tbl['pct_positive_ic']) >= args.min_ic_stability,
        ), index=tbl.index, dtype=bool)
        mask = mask & stability_ok
        # Filter 3: require enough years of data to trust ICIR (prevents fraud_label n=1 style inflation)
        mask = mask & (tbl['n_years'] >= args.min_ic_years)
        candidates = tbl[mask].index.tolist()
        n_before = int((tbl['mean_ic'].abs() > args.min_ic).sum())
        n_stability_dropped = n_before - int(mask.sum())
        top_n = candidates[:args.top_n]
        print(f'\n  {h}: {n_before} pass |IC|>{args.min_ic} → {n_stability_dropped} dropped by stability/years filter → {len(candidates)} remain → keeping top {len(top_n)} by ICIR')

        # Force-include price/momentum features per horizon.
        # ICIR selection on short-horizon targets naturally ranks fundamental/value features
        # first, systematically under-selecting momentum features that stabilise the 3y/5y models.
        _force_map = {'6m': FORCE_INCLUDE_6M, '1y': FORCE_INCLUDE_1Y, '2y': FORCE_INCLUDE_2Y}
        for forced in _force_map.get(h, []):
            if forced not in top_n and forced in tbl.index:
                top_n.append(forced)
                row = tbl.loc[forced]
                print(f'    Force-include {forced} (ICIR={row["icir"]:.3f}, pct_pos={row["pct_positive_ic"]:.2f})')

        if not args.no_dedup:
            top_n = deduplicate_features(df_train, top_n, corr_threshold=0.85)

        selected_features[h] = top_n
        print(f'    Final: {len(top_n)} features')
        print(f'    Top 10: {", ".join(top_n[:10])}')

    print('\nTraining LightGBM models...')
    _old_meta_path = MODELS_DIR / 'model_meta.json'
    _old_meta = json.loads(_old_meta_path.read_text()) if _old_meta_path.exists() else {}

    _all_selected = list({f for feats in selected_features.values() for f in feats})
    if args.sector_zscore:
        print('  Applying sector z-score normalization to training split...')
        df_train = sector_zscore_normalize(df_train, _all_selected)

    model_meta = {}
    for h, (ret_col, beat_col) in HORIZONS.items():
        print(f'  {h}...', end=' ', flush=True)
        clf, feats, y_train, train_medians = train_model(df_train, selected_features[h], beat_col)

        def _eval_split(split_df: pd.DataFrame) -> float:
            sub = split_df[split_df[beat_col].notna()].copy()
            feats_avail = [f for f in feats if f in sub.columns]
            if len(sub) < 30 or sub[beat_col].nunique() < 2:
                return float('nan')
            X = sub[feats_avail].fillna(pd.Series(train_medians))
            y = sub[beat_col].astype(int)
            return roc_auc_score(y, clf.predict_proba(X)[:, 1])

        val_auc  = _eval_split(df_val)
        test_auc = _eval_split(df_test)

        # Logistic regression baseline (same features, same split, same medians)
        lr_pipe = train_baseline(df_train, feats, beat_col, train_medians)

        def _eval_baseline(split_df: pd.DataFrame) -> float:
            sub = split_df[split_df[beat_col].notna()].copy()
            feats_avail = [f for f in feats if f in sub.columns]
            if len(sub) < 30 or sub[beat_col].nunique() < 2:
                return float('nan')
            X = sub[feats_avail].fillna(pd.Series(train_medians))
            y = sub[beat_col].astype(int)
            return roc_auc_score(y, lr_pipe.predict_proba(X)[:, 1])

        lr_val_auc  = _eval_baseline(df_val)
        lr_test_auc = _eval_baseline(df_test)

        joblib.dump(clf,     MODELS_DIR / f'model_{h}.joblib')
        joblib.dump(lr_pipe, MODELS_DIR / f'baseline_lr_{h}.joblib')
        imp_df = feature_importance_df(clf, feats)
        imp_df.to_csv(REPORTS / f'feature_importance_{h}.csv', index=False)

        shap_top: list[str] = []
        if not args.no_shap:
            _sub = df_train[df_train[beat_col].notna()].copy()
            _feats_avail = [f for f in feats if f in _sub.columns]
            X_train_df = _sub[_feats_avail].fillna(pd.Series(train_medians))
            shap_df = compute_shap_importance(clf, X_train_df)
            if shap_df is not None:
                shap_df.to_csv(REPORTS / f'shap_importance_{h}.csv', index=False)
                shap_top = shap_df['feature'].head(10).tolist()

        model_meta[h] = {
            'features':       feats,
            'ret_col':        ret_col,
            'beat_col':       beat_col,
            'train_cutoff':   train_cutoff,
            'val_end':        val_end,
            'sector_neutral': args.sector_neutral,
            'n_train':        int(len(y_train)),
            'pos_rate':       float(y_train.mean()),
            'val_auc':        round(val_auc,   4),
            'test_auc':       round(test_auc,  4),
            'lr_val_auc':     round(lr_val_auc,  4),
            'lr_test_auc':    round(lr_test_auc, 4),
            'train_medians':  train_medians,
            'shap_top_features': shap_top,
        }
        if args.clean_training:
            model_meta[h]['training_filter'] = 'fraud_suspect==0 & piotroski_roa_pos==1 & beneish_m_score<-1.78'
        if _old_meta.get(h, {}).get('best_params'):
            model_meta[h]['best_params'] = _old_meta[h]['best_params']
        print(f'done ({len(feats)} features, {len(y_train):,} rows, '
              f'LGBM val={val_auc:.3f}/test={test_auc:.3f} | '
              f'LR val={lr_val_auc:.3f}/test={lr_test_auc:.3f})')

    (MODELS_DIR / 'model_meta.json').write_text(json.dumps(model_meta, indent=2))
    print(f'\nAll models saved → {MODELS_DIR}/')
    print(f'IC tables + importance reports → {REPORTS}/')

    if args.walk_forward:
        print('\nRunning walk-forward CV (expanding window)...')
        override_params_per_horizon = None
        if args.use_tuned_params:
            override_params_per_horizon = {
                h: model_meta[h].get('best_params')
                for h in model_meta
                if model_meta[h].get('best_params')
            }
            if override_params_per_horizon:
                print(f'  Using tuned params for: {list(override_params_per_horizon.keys())}')
            else:
                print('  WARNING: --use-tuned-params set but no best_params found in model_meta.json')
        wf_aucs = walk_forward_cv(df, selected_features, train_cutoff,
                                  override_params_per_horizon=override_params_per_horizon,
                                  embargo_years=args.embargo_years,
                                  ensemble=args.ensemble)
        if wf_aucs:
            for h, mean_auc in wf_aucs.items():
                if h in model_meta:
                    model_meta[h]['wf_mean_auc'] = mean_auc
            (MODELS_DIR / 'model_meta.json').write_text(json.dumps(model_meta, indent=2))
            print(f'model_meta.json updated with wf_mean_auc values')

    print('\n── Summary ─────────────────────────────────')
    print(f'  {"Horizon":<6}  {"LGBM val":>9}  {"LGBM test":>10}  {"LR val":>8}  {"LR test":>9}')
    for h in HORIZONS:
        m = model_meta[h]
        print(f'  {h:<6}  {m["val_auc"]:>9.4f}  {m["test_auc"]:>10.4f}  '
              f'{m["lr_val_auc"]:>8.4f}  {m["lr_test_auc"]:>9.4f}')

    if args.oot_eval:
        run_oot_diagnostic(df)


if __name__ == '__main__':
    main()
