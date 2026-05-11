"""
Train and save ML models (1y / 3y / 5y) from historical_dataset_clean.parquet.

Temporal split (no data leakage):
  train  : fiscal_year <= TRAIN_CUTOFF  (default 2021)
  val    : TRAIN_CUTOFF < fiscal_year <= VAL_END  (default 2022–2023)
  test   : fiscal_year > VAL_END  (default 2024+)

Feature selection pipeline (on TRAIN split only):
  1. IC analysis (Spearman rank correlation with forward returns, year-by-year)
  2. Select top-N by |ICIR| (IC / StdIC) — keeps consistent, stable predictors
  3. Drop near-duplicate features (Spearman corr > 0.90 within the top set)
  4. Train LightGBM classifier to predict top-quartile forward returns

Output: models/model_{h}.joblib + models/model_meta.json
         reports/feature_importance_{h}.csv
         reports/shap_importance_{h}.csv  (mean |SHAP| per feature)

Usage:
    python3 scripts/train_models.py
    python3 scripts/train_models.py --top-n 50    # allow more features
    python3 scripts/train_models.py --no-dedup    # skip correlation pruning
    python3 scripts/train_models.py --train-cutoff 2020  # earlier cutoff
    python3 scripts/train_models.py --no-shap     # skip SHAP computation
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
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    import shap as _shap
    SHAP_AVAILABLE = True
except ImportError:
    SHAP_AVAILABLE = False

BASE       = Path(__file__).parent.parent
DATA_PATH  = BASE / 'data' / 'historical_dataset_clean.parquet'
MODELS_DIR = BASE / 'models'
REPORTS    = BASE / 'reports'
MODELS_DIR.mkdir(exist_ok=True)
REPORTS.mkdir(exist_ok=True)

import sys
sys.path.insert(0, str(BASE))
from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext

TRAIN_CUTOFF = 2021   # last fiscal year included in training
VAL_END      = 2023   # val = (TRAIN_CUTOFF, VAL_END]; test = > VAL_END

HORIZONS = {
    '1y': ('forward_return_1y', 'beat_local_market_1y'),
    '3y': ('forward_return_3y', 'beat_local_market_3y'),
    '5y': ('forward_return_5y', 'beat_local_market_5y'),
}

EXCLUDE = {
    # identifiers & metadata
    'cik', 'ticker', 'name', 'filed_date', 'fiscal_year', 'fiscal_quarter',
    'period_type', 'exchange', 'sic_code', 'sic_description', 'market',
    'country', 'accounting_std', 'size_category_label', 'corp_code', 'acc_mt',
    # raw dollar amounts — size-contaminated; normalised versions used instead
    'revenue', 'net_income', 'gross_profit', 'operating_income', 'pretax_income',
    'cogs', 'sga_expense', 'rd_expense', 'depreciation', 'da_expense',
    'operating_cash_flow', 'financing_cash_flow', 'investing_cash_flow',
    'capex', 'fcf',
    'long_term_debt', 'short_term_debt', 'total_debt',
    'total_assets', 'total_equity', 'current_assets', 'current_liabilities',
    'accounts_receivable', 'accounts_payable', 'receivables',
    'cash', 'intangibles', 'goodwill', 'ppe_net', 'noa',
    'market_cap_at_filing', 'tax_expense', 'interest_expense',
    'common_shares_outstanding', 'eps_diluted', 'eps_basic',
    'retained_earnings', 'additional_paid_in_capital', 'inventory',
}
EXCLUDE_PATTERNS = ['forward_return', 'beat_local_market', 'excess_return_local',
                    'benchmark_return']


def load_data() -> pd.DataFrame:
    df_raw = pd.read_parquet(DATA_PATH)
    df = df_raw[df_raw['period_type'] == 'annual'].copy()
    df = df[df['fiscal_year'].between(2008, 2025)].copy()

    df = df.sort_values('total_assets', ascending=False, na_position='last')
    df = df.drop_duplicates(subset=['ticker', 'fiscal_year'], keep='first')

    for col in [c for c in df.columns if 'growth_yoy' in c]:
        lo, hi = df[col].quantile(0.01), df[col].quantile(0.99)
        df[col] = df[col].clip(lo, hi)

    for col in ['forward_return_1y', 'forward_return_3y', 'forward_return_5y']:
        if col in df.columns:
            df[col] = df[col].clip(-1.0, 5.0)

    df = add_piotroski_ext(df)
    df = add_normalised_ratios(df)
    return df.reset_index(drop=True)


def get_candidates(df: pd.DataFrame) -> list[str]:
    return [
        c for c in df.columns
        if c not in EXCLUDE
        and not any(p in c for p in EXCLUDE_PATTERNS)
        and df[c].dtype in [np.float64, np.float32, np.int64, np.int32, 'Int64']
        and df[c].notna().mean() > 0.10
    ]


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


def deduplicate_features(df: pd.DataFrame, features: list[str], corr_threshold: float = 0.90) -> list[str]:
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


def train_model(df_train: pd.DataFrame, features: list[str], beat_col: str) -> tuple:
    sub = df_train[df_train[beat_col].notna()].copy()
    feats = [f for f in features if f in sub.columns]
    train_medians = sub[feats].median().to_dict()
    X = sub[feats].fillna(pd.Series(train_medians))
    y = sub[beat_col].astype(int)

    pos = int((y == 1).sum())
    neg = int((y == 0).sum())
    clf = lgb.LGBMClassifier(
        n_estimators=400,
        max_depth=5,
        learning_rate=0.04,
        num_leaves=31,
        subsample=0.8,
        colsample_bytree=0.7,
        min_child_samples=30,
        scale_pos_weight=neg / max(pos, 1),
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--top-n', type=int, default=40,
                        help='Max features per horizon after IC ranking (default: 40)')
    parser.add_argument('--no-dedup', action='store_true',
                        help='Skip correlation-based deduplication')
    parser.add_argument('--min-ic', type=float, default=0.02,
                        help='Minimum |mean IC| to enter candidate set (default: 0.02)')
    parser.add_argument('--sector-neutral', action='store_true',
                        help='Compute IC within SIC sectors (sector-neutral IC)')
    parser.add_argument('--train-cutoff', type=int, default=TRAIN_CUTOFF,
                        help=f'Last fiscal_year in training set (default: {TRAIN_CUTOFF})')
    parser.add_argument('--val-end', type=int, default=VAL_END,
                        help=f'Last fiscal_year in validation set (default: {VAL_END})')
    parser.add_argument('--no-shap', action='store_true',
                        help='Skip SHAP computation (faster run)')
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
    print(f'  Train : fiscal_year <= {train_cutoff}, filed < {_cutoff_date.date()} → {len(df_train):,} rows ({pit_excluded:,} PIT-excluded)')
    print(f'  Val   : {train_cutoff+1}–{val_end} → {len(df_val):,} rows')
    print(f'  Test  : > {val_end} → {len(df_test):,} rows')

    print('Computing feature candidates...')
    all_features = get_candidates(df_train)
    print(f'  {len(all_features)} candidates')

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
        candidates = tbl[tbl['mean_ic'].abs() > args.min_ic].index.tolist()
        top_n = candidates[:args.top_n]
        print(f'\n  {h}: {len(candidates)} pass |IC|>{args.min_ic} → keeping top {len(top_n)} by ICIR')

        if not args.no_dedup:
            top_n = deduplicate_features(df_train, top_n, corr_threshold=0.90)

        selected_features[h] = top_n
        print(f'    Final: {len(top_n)} features')
        print(f'    Top 10: {", ".join(top_n[:10])}')

    print('\nTraining LightGBM models...')
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
        print(f'done ({len(feats)} features, {len(y_train):,} rows, '
              f'LGBM val={val_auc:.3f}/test={test_auc:.3f} | '
              f'LR val={lr_val_auc:.3f}/test={lr_test_auc:.3f})')

    (MODELS_DIR / 'model_meta.json').write_text(json.dumps(model_meta, indent=2))
    print(f'\nAll models saved → {MODELS_DIR}/')
    print(f'IC tables + importance reports → {REPORTS}/')

    print('\n── Summary ─────────────────────────────────')
    print(f'  {"Horizon":<6}  {"LGBM val":>9}  {"LGBM test":>10}  {"LR val":>8}  {"LR test":>9}')
    for h in HORIZONS:
        m = model_meta[h]
        print(f'  {h:<6}  {m["val_auc"]:>9.4f}  {m["test_auc"]:>10.4f}  '
              f'{m["lr_val_auc"]:>8.4f}  {m["lr_test_auc"]:>9.4f}')


if __name__ == '__main__':
    main()
