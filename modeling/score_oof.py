"""
generate_oof_scores.py — Walk-forward out-of-fold (OOF) ML scoring.

Eliminates in-sample contamination from score_historical.py (which scores ALL rows,
including training rows, with the final trained model).

For each fiscal year Y (expanding window):
  Train : fiscal_year < Y  AND  filed_date <= Dec 31 of (Y-1)
  Score : fiscal_year == Y

Writes ml_1y_oof, ml_3y_oof, ml_5y_oof to the parquet.
Training-window rows (before first scored year) get NaN.

Feature sets are loaded from models/feature_sets_{h}.json (Phase B selection pipeline output).
Falls back to model_meta.json if feature_sets files are missing.

Usage:
    python3 scripts/generate_oof_scores.py
    python3 scripts/generate_oof_scores.py --min-train-years 6
    python3 scripts/generate_oof_scores.py --horizons 1y 3y
    python3 scripts/generate_oof_scores.py --dry-run
    python3 scripts/generate_oof_scores.py --parquet data/historical_dataset_clean.parquet

Output:
    data/historical_dataset_clean.parquet  — ml_1y_oof, ml_3y_oof, ml_5y_oof columns added
    reports/oof_auc_{h}.csv               — per-fold AUC for audit
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
warnings.filterwarnings('ignore')

from pathlib import Path

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

from _root import ROOT
from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext
from modeling.constants import EXCLUDE_COLS, EXCLUDE_PATTERNS, load_data, get_feature_candidates

BASE = ROOT
DATA_PATH  = BASE / 'data' / 'historical_dataset_clean.parquet'
MODELS_DIR = BASE / 'models'
REPORTS    = BASE / 'reports'
REPORTS.mkdir(exist_ok=True)

ALL_HORIZONS = {
    '6m': ('forward_return_6m', 'beat_local_market_6m'),
    '1y': ('forward_return_1y', 'beat_local_market_1y'),
    '2y': ('forward_return_2y', 'beat_local_market_2y'),
    '3y': ('forward_return_3y', 'beat_local_market_3y'),
    '5y': ('forward_return_5y', 'beat_local_market_5y'),
}




def load_feature_set(horizon: str, meta: dict | None) -> list[str] | None:
    """Load pre-selected features from Phase B feature_sets file, with fallback to model_meta."""
    fs_path = MODELS_DIR / f'feature_sets_{horizon}.json'
    if fs_path.exists():
        data = json.loads(fs_path.read_text())
        feats = data.get('features', [])
        if feats:
            print(f'  [{horizon}] Loaded {len(feats)} features from feature_sets_{horizon}.json')
            return feats

    if meta and horizon in meta:
        feats = meta[horizon].get('features', [])
        if feats:
            print(f'  [{horizon}] Loaded {len(feats)} features from model_meta.json (fallback)')
            return feats

    return None


get_candidates = get_feature_candidates


def train_fold(df_train: pd.DataFrame, features: list[str], beat_col: str,
               n_estimators: int = 600) -> tuple:
    """Train a single OOF fold model."""
    sub = df_train[df_train[beat_col].notna()].copy()
    feats = [f for f in features if f in sub.columns]
    if not feats:
        return None, [], {}, 0

    train_medians = sub[feats].median().to_dict()
    X = sub[feats].fillna(pd.Series(train_medians))
    y = sub[beat_col].astype(int)

    pos = int((y == 1).sum())
    neg = int((y == 0).sum())

    clf = lgb.LGBMClassifier(
        n_estimators=n_estimators,
        max_depth=6,
        learning_rate=0.03,
        num_leaves=63,
        subsample=0.8,
        colsample_bytree=0.7,
        min_child_samples=20,
        scale_pos_weight=neg / max(pos, 1),
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    clf.fit(X, y)
    return clf, feats, train_medians, len(y)


def run_oof(df: pd.DataFrame, horizon: str, beat_col: str,
            features: list[str], min_train_years: int,
            n_estimators: int) -> tuple[dict[int, np.ndarray], list[dict]]:
    """
    Run expanding-window OOF scoring.

    Returns:
        scores_by_year : {fiscal_year: ndarray of scores for that year's rows in df}
        fold_records   : list of dicts for AUC CSV
    """
    filed = pd.to_datetime(df.get('filed_date', pd.NaT), errors='coerce')
    first_year = int(df['fiscal_year'].min())
    max_year   = int(df['fiscal_year'].max())
    first_score_year = first_year + min_train_years

    scores_by_year: dict[int, np.ndarray] = {}
    fold_records: list[dict] = []

    for test_year in range(first_score_year, max_year + 1):
        cutoff_date = pd.Timestamp(f'{test_year}-01-01')

        # Training rows: fiscal_year < test_year AND filed_date <= Dec 31 of test_year-1
        train_mask = (
            (df['fiscal_year'] < test_year) &
            (filed.isna() | (filed < cutoff_date))
        )
        df_tr = df[train_mask].copy()

        # Test rows: fiscal_year == test_year
        df_te = df[df['fiscal_year'] == test_year].copy()
        df_te_labeled = df_te[df_te[beat_col].notna()]

        if (len(df_tr[df_tr[beat_col].notna()]) < 100 or
                len(df_te) == 0):
            continue

        clf, fold_feats, medians, n_train = train_fold(df_tr, features, beat_col,
                                                        n_estimators=n_estimators)
        if clf is None:
            continue

        fa = [f for f in fold_feats if f in df_te.columns]
        X_te = df_te[fa].fillna(pd.Series(medians))
        raw_scores = clf.predict_proba(X_te)[:, 1].astype(np.float32)
        scores_by_year[test_year] = (df_te.index.values, raw_scores)

        if len(df_te_labeled) >= 20 and df_te_labeled[beat_col].nunique() == 2:
            fa_l = [f for f in fold_feats if f in df_te_labeled.columns]
            X_te_l = df_te_labeled[fa_l].fillna(pd.Series(medians))
            auc = roc_auc_score(df_te_labeled[beat_col].astype(int),
                                clf.predict_proba(X_te_l)[:, 1])
            fold_records.append({
                'horizon':    horizon,
                'fold_year':  test_year - 1,
                'test_year':  test_year,
                'auc':        round(float(auc), 4),
                'n_train':    n_train,
                'n_test':     len(df_te),
            })
            print(f'      year {test_year}: n_train={n_train:,}  n_test={len(df_te):,}  AUC={auc:.4f}')
        else:
            print(f'      year {test_year}: n_train={n_train:,}  n_test={len(df_te):,}  (no AUC — unlabeled)')

    return scores_by_year, fold_records


def main() -> None:
    parser = argparse.ArgumentParser(description='Walk-forward OOF scoring — writes ml_*_oof columns')
    parser.add_argument('--parquet', default='data/historical_dataset_clean.parquet')
    parser.add_argument('--horizons', nargs='+', default=['6m', '1y', '2y', '3y', '5y'],
                        help='Horizons to score (default: 6m 1y 2y 3y 5y)')
    parser.add_argument('--min-train-years', type=int, default=6,
                        help='Min fiscal years in training window before first OOF score (default: 6)')
    parser.add_argument('--n-estimators', type=int, default=600,
                        help='LightGBM n_estimators per fold (default: 600)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Compute scores but do NOT write parquet')
    args = parser.parse_args()

    parquet_path = Path(args.parquet)
    if not parquet_path.exists():
        sys.exit(f'ERROR: {parquet_path} not found')

    horizons = {h: ALL_HORIZONS[h] for h in args.horizons if h in ALL_HORIZONS}
    if not horizons:
        sys.exit(f'ERROR: No valid horizons in {args.horizons}. Choose from {list(ALL_HORIZONS)}')

    # Load model_meta for feature fallback
    meta = None
    meta_path = MODELS_DIR / 'model_meta.json'
    if meta_path.exists():
        with open(meta_path) as f:
            meta = json.load(f)

    print(f'Loading dataset: {parquet_path}')
    df_all = pd.read_parquet(parquet_path)
    print(f'  Shape: {df_all.shape[0]:,} × {df_all.shape[1]}')

    print('Preprocessing for OOF...')
    df = load_data(parquet_path)
    print(f'  Annual rows: {len(df):,} | '
          f'fiscal_year range: {df["fiscal_year"].min()}–{df["fiscal_year"].max()}')

    # Map processed df index back to raw parquet index for writing scores
    # Use (ticker, fiscal_year) as join key
    df_all['_oof_key'] = df_all['ticker'].astype(str) + '_' + df_all['fiscal_year'].astype(str)
    df['_oof_key'] = df['ticker'].astype(str) + '_' + df['fiscal_year'].astype(str)

    print('\nLoading feature sets...')
    feature_sets: dict[str, list[str]] = {}
    for h in horizons:
        feats = load_feature_set(h, meta)
        if feats is None:
            print(f'  [{h}] WARNING: no feature_sets_{h}.json or model_meta.json found. '
                  f'Run run_feature_selection.py or train_models.py first.')
            feats = get_candidates(df)
            print(f'  [{h}] Falling back to all {len(feats)} candidates')
        feature_sets[h] = feats

    all_fold_records: list[dict] = []

    for h, (ret_col, beat_col) in horizons.items():
        oof_col = f'ml_{h}_oof'
        print(f'\n{"=" * 60}')
        print(f'  Horizon: {h}  target={beat_col}  output={oof_col}')
        print(f'{"=" * 60}')

        if beat_col not in df.columns:
            print(f'  SKIP — {beat_col} not in dataset')
            continue

        feats = feature_sets[h]
        feats_avail = [f for f in feats if f in df.columns]
        print(f'  Features: {len(feats_avail)}/{len(feats)} available in dataset')

        scores_by_year, fold_records = run_oof(
            df=df,
            horizon=h,
            beat_col=beat_col,
            features=feats_avail,
            min_train_years=args.min_train_years,
            n_estimators=args.n_estimators,
        )
        all_fold_records.extend(fold_records)

        if not scores_by_year:
            print(f'  [{h}] No scores generated — skipping column write')
            continue

        # Aggregate scores: (original df index → score)
        idx_arr = np.concatenate([idx for idx, _ in scores_by_year.values()])
        score_arr = np.concatenate([sc for _, sc in scores_by_year.values()])
        score_series = pd.Series(score_arr, index=idx_arr, dtype=np.float32)

        # Map back to df_all via _oof_key
        # df index already aligns with processed df row positions
        key_to_score = dict(zip(df.loc[score_series.index, '_oof_key'], score_series.values))
        df_all[oof_col] = df_all['_oof_key'].map(key_to_score).astype(np.float32)

        scored_n = df_all[oof_col].notna().sum()
        pct = np.nanpercentile(df_all[oof_col].dropna(), [10, 25, 50, 75, 90])
        print(f'\n  {oof_col}: {scored_n:,} rows scored  (NaN={df_all[oof_col].isna().sum():,} training rows)')
        print(f'  p10={pct[0]:.4f} p25={pct[1]:.4f} p50={pct[2]:.4f} p75={pct[3]:.4f} p90={pct[4]:.4f}')

        if fold_records:
            wf_df = pd.DataFrame(fold_records)
            wf_df.to_csv(REPORTS / f'oof_auc_{h}.csv', index=False)
            mean_auc = wf_df['auc'].mean()
            print(f'  Walk-forward OOF AUC: mean={mean_auc:.4f}  '
                  f'min={wf_df["auc"].min():.4f}  max={wf_df["auc"].max():.4f}')
            print(f'  Saved: reports/oof_auc_{h}.csv')

    df_all.drop(columns=['_oof_key'], errors='ignore', inplace=True)

    if args.dry_run:
        print('\n[dry-run] Parquet NOT written.')
        oof_cols = [f'ml_{h}_oof' for h in horizons]
        for col in oof_cols:
            if col in df_all.columns:
                n = df_all[col].notna().sum()
                print(f'  {col}: {n:,} non-NaN rows computed')
        return

    print(f'\nWriting updated parquet → {parquet_path}')
    df_all.to_parquet(parquet_path, index=False)
    oof_cols = [f'ml_{h}_oof' for h in horizons if f'ml_{h}_oof' in df_all.columns]
    print(f'  Shape: {df_all.shape[0]:,} × {df_all.shape[1]}  |  new cols: {oof_cols}')

    # Summary AUC table
    if all_fold_records:
        summary = (pd.DataFrame(all_fold_records)
                   .groupby('horizon')['auc']
                   .agg(['mean', 'min', 'max', 'count'])
                   .round(4))
        print('\nOOF Walk-Forward AUC Summary:')
        print(summary.to_string())


if __name__ == '__main__':
    main()
