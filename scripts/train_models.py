"""
Train and save ML models (1y / 3y / 5y) from historical_dataset_clean.parquet.
Produces models/model_{h}.joblib + models/model_meta.json.
"""
from __future__ import annotations
import json
import warnings
warnings.filterwarnings('ignore')

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from pathlib import Path
from scipy import stats
from sklearn.metrics import roc_auc_score

BASE       = Path(__file__).parent.parent
DATA_PATH  = BASE / 'data' / 'historical_dataset_clean.parquet'
MODELS_DIR = BASE / 'models'
MODELS_DIR.mkdir(exist_ok=True)

HORIZONS = {
    '1y': ('forward_return_1y', 'beat_local_market_1y'),
    '3y': ('forward_return_3y', 'beat_local_market_3y'),
    '5y': ('forward_return_5y', 'beat_local_market_5y'),
}

EXCLUDE = {
    'cik', 'ticker', 'name', 'filed_date', 'fiscal_year', 'fiscal_quarter',
    'period_type', 'exchange', 'sic_code', 'sic_description', 'market',
    'country', 'accounting_std', 'size_category_label', 'corp_code', 'acc_mt',
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

    # Piotroski extensions
    ann = df.sort_values(['ticker', 'fiscal_year'])
    for src, name in [
        ('shares_outstanding', 'piotroski_shares_ok'),
        ('gross_margin',       'piotroski_delta_gm'),
        ('asset_turnover',     'piotroski_delta_at'),
    ]:
        if src in df.columns:
            df[name] = ann.groupby('ticker')[src].transform(
                lambda x: (x <= x.shift(1)).astype(float) if src == 'shares_outstanding'
                else (x > x.shift(1)).astype(float)
            )
    extra_cols = [c for c in ['piotroski_shares_ok', 'piotroski_delta_gm', 'piotroski_delta_at'] if c in df.columns]
    if extra_cols and 'piotroski_f_score' in df.columns:
        df['piotroski_f_score_9'] = df['piotroski_f_score'].astype('float64') + df[extra_cols].sum(axis=1, min_count=1)

    return df.reset_index(drop=True)


def get_all_features(df: pd.DataFrame) -> list[str]:
    return [
        c for c in df.columns
        if c not in EXCLUDE
        and not any(p in c for p in EXCLUDE_PATTERNS)
        and df[c].dtype in [np.float64, np.float32, np.int64, np.int32, 'Int64']
        and df[c].notna().mean() > 0.10
    ]


def compute_ic_table(df: pd.DataFrame, features: list[str], return_col: str) -> pd.DataFrame:
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
            corr, _ = stats.spearmanr(grp[feat], grp[return_col])
            if not np.isnan(corr):
                ics.append(corr)
        if not ics:
            continue
        mean_ic = np.mean(ics)
        std_ic = np.std(ics) + 1e-8
        records.append({
            'feature':         feat,
            'mean_ic':         mean_ic,
            'std_ic':          std_ic,
            'icir':            mean_ic / std_ic,
            'n_years':         len(ics),
            'pct_positive_ic': np.mean([ic > 0 for ic in ics]),
        })
    return (pd.DataFrame(records).set_index('feature')
              .sort_values('mean_ic', key=abs, ascending=False))


def train_model(df: pd.DataFrame, features: list[str], beat_col: str) -> tuple:
    sub = df[df[beat_col].notna()].copy()
    feats = [f for f in features if f in sub.columns]
    X = sub[feats].fillna(sub[feats].median())
    y = sub[beat_col].astype(int)

    scale_pos = (y == 0).sum() / max((y == 1).sum(), 1)
    clf = xgb.XGBClassifier(
        n_estimators=300, max_depth=5, learning_rate=0.05,
        subsample=0.8, colsample_bytree=0.7,
        scale_pos_weight=scale_pos,
        min_child_weight=20, random_state=42, n_jobs=-1,
        eval_metric='logloss', verbosity=0,
    )
    clf.fit(X, y)
    return clf, feats, y


def main():
    print('Loading data...')
    df = load_data()
    print(f'  {len(df):,} annual rows | {df["ticker"].nunique():,} companies')

    print('Computing feature candidates...')
    all_features = get_all_features(df)
    print(f'  {len(all_features)} candidates')

    print('Running IC analysis (2–4 min)...')
    ic_tables = {}
    for h, (ret_col, _) in HORIZONS.items():
        print(f'  {h}...', end=' ', flush=True)
        ic_tables[h] = compute_ic_table(df, all_features, ret_col)
        n_sig = (ic_tables[h]['mean_ic'].abs() > 0.02).sum()
        print(f'{n_sig} features with |IC|>0.02')

    selected_features = {}
    for h in HORIZONS:
        tbl = ic_tables[h]
        mask = (tbl['mean_ic'].abs() > 0.02) | (tbl['icir'].abs() > 0.50)
        selected_features[h] = list(tbl[mask].index)
        print(f'  {h}: {len(selected_features[h])} features selected')

    print('\nTraining models...')
    model_meta = {}
    for h, (ret_col, beat_col) in HORIZONS.items():
        print(f'  {h}...', end=' ', flush=True)
        clf, feats, y = train_model(df, selected_features[h], beat_col)

        # Quick OOS AUC on last 2 years
        sub = df[df[beat_col].notna()].copy()
        feats_avail = [f for f in feats if f in sub.columns]
        test = sub[sub['fiscal_year'] >= sub['fiscal_year'].max() - 1]
        if len(test) > 50:
            X_test = test[feats_avail].fillna(test[feats_avail].median())
            y_test = test[beat_col].astype(int)
            oos_auc = roc_auc_score(y_test, clf.predict_proba(X_test)[:, 1])
        else:
            oos_auc = float('nan')

        path = MODELS_DIR / f'model_{h}.joblib'
        joblib.dump(clf, path)
        model_meta[h] = {
            'features': feats,
            'ret_col':  ret_col,
            'beat_col': beat_col,
            'n_train':  int(len(y)),
            'pos_rate': float(y.mean()),
            'oos_auc':  round(oos_auc, 4),
        }
        print(f'saved ({len(feats)} features, {len(y):,} rows, OOS AUC={oos_auc:.3f})')

    meta_path = MODELS_DIR / 'model_meta.json'
    meta_path.write_text(json.dumps(model_meta, indent=2))
    print(f'\nAll models saved to {MODELS_DIR}/')
    print(f'Meta: {meta_path}')


if __name__ == '__main__':
    main()
