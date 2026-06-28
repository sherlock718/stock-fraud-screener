"""
Walk-forward backtester with transaction costs and slippage.

Improvements over v1:
  - Expanding-window median imputation in ML scoring (removes look-ahead bias)
  - Fixed per-pick cost alignment with NaN-dropped return rows
  - Calmar ratio, Sortino ratio
  - Rolling 3y Sharpe per year in output
  - --tearsheet flag for clean formatted summary

Usage:
    python3 scripts/backtester.py --strategy all
    python3 scripts/backtester.py --strategy composite --market US --top 20
    python3 scripts/backtester.py --strategy qem --top 15 --cost 40 --tearsheet

Saves results to data/backtest_results.json.
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

import sys
from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext
from _root import ROOT
from modeling.constants import (
    EXCLUDE_COLS, EXCLUDE_PATTERNS,
    BENEISH_THRESHOLD, TREE_THRESHOLD, PIOTROSKI_MIN,
    VALUE_GATE_PCT, ALTMAN_Z_MIN, MOMENTUM_12M_MIN,
)

BASE = ROOT

FULL_DATA         = BASE / 'data' / 'historical_dataset_clean.parquet'
MODELS_DIR        = BASE / 'models'
OUT_PATH          = BASE / 'data' / 'backtest_results.json'
SPY_PATH          = BASE / 'data' / 'spy_returns.csv'
ACWI_EXUS_PATH    = BASE / 'data' / 'acwi_exus_returns.csv'
MONTHLY_CACHE     = BASE / 'data' / 'monthly_prices.parquet'

DEFAULT_COST_BPS  = 30    # 30 bps round-trip (commission 10bps + slippage 20bps)
SMALLCAP_COST_BPS = 60    # Illiquidity premium for micro/small caps
RISK_FREE = 0.03          # Annual risk-free rate for Sharpe/Sortino
MIN_MARKET_CAP    = 50_000_000  # $50M floor — removes truly illiquid stocks
MAX_MARKET_CAP    = 0           # 0 = no ceiling; set to e.g. 5_000_000_000 for small/mid-cap only

# Tiered slippage (bps) by market-cap band; applied per-pick inside run_backtest
SLIPPAGE_TIERS = [
    (10_000_000_000, 20),   # large-cap  >$10B  → 20 bps
    (1_000_000_000,  30),   # mid-cap    $1B–$10B → 30 bps
    (100_000_000,    50),   # small-cap  $100M–$1B → 50 bps
    (0,              80),   # micro-cap  <$100M  → 80 bps
]
MAX_POSITION_WEIGHT = 0.20      # Max weight per stock in vol-scaled portfolio
MAX_SECTOR_WEIGHT   = 0.35      # Max total weight in any single SIC sector


def load_spy_returns() -> dict[int, float]:
    """Load SPY annual returns from data/spy_returns.csv. Returns {year: return}."""
    if SPY_PATH.exists():
        df = pd.read_csv(SPY_PATH)
        return dict(zip(df['year'].astype(int), df['spy_return'].astype(float)))
    return {}


def load_acwi_exus_returns() -> dict[int, float]:
    """Load MSCI ACWI ex-US annual returns. Returns {year: return}."""
    if ACWI_EXUS_PATH.exists():
        df = pd.read_csv(ACWI_EXUS_PATH)
        return dict(zip(df['year'].astype(int), df['acwi_exus_return'].astype(float)))
    return {}


def load_monthly_prices() -> pd.DataFrame | None:
    """Load monthly price cache built by build_monthly_price_cache.py."""
    if not MONTHLY_CACHE.exists():
        return None
    df = pd.read_parquet(MONTHLY_CACHE)
    df['date'] = pd.to_datetime(df['date'])
    return df


def compute_monthly_nav(annual_rows: list[dict], monthly_px: pd.DataFrame) -> tuple[float, int]:
    """Build a monthly portfolio NAV and return (max_drawdown, max_dd_months).

    For each backtest year, reconstruct the portfolio's monthly return path
    by weighting each pick's monthly return by its allocation weight, then
    chain these into a single continuous NAV series.

    Falls back to annual-frequency drawdown if monthly data is missing.
    """
    nav_monthly: list[float] = [1.0]

    for row in annual_rows:
        yr   = int(row['year'])
        # Portfolio holds stocks selected from fiscal_year=yr filings,
        # held for calendar year yr+1 (Jan–Dec of the following year).
        hold_start = pd.Timestamp(f'{yr+1}-01-01')
        hold_end   = pd.Timestamp(f'{yr+1}-12-31')

        picks     = row.get('_picks_valid')
        weights   = row.get('_weights')
        if picks is None or weights is None:
            # Fallback: advance NAV by the known annual return
            nav_monthly.append(nav_monthly[-1] * (1 + row['port_ret']))
            continue

        # Get monthly price data for each pick over the holding period
        tickers = picks['ticker'].tolist()
        mask = (
            (monthly_px['ticker'].isin(tickers)) &
            (monthly_px['date'] >= hold_start) &
            (monthly_px['date'] <= hold_end)
        )
        sub = monthly_px[mask].copy()

        if sub.empty:
            nav_monthly.append(nav_monthly[-1] * (1 + row['port_ret']))
            continue

        # Compute monthly returns per ticker
        sub = sub.sort_values(['ticker', 'date'])
        sub['monthly_ret'] = sub.groupby('ticker')['adj_close'].pct_change()

        # Pivot to (date × ticker) matrix, fill with 0 for missing months
        ret_matrix = sub.pivot_table(
            index='date', columns='ticker', values='monthly_ret'
        ).reindex(columns=tickers).fillna(0.0)

        if ret_matrix.empty:
            nav_monthly.append(nav_monthly[-1] * (1 + row['port_ret']))
            continue

        # Weight vector aligned to tickers order
        ticker_to_weight = dict(zip(tickers, weights))
        w = np.array([ticker_to_weight.get(t, 0.0) for t in ret_matrix.columns])
        w = w / w.sum() if w.sum() > 0 else np.ones(len(w)) / len(w)

        # Monthly portfolio returns; accumulate into NAV
        port_monthly_rets = ret_matrix.values @ w
        nav_segment = nav_monthly[-1] * np.cumprod(1 + port_monthly_rets)
        nav_monthly.extend(nav_segment.tolist())

    nav = np.array(nav_monthly)
    peak = np.maximum.accumulate(nav)
    drawdowns = (nav - peak) / np.where(peak > 0, peak, 1)
    max_dd = float(drawdowns.min())

    # Drawdown duration in months
    in_dd = drawdowns < 0
    dd_months = 0
    cur = 0
    for d in in_dd:
        cur = cur + 1 if d else 0
        dd_months = max(dd_months, cur)

    return max_dd, dd_months


def adtv_filter(yr_df: pd.DataFrame, monthly_px: pd.DataFrame | None,
                yr: int, max_pct_adtv: float = 0.01,
                aum_target: float = 200_000) -> pd.DataFrame:
    """Remove picks whose position would exceed max_pct_adtv of trailing 30d median ADTV.

    Args:
        max_pct_adtv: Max fraction of ADTV a single position may represent.
            Default 1% — retail-friendly constraint.
        aum_target: Portfolio AUM in dollars. Default $200K (retail).
            min_adtv = aum_target * max_pct_adtv.
    """
    if monthly_px is None or yr_df.empty:
        return yr_df

    obs_end   = pd.Timestamp(f'{yr}-12-31')
    obs_start = pd.Timestamp(f'{yr}-09-30')
    sub = monthly_px[
        (monthly_px['date'] >= obs_start) &
        (monthly_px['date'] <= obs_end)
    ].groupby('ticker')['adtv_30d'].median().reset_index()
    sub.columns = ['ticker', 'adtv_est']

    merged = yr_df.merge(sub, on='ticker', how='left')
    # Position can't exceed max_pct_adtv of daily volume
    min_adtv = aum_target * max_pct_adtv
    keep = merged['adtv_est'].isna() | (merged['adtv_est'] >= min_adtv)
    return yr_df[keep.values]


# ── Data helpers ──────────────────────────────────────────────────────────────

def load_full_hist() -> pd.DataFrame:
    df = pd.read_parquet(FULL_DATA)
    df = df[df['period_type'] == 'annual'].copy()
    df = add_piotroski_ext(df)
    df = add_normalised_ratios(df)
    return df.reset_index(drop=True)




def _select_features(df: pd.DataFrame) -> list[str]:
    return [
        c for c in df.columns
        if c not in EXCLUDE_COLS
        and not any(p in c for p in EXCLUDE_PATTERNS)
        and df[c].dtype in [np.float64, np.float32, np.int64, np.int32, 'Int64']
        and df[c].notna().mean() > 0.10
    ]


def _ic_rank(df: pd.DataFrame, features: list[str], ret_col: str, top_n: int = 35) -> list[str]:
    """Return top features by |ICIR| computed on df."""
    sub = df[df[ret_col].notna()]
    years = sorted(sub['fiscal_year'].unique())
    records = []
    for feat in features:
        sub2 = sub[sub[feat].notna()]
        ics = []
        for yr in years:
            g = sub2[sub2['fiscal_year'] == yr]
            if len(g) < 30:
                continue
            c, _ = stats.spearmanr(g[feat], g[ret_col])
            if not np.isnan(c):
                ics.append(c)
        if len(ics) < 3:
            continue
        mean_ic = np.mean(ics)
        std_ic  = np.std(ics) + 1e-8
        records.append((feat, abs(mean_ic / std_ic)))
    records.sort(key=lambda x: x[1], reverse=True)
    return [r[0] for r in records[:top_n]]


def load_and_score(df: pd.DataFrame) -> pd.DataFrame:
    """Walk-forward ML scoring: for each year Y, retrain on data ≤ Y-1, score year Y.

    This is the only unbiased way to use ML scores in a backtest.
    Falls back to pre-trained static models if walk-forward fails.
    Adds columns: ml_1y_wf, ml_3y_wf, ml_5y_wf.
    """
    HORIZONS_WF = {
        '1y': ('forward_return_1y', 'beat_local_market_1y'),
        '3y': ('forward_return_3y', 'beat_local_market_3y'),
        '5y': ('forward_return_5y', 'beat_local_market_5y'),
    }

    all_features = _select_features(df)
    years = sorted(y for y in df['fiscal_year'].unique() if y <= 2024)

    # Need at least 5 years of history before first score
    min_train_years = 5

    # PIT: precompute filed_date as datetime once
    _filed = pd.to_datetime(df.get('filed_date', pd.NaT), errors='coerce')

    for h, (ret_col, beat_col) in HORIZONS_WF.items():
        if beat_col not in df.columns or ret_col not in df.columns:
            continue

        scores = np.full(len(df), np.nan)
        print(f'    WF-ML {h}: training year by year...', flush=True)

        for i, score_yr in enumerate(years):
            # PIT: only include filings available before score_yr starts
            _cutoff = pd.Timestamp(f'{score_yr}-01-01')
            _pit_mask = _filed.isna() | (_filed < _cutoff)
            train_df = df[
                (df['fiscal_year'] < score_yr) & df[beat_col].notna() & _pit_mask
            ].copy()
            if train_df['fiscal_year'].nunique() < min_train_years:
                continue

            feats = _ic_rank(train_df, all_features, ret_col, top_n=35)
            feats = [f for f in feats if f in train_df.columns]
            if len(feats) < 5:
                continue

            # Expanding-window median imputation on training set
            train_med = train_df[feats].median()
            X_train = train_df[feats].fillna(train_med)
            y_train  = train_df[beat_col].astype(int)

            pos = int((y_train == 1).sum())
            neg = int((y_train == 0).sum())
            clf = lgb.LGBMClassifier(
                n_estimators=200, max_depth=4, learning_rate=0.05,
                num_leaves=20, subsample=0.8, colsample_bytree=0.7,
                min_child_samples=30, scale_pos_weight=neg / max(pos, 1),
                random_state=42, n_jobs=-1, verbose=-1,
            )
            clf.fit(X_train, y_train)

            # Score hold-out year using training medians for imputation
            score_mask = (df['fiscal_year'] == score_yr).values
            if score_mask.sum() == 0:
                continue
            X_score = df.loc[score_mask, feats].fillna(train_med)
            scores[score_mask] = clf.predict_proba(X_score)[:, 1]

        df[f'ml_{h}_wf'] = scores
        n_scored = (~np.isnan(scores)).sum()
        print(f'      {h}: {n_scored:,} rows scored walk-forward', flush=True)

    # ── Walk-forward decision tree (agreement gate) ────────────────────────────
    # Depth-4 tree trained on same data as LightGBM, provides tree_prob for ml_gates
    beat_col_3y = 'beat_local_market_3y'
    if beat_col_3y in df.columns:
        from sklearn.tree import DecisionTreeClassifier
        tree_probs = np.full(len(df), np.nan)
        print('    WF-Tree: training year by year...', flush=True)
        for score_yr in years:
            _cutoff = pd.Timestamp(f'{score_yr}-01-01')
            _pit_mask = _filed.isna() | (_filed < _cutoff)
            train_df = df[
                (df['fiscal_year'] < score_yr) & df[beat_col_3y].notna() & _pit_mask
            ].copy()
            if train_df['fiscal_year'].nunique() < min_train_years:
                continue
            feats = _ic_rank(train_df, all_features, 'forward_return_3y', top_n=35)
            feats = [f for f in feats if f in train_df.columns]
            if len(feats) < 5:
                continue
            train_med = train_df[feats].median()
            X_train = train_df[feats].fillna(train_med)
            y_train = train_df[beat_col_3y].astype(int)
            tree_clf = DecisionTreeClassifier(
                max_depth=4, min_samples_leaf=30, random_state=42
            )
            tree_clf.fit(X_train, y_train)
            score_mask = (df['fiscal_year'] == score_yr).values
            if score_mask.sum() == 0:
                continue
            X_score = df.loc[score_mask, feats].fillna(train_med)
            tree_probs[score_mask] = tree_clf.predict_proba(X_score)[:, 1]
        df['tree_prob'] = tree_probs
        n_tree = (~np.isnan(tree_probs)).sum()
        print(f'      tree: {n_tree:,} rows scored walk-forward', flush=True)

    # ── Walk-forward regression (3y return magnitude) ─────────────────────────
    ret_col_3y = 'forward_return_3y'
    if ret_col_3y in df.columns:
        reg_scores = np.full(len(df), np.nan)
        print('    WF-Reg 3y: training year by year...', flush=True)
        for score_yr in years:
            _cutoff = pd.Timestamp(f'{score_yr}-01-01')
            _pit_mask = _filed.isna() | (_filed < _cutoff)
            train_df = df[
                (df['fiscal_year'] < score_yr) & df[ret_col_3y].notna() & _pit_mask
            ].copy()
            if train_df['fiscal_year'].nunique() < min_train_years:
                continue
            # Clean training data for regression too
            clean_mask = (
                (train_df.get('fraud_suspect', pd.Series(0, index=train_df.index)) == 0) &
                (train_df.get('piotroski_roa_pos', pd.Series(1, index=train_df.index)) == 1) &
                (train_df.get('beneish_m_score', pd.Series(-3, index=train_df.index)) < -1.78)
            )
            train_df = train_df[clean_mask]
            if len(train_df) < 100:
                continue
            feats = _ic_rank(train_df, all_features, ret_col_3y, top_n=35)
            feats = [f for f in feats if f in train_df.columns]
            if len(feats) < 5:
                continue
            train_med = train_df[feats].median()
            X_train = train_df[feats].fillna(train_med)
            y_train = train_df[ret_col_3y].clip(-1, 5)
            reg = lgb.LGBMRegressor(
                n_estimators=600, max_depth=6, learning_rate=0.03,
                num_leaves=63, subsample=0.8, colsample_bytree=0.7,
                min_child_samples=20, random_state=42, n_jobs=-1, verbose=-1,
            )
            reg.fit(X_train, y_train)
            score_mask = (df['fiscal_year'] == score_yr).values
            if score_mask.sum() == 0:
                continue
            X_score = df.loc[score_mask, feats].fillna(train_med)
            reg_scores[score_mask] = reg.predict(X_score)
        df['reg_3y_wf'] = reg_scores
        n_reg = (~np.isnan(reg_scores)).sum()
        print(f'      reg_3y: {n_reg:,} rows scored walk-forward', flush=True)

    # Also keep static model scores as fallback (for years before walk-forward kicks in)
    meta_path = MODELS_DIR / 'model_meta.json'
    if meta_path.exists():
        meta = json.loads(meta_path.read_text())
        loaded: dict[str, tuple] = {}
        all_feats_set: set[str] = set()
        for h in ['1y', '3y', '5y']:
            p = MODELS_DIR / f'model_{h}.joblib'
            if not p.exists():
                continue
            clf = joblib.load(p)
            feats = [f for f in meta[h]['features'] if f in df.columns]
            loaded[h] = (clf, feats)
            all_feats_set.update(feats)

        all_feats_list = sorted(all_feats_set)
        all_years = sorted(df['fiscal_year'].unique())
        exp_med: dict[int, pd.Series] = {}
        for yr in all_years:
            exp_med[yr] = df.loc[df['fiscal_year'] <= yr, all_feats_list].median()

        for h, (clf, feats) in loaded.items():
            static_scores = np.full(len(df), np.nan)
            for yr in all_years:
                mask = (df['fiscal_year'] == yr).values
                if mask.sum() == 0:
                    continue
                X = df.loc[mask, feats].fillna(exp_med[yr][feats])
                static_scores[mask] = clf.predict_proba(X)[:, 1]
            df[f'ml_{h}'] = static_scores

    return df


# ── Strategy filter functions ─────────────────────────────────────────────────

def _ml(s: pd.DataFrame, horizon: str) -> str:
    """Return the best available ML column name for a given horizon."""
    wf = f'ml_{horizon}_wf'
    st = f'ml_{horizon}'
    if wf in s.columns and s[wf].notna().sum() > 5:
        return wf
    return st


def filter_composite(yr_df: pd.DataFrame, top_n: int, market: str | None,
                     mode: str = 'blended') -> pd.Index:
    s = yr_df.copy()
    if market:
        s = s[s['market'] == market]

    # ── Hard gates (both modes) ──────────────────────────────────────────────
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < BENEISH_THRESHOLD]
    if 'likely_delisted' in s.columns:
        s = s[s['likely_delisted'].fillna(1) == 0]

    if mode == 'ml_gates':
        # Quality gate: Piotroski >= MIN + ROA positive (filters value traps)
        if 'piotroski_f_score' in s.columns:
            s = s[s['piotroski_f_score'].fillna(0) >= PIOTROSKI_MIN]
        if 'piotroski_roa_pos' in s.columns:
            s = s[s['piotroski_roa_pos'].fillna(0) == 1]
        # Value gate: not grossly overpriced (top-half cheapness within sector)
        if 'ps_ratio_sector_pct' in s.columns:
            s = s[s['ps_ratio_sector_pct'].fillna(0.5) <= VALUE_GATE_PCT]
        # Agreement gate: tree must concur
        if 'tree_prob' in s.columns:
            s = s[s['tree_prob'].fillna(0) >= TREE_THRESHOLD]
        # Altman Z gate: exclude distressed companies
        if 'altman_z_score' in s.columns:
            s = s[s['altman_z_score'].fillna(0) > ALTMAN_Z_MIN]
        # Momentum gate: exclude structural decliners
        if 'momentum_12m_prior' in s.columns:
            s = s[s['momentum_12m_prior'].fillna(0) > MOMENTUM_12M_MIN]
        # Rank by regression 3y (return magnitude), fallback to classification
        if 'reg_3y_wf' in s.columns and s['reg_3y_wf'].notna().sum() > 5:
            return s.nlargest(top_n, 'reg_3y_wf').index
        ml_col = _ml(s, '3y')
        if ml_col not in s.columns or s[ml_col].notna().sum() == 0:
            return pd.Index([])
        return s.nlargest(top_n, ml_col).index

    # ── Blended mode (legacy default) ────────────────────────────────────────
    score = pd.Series(0.0, index=s.index)
    total_w = 0.0
    for col, w in [('value_composite', 0.25), ('quality_composite', 0.20),
                   (_ml(s, '1y'), 0.30), (_ml(s, '3y'), 0.15), ('piotroski_f_score', 0.10)]:
        if col in s.columns and s[col].notna().sum() > 5:
            score += s[col].rank(pct=True) * w
            total_w += w
    if total_w == 0:
        return pd.Index([])
    s['_score'] = score / total_w
    return s.nlargest(top_n, '_score').index


def filter_qem(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    s = yr_df.copy()
    if market:
        s = s[s['market'] == market]
    s = s[s['piotroski_f_score'].fillna(0) >= 7]
    if 'eps_growth_yoy' in s.columns:
        s = s[s['eps_growth_yoy'].fillna(-99) > 0]
    if 'momentum_12m_prior' in s.columns:
        s = s[s['momentum_12m_prior'].fillna(-99) > -0.10]
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < -1.78]
    # Value guard: exclude deeply unprofitable / overpriced stocks that are rate-sensitive
    if 'earnings_yield' in s.columns and s['earnings_yield'].notna().mean() > 0.3:
        s = s[s['earnings_yield'].fillna(-99) > 0]
    score = pd.Series(0.0, index=s.index)
    total_w = 0.0
    for col, w in [('eps_growth_yoy', 0.20), ('quality_composite', 0.25),
                   (_ml(s, '1y'), 0.25), ('momentum_12m_prior', 0.15),
                   ('value_composite', 0.15)]:
        if col in s.columns and s[col].notna().sum() > 3:
            score += s[col].rank(pct=True) * w
            total_w += w
    if total_w == 0:
        return pd.Index([])
    s['_score'] = score / total_w
    return s.nlargest(top_n, '_score').index


def filter_scdv(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    s = yr_df.copy()
    if market:
        s = s[s['market'] == market]
    if 'size_category_label' in s.columns:
        s = s[s['size_category_label'].isin(['micro', 'small'])]
    if 'pb_ratio' in s.columns and s['pb_ratio'].notna().mean() > 0.3:
        s = s[s['pb_ratio'].fillna(99) < 2.0]
    s = s[s['piotroski_f_score'].fillna(0) >= 6]
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < -1.78]
    if 'altman_z_score' in s.columns:
        s = s[s['altman_z_score'].fillna(0) > 1.81]
    score = pd.Series(0.0, index=s.index)
    total_w = 0.0
    for col, w in [('value_composite', 0.35), ('quality_composite', 0.25),
                   (_ml(s, '3y'), 0.25), ('piotroski_f_score', 0.15)]:
        if col in s.columns and s[col].notna().sum() > 3:
            score += s[col].rank(pct=True) * w
            total_w += w
    if total_w == 0:
        return pd.Index([])
    s['_score'] = score / total_w
    if 'debt_to_equity' in s.columns:
        penalty = s['debt_to_equity'].fillna(0).clip(0, 2) * 0.05
        s['_score'] = s['_score'] - penalty
    return s.nlargest(top_n, '_score').index


def filter_iarb(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    s = yr_df.copy()
    if market:
        s = s[s['market'] == market]
    s = s[s['market'] != 'US']
    if 'pb_ratio' in s.columns and s['pb_ratio'].notna().mean() > 0.3:
        s = s[s['pb_ratio'].fillna(99) < 1.5]
    s = s[s['piotroski_f_score'].fillna(0) >= 6]
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < -1.78]
    score = pd.Series(0.0, index=s.index)
    total_w = 0.0
    for col, w in [('value_composite', 0.30), ('quality_composite', 0.25),
                   (_ml(s, '3y'), 0.25), ('momentum_12m_prior', 0.20)]:
        if col in s.columns and s[col].notna().sum() > 3:
            score += s[col].rank(pct=True) * w
            total_w += w
    if total_w == 0:
        return pd.Index([])
    market_boost = {'KR': 0.05, 'BR': 0.03, 'CA': 0.02}
    s['_score'] = (score / total_w if total_w > 0 else score) + s['market'].map(market_boost).fillna(0)
    return s.nlargest(top_n, '_score').index


STRATEGIES = {
    'composite': filter_composite,
    'ml_gates':  lambda yr_df, top_n, market: filter_composite(yr_df, top_n, market, mode='ml_gates'),
    'qem':       filter_qem,
    'scdv':      filter_scdv,
    'iarb':      filter_iarb,
}


# ── Walk-forward engine ───────────────────────────────────────────────────────

MAX_FILING_LAG_MONTHS = 18   # reject filings > 18 months after fiscal year-end


def _apply_filing_lag_filter(yr_df: pd.DataFrame, yr: int,
                              max_lag_months: int) -> pd.DataFrame:
    """Drop rows where filed_date is implausibly late (look-ahead protection).

    A 10-K filed 18+ months after fiscal year-end is suspicious —
    it would not have been available at the typical Jan portfolio selection date.
    """
    if 'filed_date' not in yr_df.columns:
        return yr_df
    filed = pd.to_datetime(yr_df['filed_date'], errors='coerce')
    # Cutoff = fiscal year-end + max_lag_months
    cutoff = pd.Timestamp(f'{yr}-12-31') + pd.DateOffset(months=max_lag_months)
    mask = filed.isna() | (filed <= cutoff)
    n_dropped = (~mask).sum()
    if n_dropped > 0:
        pass  # caller can log if needed
    return yr_df[mask]


def _sic_to_sector(sic: pd.Series) -> pd.Series:
    s = pd.to_numeric(sic, errors='coerce').fillna(0).astype(int)
    sector = pd.Series('Other', index=s.index)
    sector[s.between(100,  999)]  = 'Agriculture/Mining'
    sector[s.between(1000, 1499)] = 'Mining/Resources'
    sector[s.between(1500, 1999)] = 'Construction'
    sector[s.between(2000, 3999)] = 'Manufacturing'
    sector[s.between(4000, 4999)] = 'Utilities/Transport'
    sector[s.between(5000, 5999)] = 'Trade'
    sector[s.between(6000, 6799)] = 'Finance/Insurance/RE'
    sector[s.between(7000, 7999)] = 'Services/Hospitality'
    sector[s.between(8000, 8999)] = 'Services/Professional'
    return sector


def _apply_sector_cap(weights: np.ndarray, picks_df: pd.DataFrame,
                      max_sector_weight: float) -> np.ndarray:
    """Scale down weights so no SIC sector exceeds max_sector_weight.

    Iteratively scales overweight sectors toward the cap, redistributing
    excess to under-weight sectors. Converges in ≤ N_sectors iterations.
    """
    if 'sic_code' not in picks_df.columns:
        return weights
    sectors = _sic_to_sector(picks_df['sic_code'].reset_index(drop=True)
                              if hasattr(picks_df, 'reset_index') else picks_df['sic_code'])
    w = weights.copy()
    for _ in range(len(w)):  # max iterations bounded by portfolio size
        sector_totals = {}
        for i, sec in enumerate(sectors):
            sector_totals[sec] = sector_totals.get(sec, 0.0) + w[i]
        overweight = {s: t for s, t in sector_totals.items() if t > max_sector_weight + 1e-9}
        if not overweight:
            break
        for sec, total in overweight.items():
            scale = max_sector_weight / total
            mask = sectors == sec
            w[mask.values] *= scale
        w = w / w.sum()  # renormalise
    return w


def run_backtest(df: pd.DataFrame, filter_fn, label: str,
                 top_n: int, market: str | None,
                 cost_bps: int, smallcap_cost_bps: int,
                 min_market_cap: int = MIN_MARKET_CAP,
                 max_market_cap: int = MAX_MARKET_CAP,
                 vol_weighted: bool = True,
                 fill_missing_return: float | None = None,
                 survivorship_mode: str = 'impute',
                 max_filing_lag_months: int = MAX_FILING_LAG_MONTHS,
                 filing_date_gate: bool = True,
                 spy_returns: dict | None = None,
                 acwi_exus_returns: dict | None = None,
                 is_non_us: bool = False,
                 monthly_px: pd.DataFrame | None = None,
                 use_adtv_filter: bool = True,
                 max_pct_adtv: float = 0.01,
                 aum_target: float = 200_000) -> dict:
    """Walk-forward backtest engine.

    Args:
        fill_missing_return: If set, overrides the survivorship_mode imputation
            value. Default None means survivorship_mode controls behavior.
        survivorship_mode: How to handle stocks with NaN forward_return_1y.
            'impute' (default): impute -0.5 return (missing ~ delisted).
            'drop': drop rows (optimistic, old behavior).
            'flag_only': drop but log count per year.
        max_filing_lag_months: Drop filings received more than N months after
            fiscal year-end (look-ahead protection). Default 18.
        filing_date_gate: When True, only include stocks whose filed_date
            is before the holding year start (Jan 1). Stocks filing later
            become eligible in the next year's portfolio.
        spy_returns: Dict of {year: spy_annual_return} for SPY benchmark.
            If None, falls back to equal-weight universe mean as benchmark.
        acwi_exus_returns: Dict of {year: acwi_exus_annual_return} for non-US
            benchmark. Used as primary benchmark when is_non_us=True.
        is_non_us: When True, use ACWI ex-US (or equal-weight non-US universe)
            as primary benchmark instead of SPY. SPY remains informational.
        monthly_px: Monthly price cache from build_monthly_price_cache.py.
            When provided, MaxDD is computed from a monthly NAV curve instead
            of the annual wealth index (fixes the MaxDD=0% bug).
        use_adtv_filter: When True and monthly_px is available, remove picks
            that would require trading > max_pct_adtv of 30d median ADTV.
        max_pct_adtv: Max fraction of ADTV per position (default 1%).
        aum_target: Portfolio AUM in dollars (default $200K for retail).
    """
    # Resolve effective imputation value from survivorship_mode
    if fill_missing_return is not None:
        _impute_val = fill_missing_return
        _do_impute = True
    elif survivorship_mode == 'impute':
        _impute_val = -0.5
        _do_impute = True
    else:
        _impute_val = None
        _do_impute = False

    years = sorted(y for y in df['fiscal_year'].unique() if y <= 2023)
    annual_rows = []
    total_survivorship_dropped = 0
    total_picks_attempted = 0

    for yr in years:
        yr_df = df[df['fiscal_year'] == yr].copy()

        # ── Look-ahead protection: drop implausibly late filings ──────────────
        yr_df = _apply_filing_lag_filter(yr_df, yr, max_filing_lag_months)

        # ── Filing-date gate: only include stocks filed before holding year ───
        if filing_date_gate and 'filed_date' in yr_df.columns:
            filed = pd.to_datetime(yr_df['filed_date'], errors='coerce')
            holding_start = pd.Timestamp(f'{yr + 1}-01-01')
            yr_df = yr_df[filed.isna() | (filed < holding_start)]

        # Liquidity pre-filter: remove stocks outside market cap range
        if 'market_cap_at_filing' in yr_df.columns:
            if min_market_cap > 0:
                yr_df = yr_df[yr_df['market_cap_at_filing'].fillna(0) >= min_market_cap]
            if max_market_cap > 0:
                yr_df = yr_df[yr_df['market_cap_at_filing'].fillna(0) <= max_market_cap]

        # ADTV liquidity filter: remove tickers too illiquid for a 5%-ADTV position
        if use_adtv_filter and monthly_px is not None:
            yr_df = adtv_filter(yr_df, monthly_px, yr, max_pct_adtv, aum_target)

        idx = filter_fn(yr_df, top_n, market)
        picks = yr_df.loc[idx]

        if 'forward_return_1y' not in picks.columns:
            continue

        n_picks_raw = len(picks)
        total_picks_attempted += n_picks_raw

        # ── Survivorship bias handling ────────────────────────────────────────
        missing_mask = picks['forward_return_1y'].isna()
        n_missing = int(missing_mask.sum())
        total_survivorship_dropped += n_missing

        if _do_impute and n_missing > 0:
            picks = picks.copy()
            picks.loc[missing_mask, 'forward_return_1y'] = _impute_val
            picks_valid = picks
        else:
            picks_valid = picks[~missing_mask]

        rets = picks_valid['forward_return_1y']
        if len(rets) < 3:
            continue

        # Per-pick cost: tiered by market_cap_at_filing if available, else legacy flags
        if 'market_cap_at_filing' in picks_valid.columns:
            caps = picks_valid['market_cap_at_filing'].fillna(0).values
            per_pick_cost = np.array([
                next(bps for threshold, bps in SLIPPAGE_TIERS if cap >= threshold) / 10000
                for cap in caps
            ])
        elif 'size_category_label' in picks_valid.columns:
            is_small = picks_valid['size_category_label'].isin(['micro', 'small'])
            per_pick_cost = np.where(is_small,
                                     smallcap_cost_bps / 10000,
                                     cost_bps / 10000)
        else:
            per_pick_cost = np.full(len(rets), cost_bps / 10000)

        net_rets = rets.values - per_pick_cost

        # Inverse-volatility weighting (falls back to equal-weight if vol unavailable)
        if vol_weighted and 'vol_prior_12m' in picks_valid.columns:
            raw_vol = picks_valid['vol_prior_12m'].clip(0.05, 3.0)
            raw_vol = raw_vol.fillna(raw_vol.median() if raw_vol.notna().any() else 0.4)
            inv_vol = 1.0 / raw_vol.values
            weights = inv_vol / inv_vol.sum()
            # Cap at max position weight and renormalise
            weights = np.minimum(weights, MAX_POSITION_WEIGHT)
            weights = weights / weights.sum()
            # Cap sector concentration and renormalise
            weights = _apply_sector_cap(weights, picks_valid.reset_index(drop=True),
                                        MAX_SECTOR_WEIGHT)
        else:
            weights = np.ones(len(net_rets)) / len(net_rets)

        port_ret = float(np.dot(weights, net_rets))
        cost_drag = float(np.dot(weights, per_pick_cost))

        # ── Benchmark: primary depends on US vs non-US ──────────────────────
        # Non-US strategies: ACWI ex-US (primary) or equal-weight non-US universe
        # US strategies: SPY (primary) or equal-weight universe
        # SPY always tracked as informational for all strategies
        bench_df = yr_df.copy()
        if market:
            bench_df = bench_df[bench_df['market'] == market]
        if _do_impute:
            bench_df['forward_return_1y'] = bench_df['forward_return_1y'].fillna(_impute_val)
        bench_rets_s = bench_df['forward_return_1y'].dropna()
        universe_ret = bench_rets_s.mean() if len(bench_rets_s) > 5 else np.nan
        bench_coverage = len(bench_rets_s) / max(len(bench_df), 1)

        spy_ret = spy_returns.get(int(yr)) if spy_returns else None
        acwi_ret = acwi_exus_returns.get(int(yr)) if acwi_exus_returns else None

        if is_non_us:
            bench_ret = acwi_ret if acwi_ret is not None else universe_ret
        else:
            bench_ret = spy_ret if spy_ret is not None else universe_ret

        annual_rows.append({
            'year':            yr,
            'port_ret':        port_ret,
            'bench_ret':       bench_ret,
            'spy_ret':         spy_ret,
            'acwi_ret':        acwi_ret,
            'universe_ret':    universe_ret,
            'excess':          port_ret - bench_ret if pd.notna(bench_ret) else np.nan,
            'excess_vs_spy':   port_ret - spy_ret if spy_ret is not None else np.nan,
            'excess_vs_univ':  port_ret - universe_ret if pd.notna(universe_ret) else np.nan,
            'cost_drag':       cost_drag,
            'n_picks':         len(rets),
            'hit_rate':        (rets.values > 0).mean(),
            'n_missing_ret':   n_missing,
            'bench_coverage':  round(bench_coverage, 3),
            # Stored for monthly NAV reconstruction; not serialised to JSON
            '_picks_valid':    picks_valid if monthly_px is not None else None,
            '_weights':        weights.tolist() if monthly_px is not None else None,
        })

    if not annual_rows:
        return {'label': label, 'n_years': 0, 'error': 'insufficient data'}

    res = pd.DataFrame(annual_rows)
    n = len(res)

    # Cumulative wealth index
    wealth       = np.cumprod(1 + res['port_ret'].values)
    bench_wealth = np.cumprod(1 + res['bench_ret'].fillna(0).values)
    spy_vec      = res['spy_ret'].fillna(res['universe_ret'].fillna(0)).values
    spy_wealth   = np.cumprod(1 + spy_vec)

    # Max drawdown — use monthly NAV curve when price cache is available
    if monthly_px is not None:
        max_dd, dd_dur_months = compute_monthly_nav(annual_rows, monthly_px)
    else:
        # Fallback: annual-frequency drawdown (understates true intra-year drawdowns)
        peak      = np.maximum.accumulate(wealth)
        drawdowns = (wealth - peak) / peak
        max_dd    = float(drawdowns.min())
        in_dd = drawdowns < 0
        dd_dur_months = 0
        cur_dur = 0
        for in_d in in_dd:
            cur_dur = cur_dur + 12 if in_d else 0
            dd_dur_months = max(dd_dur_months, cur_dur)

    cagr       = float(wealth[-1] ** (1 / n) - 1)
    bench_cagr = float(bench_wealth[-1] ** (1 / n) - 1)
    spy_cagr   = float(spy_wealth[-1] ** (1 / n) - 1)
    vol        = float(res['port_ret'].std())
    sharpe     = float((cagr - RISK_FREE) / vol) if vol > 0 else np.nan

    # Sortino ratio — downside deviation below zero (annual frequency)
    # When all years are positive downside_vol = 0; fall back to Sharpe as an approximation.
    n_negative = int((res['port_ret'] < 0).sum())
    downside_vol = float(np.sqrt((res['port_ret'].clip(upper=0) ** 2).mean()))
    if downside_vol > 0:
        sortino = float((cagr - RISK_FREE) / downside_vol)
    elif vol > 0:
        sortino = float(sharpe)  # all years positive: sortino >= sharpe, use sharpe as lower bound
    else:
        sortino = np.nan

    # Calmar ratio — when MaxDD < 2% (e.g. all positive annual years), use 2σ as proxy.
    # Annual MaxDD ≈ 2σ is a common conservative approximation for annual-frequency data.
    effective_dd = abs(max_dd) if abs(max_dd) >= 0.02 else max(2 * vol, 0.01)
    calmar = float(cagr / effective_dd) if effective_dd > 0 else np.nan

    # Information ratio
    excess_std = float(res['excess'].std())
    info_ratio = float(res['excess'].mean() / excess_std) if excess_std > 0 else np.nan

    # Beta vs SPY (OLS regression of port returns on SPY returns)
    spy_aligned = res['spy_ret'].dropna()
    port_aligned = res.loc[spy_aligned.index, 'port_ret']
    if len(spy_aligned) >= 5:
        slope, intercept, r_val, _, _ = stats.linregress(spy_aligned, port_aligned)
        beta_vs_spy = round(float(slope), 3)
        alpha_vs_spy = round(float(intercept), 4)
        r_squared = round(float(r_val ** 2), 3)
    else:
        beta_vs_spy = alpha_vs_spy = r_squared = None

    # Tracking error vs SPY
    excess_vs_spy = res['excess_vs_spy'].dropna()
    tracking_error = round(float(excess_vs_spy.std()), 4) if len(excess_vs_spy) >= 3 else None

    # Annual turnover — approximate from n_picks and top_n
    avg_picks = float(res['n_picks'].mean())
    annual_turnover_pct = round(avg_picks / max(top_n, 1) * 100, 1)

    # VaR 95% (historical simulation, annual)
    rets_arr = res['port_ret'].values
    var_95 = round(float(np.percentile(rets_arr, 5)) * 100, 2)

    # CVaR 99% / Expected Shortfall — mean return in the worst 1% of annual outcomes
    _tail = rets_arr[rets_arr <= np.percentile(rets_arr, 1)]
    cvar_99 = round(float(_tail.mean()) * 100, 2) if len(_tail) > 0 else var_95

    # Rolling 3y Sharpe appended to each annual row
    rolling_sharpe_3y: list[float | None] = []
    for i in range(n):
        if i < 2:
            rolling_sharpe_3y.append(None)
        else:
            w = rets_arr[i - 2:i + 1]
            rs = (w.mean() - RISK_FREE / 3) / w.std() if w.std() > 0 else None
            rolling_sharpe_3y.append(round(float(rs), 3) if rs is not None else None)

    # Bootstrap CIs (block bootstrap, 2000 samples)
    boot = bootstrap_ci(rets_arr)

    return {
        'label':                label,
        'n_years':              n,
        'cagr_pct':             round(cagr * 100, 2),
        'bench_cagr_pct':       round(bench_cagr * 100, 2),
        'excess_cagr_pct':      round((cagr - bench_cagr) * 100, 2),
        'benchmark_source':     ('ACWI_exUS' if is_non_us and acwi_exus_returns
                                 else 'SPY' if spy_returns and not is_non_us
                                 else 'equal_weight_universe'),
        'spy_cagr_pct':         round(spy_cagr * 100, 2),
        'excess_cagr_vs_spy':   round((cagr - spy_cagr) * 100, 2),
        'beta_vs_spy':          beta_vs_spy,
        'alpha_vs_spy':         alpha_vs_spy,
        'r_squared_vs_spy':     r_squared,
        'tracking_error':       tracking_error,
        'annual_turnover_pct':  annual_turnover_pct,
        'var_95_pct':           var_95,
        'cvar_99_pct':          cvar_99,
        'max_drawdown_pct':     round(max_dd * 100, 2),
        'max_drawdown_duration_months': dd_dur_months,
        'sharpe':               round(sharpe, 3) if pd.notna(sharpe) else None,
        'sortino':              round(sortino, 3) if pd.notna(sortino) else None,
        'calmar':               round(calmar, 3) if pd.notna(calmar) else None,
        'info_ratio':           round(info_ratio, 3) if pd.notna(info_ratio) else None,
        'hit_rate_pct':         round(res['hit_rate'].mean() * 100, 1),
        'avg_cost_drag_bps':    round(res['cost_drag'].mean() * 10000, 1),
        'best_year_pct':        round(res['port_ret'].max() * 100, 2),
        'worst_year_pct':       round(res['port_ret'].min() * 100, 2),
        'survivorship_mode':    survivorship_mode,
        'survivorship_pct':     round(total_survivorship_dropped / max(total_picks_attempted, 1) * 100, 1),
        'cagr_bootstrap_mean_pct':   boot.get('cagr_bootstrap_mean_pct'),
        'cagr_bootstrap_1sigma_pct': boot.get('cagr_bootstrap_1sigma_pct'),
        'sharpe_bootstrap_mean':     boot.get('sharpe_bootstrap_mean'),
        'sharpe_bootstrap_1sigma':   boot.get('sharpe_bootstrap_1sigma'),
        'annual_returns': [
            {
                'year':            int(r['year']),
                'port_pct':        round(r['port_ret'] * 100, 2),
                'bench_pct':       round(r['bench_ret'] * 100, 2) if pd.notna(r['bench_ret']) else None,
                'spy_pct':         round(r['spy_ret'] * 100, 2) if pd.notna(r.get('spy_ret', np.nan)) else None,
                'excess_pct':      round(r['excess'] * 100, 2) if pd.notna(r['excess']) else None,
                'excess_vs_spy':   round(r['excess_vs_spy'] * 100, 2) if pd.notna(r.get('excess_vs_spy', np.nan)) else None,
                'n_picks':         int(r['n_picks']),
                'n_missing_ret':   int(r['n_missing_ret']),
                'bench_coverage':  r['bench_coverage'],
                'rolling_sharpe':  rolling_sharpe_3y[i],
            }
            for i, (_, r) in enumerate(res.iterrows())
        ],
    }


# ── Tearsheet printer ─────────────────────────────────────────────────────────

def bootstrap_ci(annual_returns: np.ndarray, n_boot: int = 2000,
                 risk_free: float = RISK_FREE) -> dict:
    """Resample annual returns to produce 1σ confidence intervals for CAGR and Sharpe.

    Returns dict with keys: cagr_mean, cagr_1sigma, sharpe_mean, sharpe_1sigma.
    Uses block bootstrap (block_size=3y) to preserve mild autocorrelation.
    """
    n = len(annual_returns)
    if n < 4:
        return {}

    rng = np.random.default_rng(42)
    block_size = min(3, n // 3)
    cagrs, sharpes = [], []

    for _ in range(n_boot):
        # Block bootstrap: sample start indices, then take consecutive blocks
        n_blocks = int(np.ceil(n / block_size))
        starts = rng.integers(0, n - block_size + 1, size=n_blocks)
        sample = np.concatenate([annual_returns[s:s + block_size] for s in starts])[:n]
        c = float(np.prod(1 + sample) ** (1 / n) - 1)
        v = float(sample.std())
        s = float((c - risk_free) / v) if v > 0 else np.nan
        cagrs.append(c)
        sharpes.append(s)

    cagrs_arr = np.array(cagrs)
    sharpes_arr = np.array([s for s in sharpes if not np.isnan(s)])

    return {
        'cagr_bootstrap_mean_pct':   round(float(np.mean(cagrs_arr)) * 100, 2),
        'cagr_bootstrap_1sigma_pct': round(float(np.std(cagrs_arr)) * 100, 2),
        'sharpe_bootstrap_mean':     round(float(np.mean(sharpes_arr)), 3) if len(sharpes_arr) else None,
        'sharpe_bootstrap_1sigma':   round(float(np.std(sharpes_arr)), 3) if len(sharpes_arr) else None,
    }


def print_tearsheet(result: dict) -> None:
    if result.get('n_years', 0) == 0:
        print(f"  {result['label']}: {result.get('error', 'no data')}")
        return
    sep = '─' * 68
    bench_src = result.get('benchmark_source', 'unknown')
    spy_cagr  = result.get('spy_cagr_pct')
    exc_spy   = result.get('excess_cagr_vs_spy')
    print(f'\n{sep}')
    print(f'  {result["label"]}')
    print(sep)
    print(f'  Period:          {result["n_years"]} years  |  benchmark: {bench_src}')
    print(f'  CAGR:            {result["cagr_pct"]:+.1f}%  '
          f'(bench {result["bench_cagr_pct"]:+.1f}%  |  excess {result["excess_cagr_pct"]:+.1f}%)')
    if spy_cagr is not None:
        print(f'  vs SPY:          SPY {spy_cagr:+.1f}%  |  excess vs SPY {exc_spy:+.1f}%')
    beta = result.get('beta_vs_spy')
    alpha = result.get('alpha_vs_spy')
    r2    = result.get('r_squared_vs_spy')
    te    = result.get('tracking_error')
    if beta is not None:
        print(f'  Factor Attr:     beta={beta:.2f}  alpha={alpha:.4f}  R²={r2:.2f}  '
              f'tracking_err={te:.3f}' if te else
              f'  Factor Attr:     beta={beta:.2f}  alpha={alpha:.4f}  R²={r2:.2f}')
    print(f'  Sharpe:          {result["sharpe"]}')
    boot_s = result.get('sharpe_bootstrap_mean')
    boot_s1 = result.get('sharpe_bootstrap_1sigma')
    if boot_s is not None:
        print(f'  Sharpe CI 1σ:    {boot_s:.3f} ± {boot_s1:.3f}  '
              f'[{boot_s - boot_s1:.3f}, {boot_s + boot_s1:.3f}]')
    boot_c = result.get('cagr_bootstrap_mean_pct')
    boot_c1 = result.get('cagr_bootstrap_1sigma_pct')
    if boot_c is not None:
        print(f'  CAGR CI 1σ:      {boot_c:+.1f}% ± {boot_c1:.1f}%  '
              f'[{boot_c - boot_c1:+.1f}%, {boot_c + boot_c1:+.1f}%]')
    print(f'  Sortino:         {result["sortino"]}')
    print(f'  Calmar:          {result["calmar"]}')
    print(f'  Info Ratio:      {result["info_ratio"]}')
    print(f'  Max Drawdown:    {result["max_drawdown_pct"]:.1f}%  '
          f'(duration {result.get("max_drawdown_duration_months", 0)} months)')
    print(f'  VaR 95%:         {result.get("var_95_pct", "N/A")}%  (annual)')
    print(f'  CVaR 99%:        {result.get("cvar_99_pct", "N/A")}%  (expected shortfall)')
    print(f'  Turnover:        ~{result.get("annual_turnover_pct", "N/A")}% annual')
    print(f'  Hit Rate:        {result["hit_rate_pct"]:.0f}%')
    print(f'  Avg Cost Drag:   {result["avg_cost_drag_bps"]:.0f} bps')
    print(f'  Survivorship:    {result.get("survivorship_pct", 0):.1f}% picks had missing return')
    print(f'  Best / Worst:    {result["best_year_pct"]:+.1f}% / {result["worst_year_pct"]:+.1f}%')
    print(f'\n  Year  Port%   SPY%   Exc-SPY%  Picks  Missing  Roll3ySharpe')
    for row in result['annual_returns']:
        spy_p = f'{row["spy_pct"]:+.1f}' if row.get('spy_pct') is not None else '  N/A '
        exc_p = f'{row.get("excess_vs_spy"):+.1f}' if row.get('excess_vs_spy') is not None else '  N/A '
        rs    = f'{row["rolling_sharpe"]:.2f}' if row['rolling_sharpe'] is not None else '  — '
        mis   = row.get('n_missing_ret', 0)
        print(f'  {row["year"]}  {row["port_pct"]:+5.1f}  {spy_p:>6}  {exc_p:>8}   {row["n_picks"]:3d}      {mis:3d}    {rs}')
    print(sep)


# ── CLI entry ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Walk-forward backtester')
    parser.add_argument('--strategy', default='all',
                        choices=['all', 'composite', 'ml_gates', 'qem', 'scdv', 'iarb'])
    parser.add_argument('--market',   default=None,  help='Filter to one market (e.g. US)')
    parser.add_argument('--top',      default=15,    type=int, help='Top N picks per year (default 15 for balanced config)')
    parser.add_argument('--cost',     default=DEFAULT_COST_BPS, type=int,
                        help=f'Round-trip cost in bps (default {DEFAULT_COST_BPS})')
    parser.add_argument('--smallcap_cost', default=SMALLCAP_COST_BPS, type=int,
                        help=f'Round-trip cost for micro/small caps in bps (default {SMALLCAP_COST_BPS})')
    parser.add_argument('--min-cap', default=MIN_MARKET_CAP, type=int,
                        help=f'Min market cap filter in USD (default {MIN_MARKET_CAP:,}, 0 to disable)')
    parser.add_argument('--max-cap', default=MAX_MARKET_CAP, type=int,
                        help=f'Max market cap filter in USD (default {MAX_MARKET_CAP} = no ceiling)')
    parser.add_argument('--equal-weight', action='store_true',
                        help='Use equal-weight instead of inverse-volatility weighting')
    parser.add_argument('--fill-missing', type=float, default=None, metavar='RETURN',
                        help='Override survivorship imputation value '
                             '(e.g. -0.5 for worst-case survivorship bias)')
    parser.add_argument('--survivorship-mode', choices=['impute', 'drop', 'flag_only'],
                        default='impute',
                        help='How to handle missing forward returns: impute -50%% (default), '
                             'drop (optimistic), or flag_only (drop + log)')
    parser.add_argument('--max-filing-lag', type=int, default=MAX_FILING_LAG_MONTHS,
                        help=f'Max months between fiscal year-end and filed_date '
                             f'(look-ahead filter, default {MAX_FILING_LAG_MONTHS})')
    parser.add_argument('--no-filing-gate', action='store_true',
                        help='Disable filing-date gate (allow stocks not yet filed into portfolio)')
    parser.add_argument('--tearsheet', action='store_true',
                        help='Print detailed tearsheet for each strategy')
    parser.add_argument('--no-adtv', action='store_true',
                        help='Disable ADTV liquidity filter (use if monthly_prices.parquet not built)')
    parser.add_argument('--aum-target', type=float, default=200_000,
                        help='Portfolio AUM in dollars for ADTV filter (default $200K retail)')
    args = parser.parse_args()

    print('Loading + scoring full historical data...')
    df = load_full_hist()
    df = load_and_score(df)
    print(f'  {len(df):,} annual rows across {df["fiscal_year"].nunique()} years')

    # Load SPY benchmark data
    spy_returns = load_spy_returns()
    if spy_returns:
        print(f'  SPY benchmark loaded: {min(spy_returns)} – {max(spy_returns)} '
              f'({len(spy_returns)} years, mean {sum(spy_returns.values())/len(spy_returns):+.1%})')
    else:
        print('  SPY data not found (data/spy_returns.csv) — using equal-weight universe mean. '
              'Run python3 -m data_io.fetch_spy_returns to get SPY data.')

    # Load ACWI ex-US benchmark for non-US strategies
    acwi_exus_returns = load_acwi_exus_returns()
    if acwi_exus_returns:
        print(f'  ACWI ex-US benchmark loaded: {min(acwi_exus_returns)} – {max(acwi_exus_returns)} '
              f'({len(acwi_exus_returns)} years, mean {sum(acwi_exus_returns.values())/len(acwi_exus_returns):+.1%})')
    else:
        print('  ACWI ex-US data not found — non-US strategies will use equal-weight universe.')

    # Load monthly price cache for true MaxDD and ADTV filter
    monthly_px = load_monthly_prices()
    if monthly_px is not None:
        print(f'  Monthly price cache loaded: {len(monthly_px):,} rows '
              f'({monthly_px["ticker"].nunique()} tickers, '
              f'{monthly_px["date"].min().date()} – {monthly_px["date"].max().date()})')
    else:
        print('  Monthly price cache not found (data/monthly_prices.parquet) — '
              'MaxDD will use annual approximation, ADTV filter disabled. '
              'Run python3 -m pipeline.build_monthly_price_cache to build it.')

    to_run = list(STRATEGIES.keys()) if args.strategy == 'all' else [args.strategy]
    results = {}

    for key in to_run:
        fn = STRATEGIES[key]
        mkt_label = args.market or 'all'
        label = f'{key.upper()} | {mkt_label} | top{args.top} | {args.cost}bps'
        # iarb is non-US by definition; also non-US if market is explicitly non-US
        strategy_is_non_us = (key == 'iarb') or (args.market and args.market.upper() != 'US')
        print(f'  Backtesting {label}...', end=' ', flush=True)
        result = run_backtest(df, fn, label, args.top, args.market,
                              args.cost, args.smallcap_cost,
                              min_market_cap=args.min_cap,
                              max_market_cap=args.max_cap,
                              vol_weighted=not args.equal_weight,
                              fill_missing_return=args.fill_missing,
                              survivorship_mode=args.survivorship_mode,
                              max_filing_lag_months=args.max_filing_lag,
                              filing_date_gate=not args.no_filing_gate,
                              spy_returns=spy_returns,
                              acwi_exus_returns=acwi_exus_returns,
                              is_non_us=strategy_is_non_us,
                              monthly_px=monthly_px,
                              use_adtv_filter=not args.no_adtv,
                              aum_target=args.aum_target)
        results[key] = result

        if result.get('n_years', 0) > 0:
            bench_str = f'SPY={result.get("spy_cagr_pct", "N/A"):+.1f}%' if spy_returns else f'bench={result["bench_cagr_pct"]:+.1f}%'
            print(
                f'CAGR={result["cagr_pct"]:+.1f}%  '
                f'{bench_str}  '
                f'excess_vs_SPY={result.get("excess_cagr_vs_spy", "N/A"):+.1f}%  '
                f'beta={result.get("beta_vs_spy", "N/A")}  '
                f'Sharpe={result.get("sharpe","N/A")}  '
                f'MaxDD={result["max_drawdown_pct"]:.1f}%'
            )
        else:
            print(result.get('error', 'no data'))

    out = {
        'generated_at':      pd.Timestamp.now().isoformat(),
        'cost_bps':          args.cost,
        'top_n':             args.top,
        'market':            args.market,
        'min_market_cap':    args.min_cap,
        'max_market_cap':    args.max_cap,
        'vol_weighted':      not args.equal_weight,
        'fill_missing':      args.fill_missing,
        'survivorship_mode': args.survivorship_mode,
        'max_filing_lag':    args.max_filing_lag,
        'filing_date_gate':  not args.no_filing_gate,
        'adtv_filter':       not args.no_adtv and monthly_px is not None,
        'aum_target':        args.aum_target,
        'monthly_nav_maxdd': monthly_px is not None,
        'strategies':        results,
    }
    OUT_PATH.write_text(json.dumps(out, indent=2, default=str))
    print(f'\nSaved: {OUT_PATH}')


if __name__ == '__main__':
    main()
