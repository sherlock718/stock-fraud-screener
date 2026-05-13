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

Saves results to data/backtest_results.json (consumed by app_v2.py).
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
sys.path.insert(0, str(Path(__file__).parent.parent))
from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext

BASE       = Path(__file__).parent.parent
FULL_DATA  = BASE / 'data' / 'historical_dataset_clean.parquet'
MODELS_DIR = BASE / 'models'
OUT_PATH   = BASE / 'data' / 'backtest_results.json'
SPY_PATH   = BASE / 'data' / 'spy_returns.csv'

DEFAULT_COST_BPS  = 30    # 30 bps round-trip (commission 10bps + slippage 20bps)
SMALLCAP_COST_BPS = 60    # Illiquidity premium for micro/small caps
RISK_FREE = 0.03          # Annual risk-free rate for Sharpe/Sortino
MIN_MARKET_CAP    = 50_000_000  # $50M floor — removes truly illiquid stocks

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


# ── Data helpers ──────────────────────────────────────────────────────────────

def load_full_hist() -> pd.DataFrame:
    df = pd.read_parquet(FULL_DATA)
    df = df[df['period_type'] == 'annual'].copy()
    df = add_piotroski_ext(df)
    df = add_normalised_ratios(df)
    return df.reset_index(drop=True)


EXCLUDE_COLS = {
    'cik', 'ticker', 'name', 'filed_date', 'fiscal_year', 'fiscal_quarter',
    'period_type', 'exchange', 'sic_code', 'sic_description', 'market',
    'country', 'accounting_std', 'size_category_label', 'corp_code', 'acc_mt',
    'revenue', 'net_income', 'gross_profit', 'operating_income', 'pretax_income',
    'cogs', 'sga_expense', 'rd_expense', 'depreciation', 'da_expense',
    'operating_cash_flow', 'financing_cash_flow', 'investing_cash_flow',
    'capex', 'fcf', 'long_term_debt', 'short_term_debt', 'total_debt',
    'total_assets', 'total_equity', 'current_assets', 'current_liabilities',
    'accounts_receivable', 'accounts_payable', 'receivables',
    'cash', 'intangibles', 'goodwill', 'ppe_net', 'noa',
    'market_cap_at_filing', 'tax_expense', 'interest_expense',
    'common_shares_outstanding', 'eps_diluted', 'eps_basic',
    'retained_earnings', 'additional_paid_in_capital', 'inventory',
}
EXCLUDE_PATTERNS = ['forward_return', 'beat_local_market', 'excess_return_local', 'benchmark_return']


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


def filter_composite(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    s = yr_df.copy()
    if market:
        s = s[s['market'] == market]
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
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < -1.78]
    if 'likely_delisted' in s.columns:
        s = s[s['likely_delisted'].fillna(1) == 0]
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
                 vol_weighted: bool = True,
                 fill_missing_return: float | None = None,
                 max_filing_lag_months: int = MAX_FILING_LAG_MONTHS,
                 spy_returns: dict | None = None) -> dict:
    """Walk-forward backtest engine.

    Args:
        fill_missing_return: If set, impute this return for picked stocks with
            NaN forward_return_1y instead of dropping them. Use -0.5 to
            model worst-case survivorship (missing = delisted).
        max_filing_lag_months: Drop filings received more than N months after
            fiscal year-end (look-ahead protection). Default 18.
        spy_returns: Dict of {year: spy_annual_return} for SPY benchmark.
            If None, falls back to equal-weight universe mean as benchmark.
    """
    years = sorted(y for y in df['fiscal_year'].unique() if y <= 2023)
    annual_rows = []
    total_survivorship_dropped = 0
    total_picks_attempted = 0

    for yr in years:
        yr_df = df[df['fiscal_year'] == yr].copy()

        # ── Look-ahead protection: drop implausibly late filings ──────────────
        yr_df = _apply_filing_lag_filter(yr_df, yr, max_filing_lag_months)

        # Liquidity pre-filter: remove stocks below market cap threshold
        if min_market_cap > 0 and 'market_cap_at_filing' in yr_df.columns:
            yr_df = yr_df[yr_df['market_cap_at_filing'].fillna(0) >= min_market_cap]
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

        if fill_missing_return is not None and n_missing > 0:
            picks = picks.copy()
            picks.loc[missing_mask, 'forward_return_1y'] = fill_missing_return
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

        # ── Benchmark: SPY (primary) + equal-weight universe (secondary) ──
        # SPY is used for excess_cagr_pct; universe mean kept as bench_universe_pct
        bench_df = yr_df.copy()
        if market:
            bench_df = bench_df[bench_df['market'] == market]
        if fill_missing_return is not None:
            bench_df['forward_return_1y'] = bench_df['forward_return_1y'].fillna(fill_missing_return)
        bench_rets_s = bench_df['forward_return_1y'].dropna()
        universe_ret = bench_rets_s.mean() if len(bench_rets_s) > 5 else np.nan
        bench_coverage = len(bench_rets_s) / max(len(bench_df), 1)

        # Primary benchmark: SPY for this calendar year
        spy_ret = spy_returns.get(int(yr)) if spy_returns else None
        bench_ret = spy_ret if spy_ret is not None else universe_ret

        annual_rows.append({
            'year':            yr,
            'port_ret':        port_ret,
            'bench_ret':       bench_ret,
            'spy_ret':         spy_ret,
            'universe_ret':    universe_ret,
            'excess':          port_ret - bench_ret if pd.notna(bench_ret) else np.nan,
            'excess_vs_spy':   port_ret - spy_ret if spy_ret is not None else np.nan,
            'excess_vs_univ':  port_ret - universe_ret if pd.notna(universe_ret) else np.nan,
            'cost_drag':       cost_drag,
            'n_picks':         len(rets),
            'hit_rate':        (rets.values > 0).mean(),
            'n_missing_ret':   n_missing,
            'bench_coverage':  round(bench_coverage, 3),
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

    # Max drawdown
    peak      = np.maximum.accumulate(wealth)
    drawdowns = (wealth - peak) / peak
    max_dd    = float(drawdowns.min())

    # Drawdown duration
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
        'benchmark_source':     'SPY' if spy_returns else 'equal_weight_universe',
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
                        choices=['all', 'composite', 'qem', 'scdv', 'iarb'])
    parser.add_argument('--market',   default=None,  help='Filter to one market (e.g. US)')
    parser.add_argument('--top',      default=20,    type=int, help='Top N picks per year')
    parser.add_argument('--cost',     default=DEFAULT_COST_BPS, type=int,
                        help=f'Round-trip cost in bps (default {DEFAULT_COST_BPS})')
    parser.add_argument('--smallcap_cost', default=SMALLCAP_COST_BPS, type=int,
                        help=f'Round-trip cost for micro/small caps in bps (default {SMALLCAP_COST_BPS})')
    parser.add_argument('--min-cap', default=MIN_MARKET_CAP, type=int,
                        help=f'Min market cap filter in USD (default {MIN_MARKET_CAP:,}, 0 to disable)')
    parser.add_argument('--equal-weight', action='store_true',
                        help='Use equal-weight instead of inverse-volatility weighting')
    parser.add_argument('--fill-missing', type=float, default=None, metavar='RETURN',
                        help='Impute this return for picks with missing forward_return_1y '
                             '(e.g. -0.5 for worst-case survivorship bias)')
    parser.add_argument('--max-filing-lag', type=int, default=MAX_FILING_LAG_MONTHS,
                        help=f'Max months between fiscal year-end and filed_date '
                             f'(look-ahead filter, default {MAX_FILING_LAG_MONTHS})')
    parser.add_argument('--tearsheet', action='store_true',
                        help='Print detailed tearsheet for each strategy')
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
              'Run scripts/fetch_spy_returns.py to get SPY data.')

    to_run = list(STRATEGIES.keys()) if args.strategy == 'all' else [args.strategy]
    results = {}

    for key in to_run:
        fn = STRATEGIES[key]
        mkt_label = args.market or 'all'
        label = f'{key.upper()} | {mkt_label} | top{args.top} | {args.cost}bps'
        print(f'  Backtesting {label}...', end=' ', flush=True)
        result = run_backtest(df, fn, label, args.top, args.market,
                              args.cost, args.smallcap_cost,
                              min_market_cap=args.min_cap,
                              vol_weighted=not args.equal_weight,
                              fill_missing_return=args.fill_missing,
                              max_filing_lag_months=args.max_filing_lag,
                              spy_returns=spy_returns)
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
        'vol_weighted':      not args.equal_weight,
        'fill_missing':      args.fill_missing,
        'max_filing_lag':    args.max_filing_lag,
        'strategies':        results,
    }
    OUT_PATH.write_text(json.dumps(out, indent=2, default=str))
    print(f'\nSaved: {OUT_PATH}')


if __name__ == '__main__':
    main()
