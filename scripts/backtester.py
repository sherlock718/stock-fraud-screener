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

BASE       = Path(__file__).parent.parent
FULL_DATA  = BASE / 'data' / 'historical_dataset_clean.parquet'
MODELS_DIR = BASE / 'models'
OUT_PATH   = BASE / 'data' / 'backtest_results.json'

DEFAULT_COST_BPS  = 30    # 30 bps round-trip (commission 10bps + slippage 20bps)
SMALLCAP_COST_BPS = 60    # Illiquidity premium for micro/small caps
RISK_FREE = 0.03          # Annual risk-free rate for Sharpe/Sortino
MIN_MARKET_CAP    = 50_000_000  # $50M floor — removes truly illiquid stocks
MAX_POSITION_WEIGHT = 0.20      # Max weight per stock in vol-scaled portfolio
MAX_SECTOR_WEIGHT   = 0.35      # Max total weight in any single SIC sector


# ── Data helpers ──────────────────────────────────────────────────────────────

def _add_normalised_ratios(df: pd.DataFrame) -> pd.DataFrame:
    ta  = df.get('total_assets')
    pti = df.get('pretax_income')
    if ta is not None:
        ta_safe = ta.replace(0, np.nan)
        for src, dst in [
            ('intangibles',         'intangibles_to_assets'),
            ('goodwill',            'goodwill_to_assets'),
            ('depreciation',        'depreciation_to_assets'),
            ('financing_cash_flow', 'financing_cashflow_to_assets'),
            ('fcf',                 'fcf_to_assets'),
        ]:
            if src in df.columns and dst not in df.columns:
                df[dst] = df[src] / ta_safe
    if 'tax_expense' in df.columns and pti is not None and 'effective_tax_rate' not in df.columns:
        pos = pti > 0
        df['effective_tax_rate'] = np.nan
        df.loc[pos, 'effective_tax_rate'] = df.loc[pos, 'tax_expense'] / pti[pos]
    return df


def _add_piotroski_ext(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(['ticker', 'fiscal_year'])
    for src, name in [
        ('shares_outstanding', 'piotroski_shares_ok'),
        ('gross_margin',       'piotroski_delta_gm'),
        ('asset_turnover',     'piotroski_delta_at'),
    ]:
        if src in df.columns:
            df[name] = df.groupby('ticker')[src].transform(
                lambda x: (x <= x.shift(1)).astype(float) if src == 'shares_outstanding'
                else (x > x.shift(1)).astype(float)
            )
    extra = [c for c in ['piotroski_shares_ok', 'piotroski_delta_gm', 'piotroski_delta_at']
             if c in df.columns]
    if extra and 'piotroski_f_score' in df.columns:
        df['piotroski_f_score_9'] = (
            df['piotroski_f_score'].astype('float64') + df[extra].sum(axis=1, min_count=1)
        )
    return df


def load_full_hist() -> pd.DataFrame:
    df = pd.read_parquet(FULL_DATA)
    df = df[df['period_type'] == 'annual'].copy()
    df = _add_piotroski_ext(df)
    df = _add_normalised_ratios(df)
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

    for h, (ret_col, beat_col) in HORIZONS_WF.items():
        if beat_col not in df.columns or ret_col not in df.columns:
            continue

        scores = np.full(len(df), np.nan)
        print(f'    WF-ML {h}: training year by year...', flush=True)

        for i, score_yr in enumerate(years):
            train_df = df[(df['fiscal_year'] < score_yr) & df[beat_col].notna()].copy()
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
                 max_filing_lag_months: int = MAX_FILING_LAG_MONTHS) -> dict:
    """Walk-forward backtest engine.

    Args:
        fill_missing_return: If set, impute this return for picked stocks with
            NaN forward_return_1y instead of dropping them. Use -0.5 to
            model worst-case survivorship (missing = delisted).
        max_filing_lag_months: Drop filings received more than N months after
            fiscal year-end (look-ahead protection). Default 18.
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

        # Per-pick cost aligned to valid picks
        if 'size_category_label' in picks_valid.columns:
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

        # ── Benchmark: equal-weight all eligible stocks in same market/year ──
        bench_df = yr_df.copy()
        if market:
            bench_df = bench_df[bench_df['market'] == market]
        if fill_missing_return is not None:
            bench_df['forward_return_1y'] = bench_df['forward_return_1y'].fillna(fill_missing_return)
        bench_rets_s = bench_df['forward_return_1y'].dropna()
        bench_ret = bench_rets_s.mean() if len(bench_rets_s) > 5 else np.nan
        bench_coverage = len(bench_rets_s) / max(len(bench_df), 1)

        annual_rows.append({
            'year':            yr,
            'port_ret':        port_ret,
            'bench_ret':       bench_ret,
            'excess':          port_ret - bench_ret if pd.notna(bench_ret) else np.nan,
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

    # Max drawdown
    peak      = np.maximum.accumulate(wealth)
    drawdowns = (wealth - peak) / peak
    max_dd    = float(drawdowns.min())

    cagr       = float(wealth[-1] ** (1 / n) - 1)
    bench_cagr = float(bench_wealth[-1] ** (1 / n) - 1)
    vol        = float(res['port_ret'].std())
    sharpe     = float((cagr - RISK_FREE) / vol) if vol > 0 else np.nan

    # Sortino ratio — require ≥3 negative years for a reliable downside estimate
    n_negative = int((res['port_ret'] < 0).sum())
    downside_vol = float(np.sqrt((res['port_ret'].clip(upper=0) ** 2).mean()))
    sortino = float((cagr - RISK_FREE) / downside_vol) if (downside_vol > 0 and n_negative >= 3) else np.nan

    # Calmar ratio — require MaxDD ≥ 2% for a meaningful estimate
    calmar = float(cagr / abs(max_dd)) if abs(max_dd) >= 0.02 else np.nan

    # Information ratio
    excess_std = float(res['excess'].std())
    info_ratio = float(res['excess'].mean() / excess_std) if excess_std > 0 else np.nan

    # Rolling 3y Sharpe appended to each annual row
    rets_arr = res['port_ret'].values
    rolling_sharpe_3y: list[float | None] = []
    for i in range(n):
        if i < 2:
            rolling_sharpe_3y.append(None)
        else:
            w = rets_arr[i - 2:i + 1]
            rs = (w.mean() - RISK_FREE / 3) / w.std() if w.std() > 0 else None
            rolling_sharpe_3y.append(round(float(rs), 3) if rs is not None else None)

    return {
        'label':             label,
        'n_years':           n,
        'cagr_pct':          round(cagr * 100, 2),
        'bench_cagr_pct':    round(bench_cagr * 100, 2),
        'excess_cagr_pct':   round((cagr - bench_cagr) * 100, 2),
        'sharpe':            round(sharpe, 3) if pd.notna(sharpe) else None,
        'sortino':           round(sortino, 3) if pd.notna(sortino) else None,
        'calmar':            round(calmar, 3) if pd.notna(calmar) else None,
        'info_ratio':        round(info_ratio, 3) if pd.notna(info_ratio) else None,
        'max_drawdown_pct':  round(max_dd * 100, 2),
        'hit_rate_pct':      round(res['hit_rate'].mean() * 100, 1),
        'avg_cost_drag_bps': round(res['cost_drag'].mean() * 10000, 1),
        'best_year_pct':     round(res['port_ret'].max() * 100, 2),
        'worst_year_pct':      round(res['port_ret'].min() * 100, 2),
        'survivorship_pct':    round(total_survivorship_dropped / max(total_picks_attempted, 1) * 100, 1),
        'annual_returns': [
            {
                'year':            int(r['year']),
                'port_pct':        round(r['port_ret'] * 100, 2),
                'bench_pct':       round(r['bench_ret'] * 100, 2) if pd.notna(r['bench_ret']) else None,
                'excess_pct':      round(r['excess'] * 100, 2) if pd.notna(r['excess']) else None,
                'n_picks':         int(r['n_picks']),
                'n_missing_ret':   int(r['n_missing_ret']),
                'bench_coverage':  r['bench_coverage'],
                'rolling_sharpe':  rolling_sharpe_3y[i],
            }
            for i, (_, r) in enumerate(res.iterrows())
        ],
    }


# ── Tearsheet printer ─────────────────────────────────────────────────────────

def print_tearsheet(result: dict) -> None:
    if result.get('n_years', 0) == 0:
        print(f"  {result['label']}: {result.get('error', 'no data')}")
        return
    sep = '─' * 60
    print(f'\n{sep}')
    print(f'  {result["label"]}')
    print(sep)
    print(f'  Period:        {result["n_years"]} years')
    print(f'  CAGR:          {result["cagr_pct"]:+.1f}%  (bench {result["bench_cagr_pct"]:+.1f}%  |  excess {result["excess_cagr_pct"]:+.1f}%)')
    print(f'  Sharpe:        {result["sharpe"]}')
    print(f'  Sortino:       {result["sortino"]}')
    print(f'  Calmar:        {result["calmar"]}')
    print(f'  Info Ratio:    {result["info_ratio"]}')
    print(f'  Max Drawdown:  {result["max_drawdown_pct"]:.1f}%')
    print(f'  Hit Rate:      {result["hit_rate_pct"]:.0f}%')
    print(f'  Avg Cost Drag: {result["avg_cost_drag_bps"]:.0f} bps')
    print(f'  Survivorship:  {result.get("survivorship_pct", 0):.1f}% picks had missing return')
    print(f'  Best Year:     {result["best_year_pct"]:+.1f}%')
    print(f'  Worst Year:    {result["worst_year_pct"]:+.1f}%')
    print(f'\n  Year    Port%   Bench%  Excess%  Picks  Missing  BenchCov  Roll3ySharpe')
    for row in result['annual_returns']:
        bp  = f'{row["bench_pct"]:+.1f}' if row['bench_pct'] is not None else '  N/A '
        ep  = f'{row["excess_pct"]:+.1f}' if row['excess_pct'] is not None else '  N/A '
        rs  = f'{row["rolling_sharpe"]:.2f}' if row['rolling_sharpe'] is not None else '  — '
        mis = row.get('n_missing_ret', 0)
        cov = f'{row.get("bench_coverage", 0):.0%}'
        print(f'  {row["year"]}   {row["port_pct"]:+5.1f}   {bp:>6}  {ep:>7}   {row["n_picks"]:3d}      {mis:3d}    {cov:>7}    {rs}')
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
                              max_filing_lag_months=args.max_filing_lag)
        results[key] = result

        if result.get('n_years', 0) > 0:
            print(
                f'CAGR={result["cagr_pct"]:+.1f}%  '
                f'bench={result["bench_cagr_pct"]:+.1f}%  '
                f'excess={result["excess_cagr_pct"]:+.1f}%  '
                f'Sharpe={result.get("sharpe","N/A")}  '
                f'Sortino={result.get("sortino","N/A")}  '
                f'Calmar={result.get("calmar","N/A")}  '
                f'MaxDD={result["max_drawdown_pct"]:.1f}%'
            )
        else:
            print(result.get('error', 'no data'))

        if args.tearsheet:
            print_tearsheet(result)

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
