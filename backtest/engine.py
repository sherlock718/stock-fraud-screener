"""
Walk-forward backtester with transaction costs and slippage.

Improvements over v1:
  - Manifest-backed OOS-only historical score consumption
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

import numpy as np
import pandas as pd
from pathlib import Path

import sys
from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext
from _root import ROOT
from modeling.constants import (
    BENEISH_THRESHOLD, TREE_THRESHOLD, PIOTROSKI_MIN,
    VALUE_GATE_PCT, ALTMAN_Z_MIN, MOMENTUM_12M_MIN,
)
from modeling.label_eligibility import LABEL_POLICIES, OBSERVED_ONLY
from modeling.prediction_lineage import (
    ScoreRequirement,
    complete_top_n,
    validate_historical_scores,
)
from backtest.monthly_nav import (
    OBSERVED_ONLY as OBSERVED_RETURN_ONLY,
    RETURN_POLICIES,
    annual_returns_from_nav,
    build_monthly_nav,
    compute_nav_metrics,
)

BASE = ROOT

FULL_DATA         = BASE / 'data' / 'historical_dataset_clean.parquet'
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
    """Compatibility wrapper around the fail-closed canonical NAV builder."""
    result = build_monthly_nav(annual_rows, monthly_px)
    if not result["available"]:
        raise ValueError(result["exclusions"])
    metrics = compute_nav_metrics(result["nav"])
    return metrics["max_drawdown"], metrics["max_drawdown_duration_months"]


def adtv_filter(yr_df: pd.DataFrame, monthly_px: pd.DataFrame | None,
                yr: int, max_pct_adtv: float = 0.01,
                aum_target: float = 200_000,
                target_n: int = 15) -> pd.DataFrame:
    """Remove picks whose position would exceed max_pct_adtv of trailing 30d median ADTV.

    Args:
        max_pct_adtv: Max fraction of ADTV a single position may represent.
            Default 1% — retail-friendly constraint.
        aum_target: Portfolio AUM in dollars. Default $200K (retail).
        target_n: Equal-weight portfolio target size. Planned position is
            ``aum_target / target_n`` and minimum ADTV is that position divided
            by ``max_pct_adtv``.
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

    if max_pct_adtv <= 0 or target_n <= 0 or aum_target < 0:
        raise ValueError(
            "ADTV inputs require max_pct_adtv > 0, target_n > 0, and aum_target >= 0"
        )

    merged = yr_df.merge(sub, on='ticker', how='left')
    # Position can't exceed max_pct_adtv of daily volume
    min_adtv = (aum_target / target_n) / max_pct_adtv
    keep = merged['adtv_est'].notna() & (merged['adtv_est'] >= min_adtv)
    return yr_df[keep.values]


# ── Data helpers ──────────────────────────────────────────────────────────────

def load_full_hist() -> pd.DataFrame:
    df = pd.read_parquet(FULL_DATA)
    df = df[df['period_type'] == 'annual'].copy()
    df = add_piotroski_ext(df)
    df = add_normalised_ratios(df)
    return df.reset_index(drop=True)

def load_and_score(
    df: pd.DataFrame, label_policy: str = OBSERVED_ONLY
) -> pd.DataFrame:
    """Load already-generated manifest-backed OOS scores without fitting models.

    Session 6A removes the historical scorer's implicit training/final-model
    path.  Strategy filters validate the row manifests for the exact roles they
    consume.  Unmanifested legacy/static values remain ineligible evidence and
    are never used as a fallback.
    """
    if label_policy not in LABEL_POLICIES:
        raise ValueError(f"Unknown label policy: {label_policy}")
    return df.drop(columns=["ml_1y", "ml_3y", "ml_5y"], errors="ignore").copy()


# ── Strategy filter functions ─────────────────────────────────────────────────

ENGINE_REQUIREMENTS = {
    "composite": (
        ScoreRequirement("ml_1y_wf", "classifier_ranker", "1y"),
        ScoreRequirement("ml_3y_wf", "classifier_ranker", "3y"),
    ),
    "ml_gates": (
        ScoreRequirement("tree_prob", "tree_agreement_gate", "3y"),
        ScoreRequirement("reg_3y_wf", "regression_ranker", "3y"),
    ),
    "qem": (ScoreRequirement("ml_1y_wf", "classifier_ranker", "1y"),),
    "scdv": (ScoreRequirement("ml_3y_wf", "classifier_ranker", "3y"),),
    "iarb": (ScoreRequirement("ml_3y_wf", "classifier_ranker", "3y"),),
}


def _score_eligible(
    yr_df: pd.DataFrame, requirements: tuple[ScoreRequirement, ...]
) -> pd.DataFrame:
    mask = validate_historical_scores(yr_df, requirements)
    return yr_df.loc[mask].copy()


def filter_composite(yr_df: pd.DataFrame, top_n: int, market: str | None,
                     mode: str = 'blended') -> pd.Index:
    strategy = "ml_gates" if mode == "ml_gates" else "composite"
    s = _score_eligible(yr_df, ENGINE_REQUIREMENTS[strategy])
    if market:
        s = s[s['market'] == market]

    # ── Hard gates (both modes) ──────────────────────────────────────────────
    if 'beneish_m_score' in s.columns:
        s = s[s['beneish_m_score'].fillna(0) < BENEISH_THRESHOLD]
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
        return complete_top_n(
            yr_df, s, score_col="reg_3y_wf", target_n=top_n
        )

    # ── Blended mode (legacy default) ────────────────────────────────────────
    score = pd.Series(0.0, index=s.index)
    total_w = 0.0
    required_cols = {req.score_col for req in ENGINE_REQUIREMENTS["composite"]}
    for col, w in [('value_composite', 0.25), ('quality_composite', 0.20),
                   ('ml_1y_wf', 0.30), ('ml_3y_wf', 0.15), ('piotroski_f_score', 0.10)]:
        if col in s.columns and (col in required_cols or s[col].notna().sum() > 5):
            score += s[col].rank(pct=True) * w
            total_w += w
    if total_w == 0:
        return pd.Index([])
    s['_score'] = score / total_w
    return complete_top_n(yr_df, s, score_col="_score", target_n=top_n)


def filter_qem(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    s = _score_eligible(yr_df, ENGINE_REQUIREMENTS["qem"])
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
    required_cols = {req.score_col for req in ENGINE_REQUIREMENTS["qem"]}
    for col, w in [('eps_growth_yoy', 0.20), ('quality_composite', 0.25),
                   ('ml_1y_wf', 0.25), ('momentum_12m_prior', 0.15),
                   ('value_composite', 0.15)]:
        if col in s.columns and (col in required_cols or s[col].notna().sum() > 3):
            score += s[col].rank(pct=True) * w
            total_w += w
    if total_w == 0:
        return pd.Index([])
    s['_score'] = score / total_w
    return complete_top_n(yr_df, s, score_col="_score", target_n=top_n)


def filter_scdv(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    s = _score_eligible(yr_df, ENGINE_REQUIREMENTS["scdv"])
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
    required_cols = {req.score_col for req in ENGINE_REQUIREMENTS["scdv"]}
    for col, w in [('value_composite', 0.35), ('quality_composite', 0.25),
                   ('ml_3y_wf', 0.25), ('piotroski_f_score', 0.15)]:
        if col in s.columns and (col in required_cols or s[col].notna().sum() > 3):
            score += s[col].rank(pct=True) * w
            total_w += w
    if total_w == 0:
        return pd.Index([])
    s['_score'] = score / total_w
    if 'debt_to_equity' in s.columns:
        penalty = s['debt_to_equity'].fillna(0).clip(0, 2) * 0.05
        s['_score'] = s['_score'] - penalty
    return complete_top_n(yr_df, s, score_col="_score", target_n=top_n)


def filter_iarb(yr_df: pd.DataFrame, top_n: int, market: str | None) -> pd.Index:
    s = _score_eligible(yr_df, ENGINE_REQUIREMENTS["iarb"])
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
    required_cols = {req.score_col for req in ENGINE_REQUIREMENTS["iarb"]}
    for col, w in [('value_composite', 0.30), ('quality_composite', 0.25),
                   ('ml_3y_wf', 0.25), ('momentum_12m_prior', 0.20)]:
        if col in s.columns and (col in required_cols or s[col].notna().sum() > 3):
            score += s[col].rank(pct=True) * w
            total_w += w
    if total_w == 0:
        return pd.Index([])
    market_boost = {'KR': 0.05, 'BR': 0.03, 'CA': 0.02}
    s['_score'] = (score / total_w if total_w > 0 else score) + s['market'].map(market_boost).fillna(0)
    return complete_top_n(yr_df, s, score_col="_score", target_n=top_n)


STRATEGIES = {
    'composite': filter_composite,
    'ml_gates':  lambda yr_df, top_n, market: filter_composite(yr_df, top_n, market, mode='ml_gates'),
    'qem':       filter_qem,
    'scdv':      filter_scdv,
    'iarb':      filter_iarb,
}


# ── Walk-forward engine ───────────────────────────────────────────────────────

MAX_FILING_LAG_MONTHS = 18   # reject filings > 18 months after fiscal year-end

ASOF_HISTORICAL_GATES = {
    "asof_listing_eligible": True,
    "asof_filing_stale": False,
    "asof_delisting_notice_known": False,
    "asof_quote_recent": True,
    "asof_adtv_eligible": True,
}


def _apply_asof_historical_gates(
    yr_df: pd.DataFrame, decision_timestamp: pd.Timestamp
) -> pd.DataFrame:
    """Apply only separately named, provenance-backed decision-time gates.

    The future-derived ``likely_delisted`` annotation is deliberately ignored.
    When an as-of gate is supplied, its value, timestamp, and source must all be
    present; incomplete evidence fails closed for that row.
    """
    eligible = pd.Series(True, index=yr_df.index)
    exclusions: dict[object, list[str]] = {}
    for field, required_value in ASOF_HISTORICAL_GATES.items():
        if field not in yr_df.columns:
            continue
        timestamp_col = f"{field}_timestamp"
        source_col = f"{field}_source"
        if timestamp_col not in yr_df.columns or source_col not in yr_df.columns:
            valid = pd.Series(False, index=yr_df.index)
        else:
            timestamps = pd.to_datetime(yr_df[timestamp_col], errors="coerce")
            sources = yr_df[source_col].fillna("").astype(str).str.strip()
            valid = (
                yr_df[field].notna()
                & timestamps.notna()
                & (timestamps <= decision_timestamp)
                & sources.ne("")
                & yr_df[field].eq(required_value)
            )
        for idx in yr_df.index[~valid]:
            exclusions.setdefault(idx, []).append(f"{field}:ineligible_or_unproven")
        eligible &= valid
    result = yr_df.loc[eligible].copy()
    result.attrs.update(yr_df.attrs)
    result.attrs["asof_gate_exclusions"] = exclusions
    return result


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
                 survivorship_mode: str = OBSERVED_RETURN_ONLY,
                 return_policy: str | None = None,
                 max_filing_lag_months: int = MAX_FILING_LAG_MONTHS,
                 filing_date_gate: bool = True,
                 spy_returns: dict | None = None,
                 acwi_exus_returns: dict | None = None,
                 is_non_us: bool = False,
                 monthly_px: pd.DataFrame | None = None,
                 corporate_actions: pd.DataFrame | None = None,
                 monthly_risk_free: pd.Series | pd.DataFrame | dict | None = None,
                 use_adtv_filter: bool = True,
                 max_pct_adtv: float = 0.01,
                 aum_target: float = 200_000) -> dict:
    """Walk-forward backtest engine.

    Args:
        fill_missing_return: Deprecated and rejected. Missing labels/prices may
            not select an implicit return policy.
        survivorship_mode: Backward-compatible name for one of the three
            explicit return policies. Prefer ``return_policy``.
        return_policy: ``observed_only``, ``include_policy_imputed_50``, or
            ``include_policy_imputed_100``.
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
        monthly_px: Frozen monthly total-return price evidence. Complete
            selected-holding coverage is required for official performance.
        corporate_actions: Dated resolution/unresolved-event evidence used only
            when selected-holding price coverage ends.
        monthly_risk_free: Frozen, time-aligned monthly risk-free returns.
            Sharpe and Sortino are unavailable when coverage is incomplete.
        use_adtv_filter: When True and monthly_px is available, remove picks
            that would require trading > max_pct_adtv of 30d median ADTV.
        max_pct_adtv: Max fraction of ADTV per position (default 1%).
        aum_target: Portfolio AUM in dollars (default $200K for retail).
    """
    if fill_missing_return is not None:
        raise ValueError(
            "fill_missing_return is prohibited; select an explicit return_policy"
        )
    effective_return_policy = return_policy or survivorship_mode
    if effective_return_policy not in RETURN_POLICIES:
        raise ValueError(
            f"return_policy must be one of {RETURN_POLICIES}; got {effective_return_policy!r}"
        )

    years = sorted(y for y in df['fiscal_year'].unique() if y <= 2023)
    annual_rows = []
    score_coverage: list[dict] = []

    for yr in years:
        yr_df = df[df['fiscal_year'] == yr].copy()

        # ── Look-ahead protection: drop implausibly late filings ──────────────
        yr_df = _apply_filing_lag_filter(yr_df, yr, max_filing_lag_months)

        # ── Filing-date gate: only include stocks filed before holding year ───
        if filing_date_gate and 'filed_date' in yr_df.columns:
            filed = pd.to_datetime(yr_df['filed_date'], errors='coerce')
            holding_start = pd.Timestamp(f'{yr + 1}-01-01')
            yr_df = yr_df[filed.isna() | (filed < holding_start)]

        yr_df = _apply_asof_historical_gates(
            yr_df, pd.Timestamp(f'{yr + 1}-01-01')
        )

        # Liquidity pre-filter: remove stocks outside market cap range
        if 'market_cap_at_filing' in yr_df.columns:
            if min_market_cap > 0:
                yr_df = yr_df[yr_df['market_cap_at_filing'].fillna(0) >= min_market_cap]
            if max_market_cap > 0:
                yr_df = yr_df[yr_df['market_cap_at_filing'].fillna(0) <= max_market_cap]

        # Planned equal-weight position must stay within the configured ADTV share.
        if use_adtv_filter and monthly_px is not None:
            yr_df = adtv_filter(
                yr_df, monthly_px, yr, max_pct_adtv, aum_target, target_n=top_n
            )

        idx = filter_fn(yr_df, top_n, market)
        coverage = dict(yr_df.attrs.get("historical_score_coverage", {}))
        coverage.setdefault("target_n", int(top_n))
        coverage.setdefault("selected_count", int(len(idx)))
        if len(idx) != top_n:
            coverage["selected_count"] = int(len(idx))
            coverage["period_exclusion_reason"] = (
                coverage.get("period_exclusion_reason")
                or "insufficient_target_n_coverage"
            )
            score_coverage.append({"year": int(yr), **coverage})
            continue
        score_coverage.append({"year": int(yr), **coverage})
        picks = yr_df.loc[idx]

        picks_valid = picks.copy()

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
            per_pick_cost = np.full(len(picks_valid), cost_bps / 10000)

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
            weights = np.ones(len(picks_valid)) / len(picks_valid)

        cost_drag = float(np.dot(weights, per_pick_cost))

        annual_rows.append({
            'year':            yr,
            'cost_drag':       cost_drag,
            'n_picks':         len(picks_valid),
            '_picks_valid':    picks_valid,
            '_weights':        weights.tolist(),
            '_per_pick_cost':  per_pick_cost.tolist(),
        })

    coverage_gaps = [
        row for row in score_coverage if row.get("period_exclusion_reason")
    ]
    if coverage_gaps:
        return {
            'label': label,
            'n_years': 0,
            'error': 'incomplete official score coverage',
            'official_performance_available': False,
            'unavailable_years': [int(row['year']) for row in coverage_gaps],
            'score_coverage': score_coverage,
        }

    if not annual_rows:
        return {
            'label': label,
            'n_years': 0,
            'error': 'insufficient data',
            'official_performance_available': False,
            'score_coverage': score_coverage,
        }
    nav_result = build_monthly_nav(
        annual_rows,
        monthly_px,
        return_policy=effective_return_policy,
        corporate_actions=corporate_actions,
    )
    if not nav_result["available"]:
        return {
            "label": label,
            "n_years": 0,
            "error": "incomplete canonical monthly NAV evidence",
            "official_performance_available": False,
            "return_policy": effective_return_policy,
            "nav_exclusions": nav_result["exclusions"],
            "score_coverage": score_coverage,
        }

    nav = nav_result["nav"]
    metrics = compute_nav_metrics(nav, monthly_risk_free=monthly_risk_free)
    annual = annual_returns_from_nav(nav)
    annual_by_holding_year = {int(row["year"]) + 1: row for row in annual_rows}
    annual_output = []
    for row in annual:
        source = annual_by_holding_year[row["year"]]
        annual_output.append({
            "year": row["year"] - 1,
            "holding_year": row["year"],
            "port_pct": round(row["net_return"] * 100, 2),
            "monthly_product_pct": round(row["monthly_product_return"] * 100, 12),
            "reconciliation_error": row["reconciliation_error"],
            "n_picks": int(source["n_picks"]),
            "cost_drag_bps": round(float(source["cost_drag"]) * 10000, 2),
        })
    annual_returns = np.array([row["net_return"] for row in annual], dtype=float)
    monthly_returns = nav["monthly_net_return"].dropna().to_numpy(dtype=float)
    avg_picks = float(np.mean([row["n_picks"] for row in annual_rows]))
    return {
        "label": label,
        "n_years": len(annual),
        "official_performance_available": True,
        "return_policy": effective_return_policy,
        "metric_nav_column": metrics["metric_nav_column"],
        "cagr_pct": round(metrics["cagr"] * 100, 2) if pd.notna(metrics["cagr"]) else None,
        "bench_cagr_pct": None,
        "excess_cagr_pct": None,
        "benchmark_source": None,
        "spy_cagr_pct": None,
        "excess_cagr_vs_spy": None,
        "beta_vs_spy": None,
        "alpha_vs_spy": None,
        "r_squared_vs_spy": None,
        "tracking_error": None,
        "annual_turnover_pct": round(avg_picks / max(top_n, 1) * 100, 1),
        "var_95_pct": round(float(np.percentile(monthly_returns, 5)) * 100, 2),
        "cvar_99_pct": round(float(monthly_returns[monthly_returns <= np.percentile(monthly_returns, 1)].mean()) * 100, 2),
        "max_drawdown_pct": round(metrics["max_drawdown"] * 100, 2),
        "max_drawdown_duration_months": metrics["max_drawdown_duration_months"],
        "sharpe": round(metrics["sharpe"], 3) if pd.notna(metrics["sharpe"]) else None,
        "sortino": round(metrics["sortino"], 3) if pd.notna(metrics["sortino"]) else None,
        "calmar": round(metrics["calmar"], 3) if pd.notna(metrics["calmar"]) else None,
        "info_ratio": None,
        "hit_rate_pct": round(float((annual_returns > 0).mean()) * 100, 1),
        "avg_cost_drag_bps": round(float(np.mean([row["cost_drag"] for row in annual_rows])) * 10000, 1),
        "best_year_pct": round(float(annual_returns.max()) * 100, 2),
        "worst_year_pct": round(float(annual_returns.min()) * 100, 2),
        "best_month_pct": round(metrics["best_month"] * 100, 2),
        "worst_month_pct": round(metrics["worst_month"] * 100, 2),
        "negative_months": metrics["negative_months"],
        "score_coverage": score_coverage,
        "nav_exclusions": [],
        "monthly_nav": nav.to_dict("records"),
        "corporate_action_ledger": nav_result["ledger"].to_dict("records"),
        "cagr_bootstrap_mean_pct": None,
        "cagr_bootstrap_1sigma_pct": None,
        "sharpe_bootstrap_mean": None,
        "sharpe_bootstrap_1sigma": None,
        "annual_returns": annual_output,
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
    print(f'  Period:          {result["n_years"]} years  |  return policy: {result.get("return_policy")}')
    print(f'  CAGR:            {result["cagr_pct"]:+.1f}%')
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
    print(f'  VaR 95%:         {result.get("var_95_pct", "N/A")}%  (monthly)')
    print(f'  CVaR 99%:        {result.get("cvar_99_pct", "N/A")}%  (monthly expected shortfall)')
    print(f'  Turnover:        ~{result.get("annual_turnover_pct", "N/A")}% annual')
    print(f'  Hit Rate:        {result["hit_rate_pct"]:.0f}%')
    print(f'  Avg Cost Drag:   {result["avg_cost_drag_bps"]:.0f} bps')
    print(f'  Best / Worst:    {result["best_year_pct"]:+.1f}% / {result["worst_year_pct"]:+.1f}%')
    print(f'\n  Fiscal  Holding  Port%   Picks  Cost bps  Reconciliation error')
    for row in result['annual_returns']:
        print(
            f'  {row["year"]:6d}  {row["holding_year"]:7d}  '
            f'{row["port_pct"]:+5.1f}   {row["n_picks"]:3d}  '
            f'{row["cost_drag_bps"]:8.1f}  {row["reconciliation_error"]:+.3e}'
        )
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
    parser.add_argument('--return-policy', choices=RETURN_POLICIES,
                        default=OBSERVED_RETURN_ONLY,
                        help='Observed-only NAV or an explicit unresolved-event sensitivity')
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
    parser.add_argument('--label-policy', choices=LABEL_POLICIES, default=OBSERVED_ONLY,
                        help='Observed-only model labels or explicit policy-imputed sensitivity')
    args = parser.parse_args()

    print('Loading + scoring full historical data...')
    df = load_full_hist()
    df = load_and_score(df, label_policy=args.label_policy)
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
        print('  Monthly price cache not found — official performance will fail closed.')

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
                              return_policy=args.return_policy,
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
            print(
                f'CAGR={result["cagr_pct"]:+.1f}%  '
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
        'return_policy':     args.return_policy,
        'model_label_policy': args.label_policy,
        'max_filing_lag':    args.max_filing_lag,
        'filing_date_gate':  not args.no_filing_gate,
        'adtv_filter':       not args.no_adtv and monthly_px is not None,
        'aum_target':        args.aum_target,
        'canonical_monthly_nav': monthly_px is not None,
        'strategies':        results,
    }
    suffix = "" if args.return_policy == OBSERVED_RETURN_ONLY else f"_{args.return_policy}"
    out_path = (OUT_PATH if not suffix else OUT_PATH.with_name(f'{OUT_PATH.stem}{suffix}{OUT_PATH.suffix}'))
    out_path.write_text(json.dumps(out, indent=2, default=str))
    print(f'\nSaved: {out_path}')


if __name__ == '__main__':
    main()
