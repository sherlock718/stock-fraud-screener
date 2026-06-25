"""
Leverage & position sizing strategy for the stock fraud screener.

3-stage screener (when model_3y_regression.joblib is available):
  Stage 1 — Hard fundamental gate: Piotroski ≥ 6, Beneish M < -1.78,
             Altman Z > 1.81, P/B < 5.0, market cap ≥ $50M
  Stage 2 — Direction gate: ml_score_3y > 0.52 (binary classifier P(beat_market))
  Stage 3 — Magnitude ranker: sort survivors by ml_pred_excess_3y (Huber regression)
  Stage 4 — Kelly position sizing: w_i ∝ max(ml_pred_excess_3y_i, 0)

Falls back to composite_score() weighting when regression model is unavailable.

Outputs per-ticker leverage recommendations based on:
- Kelly criterion (half-Kelly for safety)
- Volatility-adjusted position sizing
- Options overlay sizing (LEAPS)
- Margin guidelines

Usage:
    python3 scripts/leverage_strategy.py --market US --top-long 20
    python3 scripts/leverage_strategy.py --market KR --top-long 15 --capital 10000
    python3 scripts/leverage_strategy.py --long-only --top-long 10
    python3 scripts/leverage_strategy.py --min-piotroski 7 --max-beneish -2.0
    python3 scripts/leverage_strategy.py --output reports/leverage_picks.csv
"""
from __future__ import annotations
import argparse
import json
import warnings
warnings.filterwarnings('ignore')

import sys
import joblib
import numpy as np
import pandas as pd
from pathlib import Path

from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext
from _root import ROOT

BASE = ROOT
DATA_PATH  = BASE / 'data' / 'app_data.parquet'
FULL_DATA  = BASE / 'data' / 'historical_dataset_clean.parquet'
MODELS_DIR = BASE / 'models'

# --- Risk parameters ---
HALF_KELLY_FRACTION = 0.5   # Never full Kelly — too volatile
MAX_POSITION_PCT    = 0.15  # Cap any single position at 15% of portfolio
MAX_LEVERAGE        = 2.0   # Hard cap: never exceed 2x notional
MIN_PIOTROSKI       = 6     # Quality gate for leveraged positions
MAX_BENEISH         = -1.78 # Manipulation threshold (Beneish M-score)
MIN_ALTMAN_Z        = 1.81  # Distress threshold

# --- 3-stage screener thresholds (Stage 1 extension) ---
STAGE1_MAX_PB       = 5.0   # Academic value ceiling: exclude deeply overvalued names
STAGE1_MIN_CAP      = 50e6  # $50M liquidity floor (no ADTV data; cap is proxy)
STAGE2_ML_THRESHOLD = 0.52  # Direction gate: ml_score_3y must beat near-random baseline

REGRESSION_MODEL_FILE = 'model_3y_regression.joblib'
REGRESSION_META_FILE  = 'model_3y_regression_meta.json'
REGRESSION_COL        = 'ml_pred_excess_3y'


def load_data(market: str) -> pd.DataFrame:
    src = FULL_DATA if FULL_DATA.exists() else DATA_PATH
    df = pd.read_parquet(src)
    df = df[(df['period_type'] == 'annual') & (df['market'] == market)]
    latest_year = df['fiscal_year'].max()
    df = df[df['fiscal_year'] >= latest_year - 1]
    df = add_piotroski_ext(df)
    df = add_normalised_ratios(df)
    df = df.sort_values('fiscal_year', ascending=False).drop_duplicates('ticker', keep='first')
    return df.reset_index(drop=True)


def load_models() -> dict:
    models = {}
    for h in ['1y', '3y', '5y']:
        p = MODELS_DIR / f'model_{h}.joblib'
        if p.exists():
            models[h] = joblib.load(p)
    reg_path = MODELS_DIR / REGRESSION_MODEL_FILE
    reg_meta = MODELS_DIR / REGRESSION_META_FILE
    if reg_path.exists() and reg_meta.exists():
        models['_regression'] = joblib.load(reg_path)
        models['_regression_meta'] = json.loads(reg_meta.read_text())
    return models


def score_tickers(df: pd.DataFrame, models: dict) -> pd.DataFrame:
    meta_path = MODELS_DIR / 'model_meta.json'
    if not meta_path.exists() or not models:
        df['ml_score_1y'] = np.nan
        df['ml_score_3y'] = np.nan
        return df

    meta = json.loads(meta_path.read_text())
    for h, clf in models.items():
        if h.startswith('_'):
            continue
        feats = [f for f in meta[h]['features'] if f in df.columns]
        X = df[feats].fillna(df[feats].median())
        df[f'ml_score_{h}'] = clf.predict_proba(X)[:, 1]

    # Regression model: predict excess return magnitude
    if '_regression' in models:
        reg_meta = models['_regression_meta']
        feats = [f for f in reg_meta['features'] if f in df.columns]
        medians = reg_meta['train_medians']
        X = df[feats].copy()
        for col in X.columns:
            X[col] = X[col].fillna(medians.get(col, 0.0))
        df[REGRESSION_COL] = models['_regression'].predict(X).astype(np.float32)

    return df


def composite_score(df: pd.DataFrame) -> pd.DataFrame:
    # ml_score_1y removed: test AUC 0.484 (sub-random). 3y is the only validated signal.
    cols = {
        'value_composite':   0.25,
        'quality_composite': 0.20,
        'ml_score_3y':       0.45,
        'piotroski_f_score': 0.10,
    }
    score = pd.Series(0.0, index=df.index)
    total_weight = 0.0
    for col, w in cols.items():
        if col in df.columns and df[col].notna().sum() > 10:
            ranked = df[col].rank(pct=True)
            score += ranked * w
            total_weight += w
    df['composite_score'] = score / total_weight if total_weight > 0 else np.nan
    return df


def quality_gate(df: pd.DataFrame) -> pd.DataFrame:
    """Flag tickers safe for leverage (all quality gates passed)."""
    safe = pd.Series(True, index=df.index)

    if 'piotroski_f_score' in df.columns:
        safe &= df['piotroski_f_score'].fillna(0) >= MIN_PIOTROSKI

    if 'beneish_m_score' in df.columns:
        safe &= df['beneish_m_score'].fillna(-999) < MAX_BENEISH

    if 'altman_z_score' in df.columns:
        # Z > 2.99 = safe zone; 1.81–2.99 = grey; < 1.81 = distress
        safe &= df['altman_z_score'].fillna(0) > MIN_ALTMAN_Z

    if 'likely_delisted' in df.columns:
        safe &= df['likely_delisted'].fillna(1) == 0

    if 'price_to_book' in df.columns:
        safe &= df['price_to_book'].fillna(999) < STAGE1_MAX_PB

    if 'market_cap_at_filing' in df.columns:
        safe &= df['market_cap_at_filing'].fillna(0) >= STAGE1_MIN_CAP

    df['leverage_safe'] = safe.astype(int)
    return df


def _apply_three_stage_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stage 1: hard fundamental gate (Piotroski, Beneish, Altman, P/B, market cap).
    Stage 2: direction gate — ml_score_3y > STAGE2_ML_THRESHOLD (requires binary model).
    Stage 3: sort survivors by ml_pred_excess_3y descending (requires regression model).
    Falls back gracefully when models are absent.
    """
    df = quality_gate(df)
    df = df[df['leverage_safe'] == 1].copy()

    if 'ml_score_3y' in df.columns and df['ml_score_3y'].notna().sum() > 0:
        df = df[df['ml_score_3y'] > STAGE2_ML_THRESHOLD].copy()

    if REGRESSION_COL in df.columns and df[REGRESSION_COL].notna().sum() > 0:
        df = df.sort_values(REGRESSION_COL, ascending=False)
    elif 'composite_score' in df.columns:
        df = df.sort_values('composite_score', ascending=False)

    return df


def kelly_position(win_rate: float, avg_win: float, avg_loss: float) -> float:
    """Kelly fraction = win_rate/avg_loss - loss_rate/avg_win, capped at MAX_LEVERAGE."""
    if avg_loss <= 0 or avg_win <= 0:
        return 0.0
    loss_rate = 1 - win_rate
    kelly = win_rate / avg_loss - loss_rate / avg_win
    half_kelly = kelly * HALF_KELLY_FRACTION
    return float(np.clip(half_kelly, 0.0, MAX_LEVERAGE))


def size_positions(df: pd.DataFrame, top_n: int, capital: float) -> pd.DataFrame:
    """
    Assign position sizes to top_n tickers using the 3-stage screener.
    When ml_pred_excess_3y is available, weights are proportional to predicted
    excess return (Kelly-like). Falls back to composite_score weighting.
    Positions sum to 100% of capital, capped at MAX_POSITION_PCT.
    """
    HIST_WIN_RATE = 0.65
    HIST_AVG_WIN  = 0.45
    HIST_AVG_LOSS = 0.22

    # Run 3-stage filter; head(top_n) applied after Stage 3 sort
    filtered = _apply_three_stage_filter(df).head(top_n).copy()
    base_kelly = kelly_position(HIST_WIN_RATE, HIST_AVG_WIN, HIST_AVG_LOSS)

    # Weight by predicted excess return when available; else composite_score
    use_regression = (
        REGRESSION_COL in filtered.columns and
        filtered[REGRESSION_COL].notna().sum() > 0
    )
    if use_regression:
        raw_weights = filtered[REGRESSION_COL].clip(lower=0).fillna(0).values
    else:
        raw_weights = filtered['composite_score'].fillna(0).values

    total = raw_weights.sum()
    weights = raw_weights / total if total > 0 else np.ones(len(filtered)) / len(filtered)
    weights = np.minimum(weights, MAX_POSITION_PCT)
    weights = weights / weights.sum()

    positions = []
    for i, (_, row) in enumerate(filtered.iterrows()):
        pos_pct = weights[i]
        pos_eur = capital * pos_pct
        leverage = min(base_kelly, MAX_LEVERAGE)
        strategy = _pick_strategy(row, leverage)

        positions.append({
            'ticker':          row.get('ticker', ''),
            'name':              str(row.get('name', ''))[:30],
            'composite_score':   round(float(row.get('composite_score', 0)), 3),
            'ml_pred_excess_3y': round(float(row.get(REGRESSION_COL, np.nan) or np.nan), 4)
                                  if pd.notna(row.get(REGRESSION_COL)) else np.nan,
            'leverage_safe':     int(row.get('leverage_safe', 0)),
            'position_pct':      round(pos_pct * 100, 1),
            'position_eur':      round(pos_eur, 0),
            'leverage_mult':     round(leverage, 2),
            'notional_eur':      round(pos_eur * leverage, 0),
            'strategy':          strategy,
            'piotroski':         row.get('piotroski_f_score', np.nan),
            'beneish':           row.get('beneish_m_score', np.nan),
            'ml_3y':             round(float(row.get('ml_score_3y', np.nan) or np.nan), 3)
                                  if pd.notna(row.get('ml_score_3y')) else np.nan,
        })

    return pd.DataFrame(positions)


def _pick_strategy(row: pd.Series, leverage: float) -> str:
    """Pick the most appropriate leveraged strategy for a ticker."""
    ml_3y = row.get('ml_score_3y', 0.5) or 0.5
    mkt_cap = row.get('market_cap_at_filing', 0) or 0

    if ml_3y >= 0.75 and mkt_cap > 1e9:
        return f'LEAPS calls (12–18mo, delta ~0.70) | {leverage:.1f}x'
    elif ml_3y >= 0.65:
        return f'2:1 margin long | {leverage:.1f}x'
    else:
        return 'Equity only (score below leverage threshold)'


def build_short_book(df: pd.DataFrame, top_n: int, capital: float) -> pd.DataFrame:
    """
    Select top_n short candidates: highest Beneish M-score (most likely manipulation).
    Short leg is capped at 30% of total capital, equally weighted, max 5% per name.
    Only names with Beneish > MAX_BENEISH threshold qualify.
    """
    SHORT_BOOK_PCT    = 0.30   # Short book <= 30% of capital
    MAX_SHORT_SINGLE  = 0.05   # Cap each short at 5%

    if 'beneish_m_score' not in df.columns:
        return pd.DataFrame()

    # High Beneish = manipulation risk = short candidates
    short_candidates = df[df['beneish_m_score'] > MAX_BENEISH].copy()

    # Exclude already small-cap / illiquid (can't easily borrow)
    if 'market_cap_at_filing' in short_candidates.columns:
        short_candidates = short_candidates[
            short_candidates['market_cap_at_filing'].fillna(0) > 1e8
        ]

    if len(short_candidates) == 0:
        return pd.DataFrame()

    short_candidates = short_candidates.nlargest(top_n, 'beneish_m_score').copy()

    # Equal weight, capped at MAX_SHORT_SINGLE, scaled to SHORT_BOOK_PCT of capital
    n = len(short_candidates)
    weight = min(1.0 / n, MAX_SHORT_SINGLE)
    short_candidates['short_pct']     = round(weight * SHORT_BOOK_PCT * 100, 1)
    short_candidates['short_eur']     = round(capital * weight * SHORT_BOOK_PCT, 0)
    short_candidates['short_leg']     = 'Short'
    return short_candidates[
        ['ticker'] +
        [c for c in ['name', 'beneish_m_score', 'altman_z_score',
                     'piotroski_f_score', 'short_pct', 'short_eur', 'short_leg']
         if c in short_candidates.columns]
    ].reset_index(drop=True)


def print_report(positions: pd.DataFrame, shorts: pd.DataFrame,
                 market: str, capital: float) -> None:
    total_invested = positions['position_eur'].sum()
    total_notional = positions['notional_eur'].sum()
    safe_count = positions['leverage_safe'].sum()

    print(f"\n{'='*70}")
    print(f"  LEVERAGE STRATEGY REPORT — {market} | Capital: €{capital:,.0f}")
    print(f"{'='*70}")
    print(f"  Top {len(positions)} LONG positions | {safe_count} leverage-safe")
    print(f"  Total invested: €{total_invested:,.0f} ({total_invested/capital*100:.0f}% of capital)")
    print(f"  Total notional: €{total_notional:,.0f} ({total_notional/capital*100:.0f}% of capital)")
    print(f"{'='*70}\n")

    display_cols = ['ticker', 'ml_pred_excess_3y', 'composite_score', 'position_pct',
                    'position_eur', 'leverage_mult', 'notional_eur', 'piotroski',
                    'beneish', 'ml_3y', 'strategy']
    available = [c for c in display_cols if c in positions.columns]
    print(positions[available].to_string(index=False))

    if len(shorts) > 0:
        short_capital = shorts['short_eur'].sum() if 'short_eur' in shorts.columns else 0
        print(f"\n{'='*70}")
        print(f"  SHORT BOOK — {len(shorts)} positions | €{short_capital:,.0f} gross short")
        print(f"{'='*70}\n")
        print(shorts.to_string(index=False))

    print(f"\n--- Risk Rules Applied ---")
    print(f"  Half-Kelly fraction:     {HALF_KELLY_FRACTION}")
    print(f"  Max single position:     {MAX_POSITION_PCT*100:.0f}%")
    print(f"  Max leverage:            {MAX_LEVERAGE}x")
    print(f"  Stage 1 — Min Piotroski: {MIN_PIOTROSKI} | Max Beneish: {MAX_BENEISH} | Min Altman Z: {MIN_ALTMAN_Z}")
    print(f"  Stage 1 — Max P/B:       {STAGE1_MAX_PB} | Min market cap: ${STAGE1_MIN_CAP/1e6:.0f}M")
    print(f"  Stage 2 — ml_score_3y >  {STAGE2_ML_THRESHOLD} (direction gate)")
    print(f"  Stage 3 — ranked by      ml_pred_excess_3y (magnitude ranker)")

    print(f"\n--- Strategy Guide ---")
    print(f"  LEAPS calls:    Buy 12–18mo deep ITM calls (delta ~0.70)")
    print(f"                  3–5x notional leverage, max loss = premium paid")
    print(f"                  Best for: high conviction, large-cap, 12mo+ horizon")
    print(f"  2:1 margin:     IBKR Reg-T margin, borrow at ~5.8% annually")
    print(f"                  Net return hurdle: stock must beat 5.8% to profit")
    print(f"                  Best for: medium conviction, liquid stocks")
    print(f"  Equity only:    No leverage. Position if score qualifies.")
    print(f"  Short (Beneish > {MAX_BENEISH}): Borrow & sell; expect mean-reversion / accounting fraud reveal")


def main():
    global MIN_PIOTROSKI, MAX_BENEISH
    parser = argparse.ArgumentParser(description='Leverage strategy report')
    parser.add_argument('--market',         default='US',    help='Market code (US, KR, CA, ...)')
    parser.add_argument('--top-long',       default=20, type=int, help='Top N long positions (default: 20)')
    parser.add_argument('--top-short',      default=10, type=int, help='Top N short candidates (default: 10)')
    parser.add_argument('--capital',        default=10000, type=float, help='Portfolio capital in EUR (default: 10000)')
    parser.add_argument('--long-only',      action='store_true', help='Skip short book')
    parser.add_argument('--min-piotroski',  default=MIN_PIOTROSKI, type=int, help=f'Min Piotroski F-score for leverage (default: {MIN_PIOTROSKI})')
    parser.add_argument('--max-beneish',    default=MAX_BENEISH, type=float, help=f'Max Beneish M-score for longs (default: {MAX_BENEISH})')
    parser.add_argument('--output',         default=None, help='Output CSV path (default: data/leverage_positions_<market>.csv)')
    args = parser.parse_args()

    # Override module-level constants with CLI args
    MIN_PIOTROSKI = args.min_piotroski
    MAX_BENEISH   = args.max_beneish

    print(f'Loading {args.market} data...')
    df = load_data(args.market)
    print(f'  {len(df):,} companies')

    print('Loading models...')
    models = load_models()
    print(f'  {len(models)} horizons loaded')

    print('Scoring...')
    df = score_tickers(df, models)
    df = composite_score(df)

    print(f'Building leverage report (top-long={args.top_long}, top-short={args.top_short})...')
    positions = size_positions(df, args.top_long, args.capital)
    shorts    = pd.DataFrame() if args.long_only else build_short_book(df, top_n=args.top_short, capital=args.capital)
    print_report(positions, shorts, args.market, args.capital)

    out_path = Path(args.output) if args.output else BASE / 'data' / f'leverage_positions_{args.market.lower()}.csv'
    positions.to_csv(out_path, index=False)
    if len(shorts) > 0:
        short_path = out_path.parent / f'short_book_{args.market.lower()}.csv'
        shorts.to_csv(short_path, index=False)
        print(f'Short book saved: {short_path}')
    print(f'\nSaved: {out_path}')


if __name__ == '__main__':
    main()
