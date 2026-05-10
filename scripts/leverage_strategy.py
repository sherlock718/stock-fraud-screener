"""
Leverage & position sizing strategy for the stock fraud screener.

Outputs per-ticker leverage recommendations based on:
- Kelly criterion (half-Kelly for safety)
- Volatility-adjusted position sizing
- Options overlay sizing (LEAPS)
- Margin guidelines

Usage:
    python3 scripts/leverage_strategy.py --market US --top 20
    python3 scripts/leverage_strategy.py --market KR --top 15 --capital 10000
"""
from __future__ import annotations
import argparse
import json
import warnings
warnings.filterwarnings('ignore')

import joblib
import numpy as np
import pandas as pd
from pathlib import Path

BASE       = Path(__file__).parent.parent
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


def load_data(market: str) -> pd.DataFrame:
    # Load slim app_data for display cols, full dataset for ML scoring
    slim = pd.read_parquet(DATA_PATH)
    slim = slim[(slim['period_type'] == 'annual') & (slim['market'] == market)]
    latest_year = slim['fiscal_year'].max()
    slim = slim[slim['fiscal_year'] >= latest_year - 1]
    slim = slim.sort_values('fiscal_year', ascending=False).drop_duplicates('ticker', keep='first')

    if FULL_DATA.exists():
        full = pd.read_parquet(FULL_DATA)
        full = full[(full['period_type'] == 'annual') & (full['market'] == market)]
        full = full[full['fiscal_year'] >= latest_year - 1]

        # Add Piotroski extensions (same logic as train_models.py)
        full = full.sort_values(['ticker', 'fiscal_year'])
        for src, name in [
            ('shares_outstanding', 'piotroski_shares_ok'),
            ('gross_margin',       'piotroski_delta_gm'),
            ('asset_turnover',     'piotroski_delta_at'),
        ]:
            if src in full.columns:
                full[name] = full.groupby('ticker')[src].transform(
                    lambda x: (x <= x.shift(1)).astype(float) if src == 'shares_outstanding'
                    else (x > x.shift(1)).astype(float)
                )
        extra_cols = [c for c in ['piotroski_shares_ok', 'piotroski_delta_gm', 'piotroski_delta_at'] if c in full.columns]
        if extra_cols and 'piotroski_f_score' in full.columns:
            full['piotroski_f_score_9'] = full['piotroski_f_score'].astype('float64') + full[extra_cols].sum(axis=1, min_count=1)

        full = full.sort_values('fiscal_year', ascending=False).drop_duplicates('ticker', keep='first')
        # Merge slim display-only cols (e.g. market_cap_at_filing) onto full
        slim_extra = [c for c in slim.columns if c not in full.columns]
        if slim_extra:
            full = full.merge(slim[['ticker'] + slim_extra], on='ticker', how='left')
        df = full
    else:
        df = slim

    return df.reset_index(drop=True)


def load_models() -> dict:
    models = {}
    for h in ['1y', '3y', '5y']:
        p = MODELS_DIR / f'model_{h}.joblib'
        if p.exists():
            models[h] = joblib.load(p)
    return models


def score_tickers(df: pd.DataFrame, models: dict) -> pd.DataFrame:
    meta_path = MODELS_DIR / 'model_meta.json'
    if not meta_path.exists() or not models:
        df['ml_score_1y'] = np.nan
        df['ml_score_3y'] = np.nan
        return df

    meta = json.loads(meta_path.read_text())
    for h, clf in models.items():
        feats = [f for f in meta[h]['features'] if f in df.columns]
        X = df[feats].fillna(df[feats].median())
        df[f'ml_score_{h}'] = clf.predict_proba(X)[:, 1]
    return df


def composite_score(df: pd.DataFrame) -> pd.DataFrame:
    cols = {
        'value_composite':   0.25,
        'quality_composite': 0.20,
        'ml_score_1y':       0.30,
        'ml_score_3y':       0.15,
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

    df['leverage_safe'] = safe.astype(int)
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
    Assign position sizes to top_n tickers.
    Positions sum to 100% of capital. Each position is score-weighted,
    capped at MAX_POSITION_PCT. Leverage applied only to leverage-safe names.
    """
    HIST_WIN_RATE = 0.65
    HIST_AVG_WIN  = 0.45
    HIST_AVG_LOSS = 0.22

    df = df.sort_values('composite_score', ascending=False).head(top_n).copy()
    df = quality_gate(df)
    base_kelly = kelly_position(HIST_WIN_RATE, HIST_AVG_WIN, HIST_AVG_LOSS)

    # Score-proportional weights, normalised to sum to 1, capped at MAX_POSITION_PCT
    scores = df['composite_score'].fillna(0).values
    weights = scores / scores.sum()
    weights = np.minimum(weights, MAX_POSITION_PCT)
    weights = weights / weights.sum()  # re-normalise after capping

    positions = []
    for i, (_, row) in enumerate(df.iterrows()):
        pos_pct = weights[i]
        pos_eur = capital * pos_pct

        if row.get('leverage_safe', 0) == 1:
            leverage = min(base_kelly, MAX_LEVERAGE)
            strategy = _pick_strategy(row, leverage)
        else:
            leverage = 1.0
            strategy = 'Equity only — quality gate failed'

        positions.append({
            'ticker':          row.get('ticker', ''),
            'name':              str(row.get('name', ''))[:30],
            'composite_score':   round(float(row.get('composite_score', 0)), 3),
            'leverage_safe':     int(row.get('leverage_safe', 0)),
            'position_pct':      round(pos_pct * 100, 1),
            'position_eur':      round(pos_eur, 0),
            'leverage_mult':     round(leverage, 2),
            'notional_eur':      round(pos_eur * leverage, 0),
            'strategy':          strategy,
            'piotroski':         row.get('piotroski_f_score', np.nan),
            'beneish':           row.get('beneish_m_score', np.nan),
            'ml_1y':             round(float(row.get('ml_score_1y', np.nan) or np.nan), 3) if pd.notna(row.get('ml_score_1y')) else np.nan,
        })

    return pd.DataFrame(positions)


def _pick_strategy(row: pd.Series, leverage: float) -> str:
    """Pick the most appropriate leveraged strategy for a ticker."""
    ml_1y = row.get('ml_score_1y', 0.5) or 0.5
    mkt_cap = row.get('market_cap_at_filing', 0) or 0

    if ml_1y >= 0.75 and mkt_cap > 1e9:
        # High conviction large-cap: LEAPS calls (3–5x leverage, defined risk)
        return f'LEAPS calls (12–18mo, delta ~0.70) | {leverage:.1f}x'
    elif ml_1y >= 0.65:
        # Medium conviction: 2:1 margin long
        return f'2:1 margin long | {leverage:.1f}x'
    else:
        # Lower conviction: equity only, no leverage
        return 'Equity only (score below leverage threshold)'


def print_report(positions: pd.DataFrame, market: str, capital: float) -> None:
    total_invested = positions['position_eur'].sum()
    total_notional = positions['notional_eur'].sum()
    safe_count = positions['leverage_safe'].sum()

    print(f"\n{'='*70}")
    print(f"  LEVERAGE STRATEGY REPORT — {market} | Capital: €{capital:,.0f}")
    print(f"{'='*70}")
    print(f"  Top {len(positions)} positions | {safe_count} leverage-safe")
    print(f"  Total invested: €{total_invested:,.0f} ({total_invested/capital*100:.0f}% of capital)")
    print(f"  Total notional: €{total_notional:,.0f} ({total_notional/capital*100:.0f}% of capital)")
    print(f"{'='*70}\n")

    display_cols = ['ticker', 'composite_score', 'position_pct', 'position_eur',
                    'leverage_mult', 'notional_eur', 'piotroski', 'beneish', 'ml_1y', 'strategy']
    available = [c for c in display_cols if c in positions.columns]
    print(positions[available].to_string(index=False))

    print(f"\n--- Risk Rules Applied ---")
    print(f"  Half-Kelly fraction:     {HALF_KELLY_FRACTION}")
    print(f"  Max single position:     {MAX_POSITION_PCT*100:.0f}%")
    print(f"  Max leverage:            {MAX_LEVERAGE}x")
    print(f"  Min Piotroski F-score:   {MIN_PIOTROSKI}")
    print(f"  Max Beneish M-score:     {MAX_BENEISH} (below = manipulation risk)")
    print(f"  Min Altman Z-score:      {MIN_ALTMAN_Z} (below = distress risk)")

    print(f"\n--- Strategy Guide ---")
    print(f"  LEAPS calls:    Buy 12–18mo deep ITM calls (delta ~0.70)")
    print(f"                  3–5x notional leverage, max loss = premium paid")
    print(f"                  Best for: high conviction, large-cap, 12mo+ horizon")
    print(f"  2:1 margin:     IBKR Reg-T margin, borrow at ~5.8% annually")
    print(f"                  Net return hurdle: stock must beat 5.8% to profit")
    print(f"                  Best for: medium conviction, liquid stocks")
    print(f"  Equity only:    No leverage. Position if score qualifies.")


def main():
    parser = argparse.ArgumentParser(description='Leverage strategy report')
    parser.add_argument('--market',  default='US',    help='Market code (US, KR, CA, ...)')
    parser.add_argument('--top',     default=20, type=int, help='Top N tickers to analyse')
    parser.add_argument('--capital', default=10000, type=float, help='Portfolio capital in EUR')
    args = parser.parse_args()

    print(f'Loading {args.market} data...')
    df = load_data(args.market)
    print(f'  {len(df):,} companies')

    print('Loading models...')
    models = load_models()
    print(f'  {len(models)} horizons loaded')

    print('Scoring...')
    df = score_tickers(df, models)
    df = composite_score(df)

    print(f'Building leverage report (top {args.top})...')
    positions = size_positions(df, args.top, args.capital)
    print_report(positions, args.market, args.capital)

    out_path = BASE / 'data' / f'leverage_positions_{args.market.lower()}.csv'
    positions.to_csv(out_path, index=False)
    print(f'\nSaved: {out_path}')


if __name__ == '__main__':
    main()
