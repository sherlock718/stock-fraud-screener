"""
Core leverage & position sizing logic for the Streamlit app.
Extracted from scripts/leverage_strategy.py.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

# --- Risk parameters ---
HALF_KELLY_FRACTION = 0.5
MAX_POSITION_PCT    = 0.15
MAX_LEVERAGE        = 2.0
MIN_PIOTROSKI       = 6
MAX_BENEISH         = -1.78
MIN_ALTMAN_Z        = 1.81
HIST_WIN_RATE       = 0.65
HIST_AVG_WIN        = 0.45
HIST_AVG_LOSS       = 0.22
SHORT_BOOK_PCT      = 0.30
MAX_SHORT_SINGLE    = 0.05


def kelly_position(win_rate: float, avg_win: float, avg_loss: float) -> float:
    if avg_loss <= 0 or avg_win <= 0:
        return 0.0
    loss_rate = 1 - win_rate
    kelly = win_rate / avg_loss - loss_rate / avg_win
    return float(np.clip(kelly * HALF_KELLY_FRACTION, 0.0, MAX_LEVERAGE))


def quality_gate(df: pd.DataFrame) -> pd.DataFrame:
    safe = pd.Series(True, index=df.index)
    if 'piotroski_f_score' in df.columns:
        safe &= df['piotroski_f_score'].fillna(0) >= MIN_PIOTROSKI
    if 'beneish_m_score' in df.columns:
        safe &= df['beneish_m_score'].fillna(-999) < MAX_BENEISH
    if 'altman_z_score' in df.columns:
        safe &= df['altman_z_score'].fillna(0) > MIN_ALTMAN_Z
    if 'likely_delisted' in df.columns:
        safe &= df['likely_delisted'].fillna(1) == 0
    df = df.copy()
    df['leverage_safe'] = safe.astype(int)
    return df


def _pick_strategy(row: pd.Series, leverage: float) -> str:
    ml_1y   = row.get('ml_score_1y') or row.get('ml_score') or 0.5
    mkt_cap = row.get('market_cap_at_filing', 0) or 0
    if ml_1y >= 0.75 and mkt_cap > 1e9:
        return f'LEAPS calls (12–18mo, delta ~0.70) | {leverage:.1f}x'
    elif ml_1y >= 0.65:
        return f'2:1 margin long | {leverage:.1f}x'
    else:
        return 'Equity only (score below leverage threshold)'


def size_positions(df: pd.DataFrame, top_n: int, capital: float) -> pd.DataFrame:
    score_col = 'composite_score' if 'composite_score' in df.columns else df.columns[0]
    df = df.sort_values(score_col, ascending=False).head(top_n).copy()
    df = quality_gate(df)
    base_kelly = kelly_position(HIST_WIN_RATE, HIST_AVG_WIN, HIST_AVG_LOSS)

    scores = df[score_col].fillna(0).values
    total  = scores.sum()
    weights = scores / total if total > 0 else np.ones(len(scores)) / len(scores)
    weights = np.minimum(weights, MAX_POSITION_PCT)
    weights = weights / weights.sum()

    rows = []
    for i, (_, row) in enumerate(df.iterrows()):
        pos_pct = weights[i]
        pos_eur = capital * pos_pct
        if row.get('leverage_safe', 0) == 1:
            leverage = min(base_kelly, MAX_LEVERAGE)
            strategy = _pick_strategy(row, leverage)
        else:
            leverage = 1.0
            strategy = 'Equity only — quality gate failed'

        ml_1y = row.get('ml_score_1y') or row.get('ml_score')
        rows.append({
            'ticker':          row.get('ticker', ''),
            'name':            str(row.get('name', ''))[:30],
            'composite_score': round(float(row.get(score_col, 0)), 3),
            'leverage_safe':   int(row.get('leverage_safe', 0)),
            'position_pct':    round(pos_pct * 100, 1),
            'position_€':      round(pos_eur, 0),
            'leverage_mult':   round(leverage, 2),
            'notional_€':      round(pos_eur * leverage, 0),
            'strategy':        strategy,
            'piotroski_f':     row.get('piotroski_f_score'),
            'beneish_m':       row.get('beneish_m_score'),
            'ml_1y':           round(float(ml_1y), 3) if pd.notna(ml_1y) else None,
        })
    return pd.DataFrame(rows)


def build_short_book(df: pd.DataFrame, top_n: int, capital: float) -> pd.DataFrame:
    if 'beneish_m_score' not in df.columns:
        return pd.DataFrame()

    cands = df[df['beneish_m_score'] > MAX_BENEISH].copy()
    if 'market_cap_at_filing' in cands.columns:
        cands = cands[cands['market_cap_at_filing'].fillna(0) > 1e8]
    if len(cands) == 0:
        return pd.DataFrame()

    cands = cands.nlargest(top_n, 'beneish_m_score').copy()
    n      = len(cands)
    weight = min(1.0 / n, MAX_SHORT_SINGLE)

    out_rows = []
    for _, r in cands.iterrows():
        out_rows.append({
            'ticker':      r.get('ticker', ''),
            'name':        str(r.get('name', ''))[:30],
            'beneish_m':   round(float(r['beneish_m_score']), 3),
            'altman_z':    round(float(r['altman_z_score']), 3) if pd.notna(r.get('altman_z_score')) else None,
            'piotroski_f': r.get('piotroski_f_score'),
            'short_pct':   round(weight * SHORT_BOOK_PCT * 100, 1),
            'short_€':     round(capital * weight * SHORT_BOOK_PCT, 0),
        })
    return pd.DataFrame(out_rows)
