"""
Plain-English investment thesis generator.

Usage:
    from alpha.explain import explain_pick, explain_many
    text = explain_pick(ticker, df_row)
    report = explain_many(positions_df, raw_df)

    # CLI
    python3 alpha/explain.py --market US --top 15
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).parent.parent
sys.path.insert(0, str(BASE))


# ── Threshold constants ────────────────────────────────────────────────────────
PIOTROSKI_STRONG   = 8
PIOTROSKI_SOLID    = 6
BENEISH_CLEAN      = -2.5   # well below manipulation threshold
BENEISH_OK         = -1.78
ALTMAN_SAFE        = 2.99
ALTMAN_GREY        = 1.81
ML_CONVICTION_HIGH = 0.75
ML_CONVICTION_MED  = 0.60
REG_EXCESS_HIGH    = 0.30   # 30%+ predicted annualised excess
REG_EXCESS_MED     = 0.10


def _fmt_pct(v, decimals=1) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 'n/a'
    return f'{v * 100:.{decimals}f}%'


def _fmt_m(v) -> str:
    """Format a dollar value as $XM or $XB."""
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 'n/a'
    if abs(v) >= 1e9:
        return f'${v/1e9:.1f}B'
    return f'${v/1e6:.0f}M'


def _fmt_x(v, decimals=1) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return 'n/a'
    return f'{v:.{decimals}f}x'


def _piotroski_verdict(score) -> str:
    if pd.isna(score):
        return 'unavailable'
    score = int(score)
    if score >= PIOTROSKI_STRONG:
        return f'{score}/9 — financially very strong (top tier)'
    elif score >= PIOTROSKI_SOLID:
        return f'{score}/9 — financially sound (above quality gate)'
    else:
        return f'{score}/9 — below quality threshold'


def _beneish_verdict(m) -> str:
    if pd.isna(m):
        return 'unavailable'
    if m < BENEISH_CLEAN:
        return f'{m:.2f} — very low manipulation risk (well below −2.50 threshold)'
    elif m < BENEISH_OK:
        return f'{m:.2f} — acceptable (below −1.78 manipulation threshold)'
    else:
        return f'{m:.2f} — ⚠️  above manipulation threshold — do not hold long'


def _altman_verdict(z) -> str:
    if pd.isna(z):
        return 'unavailable'
    if z >= ALTMAN_SAFE:
        return f'{z:.2f} — safe zone (no distress risk)'
    elif z >= ALTMAN_GREY:
        return f'{z:.2f} — grey zone (monitor; not distressed yet)'
    else:
        return f'{z:.2f} — ⚠️  distress zone'


def _growth_narrative(row: pd.Series) -> str:
    rev_g = row.get('revenue_growth_yoy')
    earn_g = row.get('earnings_growth_yoy')
    parts = []
    if not pd.isna(rev_g):
        direction = 'grew' if rev_g > 0 else 'declined'
        parts.append(f'Revenue {direction} {abs(rev_g)*100:.1f}% YoY')
    if not pd.isna(earn_g):
        direction = 'grew' if earn_g > 0 else 'declined'
        parts.append(f'earnings {direction} {abs(earn_g)*100:.1f}% YoY')
    return '. '.join(parts) + '.' if parts else 'Growth data unavailable.'


def _margin_narrative(row: pd.Series) -> str:
    gm  = row.get('gross_margin')
    om  = row.get('operating_margin')
    nm  = row.get('net_margin')
    roe = row.get('return_on_equity')
    roa = row.get('return_on_assets')
    parts = []
    if not pd.isna(gm):
        parts.append(f'Gross margin {gm*100:.1f}%')
    if not pd.isna(om):
        parts.append(f'operating margin {om*100:.1f}%')
    if not pd.isna(nm):
        parts.append(f'net margin {nm*100:.1f}%')
    if not pd.isna(roe):
        parts.append(f'ROE {roe*100:.1f}%')
    if not pd.isna(roa):
        parts.append(f'ROA {roa*100:.1f}%')
    return ', '.join(parts) + '.' if parts else 'Margin data unavailable.'


def _valuation_narrative(row: pd.Series) -> str:
    pb  = row.get('price_to_book')
    pe  = row.get('price_to_earnings')
    ps  = row.get('price_to_sales')
    fcf_y = row.get('fcf_yield')
    cap = row.get('market_cap_at_filing')
    parts = []
    if not pd.isna(pb):
        cheap = 'cheap' if pb < 1.5 else ('fair' if pb < 3.0 else 'premium')
        parts.append(f'P/B {pb:.2f}x ({cheap})')
    if not pd.isna(pe) and pe > 0:
        parts.append(f'P/E {pe:.1f}x')
    if not pd.isna(ps):
        parts.append(f'P/S {ps:.2f}x')
    if not pd.isna(fcf_y):
        parts.append(f'FCF yield {fcf_y*100:.1f}%')
    if not pd.isna(cap):
        parts.append(f'Market cap {_fmt_m(cap)}')
    return ', '.join(parts) + '.' if parts else 'Valuation data unavailable.'


def _ml_narrative(row: pd.Series) -> str:
    ml3y = row.get('ml_score_3y') or row.get('ml_3y')
    pred = row.get('ml_pred_excess_3y')
    parts = []
    if not pd.isna(ml3y if ml3y else float('nan')):
        conf = 'High' if ml3y >= ML_CONVICTION_HIGH else ('Moderate' if ml3y >= ML_CONVICTION_MED else 'Low')
        parts.append(f'3-year outperformance probability: {ml3y*100:.0f}% ({conf} conviction)')
    if not pd.isna(pred if pred else float('nan')):
        lvl = 'strong' if pred >= REG_EXCESS_HIGH else ('moderate' if pred >= REG_EXCESS_MED else 'mild')
        parts.append(f'Predicted 3-year excess return vs market: +{pred*100:.1f}% ({lvl} magnitude signal)')
    return '. '.join(parts) + '.' if parts else 'Model scores unavailable.'


def _strategy_verdict(row: pd.Series, leverage_mult: float = 1.0) -> str:
    ml3y = row.get('ml_score_3y') or row.get('ml_3y') or 0
    cap  = row.get('market_cap_at_filing') or 0
    if ml3y >= ML_CONVICTION_HIGH and cap > 1e9:
        return (f'Recommended strategy: LEAPS calls (12–18mo, delta ~0.70), '
                f'{leverage_mult:.1f}x effective leverage. '
                f'High-conviction large-cap — options give asymmetric upside with capped downside.')
    elif ml3y >= ML_CONVICTION_MED:
        return (f'Recommended strategy: 2:1 margin long at IBKR (~5.8% borrow cost), '
                f'{leverage_mult:.1f}x leverage. '
                f'Stock must beat ~6% annually to profit after borrowing costs.')
    else:
        return 'Recommended strategy: Equity-only position. Conviction below leverage threshold.'


def _risk_flags(row: pd.Series) -> list[str]:
    flags = []
    rev_g = row.get('revenue_growth_yoy')
    if not pd.isna(rev_g) and rev_g < -0.15:
        flags.append(f'⚠️  Revenue down {abs(rev_g)*100:.0f}% YoY — declining top line')
    gm = row.get('gross_margin')
    if not pd.isna(gm) and gm < 0.15:
        flags.append(f'⚠️  Thin gross margin ({gm*100:.1f}%) — limited pricing power')
    cr = row.get('current_ratio')
    if not pd.isna(cr) and cr < 1.0:
        flags.append(f'⚠️  Current ratio {cr:.2f} — short-term liquidity concern')
    z = row.get('altman_z_score')
    if not pd.isna(z) and z < ALTMAN_SAFE:
        flags.append(f'⚠️  Altman Z in grey zone ({z:.2f}) — monitor solvency')
    ml3y = row.get('ml_score_3y') or row.get('ml_3y')
    if ml3y and not pd.isna(ml3y) and ml3y < 0.60:
        flags.append('⚠️  ML conviction below 60% — recommend equity-only, no leverage')
    return flags


def explain_pick(ticker: str, row: pd.Series) -> str:
    """Generate a plain-English investment thesis for a single stock."""
    name = str(row.get('name', ticker))[:40]
    yr   = row.get('fiscal_year', 'latest')
    cap  = _fmt_m(row.get('market_cap_at_filing'))

    pred_excess = row.get('ml_pred_excess_3y')
    lev_mult    = float(row.get('leverage_mult', 1.0))
    pos_pct     = row.get('position_pct', 0)
    strategy    = row.get('strategy', '')

    lines = [
        f"{'─'*60}",
        f"  {ticker} — {name}",
        f"  Fiscal year: {yr}  |  Market cap: {cap}",
        f"{'─'*60}",
        "",
        "WHY BUY",
        "-------",
    ]

    # 1. Predicted excess return (headline number)
    if not pd.isna(pred_excess if pred_excess else float('nan')):
        lines.append(
            f"The quantitative model predicts this stock will outperform the market "
            f"by +{pred_excess*100:.1f}% over the next 3 years (annualised). "
            f"This is the primary reason it ranked in the long book."
        )
    lines.append("")

    # 2. Financial quality
    lines.append("FINANCIAL QUALITY (Piotroski F-Score)")
    lines.append(f"  {_piotroski_verdict(row.get('piotroski_f_score'))}")
    lines.append(f"  {_margin_narrative(row)}")
    lines.append(f"  {_growth_narrative(row)}")
    lines.append("")

    # 3. Fraud/manipulation check
    lines.append("FRAUD & MANIPULATION RISK (Beneish M-Score)")
    lines.append(f"  {_beneish_verdict(row.get('beneish_m_score'))}")
    lines.append("")

    # 4. Bankruptcy risk
    lines.append("BANKRUPTCY / DISTRESS RISK (Altman Z-Score)")
    lines.append(f"  {_altman_verdict(row.get('altman_z_score'))}")
    lines.append("")

    # 5. Valuation
    lines.append("VALUATION")
    lines.append(f"  {_valuation_narrative(row)}")
    lines.append("")

    # 6. ML signal
    lines.append("ML SIGNAL")
    lines.append(f"  {_ml_narrative(row)}")
    lines.append("")

    # 7. Recommended trade
    lines.append("RECOMMENDED TRADE")
    lines.append(f"  {_strategy_verdict(row, lev_mult)}")
    if pos_pct:
        lines.append(f"  Position size: {pos_pct:.1f}% of portfolio")
    lines.append("")

    # 8. Risk flags
    flags = _risk_flags(row)
    if flags:
        lines.append("RISK FLAGS")
        for f in flags:
            lines.append(f"  {f}")
        lines.append("")

    lines.append("MARGIN OF SAFETY CHECKLIST")
    pf = row.get('piotroski_f_score', 0) or 0
    bm = row.get('beneish_m_score', 0) or 0
    az = row.get('altman_z_score', 0) or 0
    pb = row.get('price_to_book', 99) or 99
    lines.append(f"  {'✅' if pf >= 6 else '❌'} Piotroski ≥ 6       ({pf:.0f})")
    lines.append(f"  {'✅' if bm < -1.78 else '❌'} Beneish < −1.78   ({bm:.2f})")
    lines.append(f"  {'✅' if az > 1.81 else '❌'} Altman Z > 1.81   ({az:.2f})")
    lines.append(f"  {'✅' if pb < 5.0 else '❌'} P/B < 5.0          ({pb:.2f}x)")
    lines.append(f"  {'─'*40}")
    passed = sum([pf >= 6, bm < -1.78, az > 1.81, pb < 5.0])
    lines.append(f"  {passed}/4 criteria met — {'Strong margin of safety' if passed == 4 else 'Partial margin of safety'}")
    lines.append("")

    return '\n'.join(lines)


def explain_many(positions_df: pd.DataFrame, raw_df: pd.DataFrame) -> str:
    """Generate explanations for every ticker in a positions DataFrame."""
    sections = []
    for _, pos_row in positions_df.iterrows():
        ticker = pos_row.get('ticker', '')
        raw_rows = raw_df[raw_df['ticker'] == ticker]
        if raw_rows.empty:
            continue
        raw_row = raw_rows.iloc[0].copy()
        # merge position-level fields into raw row for a single merged view
        for col in ['ml_pred_excess_3y', 'ml_3y', 'leverage_mult', 'position_pct', 'strategy']:
            if col in pos_row.index:
                raw_row[col] = pos_row[col]
        sections.append(explain_pick(ticker, raw_row))
    return '\n'.join(sections)


# ── CLI ────────────────────────────────────────────────────────────────────────

def _cli_main():
    parser = argparse.ArgumentParser(description='Explain leverage picks in plain English')
    parser.add_argument('--market',   default='US', help='Market code (US, KR, CA …)')
    parser.add_argument('--top',      default=15,  type=int, help='Top N picks to explain')
    parser.add_argument('--capital',  default=50000, type=float, help='Portfolio capital')
    parser.add_argument('--output',   default=None, help='Write output to this .txt file')
    args = parser.parse_args()

    # Lazy import to avoid circular deps
    from pipeline.feature_library import add_normalised_ratios, add_piotroski_ext
    from scripts.leverage_strategy import (
        load_data, load_models, score_tickers, composite_score,
        size_positions, REGRESSION_COL
    )

    print(f'Loading {args.market} data...')
    df = load_data(args.market)
    models = load_models()
    df = score_tickers(df, models)
    df = composite_score(df)
    positions = size_positions(df, args.top, args.capital)

    report = explain_many(positions, df)
    print(report)

    if args.output:
        Path(args.output).write_text(report)
        print(f'Saved: {args.output}')


if __name__ == '__main__':
    _cli_main()
