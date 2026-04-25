"""
Step 3: Score each company and generate a ranked fraud report.
Composite fraud score 0-100 (higher = more suspicious).

Signal weights:
  Phase 1: Beneish (20), Piotroski (10), Accruals (10), Cash Flow Divergence (10)
  Phase 2: Altman Z-Score (20), Revenue Quality (10), Earnings Quality (10),
           Going Concern (10), Auditor Quality (5), Market signals (5+5), Insider Selling (5)

Score is normalized by available signals — missing data doesn't deflate the score.
"""

import json
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
REPORTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'reports')
os.makedirs(REPORTS_DIR, exist_ok=True)


def composite_score(signals: dict) -> float:
    """
    Combine all signals into a 0-100 fraud risk score.
    Higher = more suspicious.
    """
    score = 0
    max_score = 0

    # ── Phase 1 ──────────────────────────────────────────────────────────────

    # Beneish M-Score (weight: 20)
    beneish = signals.get('beneish', {})
    if beneish.get('score') is not None:
        max_score += 20
        if beneish['manipulator']:
            severity = min((beneish['score'] + 1.78) / 3, 1)
            score += 20 * (0.5 + 0.5 * severity)

    # Piotroski F-Score (weight: 10) — low score is bad
    piotroski = signals.get('piotroski', {})
    if piotroski.get('score') is not None:
        max_score += 10
        score += 10 * (1 - piotroski['score'] / 9)

    # Accruals Ratio (weight: 10)
    accruals = signals.get('accruals', {})
    if accruals.get('ratio') is not None:
        max_score += 10
        if accruals['red_flag']:
            severity = min(accruals['ratio'] / 0.2, 1)
            score += 10 * severity

    # Cash Flow Divergence (weight: 10)
    cfd = signals.get('cash_flow_divergence', {})
    if cfd.get('divergence') is not None:
        max_score += 10
        if cfd['red_flag']:
            severity = min(cfd['divergence'] / 1.0, 1)
            score += 10 * severity

    # ── Phase 2 ──────────────────────────────────────────────────────────────

    # Altman Z-Score (weight: 20) — low score = distress
    altman = signals.get('altman', {})
    if altman.get('score') is not None:
        max_score += 20
        z = altman['score']
        if z < 1.81:
            # Distress zone: scale from 0 to 20 as Z goes from 1.81 to -2
            severity = min((1.81 - z) / 3.81, 1)
            score += 20 * severity
        elif z < 2.99:
            # Grey zone: partial risk
            severity = (2.99 - z) / (2.99 - 1.81)
            score += 10 * severity

    # Revenue Quality (weight: 10)
    rev_quality = signals.get('revenue_quality', {})
    if rev_quality.get('ar_ratio') is not None:
        max_score += 10
        if rev_quality['red_flag']:
            ar = rev_quality['ar_ratio']
            severity = min((ar - 0.25) / 0.5, 1) if ar > 0.25 else 0
            dso = rev_quality.get('dso') or 0
            dso_severity = min((dso - 90) / 270, 1) if dso > 90 else 0
            score += 10 * max(severity, dso_severity)

    # Earnings Quality (weight: 10)
    earn_quality = signals.get('earnings_quality', {})
    if earn_quality.get('non_op_ratio') is not None:
        max_score += 10
        if earn_quality['red_flag']:
            severity = min((earn_quality['non_op_ratio'] - 0.30) / 0.70, 1)
            score += 10 * severity

    # Going Concern (weight: 10) — binary, full weight if flagged
    going_concern = signals.get('going_concern', {})
    if going_concern.get('flagged') is not None:
        max_score += 10
        if going_concern['flagged']:
            score += 10

    # Market signals: Illiquid (weight: 5) + Pump & Dump (weight: 5)
    market = signals.get('market', {})
    if market.get('avg_volume') is not None:
        max_score += 5
        if market.get('illiquid_flag'):
            score += 5

    if market.get('volume_spike_ratio') is not None:
        max_score += 5
        if market.get('pump_dump_flag'):
            # Scale by how extreme the spike is
            spike = market['volume_spike_ratio']
            severity = min((spike - 3.0) / 7.0, 1)
            score += 5 * (0.5 + 0.5 * severity)

    # Insider Selling (weight: 5)
    insider = signals.get('insider', {})
    if insider.get('net_insider_shares') is not None:
        max_score += 5
        if insider.get('insider_selling_flag'):
            score += 5

    # Auditor Quality (weight: 5) — small auditor on a large company is a red flag
    auditor = signals.get('auditor', {})
    if auditor.get('auditor_name') is not None:
        max_score += 5
        if auditor.get('small_auditor_flag'):
            score += 5

    if max_score == 0:
        return None

    return round((score / max_score) * 100, 1)


def risk_label(score: float) -> str:
    if score is None:
        return 'INSUFFICIENT DATA'
    if score >= 70:
        return 'HIGH RISK'
    if score >= 45:
        return 'MEDIUM RISK'
    return 'LOW RISK'


def generate_report(signals_list: list) -> list:
    """Score all companies and return sorted by risk."""
    scored = []

    for s in signals_list:
        fraud_score = composite_score(s)

        beneish  = s.get('beneish', {})
        piotroski = s.get('piotroski', {})
        accruals = s.get('accruals', {})
        cfd      = s.get('cash_flow_divergence', {})
        altman   = s.get('altman', {})
        rev_q    = s.get('revenue_quality', {})
        earn_q   = s.get('earnings_quality', {})
        gc       = s.get('going_concern', {})
        auditor  = s.get('auditor', {})
        market   = s.get('market', {})
        insider  = s.get('insider', {})
        value    = s.get('value', {})

        red_flags = sum(filter(None, [
            beneish.get('manipulator', False),
            piotroski.get('weak', False),
            accruals.get('red_flag', False),
            cfd.get('red_flag', False),
            altman.get('distress', False),
            rev_q.get('red_flag', False),
            earn_q.get('red_flag', False),
            gc.get('flagged', False),
            auditor.get('small_auditor_flag', False),
            market.get('pump_dump_flag', False),
            market.get('illiquid_flag', False),
            insider.get('insider_selling_flag', False),
        ]))

        scored.append({
            # Identity
            'ticker':     s.get('ticker', 'N/A'),
            'name':       s['name'],
            'exchange':   s.get('exchange', 'N/A'),
            'market_cap': s.get('market_cap'),

            # Composite
            'fraud_score':    fraud_score,
            'risk':           risk_label(fraud_score),
            'red_flags_count': red_flags,

            # Phase 1
            'beneish_score':   beneish.get('score'),
            'beneish_flag':    beneish.get('manipulator', False),
            'piotroski_score': piotroski.get('score'),
            'piotroski_weak':  piotroski.get('weak', False),
            'accruals_ratio':  accruals.get('ratio'),
            'accruals_flag':   accruals.get('red_flag', False),
            'cfd_ratio':       cfd.get('divergence'),
            'cfd_flag':        cfd.get('red_flag', False),

            # Phase 2 — financial
            'altman_score':       altman.get('score'),
            'altman_zone':        altman.get('zone'),
            'altman_flag':        altman.get('distress', False),
            'ar_ratio':           rev_q.get('ar_ratio'),
            'dso':                rev_q.get('dso'),
            'revenue_quality_flag': rev_q.get('red_flag', False),
            'non_op_ratio':       earn_q.get('non_op_ratio'),
            'earnings_quality_flag': earn_q.get('red_flag', False),
            'going_concern_flag': gc.get('flagged', False),

            # Phase 2 — auditor
            'auditor_name':         auditor.get('auditor_name'),
            'big4_auditor':         auditor.get('big4_auditor', False),
            'small_auditor_flag':   auditor.get('small_auditor_flag', False),

            # Phase 2 — market
            'avg_volume_90d':     market.get('avg_volume'),
            'volume_spike_ratio': market.get('volume_spike_ratio'),
            'price_change_90d':   market.get('price_change_90d'),
            'illiquid_flag':      market.get('illiquid_flag', False),
            'pump_dump_flag':     market.get('pump_dump_flag', False),

            # Phase 2 — insider
            'net_insider_shares':   insider.get('net_insider_shares'),
            'insider_sale_count':   insider.get('insider_sale_count'),
            'insider_buy_count':    insider.get('insider_buy_count'),
            'insider_selling_flag': insider.get('insider_selling_flag', False),

            # Phase 3 — value metrics (descriptive, not scored)
            'pe_ratio':      value.get('pe_ratio'),
            'pb_ratio':      value.get('pb_ratio'),
            'ev_ebitda':     value.get('ev_ebitda'),
            'fcf_yield':     value.get('fcf_yield'),
            'fcf':           value.get('fcf'),
            'roe':           value.get('roe'),
            'roa':           value.get('roa'),
            'gross_margin':  value.get('gross_margin'),
            'net_margin':    value.get('net_margin'),
            'debt_to_equity': value.get('debt_to_equity'),
            'current_ratio': value.get('current_ratio'),
        })

    scored.sort(key=lambda x: x['fraud_score'] or 0, reverse=True)
    return scored


def print_report(scored: list, top_n: int = 20):
    """Print top N most suspicious companies to terminal."""
    print("\n" + "=" * 80)
    print(f"FRAUD RISK SCREENER — Top {top_n} Most Suspicious Companies")
    print("=" * 80)
    print(f"{'Ticker':<8} {'Risk':<14} {'Score':>6} {'Beneish':>8} {'Altman':>8} {'Flags':>6}  Name")
    print("-" * 80)

    for c in scored[:top_n]:
        score_str   = f"{c['fraud_score']:.1f}" if c['fraud_score'] is not None else 'N/A'
        beneish_str = f"{c['beneish_score']:.2f}" if c['beneish_score'] is not None else 'N/A'
        altman_str  = f"{c['altman_score']:.2f}" if c['altman_score'] is not None else 'N/A'
        print(f"{c['ticker']:<8} {c['risk']:<14} {score_str:>6} {beneish_str:>8} {altman_str:>8} {c['red_flags_count']:>6}  {c['name'][:35]}")

    high   = sum(1 for c in scored if c['risk'] == 'HIGH RISK')
    medium = sum(1 for c in scored if c['risk'] == 'MEDIUM RISK')
    print(f"\nTotal companies screened: {len(scored)}")
    print(f"HIGH RISK: {high} | MEDIUM RISK: {medium} | LOW RISK: {len(scored) - high - medium}")


if __name__ == '__main__':
    input_path = os.path.join(DATA_DIR, 'fraud_signals.json')
    output_path = os.path.join(REPORTS_DIR, 'fraud_report.json')

    with open(input_path) as f:
        signals = json.load(f)

    print(f"Scoring {len(signals)} companies...")
    scored = generate_report(signals)

    with open(output_path, 'w') as f:
        json.dump(scored, f, indent=2)

    print_report(scored, top_n=25)
    print(f"\nFull report saved to {output_path}")
