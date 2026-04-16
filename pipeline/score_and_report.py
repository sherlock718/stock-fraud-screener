"""
Step 3: Score each company and generate a ranked fraud report.
Composite fraud score 0-100 (higher = more suspicious).
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

    # Beneish M-Score (weight: 40)
    beneish = signals.get('beneish', {})
    if beneish.get('score') is not None:
        max_score += 40
        if beneish['manipulator']:
            # Scale: -1.78 is threshold. Higher = worse.
            severity = min((beneish['score'] + 1.78) / 3, 1)  # cap at +3 above threshold
            score += 40 * (0.5 + 0.5 * severity)
        else:
            score += 0

    # Piotroski F-Score (weight: 25) — low score is bad
    piotroski = signals.get('piotroski', {})
    if piotroski.get('score') is not None:
        max_score += 25
        # Invert: score 0 = 25 points risk, score 9 = 0 points risk
        score += 25 * (1 - piotroski['score'] / 9)

    # Accruals Ratio (weight: 20)
    accruals = signals.get('accruals', {})
    if accruals.get('ratio') is not None:
        max_score += 20
        if accruals['red_flag']:
            severity = min(accruals['ratio'] / 0.2, 1)  # cap at 20% accruals
            score += 20 * severity

    # Cash Flow Divergence (weight: 15)
    cfd = signals.get('cash_flow_divergence', {})
    if cfd.get('divergence') is not None:
        max_score += 15
        if cfd['red_flag']:
            severity = min(cfd['divergence'] / 1.0, 1)  # cap at 100% divergence
            score += 15 * severity

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
        scored.append({
            'ticker': s.get('ticker', 'N/A'),
            'name': s['name'],
            'exchange': s.get('exchange', 'N/A'),
            'fraud_score': fraud_score,
            'risk': risk_label(fraud_score),
            'beneish_score': s['beneish'].get('score'),
            'beneish_flag': s['beneish'].get('manipulator', False),
            'piotroski_score': s['piotroski'].get('score'),
            'piotroski_weak': s['piotroski'].get('weak', False),
            'accruals_ratio': s['accruals'].get('ratio'),
            'accruals_flag': s['accruals'].get('red_flag', False),
            'cfd_ratio': s['cash_flow_divergence'].get('divergence'),
            'cfd_flag': s['cash_flow_divergence'].get('red_flag', False),
            'red_flags_count': sum([
                s['beneish'].get('manipulator', False),
                s['piotroski'].get('weak', False),
                s['accruals'].get('red_flag', False),
                s['cash_flow_divergence'].get('red_flag', False),
            ])
        })

    # Sort by fraud score descending (most suspicious first)
    scored.sort(key=lambda x: x['fraud_score'] or 0, reverse=True)
    return scored


def print_report(scored: list, top_n: int = 20):
    """Print top N most suspicious companies to terminal."""
    print("\n" + "="*80)
    print(f"FRAUD RISK SCREENER — Top {top_n} Most Suspicious Companies")
    print("="*80)
    print(f"{'Ticker':<8} {'Risk':<14} {'Score':>6} {'Beneish':>8} {'Piotroski':>10} {'Flags':>6}  Name")
    print("-"*80)

    for c in scored[:top_n]:
        score_str = f"{c['fraud_score']:.1f}" if c['fraud_score'] is not None else 'N/A'
        beneish_str = f"{c['beneish_score']:.2f}" if c['beneish_score'] is not None else 'N/A'
        piotroski_str = str(c['piotroski_score']) if c['piotroski_score'] is not None else 'N/A'

        print(f"{c['ticker']:<8} {c['risk']:<14} {score_str:>6} {beneish_str:>8} {piotroski_str:>10} {c['red_flags_count']:>6}  {c['name'][:35]}")

    # Summary stats
    high_risk = sum(1 for c in scored if c['risk'] == 'HIGH RISK')
    medium_risk = sum(1 for c in scored if c['risk'] == 'MEDIUM RISK')
    print(f"\nTotal companies screened: {len(scored)}")
    print(f"HIGH RISK: {high_risk} | MEDIUM RISK: {medium_risk} | LOW RISK: {len(scored) - high_risk - medium_risk}")


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
