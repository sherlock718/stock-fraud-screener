"""
Step 2: Calculate fraud signals for each company.

Phase 1 signals:
- Beneish M-Score: detects earnings manipulation (threshold: > -1.78)
- Piotroski F-Score: financial health (0-9, low = weak/fraudulent)
- Accruals Ratio: gap between reported earnings and cash (high = red flag)
- Cash Flow Divergence: net income vs operating cash flow (large gap = red flag)

Phase 2 signals:
- Altman Z-Score: bankruptcy/distress predictor (< 1.81 = distress zone)
- Revenue Quality: receivables ratio + DSO (high AR = fake/early revenue recognition)
- Earnings Quality: operating vs net income gap (non-operating items inflating earnings)
- Going Concern: flag from SEC filing disclosure
- Auditor Quality: auditor name (Big 4 vs unknown firm), small auditor flag for large companies
"""

import json
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

try:
    from value_metrics import calculate_value_metrics
except ImportError:
    from pipeline.value_metrics import calculate_value_metrics


def safe_div(a, b):
    """Safe division — returns None if dividing by zero or None inputs."""
    if a is None or b is None or b == 0:
        return None
    return a / b


# ── Phase 1 Signals ───────────────────────────────────────────────────────────

def beneish_m_score(c: dict) -> dict:
    """
    Beneish M-Score: 8-variable model to detect earnings manipulation.
    Score > -1.78 strongly suggests manipulation.
    """
    total_assets = c.get('total_assets')
    revenue = c.get('revenue')
    receivables = c.get('receivables')
    net_income = c.get('net_income')
    operating_cash_flow = c.get('operating_cash_flow')
    gross_profit = c.get('gross_profit')
    ppe_net = c.get('ppe_net')
    current_assets = c.get('current_assets')
    current_liabilities = c.get('current_liabilities')
    long_term_debt = c.get('long_term_debt')
    depreciation = c.get('depreciation')

    dsri = safe_div(receivables, revenue)
    gmi = safe_div(gross_profit, revenue)

    non_current_assets = (total_assets or 0) - (current_assets or 0) - (ppe_net or 0)
    aqi = safe_div(non_current_assets, total_assets)

    sgi = safe_div(revenue, total_assets)
    depi = safe_div(depreciation, (depreciation or 0) + (ppe_net or 0))

    total_debt = (long_term_debt or 0) + (current_liabilities or 0)
    lvgi = safe_div(total_debt, total_assets)

    tata = safe_div(
        (net_income or 0) - (operating_cash_flow or 0),
        total_assets
    )

    score = -4.84
    components = {}

    if dsri is not None:
        score += 0.920 * dsri
        components['dsri'] = round(dsri, 4)
    if gmi is not None:
        score += 0.528 * gmi
        components['gmi'] = round(gmi, 4)
    if aqi is not None:
        score += 0.404 * aqi
        components['aqi'] = round(aqi, 4)
    if sgi is not None:
        score += 0.892 * sgi
        components['sgi'] = round(sgi, 4)
    if depi is not None:
        score += 0.115 * depi
        components['depi'] = round(depi, 4)
    if tata is not None:
        score += 4.679 * tata
        components['tata'] = round(tata, 4)
    if lvgi is not None:
        score -= 0.327 * lvgi
        components['lvgi'] = round(lvgi, 4)

    return {
        'score': round(score, 4),
        'manipulator': score > -1.78,
        'components': components
    }


def piotroski_f_score(c: dict) -> dict:
    """
    Piotroski F-Score: 9 binary signals of financial health.
    Score 0-2: weak (red flag), 3-6: neutral, 7-9: strong.
    """
    net_income = c.get('net_income') or 0
    operating_cash_flow = c.get('operating_cash_flow') or 0
    total_assets = c.get('total_assets') or 1
    current_assets = c.get('current_assets') or 0
    current_liabilities = c.get('current_liabilities') or 1
    long_term_debt = c.get('long_term_debt') or 0
    gross_profit = c.get('gross_profit') or 0
    revenue = c.get('revenue') or 1

    roa = net_income / total_assets
    current_ratio = current_assets / current_liabilities
    gross_margin = gross_profit / revenue
    asset_turnover = revenue / total_assets
    accrual = (operating_cash_flow - net_income) / total_assets

    signals = {
        'F1_positive_roa':      1 if roa > 0 else 0,
        'F2_positive_cfo':      1 if operating_cash_flow > 0 else 0,
        'F3_cfo_gt_income':     1 if operating_cash_flow > net_income else 0,
        'F4_low_accruals':      1 if accrual > 0 else 0,
        'F5_low_leverage':      1 if long_term_debt / total_assets < 0.4 else 0,
        'F6_good_liquidity':    1 if current_ratio > 1 else 0,
        'F7_no_dilution':       1,
        'F8_improving_margin':  1 if gross_margin > 0.2 else 0,
        'F9_asset_turnover':    1 if asset_turnover > 0.5 else 0,
    }

    score = sum(signals.values())
    return {
        'score': score,
        'weak': score <= 2,
        'signals': signals
    }


def accruals_ratio(c: dict) -> dict:
    """
    Accruals Ratio = (Net Income - Operating Cash Flow) / Total Assets
    > 0.05 = red flag.
    """
    net_income = c.get('net_income')
    operating_cash_flow = c.get('operating_cash_flow')
    total_assets = c.get('total_assets')

    ratio = safe_div(
        (net_income or 0) - (operating_cash_flow or 0),
        total_assets
    )

    return {
        'ratio': round(ratio, 4) if ratio is not None else None,
        'red_flag': ratio is not None and ratio > 0.05
    }


def cash_flow_divergence(c: dict) -> dict:
    """
    Cash Flow Divergence = (Net Income - Operating Cash Flow) / |Net Income|
    > 0.25 = red flag.
    """
    net_income = c.get('net_income')
    operating_cash_flow = c.get('operating_cash_flow')

    if not net_income or net_income == 0:
        return {'divergence': None, 'red_flag': False}

    divergence = (net_income - (operating_cash_flow or 0)) / abs(net_income)

    return {
        'divergence': round(divergence, 4),
        'red_flag': divergence > 0.25
    }


# ── Phase 2 Signals ───────────────────────────────────────────────────────────

def altman_z_score(c: dict) -> dict:
    """
    Altman Z-Score: predicts financial distress and bankruptcy.
    Z > 2.99 = Safe zone
    1.81-2.99 = Grey zone
    Z < 1.81 = Distress zone (high fraud/failure risk)

    Z = 1.2*X1 + 1.4*X2 + 3.3*X3 + 0.6*X4 + 1.0*X5
    X1 = Working Capital / Total Assets
    X2 = Retained Earnings / Total Assets
    X3 = EBIT / Total Assets
    X4 = Market Cap / Total Liabilities
    X5 = Revenue / Total Assets
    """
    total_assets = c.get('total_assets')
    current_assets = c.get('current_assets')
    current_liabilities = c.get('current_liabilities')
    retained_earnings = c.get('retained_earnings')
    operating_income = c.get('operating_income')
    market_cap = c.get('market_cap')
    total_liabilities = c.get('total_liabilities')
    long_term_debt = c.get('long_term_debt')
    revenue = c.get('revenue')

    if not total_assets or total_assets == 0:
        return {'score': None, 'zone': None, 'distress': False}

    x1 = safe_div(
        (current_assets or 0) - (current_liabilities or 0),
        total_assets
    )
    x2 = safe_div(retained_earnings, total_assets)
    x3 = safe_div(operating_income, total_assets)

    book_debt = total_liabilities or ((long_term_debt or 0) + (current_liabilities or 0))
    x4 = safe_div(market_cap, book_debt) if market_cap else None

    x5 = safe_div(revenue, total_assets)

    score = 0.0
    components_used = 0
    if x1 is not None:
        score += 1.2 * x1
        components_used += 1
    if x2 is not None:
        score += 1.4 * x2
        components_used += 1
    if x3 is not None:
        score += 3.3 * x3
        components_used += 1
    if x4 is not None:
        score += 0.6 * x4
        components_used += 1
    if x5 is not None:
        score += 1.0 * x5
        components_used += 1

    if components_used < 3:
        return {'score': None, 'zone': None, 'distress': False}

    if score > 2.99:
        zone = 'safe'
    elif score > 1.81:
        zone = 'grey'
    else:
        zone = 'distress'

    return {
        'score': round(score, 4),
        'zone': zone,
        'distress': zone == 'distress',
    }


def revenue_quality(c: dict) -> dict:
    """
    Revenue Quality signals:
    1. AR Ratio (Receivables / Revenue) > 0.25 = revenue not converting to cash
    2. Days Sales Outstanding (DSO) > 90 days = collecting very slowly

    High AR relative to revenue = revenue may be booked before cash is collected,
    or outright fictional. Classic early sign of revenue fraud.
    """
    revenue = c.get('revenue')
    receivables = c.get('receivables')

    if not revenue or revenue == 0:
        return {'ar_ratio': None, 'dso': None, 'red_flag': False}

    ar_ratio = safe_div(receivables, revenue)
    dso = (receivables / revenue * 365) if receivables is not None else None

    red_flag = (ar_ratio is not None and ar_ratio > 0.25) or \
               (dso is not None and dso > 90)

    return {
        'ar_ratio': round(ar_ratio, 4) if ar_ratio is not None else None,
        'dso': round(dso, 1) if dso is not None else None,
        'red_flag': red_flag
    }


def earnings_quality(c: dict) -> dict:
    """
    Earnings Quality: measures how much of net income comes from non-operating sources.

    Non-operating boost ratio = (Net Income - Operating Income) / |Net Income|
    > 0.30 = red flag (30%+ of earnings from asset sales, tax windfalls, one-time gains)

    Companies that depend on non-operating income to show profit are masking
    deteriorating core business — common pre-fraud pattern.
    """
    net_income = c.get('net_income')
    operating_income = c.get('operating_income')

    if not net_income or net_income == 0 or operating_income is None:
        return {'non_op_ratio': None, 'red_flag': False}

    non_op_ratio = (net_income - operating_income) / abs(net_income)

    return {
        'non_op_ratio': round(non_op_ratio, 4),
        'red_flag': non_op_ratio > 0.30
    }


def going_concern(c: dict) -> dict:
    """
    Going concern flag — extracted from SEC EDGAR XBRL filing.
    True if the company has disclosed substantial doubt about ability to continue.
    Binary high-severity signal.
    """
    flagged = bool(c.get('going_concern', False))
    return {
        'flagged': flagged,
        'red_flag': flagged
    }


def auditor_signals(c: dict) -> dict:
    """
    Auditor quality signals enriched by enrich_auditor_going_concern.py.

    small_auditor_flag: True when a company with market cap > $50M uses an auditor
    outside the top recognised firms. Fraudulent companies often shop for auditors
    who won't ask difficult questions — small/obscure auditor on a large company
    is a well-documented red flag.
    """
    return {
        'auditor_name':       c.get('auditor_name'),
        'big4_auditor':       bool(c.get('big4_auditor', False)),
        'small_auditor_flag': bool(c.get('small_auditor_flag', False)),
    }


# ── Enrichment signals (added by separate scripts) ───────────────────────────

def market_signals(c: dict) -> dict:
    """
    Market-based signals enriched by enrich_market_signals.py.
    Returns stored values or empty defaults.
    """
    return {
        'avg_volume':         c.get('avg_volume_90d'),
        'volume_spike_ratio': c.get('volume_spike_ratio'),
        'price_change_90d':   c.get('price_change_90d'),
        'illiquid_flag':      bool(c.get('illiquid_flag', False)),
        'pump_dump_flag':     bool(c.get('pump_dump_flag', False)),
    }


def insider_signals(c: dict) -> dict:
    """
    Insider selling signals enriched by enrich_insider_signals.py.
    Returns stored values or empty defaults.
    """
    return {
        'net_insider_shares':   c.get('net_insider_shares'),
        'insider_sale_count':   c.get('insider_sale_count'),
        'insider_buy_count':    c.get('insider_buy_count'),
        'insider_selling_flag': bool(c.get('insider_selling_flag', False)),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def calculate_all_signals(companies: list) -> list:
    """Run all fraud signals on every company."""
    results = []

    for c in companies:
        signals = {
            'cik':        c['cik'],
            'name':       c['name'],
            'ticker':     c['ticker'],
            'exchange':   c.get('exchange'),
            'market_cap': c.get('market_cap'),
            'total_assets': c.get('total_assets'),
            'revenue':    c.get('revenue'),

            # Phase 1
            'beneish':             beneish_m_score(c),
            'piotroski':           piotroski_f_score(c),
            'accruals':            accruals_ratio(c),
            'cash_flow_divergence': cash_flow_divergence(c),

            # Phase 2
            'altman':              altman_z_score(c),
            'revenue_quality':     revenue_quality(c),
            'earnings_quality':    earnings_quality(c),
            'going_concern':       going_concern(c),
            'auditor':             auditor_signals(c),
            'market':              market_signals(c),
            'insider':             insider_signals(c),
            'value':               calculate_value_metrics(c),
        }
        results.append(signals)

    return results


if __name__ == '__main__':
    input_path = os.path.join(DATA_DIR, 'companies_financials.json')
    output_path = os.path.join(DATA_DIR, 'fraud_signals.json')

    with open(input_path) as f:
        companies = json.load(f)

    print(f"Calculating fraud signals for {len(companies)} companies...")
    results = calculate_all_signals(companies)

    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"Saved signals to {output_path}")
