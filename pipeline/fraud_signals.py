"""
Step 2: Calculate fraud signals for each company
- Beneish M-Score: detects earnings manipulation (threshold: > -1.78 = likely manipulator)
- Piotroski F-Score: financial health (0-9, low score = weak/fraudulent)
- Accruals Ratio: gap between reported earnings and cash (high = red flag)
- Cash Flow Divergence: net income vs operating cash flow (large gap = red flag)
"""

import json
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')


def safe_div(a, b):
    """Safe division — returns None if dividing by zero or None inputs."""
    if a is None or b is None or b == 0:
        return None
    return a / b


def beneish_m_score(c: dict) -> dict:
    """
    Beneish M-Score: 8-variable model to detect earnings manipulation.
    Score > -1.78 strongly suggests manipulation.

    Note: requires two years of data for full accuracy.
    We approximate using available single-year data where needed.
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

    # Days Sales in Receivables Index (DSRI)
    dsri = safe_div(receivables, revenue)

    # Gross Margin Index (GMI) — approximated without prior year
    gmi = safe_div(gross_profit, revenue)

    # Asset Quality Index (AQI)
    non_current_assets = (total_assets or 0) - (current_assets or 0) - (ppe_net or 0)
    aqi = safe_div(non_current_assets, total_assets)

    # Sales Growth Index (SGI) — single year, use revenue/assets as proxy
    sgi = safe_div(revenue, total_assets)

    # Depreciation Index (DEPI)
    depi = safe_div(depreciation, (depreciation or 0) + (ppe_net or 0))

    # Sales General Admin Index (SGAI) — skipped (no SGA data from EDGAR easily)
    sgai = None

    # Leverage Index (LVGI)
    total_debt = (long_term_debt or 0) + (current_liabilities or 0)
    lvgi = safe_div(total_debt, total_assets)

    # Total Accruals to Total Assets (TATA)
    tata = safe_div(
        (net_income or 0) - (operating_cash_flow or 0),
        total_assets
    )

    # Calculate score with available variables
    # Original: -4.84 + 0.920*DSRI + 0.528*GMI + 0.404*AQI + 0.892*SGI
    #           + 0.115*DEPI - 0.172*SGAI + 4.679*TATA - 0.327*LVGI
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
    shares_outstanding = c.get('shares_outstanding') or 1

    roa = net_income / total_assets
    current_ratio = current_assets / current_liabilities
    gross_margin = gross_profit / revenue
    asset_turnover = revenue / total_assets
    accrual = (operating_cash_flow - net_income) / total_assets

    signals = {
        # Profitability
        'F1_positive_roa':      1 if roa > 0 else 0,
        'F2_positive_cfo':      1 if operating_cash_flow > 0 else 0,
        'F3_cfo_gt_income':     1 if operating_cash_flow > net_income else 0,
        'F4_low_accruals':      1 if accrual > 0 else 0,

        # Leverage & Liquidity
        'F5_low_leverage':      1 if long_term_debt / total_assets < 0.4 else 0,
        'F6_good_liquidity':    1 if current_ratio > 1 else 0,
        'F7_no_dilution':       1,  # Assume no new shares (single year data)

        # Operating efficiency
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
    High positive value = earnings are not backed by cash = red flag.
    Threshold: > 0.05 (5%) is concerning.
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
    Large positive divergence = company reports profits but doesn't generate cash.
    Threshold: > 0.25 (25% gap) is a red flag.
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


def calculate_all_signals(companies: list) -> list:
    """Run all fraud signals on every company."""
    results = []

    for c in companies:
        signals = {
            'cik': c['cik'],
            'name': c['name'],
            'ticker': c['ticker'],
            'exchange': c.get('exchange'),
            'total_assets': c.get('total_assets'),
            'revenue': c.get('revenue'),
            'beneish': beneish_m_score(c),
            'piotroski': piotroski_f_score(c),
            'accruals': accruals_ratio(c),
            'cash_flow_divergence': cash_flow_divergence(c),
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
