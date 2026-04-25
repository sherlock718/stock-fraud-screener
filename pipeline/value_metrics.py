"""
Phase 3 — Value Metrics.

These are descriptive investment ratios — they do NOT contribute to the fraud score.
They are stored in the report and displayed in the UI for investment analysis.
In Phase 4b they become ML features alongside the fraud signals.

Metrics:
  P/E        Price-to-Earnings        market_cap / net_income
  P/B        Price-to-Book            market_cap / shareholders_equity
  EV/EBITDA  Enterprise Value ratio   (market_cap + debt - cash) / (op_income + depreciation)
  FCF Yield  Free Cash Flow yield     (operating_cf - capex) / market_cap
  ROE        Return on Equity         net_income / shareholders_equity
  ROA        Return on Assets         net_income / total_assets
  Gross Margin                        gross_profit / revenue
  Net Margin                          net_income / revenue
  Debt/Equity                         long_term_debt / shareholders_equity
  Current Ratio                       current_assets / current_liabilities
"""


def safe_div(a, b):
    if a is None or b is None or b == 0:
        return None
    return a / b


def calculate_value_metrics(c: dict) -> dict:
    """
    Calculate all Phase 3 value metrics for a company.
    Returns a flat dict — None for any metric where data is insufficient.
    """
    market_cap        = c.get('market_cap')
    net_income        = c.get('net_income')
    total_assets      = c.get('total_assets')
    total_liabilities = c.get('total_liabilities')
    long_term_debt    = c.get('long_term_debt') or 0
    current_assets    = c.get('current_assets') or 0
    current_liabilities = c.get('current_liabilities') or 0
    receivables       = c.get('receivables') or 0
    inventory         = c.get('inventory') or 0
    operating_income  = c.get('operating_income')
    depreciation      = c.get('depreciation') or 0
    operating_cf      = c.get('operating_cash_flow')
    capex             = c.get('capex') or 0
    gross_profit      = c.get('gross_profit')
    revenue           = c.get('revenue')

    # Shareholders' equity
    equity = None
    if total_assets is not None and total_liabilities is not None:
        equity = total_assets - total_liabilities

    result = {}

    # ── P/E ──────────────────────────────────────────────────────────────────
    # Only meaningful when earnings are positive
    if market_cap and net_income and net_income > 0:
        result['pe_ratio'] = round(market_cap / net_income, 2)
    else:
        result['pe_ratio'] = None

    # ── P/B ──────────────────────────────────────────────────────────────────
    if market_cap and equity and equity > 0:
        result['pb_ratio'] = round(market_cap / equity, 2)
    else:
        result['pb_ratio'] = None

    # ── EV / EBITDA ───────────────────────────────────────────────────────────
    # Cash proxy = current_assets - receivables - inventory
    # EV = market_cap + long_term_debt - cash_proxy
    # EBITDA = operating_income + depreciation
    if market_cap and operating_income is not None:
        cash_proxy = max(0, current_assets - receivables - inventory)
        ev         = market_cap + long_term_debt - cash_proxy
        ebitda     = operating_income + depreciation
        result['ev']       = round(ev)
        result['ebitda']   = round(ebitda)
        result['ev_ebitda'] = round(ev / ebitda, 2) if ebitda > 0 else None
    else:
        result['ev']        = None
        result['ebitda']    = None
        result['ev_ebitda'] = None

    # ── FCF Yield ─────────────────────────────────────────────────────────────
    if operating_cf is not None and market_cap and market_cap > 0:
        fcf = operating_cf - capex
        result['fcf']       = round(fcf)
        result['fcf_yield'] = round(fcf / market_cap, 4)
    else:
        result['fcf']       = None
        result['fcf_yield'] = None

    # ── ROE ───────────────────────────────────────────────────────────────────
    if net_income is not None and equity and equity > 0:
        result['roe'] = round(net_income / equity, 4)
    else:
        result['roe'] = None

    # ── ROA ───────────────────────────────────────────────────────────────────
    if net_income is not None and total_assets and total_assets > 0:
        result['roa'] = round(net_income / total_assets, 4)
    else:
        result['roa'] = None

    # ── Gross Margin ──────────────────────────────────────────────────────────
    if gross_profit is not None and revenue and revenue > 0:
        result['gross_margin'] = round(gross_profit / revenue, 4)
    else:
        result['gross_margin'] = None

    # ── Net Margin ────────────────────────────────────────────────────────────
    if net_income is not None and revenue and revenue > 0:
        result['net_margin'] = round(net_income / revenue, 4)
    else:
        result['net_margin'] = None

    # ── Debt / Equity ─────────────────────────────────────────────────────────
    if equity and equity > 0:
        result['debt_to_equity'] = round(long_term_debt / equity, 4)
    else:
        result['debt_to_equity'] = None

    # ── Current Ratio ─────────────────────────────────────────────────────────
    if current_assets and current_liabilities and current_liabilities > 0:
        result['current_ratio'] = round(current_assets / current_liabilities, 2)
    else:
        result['current_ratio'] = None

    return result
