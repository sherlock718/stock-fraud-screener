"""
Build a historical dataset for ML training — v3 (100+ features).

For each company, pulls multi-year 10-K financial data from EDGAR's company-facts
API (which stores full time-series history), then aligns each annual filing with:
  - Stock price on the filing date (entry price)
  - Stock price 12/24 months later (exit price)
  - 12/24-month forward return + beat_sp500 labels

Feature categories (~105 total):
  A. Fraud signals — Beneish, Piotroski, Accruals, CFD, Altman, Revenue/Earnings quality (15)
  B. Value metrics — Earnings yield, ROC, NCAV, gross profitability, CROIC, ratios (16)
  C. Cross-sectional accounting — margins, turnover, leverage, accruals, quality (25)
  D. YoY change features — growth rates, margin trends, deterioration signals (19)
  E. Price/momentum — prior 3/6/12m return, 52w high ratio, realized vol (5)
  F. Binary flags — derived alerts from combinations of signals (6)

Key fix vs v1/v2: historical market_cap is computed as entry_price × shares_outstanding
(not current market cap), avoiding look-ahead bias in value metrics.

Usage:
  python3 pipeline/build_historical_dataset.py
  python3 pipeline/build_historical_dataset.py --limit 100   # test run
  python3 pipeline/build_historical_dataset.py --years 3     # last 3 fiscal years
"""

import json
import os
import time
import argparse
import requests
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional

try:
    from fraud_signals import (
        beneish_m_score, piotroski_f_score, accruals_ratio,
        cash_flow_divergence, altman_z_score, revenue_quality,
        earnings_quality, going_concern
    )
    from value_metrics import calculate_value_metrics
except ImportError:
    from pipeline.fraud_signals import (
        beneish_m_score, piotroski_f_score, accruals_ratio,
        cash_flow_divergence, altman_z_score, revenue_quality,
        earnings_quality, going_concern
    )
    from pipeline.value_metrics import calculate_value_metrics

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
FINANCIALS_PATH = os.path.join(DATA_DIR, 'companies_financials.json')
OUTPUT_PATH = os.path.join(DATA_DIR, 'historical_dataset.parquet')
CHECKPOINT_PATH = os.path.join(DATA_DIR, 'historical_checkpoint.json')

EDGAR_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
HEADERS = {'User-Agent': 'stock-fraud-screener research@example.com'}

# EDGAR concept → our field name
# First match wins per field (order matters for aliases)
CONCEPT_MAP = {
    # Revenue (multiple GAAP names)
    'us-gaap/Revenues':                                     'revenue',
    'us-gaap/RevenueFromContractWithCustomerExcludingAssessedTax': 'revenue',
    'us-gaap/SalesRevenueNet':                              'revenue',
    # Income statement
    'us-gaap/NetIncomeLoss':                                'net_income',
    'us-gaap/OperatingIncomeLoss':                          'operating_income',
    'us-gaap/GrossProfit':                                  'gross_profit',
    'us-gaap/IncomeTaxExpenseBenefit':                      'tax_expense',
    'us-gaap/InterestExpense':                              'interest_expense',
    'us-gaap/InterestAndDebtExpense':                       'interest_expense',
    'us-gaap/ResearchAndDevelopmentExpense':                'rd_expense',
    'us-gaap/SellingGeneralAndAdministrativeExpense':       'sga_expense',
    # Balance sheet — assets
    'us-gaap/Assets':                                       'total_assets',
    'us-gaap/AssetsCurrent':                                'current_assets',
    'us-gaap/CashAndCashEquivalentsAtCarryingValue':        'cash',
    'us-gaap/AccountsReceivableNetCurrent':                 'receivables',
    'us-gaap/InventoryNet':                                 'inventory',
    'us-gaap/PropertyPlantAndEquipmentNet':                 'ppe_net',
    # Balance sheet — liabilities & equity
    'us-gaap/Liabilities':                                  'total_liabilities',
    'us-gaap/LiabilitiesCurrent':                           'current_liabilities',
    'us-gaap/LongTermDebt':                                 'long_term_debt',
    'us-gaap/LongTermDebtNoncurrent':                       'long_term_debt',
    'us-gaap/RetainedEarningsAccumulatedDeficit':            'retained_earnings',
    'us-gaap/StockholdersEquity':                           'equity',
    # Cash flow
    'us-gaap/NetCashProvidedByUsedInOperatingActivities':   'operating_cash_flow',
    'us-gaap/PaymentsToAcquirePropertyPlantAndEquipment':   'capex',
    # Other
    'us-gaap/DepreciationDepletionAndAmortization':         'depreciation',
    'us-gaap/Depreciation':                                 'depreciation',
    'us-gaap/CostsAndExpenses':                             'total_expenses',
    'us-gaap/CommonStockSharesOutstanding':                 'shares_outstanding',
    'us-gaap/EarningsPerShareBasic':                        'eps_basic',
    'us-gaap/EarningsPerShareDiluted':                      'eps_diluted',
    'us-gaap/CommonStockDividendsPerShareDeclared':         'dividends_per_share',
}

MIN_FISCAL_YEAR = datetime.now().year - 5   # last 5 fiscal years


# ── EDGAR helpers ──────────────────────────────────────────────────────────────

def fetch_company_facts(cik: str) -> dict:
    """Fetch all historical XBRL facts for a company from EDGAR."""
    cik_padded = str(cik).zfill(10)
    url = EDGAR_FACTS_URL.format(cik=cik_padded)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return {}


def extract_annual_values(facts: dict, concept_path: str) -> dict:
    """
    Extract annual 10-K values for a concept from EDGAR facts.
    Returns {fiscal_year: (value, filed_date)}.
    """
    namespace, concept = concept_path.split('/')
    try:
        concept_data = facts.get('facts', {}).get(namespace, {}).get(concept, {})
        units = concept_data.get('units', {})
        # Try USD first, then shares, then USD/shares (for EPS)
        values = units.get('USD', units.get('shares', units.get('USD/shares', [])))
    except Exception:
        return {}

    annual = {}
    for item in values:
        if item.get('form') not in ('10-K', '10-K/A'):
            continue
        fy = item.get('fy')
        fp = item.get('fp', '')
        if not fy or fp != 'FY':
            continue
        # Filter out malformed values (e.g. Excel date serials)
        if not isinstance(fy, int) or fy < 2000 or fy > datetime.now().year + 1:
            continue
        if fy < MIN_FISCAL_YEAR:
            continue
        filed = item.get('filed', '')
        val = item.get('val')
        if val is None:
            continue
        # Keep most recent filing for this FY (10-K/A supersedes 10-K)
        if fy not in annual or filed > annual[fy][1]:
            annual[fy] = (val, filed)

    return annual  # {fy: (value, filed_date)}


def add_yoy_features(snapshots: list) -> list:
    """
    Compute year-over-year change features between consecutive fiscal years.
    Requires snapshots sorted by fiscal_year. First year gets None for all YoY fields.

    These capture *change* signals that single-snapshot features miss:
    - Receivables growing 3x faster than revenue → earnings manipulation
    - Assets growing without revenue → empire building / low-quality
    - Share count rising → dilution, often ahead of bad news
    """
    snapshots = sorted(snapshots, key=lambda x: x['fiscal_year'])

    def _yoy(curr_val, prev_val):
        if curr_val is not None and prev_val is not None and prev_val != 0:
            return round((curr_val - prev_val) / abs(prev_val), 4)
        return None

    def _ratio(num, denom):
        if num is not None and denom and denom != 0:
            return num / denom
        return None

    for i, snap in enumerate(snapshots):
        consecutive = (i > 0 and snap['fiscal_year'] - snapshots[i-1]['fiscal_year'] == 1)
        prev = snapshots[i-1] if consecutive else {}

        # Growth rates
        snap['revenue_growth_yoy']      = _yoy(snap.get('revenue'),           prev.get('revenue'))
        snap['asset_growth_yoy']        = _yoy(snap.get('total_assets'),       prev.get('total_assets'))
        snap['receivables_growth_yoy']  = _yoy(snap.get('receivables'),        prev.get('receivables'))
        snap['inventory_growth_yoy']    = _yoy(snap.get('inventory'),          prev.get('inventory'))
        snap['net_income_growth_yoy']   = _yoy(snap.get('net_income'),         prev.get('net_income'))
        snap['gross_profit_growth_yoy'] = _yoy(snap.get('gross_profit'),       prev.get('gross_profit'))
        snap['capex_growth_yoy']        = _yoy(snap.get('capex'),              prev.get('capex'))
        snap['ocf_growth_yoy']          = _yoy(snap.get('operating_cash_flow'),prev.get('operating_cash_flow'))
        snap['shares_dilution']         = _yoy(snap.get('shares_outstanding'), prev.get('shares_outstanding'))
        snap['eps_growth_yoy']          = _yoy(snap.get('eps_diluted'),        prev.get('eps_diluted'))
        snap['equity_change_yoy']       = _yoy(snap.get('equity'),             prev.get('equity'))
        snap['debt_growth_yoy']         = _yoy(snap.get('long_term_debt'),     prev.get('long_term_debt'))
        snap['cash_change_yoy']         = _yoy(snap.get('cash'),               prev.get('cash'))
        snap['rd_growth_yoy']           = _yoy(snap.get('rd_expense'),         prev.get('rd_expense'))
        snap['sga_growth_yoy']          = _yoy(snap.get('sga_expense'),        prev.get('sga_expense'))

        # Margin / ratio changes (level delta, not %)
        def _margin_change(field_num, field_denom):
            cm = _ratio(snap.get(field_num),  snap.get(field_denom))
            pm = _ratio(prev.get(field_num),  prev.get(field_denom))
            return round(cm - pm, 4) if cm is not None and pm is not None else None

        snap['gross_margin_change']         = _margin_change('gross_profit',    'revenue')
        snap['operating_margin_change']     = _margin_change('operating_income','revenue')
        snap['net_margin_change']           = _margin_change('net_income',      'revenue')

        # AR-to-revenue ratio change (Beneish DSRI — receivables growing vs revenue)
        curr_ar_rev = _ratio(snap.get('receivables'), snap.get('revenue'))
        prev_ar_rev = _ratio(prev.get('receivables'), prev.get('revenue'))
        snap['ar_to_revenue_change'] = (
            round(curr_ar_rev - prev_ar_rev, 4)
            if curr_ar_rev is not None and prev_ar_rev is not None else None
        )

        # Asset turnover change (efficiency trend)
        curr_at = _ratio(snap.get('revenue'), snap.get('total_assets'))
        prev_at = _ratio(prev.get('revenue'), prev.get('total_assets'))
        snap['asset_turnover_change'] = (
            round(curr_at - prev_at, 4)
            if curr_at is not None and prev_at is not None else None
        )

        # Cash conversion change (earnings quality trend)
        curr_cc = _ratio(snap.get('operating_cash_flow'), snap.get('net_income'))
        prev_cc = _ratio(prev.get('operating_cash_flow'), prev.get('net_income'))
        snap['cash_conversion_change'] = (
            round(curr_cc - prev_cc, 4)
            if curr_cc is not None and prev_cc is not None else None
        )

    return snapshots


def build_annual_financials(cik: str) -> list:
    """
    Build a list of annual financial snapshots for a company.
    Returns list of dicts, one per fiscal year, with YoY features added.
    """
    facts = fetch_company_facts(cik)
    if not facts:
        return []

    by_year = defaultdict(dict)
    filed_dates = {}

    for concept_path, field_name in CONCEPT_MAP.items():
        annual = extract_annual_values(facts, concept_path)
        for fy, (val, filed) in annual.items():
            if field_name not in by_year[fy]:
                by_year[fy][field_name] = val
            if fy not in filed_dates or filed > filed_dates[fy]:
                filed_dates[fy] = filed

    snapshots = []
    for fy in sorted(by_year.keys()):
        row = dict(by_year[fy])
        row['fiscal_year'] = fy
        row['filed_date'] = filed_dates.get(fy)
        snapshots.append(row)

    snapshots = add_yoy_features(snapshots)
    return snapshots


# ── Price helpers ──────────────────────────────────────────────────────────────

def get_price_on_date(ticker: str, date_str: str) -> Optional[float]:
    """Get closing price on or just after a given date."""
    try:
        start = pd.Timestamp(date_str)
        end = start + timedelta(days=10)
        hist = yf.Ticker(ticker).history(
            start=start.strftime('%Y-%m-%d'),
            end=end.strftime('%Y-%m-%d')
        )
        if not hist.empty:
            return float(hist['Close'].iloc[0])
    except Exception:
        pass
    return None


def get_sp500_return(start_date: str, end_date: str) -> Optional[float]:
    """Get S&P 500 (SPY) return between two dates."""
    try:
        hist = yf.Ticker('SPY').history(
            start=start_date,
            end=(pd.Timestamp(end_date) + timedelta(days=5)).strftime('%Y-%m-%d')
        )
        if len(hist) < 2:
            return None
        start_price = float(hist['Close'].iloc[0])
        end_price = float(hist['Close'].iloc[-1])
        return (end_price - start_price) / start_price if start_price > 0 else None
    except Exception:
        return None


def compute_forward_return(ticker: str, filed_date: str) -> dict:
    """
    Compute 12-month and 24-month forward returns from filing date.
    """
    result = {
        'entry_price': None, 'exit_price': None,
        'forward_return_12m': None, 'sp500_return_12m': None, 'beat_sp500': None,
        'forward_return_24m': None, 'sp500_return_24m': None, 'beat_sp500_24m': None,
    }
    if not filed_date or not ticker:
        return result

    filing_ts = pd.Timestamp(filed_date)
    now = pd.Timestamp.now()
    entry = None

    # 12-month return
    if filing_ts <= now - timedelta(days=365):
        exit_date_12m = (filing_ts + timedelta(days=365)).strftime('%Y-%m-%d')
        entry = get_price_on_date(ticker, filed_date)
        exit_12 = get_price_on_date(ticker, exit_date_12m)
        if entry and exit_12 and entry > 0:
            fwd_12 = (exit_12 - entry) / entry
            sp500_12 = get_sp500_return(filed_date, exit_date_12m)
            result.update({
                'entry_price': round(entry, 4),
                'exit_price': round(exit_12, 4),
                'forward_return_12m': round(fwd_12, 4),
                'sp500_return_12m': round(sp500_12, 4) if sp500_12 else None,
                'beat_sp500': bool(sp500_12 is not None and fwd_12 > sp500_12),
            })

    # 24-month return
    if filing_ts <= now - timedelta(days=730):
        if entry is None:
            entry = get_price_on_date(ticker, filed_date)
        exit_date_24m = (filing_ts + timedelta(days=730)).strftime('%Y-%m-%d')
        exit_24 = get_price_on_date(ticker, exit_date_24m)
        if entry and exit_24 and entry > 0:
            fwd_24 = (exit_24 - entry) / entry
            sp500_24 = get_sp500_return(filed_date, exit_date_24m)
            result.update({
                'forward_return_24m': round(fwd_24, 4),
                'sp500_return_24m': round(sp500_24, 4) if sp500_24 else None,
                'beat_sp500_24m': bool(sp500_24 is not None and fwd_24 > sp500_24),
            })

    return result


def compute_price_features(ticker: str, filed_date: str, entry_price: Optional[float]) -> dict:
    """
    Compute price-based momentum and risk features using only data available at filing date.
    All features look BACKWARD from filing date — no look-ahead bias.

    - momentum_Xm_prior: stock return in X months before filing (prior momentum)
    - price_to_52w_high: where is price relative to 52-week high (nearness to high)
    - vol_prior_12m: realized annualised volatility in prior 12 months
    """
    result = {
        'momentum_12m_prior': None,
        'momentum_6m_prior':  None,
        'momentum_3m_prior':  None,
        'price_to_52w_high':  None,
        'vol_prior_12m':      None,
    }
    if not filed_date or not entry_price or entry_price <= 0:
        return result

    try:
        filing_ts = pd.Timestamp(filed_date)
        # Fetch ~14 months of daily data ending at filing date
        start = (filing_ts - timedelta(days=430)).strftime('%Y-%m-%d')
        end   = (filing_ts + timedelta(days=5)).strftime('%Y-%m-%d')
        hist = yf.Ticker(ticker).history(start=start, end=end)

        if len(hist) < 30:
            return result

        # Use actual entry price (price at filing date) as the endpoint
        # hist index is the date; find closest date at or before filing
        hist.index = hist.index.tz_localize(None) if hist.index.tz else hist.index
        past_data = hist[hist.index <= filing_ts]
        if past_data.empty:
            return result

        closes = past_data['Close']
        n = len(closes)

        def _momentum_bars(n_bars):
            if n_bars >= n:
                return None
            past_price = float(closes.iloc[-(n_bars + 1)])
            return round((entry_price - past_price) / past_price, 4) if past_price > 0 else None

        result['momentum_12m_prior'] = _momentum_bars(252)
        result['momentum_6m_prior']  = _momentum_bars(126)
        result['momentum_3m_prior']  = _momentum_bars(63)

        # 52-week high (max close in prior ~252 trading days)
        high_52w = float(closes.tail(252).max())
        if high_52w > 0:
            result['price_to_52w_high'] = round(entry_price / high_52w, 4)

        # Realized annualised volatility
        daily_returns = closes.pct_change().dropna()
        if len(daily_returns) >= 20:
            result['vol_prior_12m'] = round(float(daily_returns.std() * (252 ** 0.5)), 4)

    except Exception:
        pass

    return result


# ── Signal + feature computation ──────────────────────────────────────────────

def _safe(num, denom, decimals=4):
    """Safe division — returns None if denom is 0 or None."""
    if num is not None and denom is not None and denom != 0:
        return round(num / denom, decimals)
    return None


def compute_signals(c: dict) -> dict:
    """
    Run all fraud signals + value metrics + extended accounting features.
    c must contain raw financials (revenue, assets, etc.) + market_cap.
    """
    b   = beneish_m_score(c)
    p   = piotroski_f_score(c)
    a   = accruals_ratio(c)
    cfd = cash_flow_divergence(c)
    alt = altman_z_score(c)
    rq  = revenue_quality(c)
    eq  = earnings_quality(c)
    vm  = calculate_value_metrics(c)

    # Raw fields
    rev   = c.get('revenue')
    ni    = c.get('net_income')
    gp    = c.get('gross_profit')
    oi    = c.get('operating_income')       # EBIT proxy
    ta    = c.get('total_assets')
    ca    = c.get('current_assets')
    cl    = c.get('current_liabilities')
    tl    = c.get('total_liabilities')
    ltd   = c.get('long_term_debt')
    re    = c.get('retained_earnings')
    eq_   = c.get('equity')
    cash  = c.get('cash')
    ocf   = c.get('operating_cash_flow')
    capex = c.get('capex')
    dep   = c.get('depreciation')
    inv   = c.get('inventory')
    rec   = c.get('receivables')
    ppe   = c.get('ppe_net')
    rd    = c.get('rd_expense')
    sga   = c.get('sga_expense')
    tax   = c.get('tax_expense')
    eps_d = c.get('eps_diluted')
    divs  = c.get('dividends_per_share')
    shs   = c.get('shares_outstanding')
    mcap  = c.get('market_cap')

    # Derived intermediate values
    ebitda  = (oi + dep) if (oi is not None and dep is not None) else None
    fcf     = (ocf - capex) if (ocf is not None and capex is not None) else None
    nwc     = (ca - cl) if (ca is not None and cl is not None) else None
    net_debt = (ltd - cash) if (ltd is not None and cash is not None) else None

    out = {
        # ── Fraud signals ────────────────────────────────────────────────────
        'beneish_score':          b.get('score'),
        'beneish_flag':           b.get('manipulator', False),
        'piotroski_score':        p.get('score'),
        'piotroski_weak':         p.get('weak', False),
        'accruals_ratio':         a.get('ratio'),
        'accruals_flag':          a.get('red_flag', False),
        'cfd_ratio':              cfd.get('divergence'),
        'cfd_flag':               cfd.get('red_flag', False),
        'altman_score':           alt.get('score'),
        'altman_zone':            alt.get('zone'),
        'altman_flag':            alt.get('distress', False),
        'ar_ratio':               rq.get('ar_ratio'),
        'dso':                    rq.get('dso'),
        'revenue_quality_flag':   rq.get('red_flag', False),
        'non_op_ratio':           eq.get('non_op_ratio'),
        'earnings_quality_flag':  eq.get('red_flag', False),

        # ── Value metrics (from value_metrics.py) ───────────────────────────
        **{k: vm.get(k) for k in [
            'pe_ratio', 'pb_ratio', 'ev_ebitda', 'fcf_yield', 'fcf',
            'roe', 'roa', 'gross_margin', 'net_margin',
            'debt_to_equity', 'current_ratio',
            'earnings_yield', 'return_on_capital', 'acquirers_multiple',
            'ncav', 'ncav_ratio', 'net_net_flag',
            'gross_profitability', 'croic', 'invested_capital',
            'market_cap_segment',
        ]},

        # ── Extended cross-sectional accounting features ─────────────────────

        # Profitability & margins
        'operating_margin':       _safe(oi,     rev),
        'ebitda_margin':          _safe(ebitda, rev),
        'rd_to_revenue':          _safe(rd,     rev),
        'sga_to_revenue':         _safe(sga,    rev),
        'tax_rate':               _safe(tax,    (ni + tax) if (ni is not None and tax is not None) else None),
        'ocf_to_assets':          _safe(ocf,    ta),
        'fcf_to_assets':          _safe(fcf,    ta),

        # Efficiency / turnover
        'asset_turnover':         _safe(rev,    ta),
        'inventory_days':         round(inv / (rev / 365), 1) if inv and rev and rev > 0 else None,
        'receivables_turnover':   _safe(rev,    rec),

        # Liquidity & solvency
        'cash_to_assets':         _safe(cash,   ta),
        'cash_to_debt':           _safe(cash,   ltd) if ltd and ltd > 0 else None,
        'nwc_to_assets':          _safe(nwc,    ta),
        'equity_ratio':           _safe(eq_,    ta),
        'equity_multiplier':      _safe(ta,     eq_),    # DuPont leverage
        'liabilities_to_equity':  _safe(tl,     eq_),
        'debt_to_ebitda':         _safe(ltd,    ebitda),
        'net_debt_to_ebitda':     _safe(net_debt, ebitda),
        'interest_coverage':      _safe(oi,     c.get('interest_expense'), decimals=2) if c.get('interest_expense') and c['interest_expense'] > 0 else None,

        # Capital structure
        'retained_earnings_ratio': _safe(re,    ta),
        'capex_to_assets':        _safe(capex,  ta),
        'capex_to_revenue':       _safe(capex,  rev),
        'capex_intensity':        _safe(capex,  ocf) if ocf and ocf > 0 else None,  # maintenance burden
        'reinvestment_rate':      _safe((capex - dep) if (capex and dep) else None, oi) if oi and oi > 0 else None,
        'depreciation_to_assets': _safe(dep,    ta),
        'ppe_to_assets':          _safe(ppe,    ta),

        # Earnings quality
        'cash_conversion':        _safe(ocf,    ni) if ni and ni > 0 else None,  # OCF / NI (>1 = clean)
        'accruals_to_assets':     _safe((ni - ocf) if (ni and ocf) else None, ta),  # Sloan 1996
        'ev_to_ocf':              _safe(  # EV/OCF multiple
            ((mcap + ltd - cash) if (mcap and ltd is not None and cash is not None) else None),
            ocf
        ) if ocf and ocf > 0 else None,

        # Binary red flags derived from signals
        'high_dilution_flag':     bool(c.get('shares_dilution') is not None and c.get('shares_dilution', 0) > 0.05),
        'cash_burning_flag':      bool(c.get('cash_change_yoy') is not None and c.get('cash_change_yoy', 0) < -0.30),
        'high_rd_flag':           bool(rd and rev and rev > 0 and rd / rev > 0.15),
        'revenue_vs_ar_flag':     bool(  # AR growing >2x faster than revenue
            c.get('receivables_growth_yoy') is not None
            and c.get('revenue_growth_yoy') is not None
            and c.get('receivables_growth_yoy', 0) > 2 * max(c.get('revenue_growth_yoy', 0), 0.01)
        ),
        'earnings_ocf_diverge_flag': bool(  # Net income >> OCF → accruals warning
            ni and ocf and ni > 0 and ocf > 0 and ni > 2 * ocf
        ),
    }

    return out


# ── Main ──────────────────────────────────────────────────────────────────────

def load_checkpoint() -> set:
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH) as f:
            return set(json.load(f))
    return set()


def save_checkpoint(done: set):
    with open(CHECKPOINT_PATH, 'w') as f:
        json.dump(list(done), f)


def build_dataset(limit: int = None, years: int = 5):
    global MIN_FISCAL_YEAR
    MIN_FISCAL_YEAR = datetime.now().year - years

    with open(FINANCIALS_PATH) as f:
        companies = json.load(f)

    if limit:
        companies = companies[:limit]

    done_ciks = load_checkpoint()
    remaining = [c for c in companies if c.get('ticker') and c['cik'] not in done_ciks]
    print(f"Total: {len(companies)} | Done: {len(done_ciks)} | Remaining: {len(remaining)}")

    rows = []
    if os.path.exists(OUTPUT_PATH):
        existing_df = pd.read_parquet(OUTPUT_PATH)
        rows = existing_df.to_dict('records')
        print(f"Loaded {len(rows)} existing rows from checkpoint")

    for i, company in enumerate(remaining):
        cik    = company['cik']
        ticker = company['ticker']
        name   = company['name']

        snapshots = build_annual_financials(cik)

        for snap in snapshots:
            snap['cik']      = cik
            snap['ticker']   = ticker
            snap['name']     = name
            snap['exchange'] = company.get('exchange')

            # ── Step 1: compute forward returns (gives us entry_price) ────────
            fwd = compute_forward_return(ticker, snap.get('filed_date'))
            snap.update(fwd)

            # ── Step 2: compute price/momentum features ───────────────────────
            price_feats = compute_price_features(
                ticker, snap.get('filed_date'), snap.get('entry_price')
            )
            snap.update(price_feats)

            # ── Step 3: set historical market cap (no look-ahead bias) ────────
            # Use price at filing × shares from EDGAR — far more accurate than
            # current market cap which introduces future information.
            if snap.get('entry_price') and snap.get('shares_outstanding'):
                snap['market_cap'] = snap['entry_price'] * snap['shares_outstanding']
            else:
                snap['market_cap'] = company.get('market_cap')  # fallback to current

            # ── Step 4: compute all signals (now using historical market_cap) ─
            signals = compute_signals(snap)
            snap.update(signals)

            rows.append(snap)

        done_ciks.add(cik)

        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(remaining)} — {ticker} — {len(snapshots)} years — total rows: {len(rows)}")
            df_tmp = pd.DataFrame(rows)
            df_tmp.to_parquet(OUTPUT_PATH, index=False)
            save_checkpoint(done_ciks)

        time.sleep(0.3)

    df = pd.DataFrame(rows)
    df.to_parquet(OUTPUT_PATH, index=False)
    save_checkpoint(done_ciks)

    labeled_12m = df['forward_return_12m'].notna().sum() if 'forward_return_12m' in df.columns else 0
    labeled_24m = df['forward_return_24m'].notna().sum() if 'forward_return_24m' in df.columns else 0
    print(f"\nDone. {len(rows):,} rows across {len(done_ciks):,} companies → {OUTPUT_PATH}")
    print(f"Features: {len(df.columns)} columns")
    print(f"12m labels: {labeled_12m:,}/{len(df):,} | 24m labels: {labeled_24m:,}/{len(df):,}")
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int,  default=None, help='Limit to N companies (for testing)')
    parser.add_argument('--years', type=int,  default=5,    help='How many fiscal years back (default 5)')
    args = parser.parse_args()

    build_dataset(limit=args.limit, years=args.years)
