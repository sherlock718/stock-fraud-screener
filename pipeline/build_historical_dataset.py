"""
Build a historical dataset for ML training.

For each company, pulls multi-year 10-K financial data from EDGAR's company-facts
API (which stores full time-series history), then aligns each annual filing with:
  - Stock price on the filing date (entry price)
  - Stock price 12 months later (exit price)
  - 12-month forward return = (exit - entry) / entry

EDGAR company-facts API stores all historical values per concept — we extract
annual 10-K values for each fiscal year going back up to 5 years.

Output: data/historical_dataset.parquet
  Rows: one per (company, fiscal_year) — typically 4-5 rows per company
  Columns: all fraud signals + value metrics + forward_return_12m + beat_sp500

Usage:
  python3 pipeline/build_historical_dataset.py
  python3 pipeline/build_historical_dataset.py --limit 500   # test run
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
CONCEPT_MAP = {
    'us-gaap/Revenues':                         'revenue',
    'us-gaap/RevenueFromContractWithCustomerExcludingAssessedTax': 'revenue',
    'us-gaap/SalesRevenueNet':                  'revenue',
    'us-gaap/NetIncomeLoss':                    'net_income',
    'us-gaap/OperatingIncomeLoss':              'operating_income',
    'us-gaap/GrossProfit':                      'gross_profit',
    'us-gaap/Assets':                           'total_assets',
    'us-gaap/AssetsCurrent':                    'current_assets',
    'us-gaap/Liabilities':                      'total_liabilities',
    'us-gaap/LiabilitiesCurrent':               'current_liabilities',
    'us-gaap/LongTermDebt':                     'long_term_debt',
    'us-gaap/LongTermDebtNoncurrent':           'long_term_debt',
    'us-gaap/RetainedEarningsAccumulatedDeficit': 'retained_earnings',
    'us-gaap/AccountsReceivableNetCurrent':     'receivables',
    'us-gaap/InventoryNet':                     'inventory',
    'us-gaap/PropertyPlantAndEquipmentNet':     'ppe_net',
    'us-gaap/DepreciationDepletionAndAmortization': 'depreciation',
    'us-gaap/Depreciation':                     'depreciation',
    'us-gaap/NetCashProvidedByUsedInOperatingActivities': 'operating_cash_flow',
    'us-gaap/PaymentsToAcquirePropertyPlantAndEquipment': 'capex',
    'us-gaap/CostsAndExpenses':                 'total_expenses',
    # Added for YoY features + new cross-sectional metrics
    'us-gaap/InterestExpense':                  'interest_expense',
    'us-gaap/InterestAndDebtExpense':           'interest_expense',
    'us-gaap/CommonStockSharesOutstanding':     'shares_outstanding',
}

MIN_FISCAL_YEAR = datetime.now().year - 5   # last 5 fiscal years


# ── EDGAR helpers ─────────────────────────────────────────────────────────────

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
    concept_path e.g. 'us-gaap/Revenues'
    """
    namespace, concept = concept_path.split('/')
    try:
        units = facts.get('facts', {}).get(namespace, {}).get(concept, {}).get('units', {})
        values = units.get('USD', units.get('shares', []))
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
        # Filter out malformed values (e.g. Excel serial numbers)
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

    YoY features capture *change* signals that fraud detectors miss on a single snapshot:
    - Revenue growing slower than receivables → earnings manipulation
    - Asset growth outpacing revenue → empire building / low-quality growth
    - Share count rising → dilution, often ahead of bad news
    """
    snapshots = sorted(snapshots, key=lambda x: x['fiscal_year'])

    def _yoy(curr_val, prev_val):
        """Safe YoY growth rate."""
        if curr_val is not None and prev_val is not None and prev_val != 0:
            return round((curr_val - prev_val) / abs(prev_val), 4)
        return None

    for i, snap in enumerate(snapshots):
        if i == 0 or (snap['fiscal_year'] - snapshots[i-1]['fiscal_year']) != 1:
            # No prior year or non-consecutive — leave YoY fields as None
            snap['revenue_growth_yoy']    = None
            snap['asset_growth_yoy']      = None
            snap['receivables_growth_yoy']= None
            snap['inventory_growth_yoy']  = None
            snap['net_income_growth_yoy'] = None
            snap['capex_growth_yoy']      = None
            snap['shares_dilution']       = None
            snap['ar_to_revenue_change']  = None
            snap['gross_margin_change']   = None
            continue

        prev = snapshots[i-1]

        snap['revenue_growth_yoy']     = _yoy(snap.get('revenue'),      prev.get('revenue'))
        snap['asset_growth_yoy']       = _yoy(snap.get('total_assets'),  prev.get('total_assets'))
        snap['receivables_growth_yoy'] = _yoy(snap.get('receivables'),   prev.get('receivables'))
        snap['inventory_growth_yoy']   = _yoy(snap.get('inventory'),     prev.get('inventory'))
        snap['net_income_growth_yoy']  = _yoy(snap.get('net_income'),    prev.get('net_income'))
        snap['capex_growth_yoy']       = _yoy(snap.get('capex'),         prev.get('capex'))
        snap['shares_dilution']        = _yoy(snap.get('shares_outstanding'), prev.get('shares_outstanding'))

        # AR-to-revenue ratio change (Beneish DSRI component)
        # Rising = receivables growing faster than revenue = earnings quality warning
        def _ar_rev(s):
            r, rev = s.get('receivables'), s.get('revenue')
            return r / rev if (r is not None and rev and rev > 0) else None

        curr_ar_rev = _ar_rev(snap)
        prev_ar_rev = _ar_rev(prev)
        snap['ar_to_revenue_change'] = (
            round(curr_ar_rev - prev_ar_rev, 4)
            if curr_ar_rev is not None and prev_ar_rev is not None else None
        )

        # Gross margin change (level, not %)
        def _gm(s):
            gp, rev = s.get('gross_profit'), s.get('revenue')
            return gp / rev if (gp is not None and rev and rev > 0) else None

        curr_gm = _gm(snap)
        prev_gm = _gm(prev)
        snap['gross_margin_change'] = (
            round(curr_gm - prev_gm, 4)
            if curr_gm is not None and prev_gm is not None else None
        )

    return snapshots


def build_annual_financials(cik: str) -> list:
    """
    Build a list of annual financial snapshots for a company.
    Returns list of dicts, one per fiscal year.
    """
    facts = fetch_company_facts(cik)
    if not facts:
        return []

    # Collect values per concept per year
    by_year = defaultdict(dict)
    filed_dates = {}

    for concept_path, field_name in CONCEPT_MAP.items():
        annual = extract_annual_values(facts, concept_path)
        for fy, (val, filed) in annual.items():
            # Don't overwrite if already set (first match wins for same field)
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

    # Add YoY change features (requires sorted multi-year data)
    snapshots = add_yoy_features(snapshots)

    return snapshots


# ── Price helpers ─────────────────────────────────────────────────────────────

def get_price_on_date(ticker: str, date_str: str) -> Optional[float]:
    """Get closing price on or just after a given date."""
    try:
        start = pd.Timestamp(date_str)
        end = start + timedelta(days=10)  # allow a few trading days
        hist = yf.Ticker(ticker).history(start=start.strftime('%Y-%m-%d'),
                                          end=end.strftime('%Y-%m-%d'))
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
    Returns entry_price, exit_price, forward_return_12m, sp500_return_12m, beat_sp500,
            forward_return_24m, sp500_return_24m, beat_sp500_24m.
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

    entry = None  # fetch once, reuse for both horizons

    # ── 12-month return ───────────────────────────────────────────────────────
    cutoff_12m = now - timedelta(days=365)
    if filing_ts <= cutoff_12m:
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

    # ── 24-month return ───────────────────────────────────────────────────────
    cutoff_24m = now - timedelta(days=730)
    if filing_ts <= cutoff_24m:
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


# ── Signal computation ────────────────────────────────────────────────────────

def compute_signals(c: dict) -> dict:
    """Run all fraud signals + value metrics on a financial snapshot."""
    b = beneish_m_score(c)
    p = piotroski_f_score(c)
    a = accruals_ratio(c)
    cfd = cash_flow_divergence(c)
    alt = altman_z_score(c)
    rq = revenue_quality(c)
    eq = earnings_quality(c)
    vm = calculate_value_metrics(c)

    return {
        # Beneish
        'beneish_score': b.get('score'),
        'beneish_flag': b.get('manipulator', False),
        # Piotroski
        'piotroski_score': p.get('score'),
        'piotroski_weak': p.get('weak', False),
        # Accruals
        'accruals_ratio': a.get('ratio'),
        'accruals_flag': a.get('red_flag', False),
        # Cash flow divergence
        'cfd_ratio': cfd.get('divergence'),
        'cfd_flag': cfd.get('red_flag', False),
        # Altman
        'altman_score': alt.get('score'),
        'altman_zone': alt.get('zone'),
        'altman_flag': alt.get('distress', False),
        # Revenue quality
        'ar_ratio': rq.get('ar_ratio'),
        'dso': rq.get('dso'),
        'revenue_quality_flag': rq.get('red_flag', False),
        # Earnings quality
        'non_op_ratio': eq.get('non_op_ratio'),
        'earnings_quality_flag': eq.get('red_flag', False),
        # Value metrics
        **{k: vm.get(k) for k in [
            'pe_ratio', 'pb_ratio', 'ev_ebitda', 'fcf_yield', 'fcf',
            'roe', 'roa', 'gross_margin', 'net_margin',
            'debt_to_equity', 'current_ratio',
            'earnings_yield', 'return_on_capital', 'acquirers_multiple',
            'ncav', 'ncav_ratio', 'net_net_flag',
            'gross_profitability', 'croic', 'invested_capital',
            'market_cap_segment',
        ]},
        # Cross-sectional features (no prior year needed)
        'capex_to_assets': (
            round(c.get('capex') / c.get('total_assets'), 4)
            if c.get('capex') is not None and c.get('total_assets') else None
        ),
        'capex_to_revenue': (
            round(c.get('capex') / c.get('revenue'), 4)
            if c.get('capex') is not None and c.get('revenue') else None
        ),
        'interest_coverage': (
            round(c.get('operating_income') / c.get('interest_expense'), 2)
            if c.get('operating_income') is not None and c.get('interest_expense') and c['interest_expense'] > 0 else None
        ),
        'retained_earnings_ratio': (
            round(c.get('retained_earnings') / c.get('total_assets'), 4)
            if c.get('retained_earnings') is not None and c.get('total_assets') else None
        ),
    }


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
    # Load existing rows if resuming
    if os.path.exists(OUTPUT_PATH):
        existing_df = pd.read_parquet(OUTPUT_PATH)
        rows = existing_df.to_dict('records')
        print(f"Loaded {len(rows)} existing rows from checkpoint")

    for i, company in enumerate(remaining):
        cik = company['cik']
        ticker = company['ticker']
        name = company['name']

        snapshots = build_annual_financials(cik)

        for snap in snapshots:
            # Add identity fields and market cap from current data
            snap['cik'] = cik
            snap['ticker'] = ticker
            snap['name'] = name
            snap['exchange'] = company.get('exchange')
            snap['market_cap'] = company.get('market_cap')  # current only

            # Compute all signals
            signals = compute_signals(snap)
            snap.update(signals)

            # Compute forward return
            fwd = compute_forward_return(ticker, snap.get('filed_date'))
            snap.update(fwd)

            rows.append(snap)

        done_ciks.add(cik)

        if (i + 1) % 20 == 0:
            print(f"  {i+1}/{len(remaining)} — {ticker} — {len(snapshots)} years — total rows: {len(rows)}")
            # Save checkpoint
            df = pd.DataFrame(rows)
            df.to_parquet(OUTPUT_PATH, index=False)
            save_checkpoint(done_ciks)

        time.sleep(0.3)  # be gentle with EDGAR + yfinance

    # Final save
    df = pd.DataFrame(rows)
    df.to_parquet(OUTPUT_PATH, index=False)
    save_checkpoint(done_ciks)

    print(f"\nDone. {len(rows)} rows across {len(done_ciks)} companies → {OUTPUT_PATH}")
    print(f"Forward returns available: {df['forward_return_12m'].notna().sum()}/{len(df)}")
    return df


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None, help='Limit to N companies (for testing)')
    parser.add_argument('--years', type=int, default=5, help='How many fiscal years back (default 5)')
    args = parser.parse_args()

    build_dataset(limit=args.limit, years=args.years)
