"""
Step 2 — Build raw annual + quarterly financial snapshots from EDGAR XBRL.

For each company in data/tickers.parquet:
  - Fetch company-facts API (full XBRL time-series)
  - Extract annual (10-K, fp='FY') snapshots
  - Extract quarterly (10-Q, fp in Q1/Q2/Q3) snapshots
  - Compute YoY change features between consecutive periods
  - Save to data/snapshots.parquet

Checkpoint: saves progress every CHECKPOINT_EVERY companies.
Safe to interrupt — resumes automatically on restart.

Output schema (one row per company × period):
  cik, ticker, name, exchange, sic_code, market, country, accounting_std,
  fiscal_year, fiscal_quarter, period_type, filed_date,
  [all raw financial fields],
  [yoy change features]
"""

import json
import os
import sys
import time
import argparse
import requests
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from typing import Optional

BASE       = Path(__file__).parent.parent
DATA       = BASE / 'data'
TICKERS    = DATA / 'tickers.parquet'
OUT        = DATA / 'snapshots.parquet'
CKPT       = DATA / 'snapshots_checkpoint.json'
HEADERS    = {'User-Agent': 'AlphaResearchPipeline research@alpharesearch.io'}
CHECKPOINT_EVERY = 50

# ── EDGAR XBRL concept map ─────────────────────────────────────────────────────
# Format: 'namespace/ConceptName': 'field_name'
# Multiple fallback concepts listed in order — first found wins.

CONCEPT_MAP = {
    # Income Statement
    'us-gaap/Revenues':                                          'revenue',
    'us-gaap/RevenueFromContractWithCustomerExcludingAssessedTax': 'revenue',
    'us-gaap/RevenueFromContractWithCustomerIncludingAssessedTax': 'revenue',
    'us-gaap/SalesRevenueNet':                                   'revenue',
    'us-gaap/SalesRevenueGoodsNet':                              'revenue',
    'us-gaap/NetIncomeLoss':                                     'net_income',
    'us-gaap/GrossProfit':                                       'gross_profit',
    'us-gaap/OperatingIncomeLoss':                               'operating_income',
    'us-gaap/CostOfGoodsAndServicesSold':                        'cogs',
    'us-gaap/CostOfRevenue':                                     'cogs',
    'us-gaap/ResearchAndDevelopmentExpense':                     'rd_expense',
    'us-gaap/SellingGeneralAndAdministrativeExpense':             'sga_expense',
    'us-gaap/IncomeTaxExpenseBenefit':                           'tax_expense',
    'us-gaap/InterestExpense':                                   'interest_expense',
    'us-gaap/NonoperatingIncomeExpense':                         'non_operating_income',
    'us-gaap/EarningsPerShareDiluted':                           'eps_diluted',
    'us-gaap/EarningsPerShareBasic':                             'eps_basic',
    'us-gaap/CommonStockDividendsPerShareDeclared':              'dividends_per_share',

    # Balance Sheet — Assets
    'us-gaap/Assets':                                            'total_assets',
    'us-gaap/AssetsCurrent':                                     'current_assets',
    'us-gaap/CashAndCashEquivalentsAtCarryingValue':             'cash',
    'us-gaap/AccountsReceivableNetCurrent':                      'receivables',
    'us-gaap/InventoryNet':                                      'inventory',
    'us-gaap/PropertyPlantAndEquipmentNet':                      'ppe_net',
    'us-gaap/PropertyPlantAndEquipmentGross':                    'ppe_gross',
    'us-gaap/IntangibleAssetsNetExcludingGoodwill':              'intangibles',
    'us-gaap/Goodwill':                                          'goodwill',
    'us-gaap/OtherAssetsNoncurrent':                             'other_noncurrent_assets',

    # Balance Sheet — Liabilities
    'us-gaap/Liabilities':                                       'total_liabilities',
    'us-gaap/LiabilitiesCurrent':                                'current_liabilities',
    'us-gaap/AccountsPayableCurrent':                            'accounts_payable',
    'us-gaap/LongTermDebt':                                      'long_term_debt',
    'us-gaap/LongTermDebtNoncurrent':                            'long_term_debt',
    'us-gaap/ShortTermBorrowings':                               'short_term_debt',

    # Balance Sheet — Equity
    'us-gaap/StockholdersEquity':                                'equity',
    'us-gaap/RetainedEarningsAccumulatedDeficit':                'retained_earnings',
    'us-gaap/CommonStockSharesOutstanding':                      'shares_outstanding',

    # Cash Flow Statement
    'us-gaap/NetCashProvidedByUsedInOperatingActivities':        'operating_cash_flow',
    'us-gaap/PaymentsToAcquirePropertyPlantAndEquipment':        'capex',
    'us-gaap/DepreciationDepletionAndAmortization':              'depreciation',
    'us-gaap/Depreciation':                                      'depreciation',
    'us-gaap/NetCashProvidedByUsedInFinancingActivities':        'financing_cash_flow',

    # Going concern
    'us-gaap/SubstantialDoubtAboutGoingConcernTextBlock':        'going_concern_flag',
}

# Fields where unit is 'shares' not 'USD'
SHARES_FIELDS = {'shares_outstanding', 'eps_diluted', 'eps_basic', 'dividends_per_share'}
# Fields that are boolean (presence = True)
BOOL_FIELDS = {'going_concern_flag'}

# Map concept → field (multi-fallback: first concept to yield a value wins per field)
# Build reverse map: field → [concepts in priority order]
FIELD_CONCEPTS = defaultdict(list)
for concept, field in CONCEPT_MAP.items():
    FIELD_CONCEPTS[field].append(concept)


def edgar_rate_wait(_last=[0.0]):
    interval = 0.13
    now = time.time()
    wait = interval - (now - _last[0])
    if wait > 0:
        time.sleep(wait)
    _last[0] = time.time()


def fetch_company_facts(cik: str, retries=4) -> Optional[dict]:
    url = f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
    for attempt in range(retries):
        edgar_rate_wait()
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                wait = 10 * (attempt + 1)
                print(f'  Rate limited CIK {cik} — waiting {wait}s')
                time.sleep(wait)
        except Exception as e:
            if attempt == retries - 1:
                return None
            time.sleep(2 ** attempt)
    return None


def extract_concept_series(facts: dict, concept_path: str, is_shares=False, is_bool=False):
    """
    Extract time-series values for a single XBRL concept.
    Returns dict: {(fy, fp): (value, filed_date)}

    Point-in-time policy: for each (fy, fp), keep the value from the EARLIEST
    primary filing (10-K, 10-Q, 20-F). Later amendments (10-K/A, 8-K, etc.) are
    ignored for the snapshot — they represent information not available at the
    original filing date. This prevents look-ahead leakage from restatements.

    Ordering note: SEC API records are NOT guaranteed chronologically ordered,
    so we collect all entries first, then select the earliest primary filing.
    """
    namespace, concept = concept_path.split('/', 1)
    try:
        ns_data = facts['facts'].get(namespace, {})
        if concept not in ns_data:
            return {}
        units = ns_data[concept].get('units', {})

        if is_bool:
            entries = []
            for unit_entries in units.values():
                entries.extend(unit_entries)
            return {(e.get('fy'), e.get('fp')): (True, e.get('filed', ''))
                    for e in entries if e.get('fy')}

        if is_shares:
            unit_data = units.get('shares', units.get('USD/shares', []))
        else:
            unit_data = units.get('USD', [])

        # Collect all valid entries grouped by (fy, fp)
        from collections import defaultdict
        by_period = defaultdict(list)
        for e in unit_data:
            fy = e.get('fy')
            fp = e.get('fp', '')
            filed = e.get('filed', '')
            val = e.get('val')
            if fy is None or val is None:
                continue
            if not isinstance(fy, int) or fy < 2005 or fy > datetime.now().year + 1:
                continue
            by_period[(fy, fp)].append((val, filed, e.get('form', '')))

        # For each period, select the PIT-correct value:
        # 1. Prefer the earliest primary filing (10-K, 10-Q, 20-F, 10-KSB)
        # 2. If no primary filing exists, fall back to earliest of any form
        PRIMARY_FORMS = {'10-K', '10-Q', '20-F', '10-KSB', '10-QSB'}
        result = {}
        for key, entries in by_period.items():
            primary = [(v, f, form) for v, f, form in entries if form in PRIMARY_FORMS]
            if primary:
                # Earliest primary filing (by filed date)
                primary.sort(key=lambda x: x[1])
                result[key] = (primary[0][0], primary[0][1])
            else:
                # No primary filing — use earliest available
                entries.sort(key=lambda x: x[1])
                result[key] = (entries[0][0], entries[0][1])

        return result
    except Exception:
        return {}


def extract_field_series(facts: dict, field: str) -> dict:
    """
    Extract time-series for a field, trying multiple concept fallbacks.
    Returns dict: {(fy, fp): (value, filed_date)}
    """
    is_shares = field in SHARES_FIELDS
    is_bool   = field in BOOL_FIELDS
    merged = {}
    for concept in FIELD_CONCEPTS.get(field, []):
        series = extract_concept_series(facts, concept, is_shares, is_bool)
        for key, val in series.items():
            if key not in merged:  # first concept that has a value wins
                merged[key] = val
    return merged


def build_period_snapshots(facts: dict) -> list:
    """
    Build a list of financial snapshots (one per fiscal period).
    Returns list of dicts, sorted by (fiscal_year, period_type).
    """
    # Gather all unique (fy, fp) periods that appear in the data
    all_periods = set()
    field_data = {}
    for field in FIELD_CONCEPTS:
        series = extract_field_series(facts, field)
        field_data[field] = series
        all_periods.update(series.keys())

    # Build a snapshot per period
    snapshots = []
    for (fy, fp) in sorted(all_periods, key=lambda x: (x[0] or '', x[1] or '')):
        # Determine period type
        if fp == 'FY':
            period_type = 'annual'
            fiscal_quarter = None
        elif fp in ('Q1', 'Q2', 'Q3'):
            period_type = 'quarterly'
            fiscal_quarter = f'{fy}{fp}'
        elif fp == 'Q4':
            # Q4 from a 10-K is the annual filing — skip to avoid double-counting
            continue
        else:
            continue  # Skip CY, H1, H2 etc.

        snap = {
            'fiscal_year':     fy,
            'fiscal_quarter':  fiscal_quarter,
            'period_type':     period_type,
            'filed_date':      None,
        }

        latest_filed = ''
        for field, series in field_data.items():
            if (fy, fp) in series:
                val, filed = series[(fy, fp)]
                snap[field] = val
                if filed > latest_filed:
                    latest_filed = filed

        snap['filed_date'] = latest_filed if latest_filed else None

        # Only keep if we have at least assets and revenue
        if snap.get('total_assets') and snap.get('revenue'):
            snapshots.append(snap)

    return snapshots


def _yoy(curr, prev):
    if curr is not None and prev is not None and prev != 0:
        return round((curr - prev) / abs(prev), 4)
    return None


def _delta(curr, prev):
    """Simple level change for ratios (margin change etc.)"""
    if curr is not None and prev is not None:
        return round(curr - prev, 4)
    return None


def _ratio(num, denom):
    if num is not None and denom and denom != 0:
        return num / denom
    return None


def add_yoy_features(snapshots: list) -> list:
    """
    Compute YoY change features between consecutive periods of the same type.
    Annual periods compared to prior annual; quarterly compared to same quarter prior year.
    """
    # Separate annual and quarterly for YoY computation
    annual = [s for s in snapshots if s['period_type'] == 'annual']
    quarterly = [s for s in snapshots if s['period_type'] == 'quarterly']

    def _add_yoy(periods, key_fn):
        periods = sorted(periods, key=key_fn)
        for i, snap in enumerate(periods):
            prev = None
            # For annual: previous year = current year - 1
            # For quarterly: same quarter previous year
            for j in range(i - 1, -1, -1):
                candidate = periods[j]
                if snap['period_type'] == 'annual':
                    if candidate['fiscal_year'] == snap['fiscal_year'] - 1:
                        prev = candidate
                        break
                else:
                    # Same quarter, previous year
                    if (candidate['fiscal_quarter'] and snap['fiscal_quarter'] and
                            candidate['fiscal_quarter'][4:] == snap['fiscal_quarter'][4:] and
                            candidate['fiscal_year'] == snap['fiscal_year'] - 1):
                        prev = candidate
                        break

            p = prev or {}

            snap['revenue_growth_yoy']      = _yoy(snap.get('revenue'),            p.get('revenue'))
            snap['asset_growth_yoy']        = _yoy(snap.get('total_assets'),        p.get('total_assets'))
            snap['receivables_growth_yoy']  = _yoy(snap.get('receivables'),         p.get('receivables'))
            snap['inventory_growth_yoy']    = _yoy(snap.get('inventory'),           p.get('inventory'))
            snap['net_income_growth_yoy']   = _yoy(snap.get('net_income'),          p.get('net_income'))
            snap['gross_profit_growth_yoy'] = _yoy(snap.get('gross_profit'),        p.get('gross_profit'))
            snap['capex_growth_yoy']        = _yoy(snap.get('capex'),               p.get('capex'))
            snap['ocf_growth_yoy']          = _yoy(snap.get('operating_cash_flow'), p.get('operating_cash_flow'))
            snap['shares_dilution']         = _yoy(snap.get('shares_outstanding'),  p.get('shares_outstanding'))
            snap['eps_growth_yoy']          = _yoy(snap.get('eps_diluted'),         p.get('eps_diluted'))
            snap['equity_change_yoy']       = _yoy(snap.get('equity'),              p.get('equity'))
            snap['debt_growth_yoy']         = _yoy(snap.get('long_term_debt'),      p.get('long_term_debt'))
            snap['cash_change_yoy']         = _yoy(snap.get('cash'),                p.get('cash'))
            snap['rd_growth_yoy']           = _yoy(snap.get('rd_expense'),          p.get('rd_expense'))
            snap['sga_growth_yoy']          = _yoy(snap.get('sga_expense'),         p.get('sga_expense'))
            snap['ap_growth_yoy']           = _yoy(snap.get('accounts_payable'),    p.get('accounts_payable'))
            snap['cogs_growth_yoy']         = _yoy(snap.get('cogs'),                p.get('cogs'))
            snap['ppe_growth_yoy']          = _yoy(snap.get('ppe_net'),             p.get('ppe_net'))

            # Margin changes (level delta)
            snap['gross_margin_change']      = _delta(_ratio(snap.get('gross_profit'),    snap.get('revenue')),
                                                      _ratio(p.get('gross_profit'),       p.get('revenue')))
            snap['operating_margin_change']  = _delta(_ratio(snap.get('operating_income'),snap.get('revenue')),
                                                      _ratio(p.get('operating_income'),   p.get('revenue')))
            snap['net_margin_change']        = _delta(_ratio(snap.get('net_income'),      snap.get('revenue')),
                                                      _ratio(p.get('net_income'),         p.get('revenue')))
            snap['ar_to_revenue_change']     = _delta(_ratio(snap.get('receivables'),     snap.get('revenue')),
                                                      _ratio(p.get('receivables'),        p.get('revenue')))
            snap['asset_turnover_change']    = _delta(_ratio(snap.get('revenue'),         snap.get('total_assets')),
                                                      _ratio(p.get('revenue'),            p.get('total_assets')))
            ocf_ni_curr = _ratio(snap.get('operating_cash_flow'), snap.get('net_income'))
            ocf_ni_prev = _ratio(p.get('operating_cash_flow'),    p.get('net_income'))
            snap['cash_conversion_change']   = _delta(ocf_ni_curr, ocf_ni_prev)

            # 3-year trend features (require i >= 2 with consecutive years)
            if i >= 2:
                prev2 = None
                for j in range(i - 2, -1, -1):
                    c2 = periods[j]
                    if snap['period_type'] == 'annual':
                        if c2['fiscal_year'] == snap['fiscal_year'] - 2:
                            prev2 = c2
                            break
                    else:
                        if (c2.get('fiscal_quarter') and snap.get('fiscal_quarter') and
                                c2['fiscal_quarter'][4:] == snap['fiscal_quarter'][4:] and
                                c2['fiscal_year'] == snap['fiscal_year'] - 2):
                            prev2 = c2
                            break
                if prev2:
                    roa_t  = _ratio(snap.get('net_income'), snap.get('total_assets'))
                    roa_t2 = _ratio(prev2.get('net_income'), prev2.get('total_assets'))
                    snap['roa_trend_3y']           = _delta(roa_t, roa_t2)
                    gm_t  = _ratio(snap.get('gross_profit'), snap.get('revenue'))
                    gm_t2 = _ratio(prev2.get('gross_profit'), prev2.get('revenue'))
                    snap['gross_margin_trend_3y']  = _delta(gm_t, gm_t2)
                    om_t  = _ratio(snap.get('operating_income'), snap.get('revenue'))
                    om_t2 = _ratio(prev2.get('operating_income'), prev2.get('revenue'))
                    snap['operating_margin_trend_3y'] = _delta(om_t, om_t2)
                    # 3-yr revenue CAGR (skip if negative revenue in either period)
                    rev_t  = snap.get('revenue')
                    rev_t2 = prev2.get('revenue')
                    if rev_t and rev_t2 and rev_t2 > 0 and rev_t > 0:
                        snap['revenue_cagr_3y'] = round((rev_t / rev_t2) ** 0.5 - 1, 4)
                    # Leverage trend
                    lev_t  = _ratio(snap.get('long_term_debt'), snap.get('total_assets'))
                    lev_t2 = _ratio(prev2.get('long_term_debt'), prev2.get('total_assets'))
                    snap['leverage_trend_3y'] = _delta(lev_t, lev_t2)
                    # Accruals average 3yr
                    acc_t  = _ratio(
                        (snap.get('net_income', 0) or 0) - (snap.get('operating_cash_flow', 0) or 0),
                        snap.get('total_assets')
                    )
                    acc_p  = _ratio(
                        (p.get('net_income', 0) or 0) - (p.get('operating_cash_flow', 0) or 0),
                        p.get('total_assets')
                    )
                    acc_t2 = _ratio(
                        (prev2.get('net_income', 0) or 0) - (prev2.get('operating_cash_flow', 0) or 0),
                        prev2.get('total_assets')
                    )
                    if all(x is not None for x in [acc_t, acc_p, acc_t2]):
                        snap['accruals_avg_3y'] = round((acc_t + acc_p + acc_t2) / 3, 4)

        return periods

    annual = _add_yoy(annual, lambda s: s['fiscal_year'])
    quarterly = _add_yoy(quarterly, lambda s: (s['fiscal_year'], s.get('fiscal_quarter', '')))

    return annual + quarterly


def load_checkpoint() -> set:
    if CKPT.exists():
        with open(CKPT) as f:
            return set(json.load(f))
    return set()


def save_checkpoint(done: set):
    with open(CKPT, 'w') as f:
        json.dump(sorted(done), f)


def run(limit=None):
    print('Step 2 — Building EDGAR snapshots (annual + quarterly)')

    if not TICKERS.exists():
        print('ERROR: data/tickers.parquet not found. Run step 1 first.')
        sys.exit(1)

    tickers_df = pd.read_parquet(TICKERS)
    companies = tickers_df.to_dict('records')
    if limit:
        companies = companies[:limit]
        print(f'  TEST MODE: {limit} companies only')

    done_ciks = load_checkpoint()
    remaining = [c for c in companies if c['cik'] not in done_ciks]
    print(f'  Total: {len(companies):,} | Done: {len(done_ciks):,} | Remaining: {len(remaining):,}')

    rows = []
    if OUT.exists() and done_ciks:
        rows = pd.read_parquet(OUT).to_dict('records')
        print(f'  Loaded {len(rows):,} existing rows from checkpoint')

    errors = 0
    for i, company in enumerate(remaining):
        cik    = company['cik']
        ticker = company.get('ticker', '')
        name   = company.get('name', '')

        facts = fetch_company_facts(cik)
        if facts is None:
            errors += 1
            done_ciks.add(cik)
            continue

        snapshots = build_period_snapshots(facts)
        if not snapshots:
            done_ciks.add(cik)
            continue

        snapshots = add_yoy_features(snapshots)

        for snap in snapshots:
            snap['cik']             = cik
            snap['ticker']          = ticker
            snap['name']            = name
            snap['exchange']        = company.get('exchange')
            snap['sic_code']        = company.get('sic_code')
            snap['sic_description'] = company.get('sic_description', '')
            snap['market']          = company.get('market', 'US')
            snap['country']         = company.get('country', 'United States')
            snap['accounting_std']  = company.get('accounting_std', 'GAAP')
            rows.append(snap)

        done_ciks.add(cik)

        if (i + 1) % CHECKPOINT_EVERY == 0:
            df_tmp = pd.DataFrame(rows)
            df_tmp.to_parquet(OUT, index=False)
            save_checkpoint(done_ciks)
            annual_rows = len([r for r in rows if r.get('period_type') == 'annual'])
            qtr_rows    = len([r for r in rows if r.get('period_type') == 'quarterly'])
            print(f'  {i+1}/{len(remaining)} — {ticker} — '
                  f'rows: {len(rows):,} (ann:{annual_rows:,} qtr:{qtr_rows:,}) '
                  f'errors:{errors}')

    # Final save
    df = pd.DataFrame(rows)
    df.to_parquet(OUT, index=False)
    save_checkpoint(done_ciks)

    annual_rows = (df['period_type'] == 'annual').sum()
    qtr_rows    = (df['period_type'] == 'quarterly').sum()
    print(f'\nStep 2 complete.')
    print(f'  Total rows: {len(df):,} (annual: {annual_rows:,} | quarterly: {qtr_rows:,})')
    print(f'  Companies with data: {df["cik"].nunique():,}')
    print(f'  Columns: {len(df.columns)}')
    print(f'  Errors (no XBRL data): {errors}')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()
    run(limit=args.limit)
