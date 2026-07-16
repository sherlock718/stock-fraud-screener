"""
Step 2 JP — Build financial snapshots from EDINET XBRL zip downloads (Japan).

For each listed company, downloads annual XBRL zip from EDINET and parses
key financial statement items from the structured XML.

EDINET API key required (free): set EDINET_API_KEY in .env
  https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/WZEK0110.html

Covers: docTypeCode 120 = 有価証券報告書 (annual securities report), 2015-2024.
Quarterly reports (130/140) optionally.

Output: data/snapshots_jp.parquet — same schema as snapshots.parquet,
  market='JP', country='JP', accounting_std='IFRS' or 'J-GAAP'
"""

from __future__ import annotations

import datetime
import io
import json
import os
import sqlite3
import time
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

BASE  = Path(__file__).parent.parent
DATA  = BASE / 'data'
OUT   = DATA / 'snapshots_jp.parquet'
TICK  = DATA / 'tickers_jp.parquet'
CACHE = DATA / 'edinet_cache.db'

EDINET_BASE = 'https://api.edinet-fsa.go.jp/api/v2'
START_YEAR  = 2015   # EDINET XBRL reliable from 2015
RATE_DELAY  = 0.5    # ~2 req/s

# ── XBRL namespace prefixes used in EDINET financial reports ──────────────────
# IFRS-based (large listed companies) + J-GAAP fallback

IFRS_ELEMENTS = {
    # Revenue
    'ifrs-full_Revenue':                                   'revenue',
    'jpigp_cor:NetSalesIFRS':                              'revenue',
    # Gross profit
    'ifrs-full_GrossProfit':                               'gross_profit',
    # Operating income
    'ifrs-full_ProfitLossFromOperatingActivities':         'operating_income',
    'jpigp_cor:OperatingProfitLossIFRS':                   'operating_income',
    # Net income
    'ifrs-full_ProfitLoss':                                'net_income',
    'ifrs-full_ProfitLossAttributableToOwnersOfParent':    'net_income',
    # Pretax income
    'ifrs-full_ProfitLossBeforeTax':                       'pretax_income',
    # Total assets
    'ifrs-full_Assets':                                    'total_assets',
    # Current assets
    'ifrs-full_CurrentAssets':                             'current_assets',
    # Cash
    'ifrs-full_CashAndCashEquivalents':                    'cash',
    # Receivables
    'ifrs-full_TradeAndOtherCurrentReceivables':           'receivables',
    'ifrs-full_TradeAndOtherReceivables':                  'receivables',
    # Inventory
    'ifrs-full_Inventories':                               'inventory',
    # Total liabilities
    'ifrs-full_Liabilities':                               'total_liabilities',
    # Current liabilities
    'ifrs-full_CurrentLiabilities':                        'current_liabilities',
    # Equity
    'ifrs-full_Equity':                                    'equity',
    'ifrs-full_EquityAttributableToOwnersOfParent':        'equity',
    # PPE
    'ifrs-full_PropertyPlantAndEquipment':                 'ppe_net',
    # LT debt
    'ifrs-full_NoncurrentPortionOfLongtermBorrowings':     'long_term_debt',
    # Cash flows
    'ifrs-full_CashFlowsFromUsedInOperatingActivities':    'operating_cash_flow',
    'ifrs-full_CashFlowsFromOperatingActivities':          'operating_cash_flow',
    'ifrs-full_PurchaseOfPropertyPlantAndEquipment':       'capex',
    'ifrs-full_CashFlowsFromUsedInFinancingActivities':    'financing_cash_flow',
    # EPS
    'ifrs-full_BasicEarningsLossPerShare':                 'eps_basic',
    'ifrs-full_DilutedEarningsLossPerShare':               'eps_diluted',
    # Shares
    'jpigp_cor:NumberOfSharesIssuedCommonStock':           'shares_outstanding',
    # R&D
    'jpigp_cor:ResearchAndDevelopmentExpensesIFRS':        'rd_expense',
    # Interest
    'ifrs-full_InterestExpense':                           'interest_expense',
    # SGA
    'ifrs-full_SalesAndMarketingExpense':                  'sga_expense',
}

JGAAP_ELEMENTS = {
    # J-GAAP (non-IFRS companies, typically smaller)
    'jppfs_cor:NetSales':                                  'revenue',
    'jppfs_cor:CostOfSales':                               'cogs',
    'jppfs_cor:GrossProfit':                               'gross_profit',
    'jppfs_cor:OperatingIncome':                           'operating_income',
    'jppfs_cor:OrdinaryIncome':                            'pretax_income',
    'jppfs_cor:NetIncome':                                 'net_income',
    'jppfs_cor:NetIncomeLoss':                             'net_income',
    'jppfs_cor:Assets':                                    'total_assets',
    'jppfs_cor:CurrentAssets':                             'current_assets',
    'jppfs_cor:CashAndDeposits':                           'cash',
    'jppfs_cor:NotesAndAccountsReceivable':                'receivables',
    'jppfs_cor:Inventories':                               'inventory',
    'jppfs_cor:Liabilities':                               'total_liabilities',
    'jppfs_cor:CurrentLiabilities':                        'current_liabilities',
    'jppfs_cor:NetAssets':                                 'equity',
    'jppfs_cor:PropertyPlantAndEquipmentNet':              'ppe_net',
    'jppfs_cor:LongTermLoans':                             'long_term_debt',
    'jppfs_cor:NetCashProvidedByUsedInOperatingActivities': 'operating_cash_flow',
    'jppfs_cor:NetCashUsedInInvestingActivities':          'cfi',
    'jppfs_cor:NetCashProvidedByUsedInFinancingActivities': 'financing_cash_flow',
    'jppfs_cor:CapitalExpenditures':                       'capex',
    'jppfs_cor:ResearchAndDevelopmentCosts':               'rd_expense',
    'jppfs_cor:InterestExpenses':                          'interest_expense',
    'jppfs_cor:SellingGeneralAndAdministrativeExpenses':   'sga_expense',
    'jppfs_cor:NetIncomePerShare':                         'eps_basic',
    'jppfs_cor:NumberOfSharesIssuedCommonStock':           'shares_outstanding',
}

ALL_ELEMENTS = {**IFRS_ELEMENTS, **JGAAP_ELEMENTS}


# ── SQLite cache ───────────────────────────────────────────────────────────────

class EdinetCache:
    def __init__(self, db_path: Path):
        self.db_path = str(db_path)
        conn = self._conn()
        conn.execute('''CREATE TABLE IF NOT EXISTS edinet_cache (
            doc_id     TEXT PRIMARY KEY,
            fetched_at TEXT,
            status     TEXT,
            data_json  TEXT
        )''')
        conn.commit()
        conn.close()

    def _conn(self):
        return sqlite3.connect(self.db_path, timeout=30)

    def has(self, doc_id: str) -> bool:
        with self._conn() as c:
            return c.execute('SELECT 1 FROM edinet_cache WHERE doc_id=?', (doc_id,)).fetchone() is not None

    def get(self, doc_id: str) -> dict | None:
        with self._conn() as c:
            row = c.execute('SELECT data_json FROM edinet_cache WHERE doc_id=?', (doc_id,)).fetchone()
            return json.loads(row[0]) if row else None

    def set(self, doc_id: str, status: str, data: dict):
        with self._conn() as c:
            c.execute('INSERT OR REPLACE INTO edinet_cache VALUES (?,?,?,?)',
                      (doc_id, datetime.datetime.utcnow().isoformat(), status, json.dumps(data)))

    def count(self) -> int:
        with self._conn() as c:
            return c.execute('SELECT COUNT(*) FROM edinet_cache').fetchone()[0]


# ── XBRL parsing ───────────────────────────────────────────────────────────────

def parse_xbrl_zip(content: bytes, edinet_code: str) -> dict:
    """
    Extract financial items from EDINET XBRL zip.
    Returns {column_name: value} dict.
    """
    result: dict[str, float] = {}
    acc_std = 'J-GAAP'

    try:
        zf = zipfile.ZipFile(io.BytesIO(content))
    except Exception:
        return result

    # Find the main XBRL document (largest .xml or .xbrl file, excluding audit)
    xml_files = [f for f in zf.namelist()
                 if (f.endswith('.xml') or f.endswith('.xbrl'))
                 and 'AuditDoc' not in f and 'manifest' not in f.lower()]

    if not xml_files:
        return result

    # Parse each XML file
    for fname in xml_files:
        try:
            tree = ET.parse(zf.open(fname))
            root = tree.getroot()

            for elem in root.iter():
                # Strip namespace prefix from tag
                tag = elem.tag
                if '}' in tag:
                    tag = tag.split('}')[1]

                # Build local:tag form for matching
                local_tag = elem.tag.replace('{', '').replace('}', ':').split('/')[-1]
                # Try various tag formats
                for try_tag in [local_tag, tag]:
                    if try_tag in ALL_ELEMENTS:
                        col = ALL_ELEMENTS[try_tag]
                        if col not in result and elem.text:
                            try:
                                result[col] = float(elem.text.replace(',', ''))
                                if 'ifrs' in try_tag.lower() or 'jpigp' in try_tag.lower():
                                    acc_std = 'IFRS'
                            except (ValueError, TypeError):
                                pass
        except Exception:
            continue

    result['_acc_std'] = acc_std
    return result


# ── Document discovery ─────────────────────────────────────────────────────────

def get_annual_doc_ids(key: str, year: int) -> list[dict]:
    """
    Get all annual report document IDs for a given fiscal year.
    Japanese fiscal year typically ends March 31, filed by June 30.
    """
    docs = []
    # Scan April-August of year+1 (filing period for March FY end)
    # Also scan October-February for companies with other fiscal year ends
    scan_ranges = [
        (f'{year+1}-04-01', f'{year+1}-07-31'),
        (f'{year}-10-01',   f'{year+1}-03-31'),
    ]

    for start_str, end_str in scan_ranges:
        start = datetime.date.fromisoformat(start_str)
        end   = datetime.date.fromisoformat(end_str)
        current = start

        while current <= end:
            date_str = current.strftime('%Y-%m-%d')
            try:
                r = requests.get(
                    f'{EDINET_BASE}/documents.json',
                    params={'date': date_str, 'type': 2, 'Subscription-Key': key},
                    timeout=15,
                )
                if r.status_code == 200:
                    for doc in r.json().get('results', []):
                        if doc.get('docTypeCode') == '120':  # annual report
                            docs.append(doc)
                time.sleep(0.2)
            except Exception:
                pass
            current += datetime.timedelta(days=1)

    return docs


# ── Main ───────────────────────────────────────────────────────────────────────

def run(years: list[int] | None = None, limit: int | None = None):
    DATA.mkdir(exist_ok=True)
    load_dotenv(BASE / '.env')
    key = os.environ.get('EDINET_API_KEY', '').strip()
    if not key:
        raise RuntimeError(
            'EDINET_API_KEY not set.\n'
            'Register free at: https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/WZEK0110.html\n'
            'Then add to .env: EDINET_API_KEY=your_key_here'
        )

    tickers = pd.read_parquet(TICK) if TICK.exists() else pd.DataFrame()
    valid_codes = set(tickers['edinet_code'].astype(str)) if not tickers.empty else None

    cache  = EdinetCache(CACHE)
    session = requests.Session()
    session.headers.update({'Subscription-Key': key})

    target_years = years or list(range(START_YEAR, 2025))
    print(f'Step 2 JP — Building EDINET snapshots for years: {target_years[0]}–{target_years[-1]}')
    print(f'  Cache entries: {cache.count():,}')

    all_rows = []

    for year in target_years:
        print(f'\n  Year {year}: discovering annual reports ...')
        docs = get_annual_doc_ids(key, year)
        print(f'  Found {len(docs):,} annual reports')

        if limit:
            docs = docs[:limit]

        for doc in docs:
            doc_id     = doc.get('docID', '')
            edinet_code = doc.get('edinetCode', '')
            filer_name = doc.get('filerName', '')
            sec_code   = doc.get('secCode', '')
            period_end = doc.get('periodEnd', '')

            if valid_codes and edinet_code not in valid_codes:
                continue

            if not doc_id:
                continue

            # Check cache
            if cache.has(doc_id):
                cached = cache.get(doc_id)
                if cached and cached.get('data'):
                    all_rows.append(cached['data'])
                continue

            # Download XBRL zip
            time.sleep(RATE_DELAY)
            try:
                r = session.get(
                    f'{EDINET_BASE}/documents/{doc_id}',
                    params={'type': 5},  # type 5 = XBRL zip
                    timeout=30,
                )
                if r.status_code != 200:
                    cache.set(doc_id, 'error', {})
                    continue

                financials = parse_xbrl_zip(r.content, edinet_code)
                acc_std    = financials.pop('_acc_std', 'J-GAAP')

                if not financials:
                    cache.set(doc_id, 'empty', {})
                    continue

                row = {
                    'cik':            edinet_code,
                    'ticker':         f"{sec_code}0.T" if sec_code and len(sec_code) == 4 else '',
                    'stock_code':     sec_code,
                    'name':           filer_name,
                    'filed_date':     doc.get('submitDateTime', '')[:10],
                    'availability_timestamp': doc.get('submitDateTime', ''),
                    'availability_provenance': 'edinet_submission',
                    'fiscal_year':    str(year),
                    'fiscal_quarter': 'FY',
                    'period_type':    'annual',
                    'market':         'JP',
                    'entity_id':      f'JP:{edinet_code}',
                    'country':        'JP',
                    'exchange':       'TSE',
                    'currency':       'JPY',
                    'accounting_std': acc_std,
                    **financials,
                }
                all_rows.append(row)
                cache.set(doc_id, 'ok', {'data': row})

            except Exception as e:
                cache.set(doc_id, 'error', {})
                continue

        print(f'  Year {year} done. Total rows so far: {len(all_rows):,}')

    if not all_rows:
        print('ERROR: no financial data extracted')
        import sys; sys.exit(1)

    combined = pd.DataFrame(all_rows)

    # Capex: EDINET reports as negative — store absolute
    if 'capex' in combined.columns:
        combined['capex'] = combined['capex'].abs()

    # YoY growth
    combined = combined.sort_values(['cik', 'filed_date']).copy()
    grow_pairs = [
        ('revenue',           'revenue_growth_yoy'),
        ('total_assets',      'asset_growth_yoy'),
        ('total_liabilities', 'debt_growth_yoy'),
        ('receivables',       'receivables_growth_yoy'),
        ('inventory',         'inventory_growth_yoy'),
        ('gross_profit',      'gross_profit_growth_yoy'),
        ('operating_cash_flow', 'ocf_growth_yoy'),
        ('net_income',        'net_income_growth_yoy'),
    ]
    for _, rows in combined.groupby('cik'):
        idx = rows.index
        for src, dst in grow_pairs:
            if src in combined.columns:
                prev  = combined.loc[idx, src].shift(1)
                curr  = combined.loc[idx, src]
                with np.errstate(divide='ignore', invalid='ignore'):
                    growth = np.where(
                        prev.notna() & (prev != 0) & curr.notna(),
                        (curr - prev) / prev.abs(), np.nan,
                    )
                combined.loc[idx, dst] = growth

    combined.to_parquet(OUT, index=False)

    print(f'\nStep 2 JP complete.')
    print(f'  Total rows:       {len(combined):,}')
    print(f'  Unique companies: {combined["cik"].nunique():,}')
    print(f'  IFRS:             {(combined["accounting_std"]=="IFRS").sum():,}')
    print(f'  J-GAAP:           {(combined["accounting_std"]=="J-GAAP").sum():,}')
    print(f'  Date range:       {combined["filed_date"].min()} → {combined["filed_date"].max()}')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--years', nargs='+', type=int, default=None)
    parser.add_argument('--limit', type=int, default=None,
                        help='Max docs per year (for testing)')
    args = parser.parse_args()
    run(years=args.years, limit=args.limit)
