"""
Step 2 KR — Build financial snapshots from DART (Korean EDGAR equivalent).

For each listed company, fetches annual + Q1/H1/Q3 financial statements
via DART fnlttSinglAcntAll API. Caches all responses to SQLite.

DART report codes:
  11011 = Annual (사업보고서)
  11013 = Q1 (1분기보고서)
  11012 = H1/Semi-annual (반기보고서)
  11014 = Q3/9-month (3분기보고서)

Output: data/snapshots_kr.parquet — same schema as snapshots.parquet,
  with market='KR', country='KR', accounting_std='K-IFRS'.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

BASE  = Path(__file__).parent.parent
DATA  = BASE / 'data'
OUT   = DATA / 'snapshots_kr.parquet'
CACHE = DATA / 'dart_cache.db'
TICK  = DATA / 'tickers_kr.parquet'

DART_BASE   = 'https://opendart.fss.or.kr/api'
START_YEAR  = 2008
RATE_DELAY  = 0.35   # ~3 req/s — conservative for DART free tier

REPORT_CODES = {
    '11011': ('annual',    'FY'),
    '11013': ('quarterly', 'Q1'),
    '11012': ('quarterly', 'Q2'),   # semi-annual in Korea = H1
    '11014': ('quarterly', 'Q3'),
}

# ── Account mapping ────────────────────────────────────────────────────────────
# Maps DART account_id (K-IFRS XBRL) → our column name.
# Only top-level totals (account_detail == '-') are extracted.

ACCOUNT_ID_MAP: dict[str, str] = {
    # Revenue
    'ifrs-full_Revenue':                                     'revenue',
    'dart_Revenue':                                          'revenue',
    'ifrs_Revenue':                                          'revenue',
    # Operating income
    'dart_OperatingIncomeLoss':                              'operating_income',
    'ifrs-full_ProfitLossFromOperatingActivities':           'operating_income',
    # Net income (consolidated attributable to parent)
    'ifrs-full_ProfitLossAttributableToOwnersOfParent':      'net_income',
    'ifrs-full_ProfitLoss':                                  'net_income',
    'dart_ProfitLoss':                                       'net_income',
    # Total assets
    'ifrs-full_Assets':                                      'total_assets',
    'dart_Assets':                                           'total_assets',
    # Total liabilities
    'ifrs-full_Liabilities':                                 'total_liabilities',
    'dart_Liabilities':                                      'total_liabilities',
    # Total equity
    'ifrs-full_Equity':                                      'total_equity',
    'ifrs-full_EquityAttributableToOwnersOfParent':          'total_equity',
    'dart_Equity':                                           'total_equity',
    # Cash & equivalents
    'ifrs-full_CashAndCashEquivalents':                      'cash',
    'dart_CashAndCashEquivalents':                           'cash',
    # Current assets / liabilities
    'ifrs-full_CurrentAssets':                               'current_assets',
    'ifrs-full_CurrentLiabilities':                          'current_liabilities',
    # Inventory
    'ifrs-full_Inventories':                                 'inventory',
    # Receivables
    'ifrs-full_TradeAndOtherCurrentReceivables':             'receivables',
    'ifrs-full_TradeAndOtherReceivables':                    'receivables',
    # Gross profit
    'ifrs-full_GrossProfit':                                 'gross_profit',
    # D&A
    'ifrs-full_DepreciationAndAmortisationExpense':          'depreciation_amortization',
    # PPE
    'ifrs-full_PropertyPlantAndEquipment':                   'ppe',
    # Retained earnings
    'ifrs-full_RetainedEarnings':                            'retained_earnings',
    # Operating cash flow
    'ifrs-full_CashFlowsFromUsedInOperatingActivities':      'cfo',
    'ifrs-full_CashFlowsFromOperatingActivities':            'cfo',
    # Capex (negative in CF statements — we store absolute value)
    'ifrs-full_PurchaseOfPropertyPlantAndEquipment':         'capex',
    # R&D
    'dart_ResearchAndDevelopmentExpense':                    'rd_expense',
    # SG&A
    'dart_SellingGeneralAndAdministrativeExpenses':          'sga',
    # Shares
    'dart_IssuedCapitalInShares':                            'shares_outstanding',
}

# Korean account name keywords (fallback for K-GAAP or non-standard XBRL IDs)
ACCOUNT_KW_MAP: dict[str, str] = {
    '매출액':           'revenue',
    '영업수익':         'revenue',
    '영업이익':         'operating_income',
    '당기순이익':       'net_income',
    '자산총계':         'total_assets',
    '부채총계':         'total_liabilities',
    '자본총계':         'total_equity',
    '현금및현금성자산': 'cash',
    '유동자산':         'current_assets',
    '유동부채':         'current_liabilities',
    '재고자산':         'inventory',
    '매출채권':         'receivables',
    '매출총이익':       'gross_profit',
    '유형자산':         'ppe',
    '이익잉여금':       'retained_earnings',
    '발행주식수':       'shares_outstanding',
}

NUMERIC_COLS = [
    'revenue', 'operating_income', 'net_income',
    'total_assets', 'total_liabilities', 'total_equity',
    'cash', 'current_assets', 'current_liabilities',
    'inventory', 'receivables', 'gross_profit',
    'depreciation_amortization', 'ppe', 'retained_earnings',
    'cfo', 'capex', 'rd_expense', 'sga', 'shares_outstanding',
]

# ── SQLite cache ───────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS dart_cache (
    cache_key  TEXT PRIMARY KEY,
    fetched_at TEXT,
    status     TEXT,
    data_json  TEXT
);
"""


class DartCache:
    def __init__(self, db_path=CACHE):
        self.db_path = str(db_path)
        conn = sqlite3.connect(self.db_path)
        conn.execute(_SCHEMA)
        conn.commit()
        conn.close()

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def has(self, key: str) -> bool:
        with self._conn() as c:
            return c.execute(
                'SELECT 1 FROM dart_cache WHERE cache_key=?', (key,)
            ).fetchone() is not None

    def get(self, key: str) -> dict | None:
        with self._conn() as c:
            row = c.execute(
                'SELECT data_json FROM dart_cache WHERE cache_key=?', (key,)
            ).fetchone()
        return json.loads(row[0]) if row else None

    def set(self, key: str, status: str, data: dict):
        with self._conn() as c:
            c.execute(
                'INSERT OR REPLACE INTO dart_cache VALUES (?,?,?,?)',
                (key, datetime.utcnow().isoformat(), status, json.dumps(data))
            )

    def count(self) -> int:
        with self._conn() as c:
            return c.execute('SELECT COUNT(*) FROM dart_cache').fetchone()[0]


# ── DART API helpers ───────────────────────────────────────────────────────────

def get_api_key() -> str:
    load_dotenv(BASE / '.env')
    key = os.environ.get('DART_API_KEY', '').strip()
    if not key:
        raise RuntimeError('DART_API_KEY not set in .env')
    return key


def parse_amount(s: str | None) -> float | None:
    """Parse DART amount string '1,234,567' or '(1,234,567)' → float."""
    if not s or str(s).strip() in ('', '-', 'None'):
        return None
    s = str(s).strip().replace(',', '').replace(' ', '')
    if s.startswith('(') and s.endswith(')'):
        s = '-' + s[1:-1]
    try:
        return float(s)
    except ValueError:
        return None


def fetch_statements(corp_code: str, bsns_year: int, reprt_code: str,
                     key: str, session: requests.Session,
                     cache: DartCache) -> list[dict]:
    """Fetch consolidated financial statement items for one (company, year, report)."""
    cache_key = f'{corp_code}_{bsns_year}_{reprt_code}'
    if cache.has(cache_key):
        cached = cache.get(cache_key)
        return cached.get('list', [])

    time.sleep(RATE_DELAY)
    try:
        r = session.get(
            f'{DART_BASE}/fnlttSinglAcntAll.json',
            params={
                'crtfc_key':  key,
                'corp_code':  corp_code,
                'bsns_year':  str(bsns_year),
                'reprt_code': reprt_code,
                'fs_div':     'CFS',   # consolidated; fall back to OFS below
            },
            timeout=8,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        cache.set(cache_key, 'error', {'list': []})
        return []

    status = data.get('status', '')
    items  = data.get('list', [])

    # 020 = rate limit exceeded — don't cache, caller will retry tomorrow
    if status == '020':
        return []

    # 013 = no consolidated data → try OFS (separate)
    if status == '013' or not items:
        time.sleep(RATE_DELAY)
        try:
            r2 = session.get(
                f'{DART_BASE}/fnlttSinglAcntAll.json',
                params={
                    'crtfc_key':  key,
                    'corp_code':  corp_code,
                    'bsns_year':  str(bsns_year),
                    'reprt_code': reprt_code,
                'fs_div':     'OFS',
                },
                timeout=8,
            )
            r2.raise_for_status()
            data2 = r2.json()
            if data2.get('status') == '020':
                return []
            items = data2.get('list', []) or []
        except Exception:
            items = []

    cache.set(cache_key, status, {'list': items})
    return items


def extract_accounts(items: list[dict]) -> dict[str, float | None]:
    """Parse account list into {column_name: value} dict."""
    result: dict[str, float | None] = {}

    for item in items:
        acct_id = item.get('account_id', '') or ''
        acct_nm = item.get('account_nm', '') or ''
        detail  = item.get('account_detail', '') or ''

        # Only top-level totals (not sub-line items)
        if detail not in ('-', '', None):
            continue

        val = parse_amount(item.get('thstrm_amount'))
        if val is None:
            continue

        # Try account_id first
        col = ACCOUNT_ID_MAP.get(acct_id)

        # Fall back to keyword match in account_nm
        if col is None:
            for kw, c in ACCOUNT_KW_MAP.items():
                if kw in acct_nm:
                    col = c
                    break

        if col and col not in result:
            result[col] = val

    return result


def rcept_to_date(rcept_no: str) -> str | None:
    """Extract filed_date from DART receipt number (first 8 chars = YYYYMMDD)."""
    if rcept_no and len(rcept_no) >= 8 and rcept_no[:8].isdigit():
        d = rcept_no[:8]
        return f'{d[:4]}-{d[4:6]}-{d[6:8]}'
    return None


def get_rcept_no(items: list[dict]) -> str | None:
    for item in items:
        rn = item.get('rcept_no', '')
        if rn and len(rn) >= 8:
            return rn
    return None


# ── YoY growth features ────────────────────────────────────────────────────────

def compute_yoy(df: pd.DataFrame) -> pd.DataFrame:
    """Add YoY growth columns. Works on the full snapshot table, grouped by ticker."""
    df = df.sort_values(['corp_code', 'period_type', 'filed_date']).copy()

    grow_pairs = [
        ('revenue',       'revenue_growth_yoy'),
        ('total_assets',  'asset_growth_yoy'),
        ('total_liabilities', 'debt_growth_yoy'),
        ('receivables',   'receivables_growth_yoy'),
        ('inventory',     'inventory_growth_yoy'),
        ('gross_profit',  'gross_profit_growth_yoy'),
        ('cfo',           'cfo_growth_yoy'),
    ]

    for grp, rows in df.groupby(['corp_code', 'period_type']):
        idx = rows.index
        for src, dst in grow_pairs:
            if src in df.columns:
                prev = df.loc[idx, src].shift(1)
                curr = df.loc[idx, src]
                with np.errstate(divide='ignore', invalid='ignore'):
                    growth = np.where(
                        prev.notna() & (prev != 0) & curr.notna(),
                        (curr - prev) / prev.abs(),
                        np.nan,
                    )
                df.loc[idx, dst] = growth

    return df


# ── Main ───────────────────────────────────────────────────────────────────────

CHECKPOINT = DATA / 'snapshots_kr_checkpoint.json'


def load_checkpoint() -> set:
    if CHECKPOINT.exists():
        return set(json.loads(CHECKPOINT.read_text()))
    return set()


def save_checkpoint(done: set):
    CHECKPOINT.write_text(json.dumps(list(done)))


def run(limit: int | None = None):
    DATA.mkdir(exist_ok=True)

    if not TICK.exists():
        print(f'ERROR: {TICK} not found — run step1_fetch_tickers_kr.py first')
        import sys; sys.exit(1)

    key     = get_api_key()
    tickers = pd.read_parquet(TICK)
    cache   = DartCache(CACHE)
    session = requests.Session()

    print('Step 2 KR — Building DART financial snapshots')
    print(f'  Companies: {len(tickers):,} | Cache entries: {cache.count():,}')

    if limit:
        tickers = tickers.head(limit).copy()
        print(f'  TEST MODE: limited to {len(tickers):,} companies')

    done       = load_checkpoint()
    todo       = tickers[~tickers['corp_code'].isin(done)].copy()
    all_rows   = []

    # Load existing partial output if resuming
    if OUT.exists() and done:
        existing = pd.read_parquet(OUT)
        all_rows = existing.to_dict('records')
        print(f'  Resuming — {len(all_rows):,} rows already saved')

    current_year = datetime.now().year
    years        = list(range(START_YEAR, current_year + 1))
    n_total      = len(todo)

    for i, (_, company) in enumerate(todo.iterrows()):
        corp_code   = company['corp_code']
        ticker      = company['ticker']
        name        = company['name']
        exchange    = company['exchange']
        acc_mt      = company.get('acc_mt', '12')

        rows_before = len(all_rows)

        for year in years:
            for reprt_code, (period_type, fp) in REPORT_CODES.items():
                items = fetch_statements(
                    corp_code, year, reprt_code, key, session, cache
                )
                if not items:
                    continue

                accounts   = extract_accounts(items)
                rcept_no   = get_rcept_no(items)
                filed_date = rcept_to_date(rcept_no)
                if not filed_date:
                    filed_date = f'{year}-04-01'   # fallback estimate

                row = {
                    'cik':           corp_code,
                    'corp_code':     corp_code,
                    'ticker':        ticker,
                    'name':          name,
                    'exchange':      exchange,
                    'fiscal_year':   year,
                    'fiscal_quarter': fp,
                    'period_type':   period_type,
                    'filed_date':    filed_date,
                    'market':        'KR',
                    'country':       'KR',
                    'accounting_std': company.get('accounting_std', 'K-IFRS'),
                    'acc_mt':        acc_mt,
                }
                row.update({col: accounts.get(col) for col in NUMERIC_COLS})

                # Derived: total_debt = total_liabilities as proxy (DART doesn't
                # separate financial debt easily without full BS parse)
                if row.get('total_liabilities') is not None:
                    row['total_debt'] = row['total_liabilities']

                # ebitda = operating_income + D&A
                oi  = row.get('operating_income')
                da  = row.get('depreciation_amortization')
                row['ebitda'] = (oi + da) if (oi is not None and da is not None) else None

                # capex: absolute value (DART reports as negative in CF)
                if row.get('capex') is not None:
                    row['capex'] = abs(row['capex'])

                all_rows.append(row)

        # Only checkpoint if this company produced actual rows (not all rate-limited)
        if len(all_rows) > rows_before:
            done.add(corp_code)

        if (i + 1) % 50 == 0:
            save_checkpoint(done)
            df_partial = pd.DataFrame(all_rows)
            df_partial.to_parquet(OUT, index=False)
            pct = 100 * (i + 1) / n_total
            print(f'  [{pct:.0f}%] {i+1:,}/{n_total:,} companies | '
                  f'{len(all_rows):,} rows | cache: {cache.count():,}')

    # Final save + YoY features
    save_checkpoint(done)
    df = pd.DataFrame(all_rows)

    if df.empty:
        print('  WARNING: no rows produced')
        import sys; sys.exit(1)

    print('  Computing YoY growth features ...')
    df = compute_yoy(df)

    df.to_parquet(OUT, index=False)

    print(f'\nStep 2 KR complete.')
    print(f'  Total rows:       {len(df):,}')
    print(f'  Unique companies: {df["corp_code"].nunique():,}')
    print(f'  Annual rows:      {(df["period_type"]=="annual").sum():,}')
    print(f'  Quarterly rows:   {(df["period_type"]=="quarterly").sum():,}')
    print(f'  Date range:       {df["filed_date"].min()} → {df["filed_date"].max()}')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()
    run(limit=args.limit)
