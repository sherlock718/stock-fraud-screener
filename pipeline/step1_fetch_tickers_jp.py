"""
Step 1 JP — Fetch Japanese listed company tickers from EDINET.

EDINET (Electronic Disclosure for Investors' NETwork) is Japan's EDGAR equivalent.
Requires a free Subscription-Key from:
  https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/WZEK0110.html

Add to .env:
  EDINET_API_KEY=your_key_here

Output: data/tickers_jp.parquet
  edinet_code, ticker, stock_code, name, exchange,
  industry_code, market, country, accounting_std
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

BASE = Path(__file__).parent.parent
DATA = BASE / 'data'
OUT  = DATA / 'tickers_jp.parquet'

EDINET_BASE = 'https://api.edinet-fsa.go.jp/api/v2'

# Major Japanese exchanges
EXCHANGE_MAP = {
    '東京証券取引所':   ('TSE',    '.T'),
    '大阪証券取引所':   ('OSE',    '.T'),
    '名古屋証券取引所': ('NSE',    '.T'),
    '福岡証券取引所':   ('FSE',    '.T'),
    '札幌証券取引所':   ('SSE',    '.T'),
}


def get_api_key() -> str:
    load_dotenv(BASE / '.env')
    key = os.environ.get('EDINET_API_KEY', '').strip()
    if not key:
        raise RuntimeError(
            'EDINET_API_KEY not set in .env.\n'
            'Get a free key at: https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/WZEK0110.html\n'
            'Add to .env: EDINET_API_KEY=your_key_here'
        )
    return key


def fetch_company_list(key: str) -> pd.DataFrame:
    """
    Fetch the full EDINET company/submitter list.
    Endpoint: GET /api/v2/companies.json
    """
    print('  Fetching EDINET company list ...')
    params = {'type': 2, 'Subscription-Key': key}

    try:
        r = requests.get(f'{EDINET_BASE}/companies.json', params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        companies = data.get('results', [])
        print(f'  Total EDINET submitters: {len(companies):,}')
        return pd.DataFrame(companies)
    except Exception as e:
        print(f'  WARN: company list endpoint failed: {e}')
        # Fallback: build company list from recent document submissions
        return fetch_companies_from_docs(key)


def fetch_companies_from_docs(key: str) -> pd.DataFrame:
    """
    Fallback: scan recent dates for annual report filers.
    docTypeCode 120 = 有価証券報告書 (annual securities report)
    """
    import datetime
    print('  Fallback: scanning recent submissions for annual report filers ...')

    seen: dict[str, dict] = {}
    # Scan last 400 days (one full year of filings)
    base_date = datetime.date(2024, 3, 31)  # fiscal year end for most Japanese companies

    for delta in range(0, 400, 7):  # weekly
        date_str = (base_date - datetime.timedelta(days=delta)).strftime('%Y-%m-%d')
        try:
            r = requests.get(
                f'{EDINET_BASE}/documents.json',
                params={'date': date_str, 'type': 2, 'Subscription-Key': key},
                timeout=15,
            )
            if r.status_code != 200:
                continue
            for doc in r.json().get('results', []):
                if doc.get('docTypeCode') == '120':  # annual report
                    code = doc.get('edinetCode', '')
                    if code and code not in seen:
                        seen[code] = {
                            'edinet_code':    code,
                            'name':           doc.get('filerName', ''),
                            'security_code':  doc.get('secCode', ''),
                            'industry':       doc.get('industryCode', ''),
                        }
            time.sleep(0.2)
        except Exception:
            continue

    print(f'  Found {len(seen):,} annual report filers')
    return pd.DataFrame(list(seen.values()))


def run():
    DATA.mkdir(exist_ok=True)
    key = get_api_key()

    print('Step 1 JP — Fetching Japanese ticker list from EDINET')

    df = fetch_company_list(key)

    if df.empty:
        print('ERROR: no companies returned from EDINET')
        import sys; sys.exit(1)

    # Normalise columns
    if 'edinetCode' in df.columns:
        df = df.rename(columns={
            'edinetCode':   'edinet_code',
            'filerName':    'name',
            'secCode':      'security_code',
            'industryCode': 'industry',
        })

    # Filter to listed companies (security_code present = exchange-listed)
    df = df[df['security_code'].notna() & (df['security_code'] != '')].copy()
    print(f'  Listed companies (have security code): {len(df):,}')

    # Build yfinance ticker: security_code + '0' + '.T'
    # Japanese security codes are 4 digits; yfinance uses 4-digit code + '0.T'
    df['stock_code'] = df['security_code'].astype(str).str.strip()
    df['ticker']     = df['stock_code'].apply(
        lambda s: f"{s}0.T" if len(s) == 4 and s.isdigit() else f"{s}.T"
    )
    df['edinet_code']    = df['edinet_code'].astype(str)
    df['exchange']       = 'TSE'
    df['market']         = 'JP'
    df['country']        = 'JP'
    df['accounting_std'] = 'IFRS'  # major companies; smaller use J-GAAP
    df['industry_code']  = df.get('industry', '').astype(str)

    result = df[['edinet_code', 'ticker', 'stock_code', 'name',
                 'exchange', 'industry_code', 'market', 'country', 'accounting_std']].copy()

    result.to_parquet(OUT, index=False)

    print(f'\nStep 1 JP complete.')
    print(f'  Total companies: {len(result):,}')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    run()
