"""
Step 1 CA — Fetch Canadian listed company tickers from TMX public API.

No API key required. Queries the TSX and TSXV company directory
(publicly accessible JSON endpoints) for all letters a-z and digits.

Output: data/tickers_ca.parquet
  cik, ticker, stock_code, name, exchange, sic_code, sic_description,
  market, country, accounting_std
"""

from __future__ import annotations

import string
import time
from pathlib import Path

import pandas as pd
import requests

BASE = Path(__file__).parent.parent
DATA = BASE / 'data'
OUT  = DATA / 'tickers_ca.parquet'

TMX_URL = 'https://www.tsx.com/json/company-directory/search/{market}/{letter}'
LETTERS = string.ascii_lowercase + string.digits

EXCHANGE_MAP = {'tsx': 'TSX', 'tsxv': 'TSXV'}
SUFFIX_MAP   = {'TSX': '.TO', 'TSXV': '.V'}

HEADERS = {'User-Agent': 'Mozilla/5.0 (research; stock screener)'}


def fetch_letter(session: requests.Session, market: str, letter: str) -> list[dict]:
    url = TMX_URL.format(market=market, letter=letter)
    try:
        r = session.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        data = r.json()
        return data.get('results', [])
    except Exception as e:
        print(f'    WARN: {market}/{letter} failed: {e}')
        return []


def run(limit: int | None = None):
    DATA.mkdir(exist_ok=True)
    print('Step 1 CA — Fetching Canadian ticker list from TMX')

    session = requests.Session()
    records: dict[str, dict] = {}  # symbol → record (dedup)

    for market_code in ('tsx', 'tsxv'):
        exchange = EXCHANGE_MAP[market_code]
        suffix   = SUFFIX_MAP[exchange]
        print(f'\n  Market: {exchange}')
        total = 0

        for letter in LETTERS:
            results = fetch_letter(session, market_code, letter)
            for item in results:
                symbol = item.get('symbol', '').strip()
                name   = item.get('name', item.get('companyName', '')).strip()
                if not symbol:
                    continue
                key = symbol.upper()
                if key not in records:
                    records[key] = {
                        'cik':            symbol.upper(),
                        'ticker':         symbol.upper() + suffix,
                        'stock_code':     symbol.upper(),
                        'name':           name,
                        'exchange':       exchange,
                        'sic_code':       None,
                        'sic_description': None,
                        'market':         'CA',
                        'country':        'Canada',
                        'accounting_std': 'IFRS',
                    }
                    total += 1
            time.sleep(0.05)

        print(f'  Found {total:,} unique {exchange} companies')
        if limit and len(records) >= limit:
            break

    df = pd.DataFrame(list(records.values()))
    if limit:
        df = df.head(limit).copy()
        print(f'\n  TEST MODE: limited to {len(df):,} companies')

    df.to_parquet(OUT, index=False)

    tsx_count  = (df['exchange'] == 'TSX').sum()
    tsxv_count = (df['exchange'] == 'TSXV').sum()
    print(f'\nStep 1 CA complete.')
    print(f'  Total companies: {len(df):,}')
    print(f'  TSX:             {tsx_count:,}')
    print(f'  TSXV:            {tsxv_count:,}')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()
    run(limit=args.limit)
