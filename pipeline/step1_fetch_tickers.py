"""
Step 1 — Fetch all US company tickers from SEC EDGAR.

Sources:
  - company_tickers.json      : ~13,000 companies with tickers (primary)
  - company_tickers_exchange.json : exchange info for listed companies
  - submissions API           : SIC code, SIC description per CIK

Output: data/tickers.parquet
  cik, ticker, name, exchange, sic_code, sic_description, market, country, accounting_std

Includes OTC companies not in exchange file (exchange='OTC').
Does NOT yet include fully delisted companies — that is added via the
delisted CIK supplemental list in a future enrichment pass.
"""

import requests
import pandas as pd
import time
import os
import sys
from pathlib import Path

BASE    = Path(__file__).parent.parent
DATA    = BASE / 'data'
OUT     = DATA / 'tickers.parquet'
HEADERS = {'User-Agent': 'AlphaResearchPipeline research@alpharesearch.io'}

EDGAR_RATE = 0.12   # seconds between EDGAR calls (~8/sec)


def fetch_json(url, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                wait = 5 * (attempt + 1)
                print(f'  Rate limited — waiting {wait}s...')
                time.sleep(wait)
        except Exception as e:
            if attempt == retries - 1:
                print(f'  Failed: {url} — {e}')
    return None


def get_sic(cik_padded, cache={}):
    """Fetch SIC code for a CIK. Cached in memory."""
    if cik_padded in cache:
        return cache[cik_padded]
    url = f'https://data.sec.gov/submissions/CIK{cik_padded}.json'
    data = fetch_json(url)
    time.sleep(EDGAR_RATE)
    if data:
        sic = data.get('sic')
        desc = data.get('sicDescription', '')
        cache[cik_padded] = (sic, desc)
        return sic, desc
    cache[cik_padded] = (None, '')
    return None, ''


def run(limit=None):
    DATA.mkdir(exist_ok=True)
    print('Step 1 — Fetching company ticker list from SEC EDGAR')

    # ── Source A: all companies with tickers (~13k) ────────────────────────
    print('  Fetching company_tickers.json ...')
    tickers_data = fetch_json('https://www.sec.gov/files/company_tickers.json')
    if not tickers_data:
        print('ERROR: could not fetch company_tickers.json')
        sys.exit(1)

    rows_primary = []
    for _, v in tickers_data.items():
        rows_primary.append({
            'cik':      str(v['cik_str']).zfill(10),
            'ticker':   v.get('ticker', '').upper(),
            'name':     v.get('title', ''),
            'exchange': None,
        })
    df_primary = pd.DataFrame(rows_primary)
    print(f'  company_tickers.json: {len(df_primary):,} companies')

    # ── Source B: exchange-listed companies (exchange info) ────────────────
    print('  Fetching company_tickers_exchange.json ...')
    exchange_data = fetch_json('https://www.sec.gov/files/company_tickers_exchange.json')
    exchange_map = {}  # cik → exchange
    if exchange_data:
        for row in exchange_data.get('data', []):
            cik_str = str(row[0]).zfill(10)
            exchange_map[cik_str] = row[3] if len(row) > 3 else None
        print(f'  Exchange data: {len(exchange_map):,} listed companies')

    df_primary['exchange'] = df_primary['cik'].map(exchange_map).fillna('OTC')

    # ── Deduplicate (some CIKs appear twice with different ticker formats) ──
    df = df_primary.drop_duplicates(subset='cik', keep='first').copy()
    print(f'  After dedup: {len(df):,} unique companies')

    # ── Limit for test mode ────────────────────────────────────────────────
    if limit:
        df = df.head(limit).copy()
        print(f'  TEST MODE: limited to {limit} companies')

    # ── Fetch SIC codes in batches ─────────────────────────────────────────
    print(f'  Fetching SIC codes for {len(df):,} companies ...')
    if not limit:
        print('  (this takes ~20 minutes — safe to interrupt, step 2 will still work)')

    sic_codes, sic_descs = [], []
    for i, row in df.iterrows():
        sic, desc = get_sic(row['cik'])
        sic_codes.append(sic)
        sic_descs.append(desc)
        if (i + 1) % 500 == 0:
            print(f'    {i+1:,}/{len(df):,} SIC codes fetched ...')

    df['sic_code']        = sic_codes
    df['sic_description'] = sic_descs

    # ── Add market metadata ────────────────────────────────────────────────
    df['market']          = 'US'
    df['country']         = 'United States'
    df['accounting_std']  = 'GAAP'

    # ── Save ──────────────────────────────────────────────────────────────
    df = df[['cik', 'ticker', 'name', 'exchange', 'sic_code',
             'sic_description', 'market', 'country', 'accounting_std']]
    df.to_parquet(OUT, index=False)

    listed = (df['exchange'] != 'OTC').sum()
    otc    = (df['exchange'] == 'OTC').sum()
    print(f'\nStep 1 complete.')
    print(f'  Total companies: {len(df):,}')
    print(f'  Exchange-listed: {listed:,}')
    print(f'  OTC / unlisted:  {otc:,}')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()
    run(limit=args.limit)
