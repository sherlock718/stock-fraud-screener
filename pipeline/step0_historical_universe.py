"""
Step 0 — Build survivorship-free historical universe from SEC EDGAR full-index.

Scans EDGAR full-index files to discover all CIKs that ever filed a 10-K or 10-K/A,
regardless of current listing status. This recovers delisted companies missing from
the live company_tickers.json endpoint.

Two modes:
  --scan    : Download and parse full-index files (slow, ~30 min for all years)
  --verify  : HEAD-check which historical CIKs have XBRL company-facts data
  --merge   : Merge verified historical CIKs into tickers.parquet

Output: data/historical_ciks.parquet
  cik, name, first_10k_year, last_10k_year, has_xbrl, source

The merge step appends new CIKs to data/tickers.parquet with exchange='DELISTED'.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests

BASE = Path(__file__).parent.parent
DATA = BASE / 'data'
OUT = DATA / 'historical_ciks.parquet'
TICKERS = DATA / 'tickers.parquet'
CACHE = DATA / 'historical_ciks_cache.json'
HEADERS = {'User-Agent': 'AlphaResearchPipeline research@alpharesearch.io'}

FULL_INDEX_URL = 'https://www.sec.gov/Archives/edgar/full-index/{year}/{qtr}/company.idx'
XBRL_API_URL = 'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
SUBMISSIONS_URL = 'https://data.sec.gov/submissions/CIK{cik}.json'

START_YEAR = 2009
END_YEAR = 2025


def scan_full_index(year: int, qtr: str) -> list[dict]:
    """Parse one quarter's full-index for 10-K/10-K/A filings."""
    url = FULL_INDEX_URL.format(year=year, qtr=qtr)
    try:
        r = requests.get(url, headers=HEADERS, timeout=60)
        if r.status_code != 200:
            print(f'  WARN: {url} returned {r.status_code}')
            return []
    except Exception as e:
        print(f'  ERROR: {url} — {e}')
        return []

    results = []
    for line in r.text.split('\n'):
        if '10-K' not in line:
            continue
        # Fixed-width format: Company Name | Form Type | CIK | Date Filed | Filename
        # Form types: 10-K, 10-K/A, 10-KSB, 10-KSB/A
        parts = line.strip().split()
        if not parts:
            continue

        # Find CIK (first purely numeric token of 5+ digits)
        cik = None
        form_type = None
        date_filed = None
        for i, p in enumerate(parts):
            if p in ('10-K', '10-K/A', '10-KSB', '10-KSB/A'):
                form_type = p
            elif p.isdigit() and len(p) >= 5 and cik is None:
                cik = p
            elif len(p) == 10 and '-' in p and cik is not None:
                date_filed = p
                break

        if cik and form_type and ('10-K' in form_type):
            # Extract company name (everything before form type)
            name_end = line.find(form_type)
            name = line[:name_end].strip() if name_end > 0 else ''
            results.append({
                'cik': cik.zfill(10),
                'name': name,
                'form_type': form_type,
                'date_filed': date_filed,
                'year': year,
                'qtr': qtr,
            })

    return results


def run_scan():
    """Download and parse full-index files for all years."""
    print(f'Step 0 — Scanning EDGAR full-index ({START_YEAR}–{END_YEAR})')

    all_filings = []
    for year in range(START_YEAR, END_YEAR + 1):
        for qtr in ['QTR1', 'QTR2', 'QTR3', 'QTR4']:
            filings = scan_full_index(year, qtr)
            all_filings.extend(filings)
            if filings:
                print(f'  {year}/{qtr}: {len(filings)} 10-K filings')
            time.sleep(0.2)

    # Aggregate by CIK
    cik_info = {}
    for f in all_filings:
        cik = f['cik']
        if cik not in cik_info:
            cik_info[cik] = {
                'cik': cik,
                'name': f['name'],
                'first_10k_year': f['year'],
                'last_10k_year': f['year'],
            }
        else:
            cik_info[cik]['first_10k_year'] = min(cik_info[cik]['first_10k_year'], f['year'])
            cik_info[cik]['last_10k_year'] = max(cik_info[cik]['last_10k_year'], f['year'])
            if not cik_info[cik]['name'] and f['name']:
                cik_info[cik]['name'] = f['name']

    # Save cache
    with open(CACHE, 'w') as fp:
        json.dump(list(cik_info.values()), fp)

    print(f'\n  Scan complete: {len(cik_info):,} unique CIKs filed 10-K in {START_YEAR}–{END_YEAR}')
    print(f'  Cache saved: {CACHE}')
    return cik_info


def run_verify():
    """Check which historical CIKs have XBRL data via HEAD requests."""
    print('Step 0 — Verifying XBRL availability for historical CIKs')

    if not CACHE.exists():
        print(f'ERROR: {CACHE} not found. Run --scan first.')
        sys.exit(1)

    with open(CACHE) as fp:
        cik_list = json.load(fp)

    # Filter to CIKs NOT already in tickers.parquet
    existing_ciks = set()
    if TICKERS.exists():
        existing_ciks = set(pd.read_parquet(TICKERS)['cik'].unique())

    new_ciks = [c for c in cik_list if c['cik'] not in existing_ciks]
    print(f'  Total historical CIKs: {len(cik_list):,}')
    print(f'  Already in tickers: {len(cik_list) - len(new_ciks):,}')
    print(f'  New (need XBRL check): {len(new_ciks):,}')

    verified = []
    for i, c in enumerate(new_ciks):
        cik = c['cik']
        url = XBRL_API_URL.format(cik=cik)
        try:
            r = requests.get(url, headers=HEADERS, timeout=15, stream=True)
            c['has_xbrl'] = r.status_code == 200
            r.close()
        except Exception:
            c['has_xbrl'] = False

        if c['has_xbrl']:
            verified.append(c)

        time.sleep(0.12)
        if (i + 1) % 200 == 0:
            print(f'    {i+1}/{len(new_ciks)} checked — {len(verified)} with XBRL')

    # Fetch SIC codes for verified CIKs
    print(f'  Fetching SIC codes for {len(verified)} verified CIKs ...')
    for c in verified:
        url = SUBMISSIONS_URL.format(cik=c['cik'])
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                data = r.json()
                c['sic_code'] = data.get('sic')
                c['sic_description'] = data.get('sicDescription', '')
        except Exception:
            c['sic_code'] = None
            c['sic_description'] = ''
        time.sleep(0.12)

    for c in verified:
        c['source'] = 'full_index_historical'

    df = pd.DataFrame(verified)
    df.to_parquet(OUT, index=False)
    print(f'\n  Verified: {len(verified):,} CIKs with XBRL data')
    print(f'  Saved: {OUT}')


def run_merge():
    """Merge verified historical CIKs into tickers.parquet."""
    print('Step 0 — Merging historical CIKs into universe')

    if not OUT.exists():
        print(f'ERROR: {OUT} not found. Run --scan then --verify first.')
        sys.exit(1)

    if not TICKERS.exists():
        print(f'ERROR: {TICKERS} not found. Run step 1 first.')
        sys.exit(1)

    historical = pd.read_parquet(OUT)
    tickers = pd.read_parquet(TICKERS)

    existing_ciks = set(tickers['cik'].unique())
    new = historical[~historical['cik'].isin(existing_ciks)].copy()

    if new.empty:
        print('  No new CIKs to add.')
        return

    # Build rows matching tickers.parquet schema
    new_rows = pd.DataFrame({
        'cik': new['cik'],
        'ticker': '',
        'name': new['name'],
        'exchange': 'DELISTED',
        'sic_code': new.get('sic_code'),
        'sic_description': new.get('sic_description', ''),
        'market': 'US',
        'country': 'United States',
        'accounting_std': 'GAAP',
    })

    # Backup original
    backup = DATA / 'tickers_pre_historical.parquet'
    if not backup.exists():
        tickers.to_parquet(backup, index=False)
        print(f'  Backup saved: {backup}')

    merged = pd.concat([tickers, new_rows], ignore_index=True)
    merged.to_parquet(TICKERS, index=False)
    print(f'  Added {len(new_rows):,} historical CIKs to tickers.parquet')
    print(f'  New total: {len(merged):,} CIKs')


def run_add_known(ciks: list[str]):
    """
    Add specific known CIKs directly (bypasses full-index scan).
    Used for known fraud/bankruptcy cases that have XBRL data.
    """
    print(f'Step 0 — Adding {len(ciks)} known historical CIKs')

    verified = []
    for cik_raw in ciks:
        cik = cik_raw.zfill(10)
        # Check XBRL availability (HEAD not supported by EDGAR, use streamed GET)
        url = XBRL_API_URL.format(cik=cik)
        try:
            r = requests.get(url, headers=HEADERS, timeout=15, stream=True)
            has_xbrl = r.status_code == 200
            r.close()
        except Exception:
            has_xbrl = False
        time.sleep(0.12)

        if not has_xbrl:
            print(f'  SKIP: CIK {cik_raw} — no XBRL data')
            continue

        # Get metadata
        url2 = SUBMISSIONS_URL.format(cik=cik)
        name, sic, sic_desc = '', None, ''
        try:
            r2 = requests.get(url2, headers=HEADERS, timeout=15)
            if r2.status_code == 200:
                data = r2.json()
                name = data.get('name', '')
                sic = data.get('sic')
                sic_desc = data.get('sicDescription', '')
        except Exception:
            pass
        time.sleep(0.12)

        verified.append({
            'cik': cik,
            'name': name,
            'sic_code': sic,
            'sic_description': sic_desc,
            'has_xbrl': True,
            'source': 'known_fraud_supplement',
        })
        print(f'  OK: CIK {cik_raw} — {name}')

    if not verified:
        print('  No CIKs verified. Nothing to add.')
        return

    df = pd.DataFrame(verified)

    # Append to historical_ciks.parquet if it exists
    if OUT.exists():
        existing = pd.read_parquet(OUT)
        df = pd.concat([existing, df], ignore_index=True).drop_duplicates(subset='cik', keep='last')

    df.to_parquet(OUT, index=False)
    print(f'  Saved {len(verified)} CIKs to {OUT}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Step 0 — Historical universe builder')
    parser.add_argument('--scan', action='store_true', help='Scan EDGAR full-index for historical CIKs')
    parser.add_argument('--verify', action='store_true', help='Verify XBRL availability')
    parser.add_argument('--merge', action='store_true', help='Merge into tickers.parquet')
    parser.add_argument('--add-known', nargs='+', help='Add specific known CIKs (space-separated)')
    args = parser.parse_args()

    DATA.mkdir(exist_ok=True)

    if args.scan:
        run_scan()
    elif args.verify:
        run_verify()
    elif args.merge:
        run_merge()
    elif args.add_known:
        run_add_known(args.add_known)
    else:
        parser.print_help()
