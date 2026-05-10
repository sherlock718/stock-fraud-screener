"""
Step 1 KR — Fetch Korean listed company tickers from DART.

Downloads the full DART corp_code list (XML zip), filters to stock-listed
companies on KOSPI/KOSDAQ/KONEX, and enriches with company details.

Output: data/tickers_kr.parquet
  corp_code, ticker, stock_code, name, exchange,
  industry_code, industry_name, market, country, accounting_std
"""

from __future__ import annotations

import io
import os
import time
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import requests
import pandas as pd
from dotenv import load_dotenv

BASE = Path(__file__).parent.parent
DATA = BASE / 'data'
OUT  = DATA / 'tickers_kr.parquet'

DART_BASE = 'https://opendart.fss.or.kr/api'


def get_api_key() -> str:
    load_dotenv(BASE / '.env')
    key = os.environ.get('DART_API_KEY', '').strip()
    if not key:
        raise RuntimeError('DART_API_KEY not set in .env')
    return key


def download_corp_codes(key: str) -> pd.DataFrame:
    """Download full DART corp code list (XML zip). Returns listed companies only."""
    print('  Downloading DART corp code list ...')
    r = requests.get(
        f'{DART_BASE}/corpCode.xml',
        params={'crtfc_key': key},
        timeout=60,
    )
    r.raise_for_status()

    zf = zipfile.ZipFile(io.BytesIO(r.content))
    xml_content = zf.read('CORPCODE.xml')
    root = ET.fromstring(xml_content)

    records = []
    for item in root.findall('list'):
        stock_code = item.findtext('stock_code', '').strip()
        if not stock_code or stock_code == ' ':
            continue  # unlisted entity — skip
        records.append({
            'corp_code':  item.findtext('corp_code', '').strip(),
            'name':       item.findtext('corp_name', '').strip(),
            'stock_code': stock_code,
        })

    df = pd.DataFrame(records)
    print(f'  Listed companies in DART: {len(df):,}')
    return df


def fetch_company_detail(corp_code: str, key: str,
                          session: requests.Session) -> dict:
    """Fetch exchange, industry, fiscal year end from DART company endpoint."""
    try:
        r = session.get(
            f'{DART_BASE}/company.json',
            params={'crtfc_key': key, 'corp_code': corp_code},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        if data.get('status') == '000':
            return data
    except Exception:
        pass
    return {}


CORP_CLS_MAP = {
    'Y': ('KOSPI',  '.KS'),
    'K': ('KOSDAQ', '.KQ'),
    'N': ('KONEX',  '.KQ'),
}


def run(limit: int | None = None):
    DATA.mkdir(exist_ok=True)
    key = get_api_key()

    print('Step 1 KR — Fetching Korean ticker list from DART')

    df = download_corp_codes(key)
    if limit:
        df = df.head(limit).copy()
        print(f'  TEST MODE: limited to {len(df):,} companies')

    session = requests.Session()
    enriched = []
    skipped_delisted = 0

    for i, row in df.iterrows():
        time.sleep(0.12)  # ~8 req/s
        detail = fetch_company_detail(row['corp_code'], key, session)

        corp_cls = detail.get('corp_cls', '')
        if corp_cls not in CORP_CLS_MAP:
            skipped_delisted += 1
            continue  # not currently listed (E = delisted/unlisted)

        exchange, suffix = CORP_CLS_MAP[corp_cls]
        acc_std = 'K-IFRS'

        enriched.append({
            'corp_code':      row['corp_code'],
            'ticker':         row['stock_code'] + suffix,   # e.g. '005930.KS'
            'stock_code':     row['stock_code'],
            'name':           row['name'],
            'exchange':       exchange,
            'industry_code':  detail.get('induty_code', ''),
            'industry_name':  detail.get('induty_nm', ''),
            'acc_mt':         detail.get('acc_mt', '12'),   # fiscal year end month
            'market':         'KR',
            'country':        'KR',
            'accounting_std': acc_std,
        })

        if (i + 1) % 200 == 0:
            print(f'  [{i+1:,}/{len(df):,}] enriched ({len(enriched):,} listed, {skipped_delisted:,} skipped)')

    result = pd.DataFrame(enriched)
    result.to_parquet(OUT, index=False)

    print(f'\nStep 1 KR complete.')
    print(f'  Total companies: {len(result):,}')
    print(f'  KOSPI:  {(result["exchange"] == "KOSPI").sum():,}')
    print(f'  KOSDAQ: {(result["exchange"] == "KOSDAQ").sum():,}')
    print(f'  KONEX:  {(result["exchange"] == "KONEX").sum():,}')
    print(f'  Skipped (delisted/unlisted): {skipped_delisted:,}')
    print(f'  Saved:  {OUT}')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()
    run(limit=args.limit)
