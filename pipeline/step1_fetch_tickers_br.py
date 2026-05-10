"""
Step 1 BR — Fetch Brazilian listed company tickers from B3 via CVM + brapi.dev.

Sources:
  - CVM (Comissão de Valores Mobiliários): active exchange-listed company register
  - brapi.dev: free B3 ticker list (1,800+ symbols)

Matching: CVM company names → B3 tickers via fuzzy name matching.
Companies with no ticker match are kept (financial data only, no price).

Output: data/tickers_br.parquet
  cd_cvm, ticker, stock_code, name, exchange, industry_code,
  market, country, accounting_std
"""

from __future__ import annotations

import io
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

BASE = Path(__file__).parent.parent
DATA = BASE / 'data'
OUT  = DATA / 'tickers_br.parquet'

CVM_CAD_URL    = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv'
BRAPI_LIST_URL = 'https://brapi.dev/api/available'


def fetch_cvm_companies() -> pd.DataFrame:
    """Download CVM company register, filter to active BOLSA-listed companies."""
    print('  Downloading CVM company register ...')
    r = requests.get(CVM_CAD_URL, timeout=30)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), sep=';', encoding='latin1')

    # Active companies listed on exchange
    df = df[(df['SIT'] == 'ATIVO') & (df['TP_MERC'] == 'BOLSA')].copy()
    df = df[['CD_CVM', 'DENOM_SOCIAL', 'DENOM_COMERC', 'SETOR_ATIV', 'CNPJ_CIA']].copy()
    df.columns = ['cd_cvm', 'name_full', 'name_comercial', 'sector', 'cnpj']
    df['cd_cvm'] = df['cd_cvm'].astype(str).str.zfill(7)
    print(f'  Active BOLSA companies: {len(df):,}')
    return df


def fetch_b3_tickers() -> list[str]:
    """Fetch all B3 tickers from brapi.dev."""
    print('  Fetching B3 ticker list from brapi.dev ...')
    r = requests.get(BRAPI_LIST_URL, timeout=15)
    r.raise_for_status()
    data = r.json()
    tickers = data.get('stocks', []) if isinstance(data, dict) else data
    # Filter to common shares (end in 3) + preferred (4) — skip ETFs, FIIs (11), DRs (34)
    tickers = [t for t in tickers if re.match(r'^[A-Z]{4}[34]$', t)]
    print(f'  B3 tickers (ordinary + preferred): {len(tickers):,}')
    return tickers


def normalise_name(s: str) -> str:
    """Strip common suffixes and normalise for matching."""
    s = str(s).upper()
    for suffix in [' S.A.', ' S/A', ' SA', ' LTDA', ' LTDA.', ' CIA.', ' CIA',
                   ' HOLDINGS', ' HOLDING', ' GROUP', ' PARTICIPAÇÕES', ' PARTICIPACOES',
                   ' PARTICIPAÇÃO', ' PARTICIPACAO', ' DO BRASIL', ' BRASIL']:
        s = s.replace(suffix, '')
    s = re.sub(r'[^A-Z0-9 ]', '', s).strip()
    return s


def match_tickers(companies: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """
    Best-effort match: for each CVM company, find the most liquid B3 ticker.
    Matching strategy:
      1. DENOM_COMERC exact normalised match to known company names
      2. First 4 chars of ticker against company abbreviation
    For unmatched: ticker = ''
    """
    print('  Matching CVM companies to B3 tickers ...')

    # Fetch company names for tickers from brapi (batch)
    ticker_map: dict[str, str] = {}  # ticker → normalised company name
    try:
        # brapi /quote supports up to 10 tickers per call
        batch_size = 20
        for i in range(0, min(len(tickers), 400), batch_size):
            batch = ','.join(tickers[i:i + batch_size])
            r = requests.get(f'https://brapi.dev/api/quote/{batch}', timeout=15)
            if r.status_code == 200:
                for item in r.json().get('results', []):
                    sym = item.get('symbol', '')
                    name = normalise_name(item.get('longName') or item.get('shortName') or '')
                    if sym and name:
                        ticker_map[sym] = name
            time.sleep(0.2)
    except Exception as e:
        print(f'    WARN: brapi batch failed: {e}')

    # Build reverse: normalised_name → ticker (prefer ordinary '3' over preferred '4')
    name_to_ticker: dict[str, str] = {}
    for ticker, name in ticker_map.items():
        if name not in name_to_ticker:
            name_to_ticker[name] = ticker
        elif ticker.endswith('3'):  # prefer ordinary shares
            name_to_ticker[name] = ticker

    # Match CVM companies
    results = []
    for _, row in companies.iterrows():
        cvm_name = normalise_name(row['name_comercial'] or row['name_full'])
        cvm_full = normalise_name(row['name_full'])

        matched_ticker = name_to_ticker.get(cvm_name) or name_to_ticker.get(cvm_full) or ''

        # Fallback: first 4 letters of commercial name match ticker prefix
        if not matched_ticker:
            prefix = re.sub(r'[^A-Z]', '', cvm_name)[:4]
            candidates = [t for t in tickers if t.startswith(prefix)]
            if len(candidates) == 1:
                matched_ticker = candidates[0]

        suffix = '.SA' if matched_ticker else ''
        results.append({
            'cd_cvm':         row['cd_cvm'],
            'ticker':         matched_ticker + suffix,
            'stock_code':     matched_ticker,
            'name':           row['name_full'].title(),
            'exchange':       'B3',
            'industry_code':  str(row['sector']),
            'market':         'BR',
            'country':        'BR',
            'accounting_std': 'IFRS',
        })

    return pd.DataFrame(results)


def run():
    DATA.mkdir(exist_ok=True)
    load_dotenv(BASE / '.env')

    print('Step 1 BR — Fetching Brazilian ticker list')

    companies = fetch_cvm_companies()
    tickers   = fetch_b3_tickers()
    result    = match_tickers(companies, tickers)

    matched = (result['ticker'] != '').sum()
    result.to_parquet(OUT, index=False)

    print(f'\nStep 1 BR complete.')
    print(f'  Total companies:   {len(result):,}')
    print(f'  Ticker matched:    {matched:,}')
    print(f'  No ticker (fin only): {len(result) - matched:,}')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    run()
