"""
Step 1 BR — Fetch Brazilian listed company tickers from B3 via CVM + brapi.dev.

Sources:
  - CVM (Comissão de Valores Mobiliários): active exchange-listed company register
  - brapi.dev: free B3 ticker list (1,800+ symbols, /api/available endpoint only)

Matching: pure text heuristics — no per-ticker API calls, no ticker count cap.
  1. First 4 letters of normalised commercial name → ticker root
  2. Acronym of first 4 words of normalised name → ticker root
  3. Same applied to full legal name as fallback

Companies with no ticker match are kept (financial data only, no price).

Output: data/tickers_br.parquet
  cd_cvm, ticker, stock_code, name, exchange, industry_code,
  market, country, accounting_std
"""

from __future__ import annotations

import io
import re
from pathlib import Path

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


def _acronym(words: list[str], n: int = 4) -> str:
    """First letter of each of the first n words."""
    return ''.join(w[0] for w in words[:n] if w)


def match_tickers(companies: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """
    Match CVM companies to B3 tickers using pure text heuristics — no API calls,
    no cap on ticker count.

    Strategy (applied in order, first match wins):
      1. First 4 non-space letters of the normalised commercial name == ticker root
      2. Acronym of first 4 words of normalised commercial name == ticker root
      3. Same two strategies applied to normalised full legal name

    Ticker root = first 4 letters of the ticker symbol (e.g. 'PETR' from 'PETR4').
    When a root has both ordinary (3) and preferred (4) shares, ordinary is preferred.
    """
    print('  Matching CVM companies to B3 tickers (text heuristics, no API cap) ...')

    # Build root → preferred ticker (ordinary '3' beats preferred '4')
    root_to_ticker: dict[str, str] = {}
    for t in tickers:
        root = t[:4]
        existing = root_to_ticker.get(root)
        if existing is None:
            root_to_ticker[root] = t
        elif t.endswith('3'):
            root_to_ticker[root] = t

    def best_match(norm_name: str) -> str:
        letters = re.sub(r'[^A-Z]', '', norm_name)
        prefix4 = letters[:4]
        if prefix4 in root_to_ticker:
            return root_to_ticker[prefix4]
        words = norm_name.split()
        acro = _acronym(words, 4)
        if len(acro) == 4 and acro in root_to_ticker:
            return root_to_ticker[acro]
        # 3-letter acronym fallback (e.g. "VALE" matches VALE3 but CVM name is 3 words)
        acro3 = _acronym(words, 3)
        if len(acro3) == 3:
            candidates = [t for root, t in root_to_ticker.items() if root.startswith(acro3)]
            if len(candidates) == 1:
                return candidates[0]
        return ''

    results = []
    for _, row in companies.iterrows():
        cvm_comercial = normalise_name(row['name_comercial'] or row['name_full'])
        cvm_full      = normalise_name(row['name_full'])

        matched_ticker = best_match(cvm_comercial) or best_match(cvm_full)

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
