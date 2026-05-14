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
    # Include share classes 3-9 (ordinary 3, preferred 4/5/6/7/8, units 9)
    # Excludes ETFs, FIIs (11), BDRs (34)
    tickers = [t for t in tickers if re.match(r'^[A-Z]{4}[3-9]$', t)]
    print(f'  B3 tickers (classes 3-9): {len(tickers):,}')
    return tickers


# Curated cd_cvm → ticker overrides for companies whose names don't yield
# their ticker symbol via text heuristics (typically banks with internal
# acronyms: BBDC, BBAS, CMIG, BRSR, etc.).
# Keys are zero-padded 7-digit CVM registration codes.
CURATED_OVERRIDES: dict[str, str] = {
    '0000094': 'PNVL3',  # Panatlantica S.A.
    '0000906': 'BBDC3',  # Banco Bradesco S.A.
    '0001023': 'BBAS3',  # Banco do Brasil S.A.
    '0001210': 'BRSR3',  # Banrisul (Banco Estado Rio Grande do Sul)
    '0001325': 'BMEB3',  # Banco Mercantil do Brasil
    '0001520': 'BDLL4',  # Bardella S.A.
    '0001694': 'MNPR3',  # Bicicletas Monark
    '0002453': 'CMIG3',  # CEMIG (Cia Energética de Minas Gerais)
    '0002461': 'CLSC4',  # CELESC (Centrais Elétricas de Santa Catarina)
    '0003069': 'FESA3',  # FERBASA (Cia Ferro Ligas da Bahia)
    '0003077': 'SNSY5',  # Cia Fiação Tecidos Cedro Cachoeira
}

# Stop-words that add noise to name → ticker-root matching
_MATCH_STOP = frozenset({
    'BCO', 'BANCO', 'CIA', 'COMPANHIA', 'DO', 'DA', 'DE', 'DOS', 'DAS',
    'E', 'SA', 'SPA', 'EM', 'RECUPERACAO', 'JUDICIAL',
    'PARTICIPACOES', 'PARTICIPACAO', 'HOLDINGS', 'HOLDING',
})


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
    When a root has both ordinary (3) and preferred (4/5/6) shares, ordinary (3) is preferred.
    """
    print('  Matching CVM companies to B3 tickers (text heuristics + curated overrides) ...')

    # Build root → preferred ticker (lower share class number = more liquid/preferred)
    root_to_ticker: dict[str, str] = {}
    for t in sorted(tickers):  # sorted so class 3 comes before 4, 5 etc.
        root = t[:4]
        existing = root_to_ticker.get(root)
        if existing is None:
            root_to_ticker[root] = t
        elif t[-1] < existing[-1]:  # lower class number preferred (3 < 4 < 5 ...)
            root_to_ticker[root] = t

    def best_match(norm_name: str) -> str:
        """Try multiple strategies; return first match or ''."""
        # Strategy 1: first 4 letters of name (all non-space chars)
        letters = re.sub(r'[^A-Z]', '', norm_name)
        prefix4 = letters[:4]
        if prefix4 in root_to_ticker:
            return root_to_ticker[prefix4]

        words = norm_name.split()
        meaningful = [w for w in words if w and w not in _MATCH_STOP and len(w) > 1]

        # Strategy 2: first 4 letters of first meaningful word
        if meaningful:
            p4w = meaningful[0][:4]
            if p4w in root_to_ticker:
                return root_to_ticker[p4w]

        # Strategy 3: 4-letter acronym of first 4 meaningful words
        acro4 = _acronym(meaningful, 4)
        if len(acro4) == 4 and acro4 in root_to_ticker:
            return root_to_ticker[acro4]

        # Strategy 4: first letters of first 2 words + first 2 chars of 3rd word
        if len(meaningful) >= 3:
            acro2_2 = ''.join(w[0] for w in meaningful[:2]) + meaningful[2][:2]
            if len(acro2_2) == 4 and acro2_2 in root_to_ticker:
                return root_to_ticker[acro2_2]

        # Strategy 5: first 4 letters of second meaningful word
        if len(meaningful) >= 2:
            p4w2 = meaningful[1][:4]
            if p4w2 in root_to_ticker:
                return root_to_ticker[p4w2]

        # Strategy 6: 3-letter prefix unique match
        acro3 = _acronym(meaningful, 3)
        if len(acro3) == 3:
            candidates = [t for r, t in root_to_ticker.items() if r.startswith(acro3)]
            if len(candidates) == 1:
                return candidates[0]

        return ''

    results = []
    for _, row in companies.iterrows():
        cd_cvm = str(row['cd_cvm']).zfill(7)

        # Curated override takes priority over heuristics
        if cd_cvm in CURATED_OVERRIDES:
            matched_ticker = CURATED_OVERRIDES[cd_cvm]
        else:
            cvm_comercial = normalise_name(row['name_comercial'] or row['name_full'])
            cvm_full      = normalise_name(row['name_full'])
            matched_ticker = best_match(cvm_comercial) or best_match(cvm_full)

        suffix = '.SA' if matched_ticker else ''
        results.append({
            'cd_cvm':         cd_cvm,
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
