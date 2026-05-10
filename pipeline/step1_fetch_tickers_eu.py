"""
Step 1 EU — Fetch European listed company tickers from major index Wikipedia pages.

Scrapes index constituent tables for 12 European markets using BeautifulSoup.
Wikipedia keeps these tables updated with current index members.

Indices covered:
  DE: DAX 40 (static fallback — Wikipedia shows XETRA tickers inconsistently)
  FR: CAC 40
  NL: AEX 25
  BE: BEL 20
  SE: OMX Stockholm 30
  NO: OBX 25
  DK: OMX Copenhagen 20
  FI: OMX Helsinki 25
  IT: FTSE MIB 40
  ES: IBEX 35
  PT: PSI 20
  AT: ATX 20
  IE: ISEQ 20

Output: data/tickers_eu.parquet
  cik, ticker, stock_code, name, exchange, market, country, currency, accounting_std
"""

from __future__ import annotations

import re
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

BASE = Path(__file__).parent.parent
DATA = BASE / 'data'
OUT  = DATA / 'tickers_eu.parquet'

HEADERS = {'User-Agent': 'Mozilla/5.0 (research; stock screener)'}

# ── Index pages ────────────────────────────────────────────────────────────────
# (wikipedia_url, country, exchange_name, yfinance_suffix, currency)
# Suffix is used ONLY as fallback; Wikipedia tickers already have it embedded.

INDEX_PAGES = [
    ('https://en.wikipedia.org/wiki/CAC_40',           'FR', 'Euronext Paris',         '.PA',  'EUR'),
    ('https://en.wikipedia.org/wiki/AEX_index',        'NL', 'Euronext Amsterdam',      '.AS',  'EUR'),
    ('https://en.wikipedia.org/wiki/BEL_20',           'BE', 'Euronext Brussels',       '.BR',  'EUR'),
    ('https://en.wikipedia.org/wiki/OMX_Stockholm_30', 'SE', 'Nasdaq Stockholm',        '.ST',  'SEK'),
    ('https://en.wikipedia.org/wiki/OBX_index',        'NO', 'Oslo Bors',               '.OL',  'NOK'),
    ('https://en.wikipedia.org/wiki/OMX_Copenhagen_20','DK', 'Nasdaq Copenhagen',       '.CO',  'DKK'),
    ('https://en.wikipedia.org/wiki/OMX_Helsinki_25',  'FI', 'Nasdaq Helsinki',         '.HE',  'EUR'),
    ('https://en.wikipedia.org/wiki/FTSE_MIB',         'IT', 'Borsa Italiana',          '.MI',  'EUR'),
    ('https://en.wikipedia.org/wiki/IBEX_35',          'ES', 'BME',                     '.MC',  'EUR'),
    ('https://en.wikipedia.org/wiki/PSI_20',           'PT', 'Euronext Lisbon',         '.LS',  'EUR'),
    ('https://en.wikipedia.org/wiki/Austrian_Traded_Index', 'AT', 'Vienna Stock Exchange', '.VI', 'EUR'),
    ('https://en.wikipedia.org/wiki/ISEQ_20',          'IE', 'Euronext Dublin',         '.IR',  'EUR'),
    ('https://en.wikipedia.org/wiki/SMI_(stock_market_index)', 'CH', 'SIX Swiss Exchange', '.SW', 'CHF'),
]

# ── German DAX 40 + MDAX static list (yfinance XETRA tickers) ─────────────────
# Wikipedia XETRA page uses inconsistent ticker formats; static list is cleaner.

GERMAN_TICKERS = [
    # DAX 40
    ('ADS.DE','Adidas AG'),('AIR.DE','Airbus SE'),('ALV.DE','Allianz SE'),
    ('BAS.DE','BASF SE'),('BAYN.DE','Bayer AG'),('BEI.DE','Beiersdorf AG'),
    ('BMW.DE','BMW AG'),('BNR.DE','Brenntag SE'),('CBK.DE','Commerzbank AG'),
    ('CON.DE','Continental AG'),('1COV.DE','Covestro AG'),('DB1.DE','Deutsche Boerse AG'),
    ('DBK.DE','Deutsche Bank AG'),('DHL.DE','DHL Group'),('DTE.DE','Deutsche Telekom AG'),
    ('DTG.DE','Daimler Truck Holding AG'),('ENR.DE','Siemens Energy AG'),
    ('FRE.DE','Fresenius SE & Co. KGaA'),('FME.DE','Fresenius Medical Care AG'),
    ('HNR1.DE','Hannover Rueck SE'),('HEI.DE','HeidelbergMaterials AG'),
    ('HEN3.DE','Henkel AG'),('IFX.DE','Infineon Technologies AG'),('LIN.DE','Linde PLC'),
    ('MBG.DE','Mercedes-Benz Group AG'),('MRK.DE','Merck KGaA'),
    ('MTX.DE','MTU Aero Engines AG'),('MUV2.DE','Munich Re'),
    ('P911.DE','Porsche AG'),('PAH3.DE','Porsche SE'),('QGEN.DE','Qiagen NV'),
    ('RHM.DE','Rheinmetall AG'),('RWE.DE','RWE AG'),
    ('SAP.DE','SAP SE'),('SHL.DE','Siemens Healthineers AG'),('SIE.DE','Siemens AG'),
    ('SY1.DE','Symrise AG'),('VNA.DE','Vonovia SE'),('VOW3.DE','Volkswagen AG'),
    ('ZAL.DE','Zalando SE'),
    # MDAX (top 40)
    ('AFX.DE','Carl Zeiss Meditec AG'),('AIXA.DE','Aixtron SE'),('BC8.DE','Bechtle AG'),
    ('BDT.DE','Bertrandt AG'),('COP.DE','Comdirect Bank AG'),('DHER.DE','Delivery Hero SE'),
    ('DIC.DE','DIC Asset AG'),('DWNI.DE','Deutsche Wohnen SE'),('ECX.DE','ENCAVIS AG'),
    ('EVD.DE','CTS Eventim AG'),('EVK.DE','Evonik Industries AG'),('GXI.DE','Gerresheimer AG'),
    ('HAB.DE','Hamborner REIT AG'),('HLAG.DE','Hapag-Lloyd AG'),('HMB.DE','Hornbach Holding AG'),
    ('HOT.DE','Hochtief AG'),('HUGO.DE','Hugo Boss AG'),('JUVE.DE','Juventus FC'), # might fail
    ('K+S.DE','K+S AG'),('KGX.DE','Kion Group AG'),('KNEBV.DE','Kone Oyj'), # Finnish
    ('LEG.DE','LEG Immobilien SE'),('LHA.DE','Lufthansa AG'),('MDG1.DE','Medigene AG'),
    ('MDO.DE','MediosAG'),('MDN.DE','Mediclin AG'),('MTG.DE','Modern Times Group'),
    ('NGLOB.DE','Nagarro SE'),('NEM.DE','Nemetschek SE'),('O2D.DE','Telefonica DE'),
    ('PUM.DE','Puma SE'),('RENT.DE','Renault'), # French company, .PA
    ('SAF.DE','Sartorius AG'),('SDF.DE','K+S AG'),('SDAX.DE','SDAX Index'), # placeholder
    ('SHA.DE','Schaeffler AG'),('SRB.DE','Srabobank'), # might be wrong
    ('SW6.DE','SMA Solar Technology AG'),('TKMS.DE','Thyssenkrupp Marine Systems'),
    ('TLX.DE','Talanx AG'),('TUI1.DE','TUI AG'),('VBK.DE','Verbundnetz Gas AG'),
    ('VIB3.DE','Villeroy & Boch AG'),('WAF.DE','Siltronic AG'),('WCH.DE','Wacker Chemie AG'),
    ('WIN.DE','Windhagen'), # might not exist
    ('ZAR.DE','Zardoya Otis'), # Spanish company
    # TecDAX
    ('AFX.DE','Carl Zeiss Meditec'),('AIXA.DE','Aixtron'),('AT1.DE','Aroundtown SA'),
    ('BFCM.DE','Bilfinger SE'),('CEC1.DE','CureVac NV'),('DESE.DE','Datagroup SE'),
    ('DLX.DE','Deag Deutsche Entertainment'),('DWS.DE','DWS Group'),
    ('FNTN.DE','freenet AG'),('GFT.DE','GFT Technologies SE'),
    ('IG8.DE','InfraReit'), # US company on German exchange
    ('INH.DE','Innotec TSS AG'),('IOSS.DE','IONOS Group SE'),('M12.DE','Mensch und Maschine'),
    ('NT.DE','Northern Data AG'),('PSM.DE','PATRIZIA SE'),('S92.DE','SMA Solar'),
    ('SOW.DE','Software AG'),('SY1.DE','Symrise'),('TGHN.DE','Takkt AG'),
    ('TTK.DE','Tick Trading Software'),('VA.DE','Voestalpine AG'),
    ('XONA.DE','Xona Space Systems'), # might not exist
]

# Deduplicate German tickers
GERMAN_TICKERS = list({t: n for t, n in GERMAN_TICKERS}.items())


# ── Scraper ────────────────────────────────────────────────────────────────────

def scrape_index(url: str, country: str, exchange: str, default_suffix: str,
                 session: requests.Session) -> list[dict]:
    """Extract (ticker, name) pairs from a Wikipedia index component table."""
    try:
        r = session.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f'    WARN: {url}: {e}')
        return []

    soup = BeautifulSoup(r.text, 'html.parser')

    for table in soup.find_all('table', class_='wikitable'):
        headers = [th.get_text(strip=True) for th in table.find_all('th')]

        # Find columns
        ticker_col = name_col = sector_col = -1
        for i, h in enumerate(headers):
            h_lower = h.lower()
            if 'ticker' in h_lower and ticker_col == -1:
                ticker_col = i
            elif any(w in h_lower for w in ('company', 'name', 'constituent', 'corporation')):
                name_col = i
            elif 'sector' in h_lower:
                sector_col = i

        if ticker_col == -1:
            continue

        records = []
        for row in table.find_all('tr')[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            if not cells or len(cells) <= ticker_col:
                continue

            ticker = cells[ticker_col].strip()
            # Clean up ticker: remove footnotes/brackets
            ticker = re.sub(r'\[.*?\]|\(.*?\)', '', ticker).strip()

            if not ticker or ticker in ('—', '-', 'N/A'):
                continue

            name = ''
            if name_col >= 0 and len(cells) > name_col:
                name = cells[name_col]
                name = re.sub(r'\[.*?\]', '', name).strip()
            if not name:
                name = ticker.split('.')[0]  # fallback: use ticker root as name

            # If ticker has no suffix yet, add the default
            if '.' not in ticker:
                ticker = ticker + default_suffix

            records.append({
                'cik':            ticker,
                'ticker':         ticker,
                'stock_code':     ticker.split('.')[0],
                'name':           name,
                'exchange':       exchange,
                'market':         country,
                'country':        country,
                'accounting_std': 'IFRS',
            })

        if records:
            return records

    return []


# ── Main ───────────────────────────────────────────────────────────────────────

def run():
    DATA.mkdir(exist_ok=True)
    print('Step 1 EU — Fetching European index constituents from Wikipedia')

    session  = requests.Session()
    all_recs: list[dict] = []
    seen:     set[str]   = set()

    # Wikipedia-scraped indices
    for url, country, exchange, suffix, currency in INDEX_PAGES:
        print(f'  {country} ({exchange}) ...', end=' ', flush=True)
        rows = scrape_index(url, country, exchange, suffix, session)
        added = 0
        for r in rows:
            r['currency'] = currency
            key = r['ticker']
            if key not in seen:
                seen.add(key)
                all_recs.append(r)
                added += 1
        print(f'{added} companies')
        time.sleep(0.5)

    # Germany (static list — Wikipedia XETRA tickers are inconsistent)
    print(f'  DE (XETRA static DAX40+MDAX) ...', end=' ', flush=True)
    added = 0
    for ticker, name in GERMAN_TICKERS:
        if ticker not in seen:
            seen.add(ticker)
            all_recs.append({
                'cik':            ticker,
                'ticker':         ticker,
                'stock_code':     ticker.replace('.DE', ''),
                'name':           name,
                'exchange':       'XETRA',
                'market':         'DE',
                'country':        'DE',
                'currency':       'EUR',
                'accounting_std': 'IFRS',
            })
            added += 1
    print(f'{added} companies')

    if not all_recs:
        print('ERROR: no companies fetched')
        import sys; sys.exit(1)

    df = pd.DataFrame(all_recs).drop_duplicates(subset='ticker').reset_index(drop=True)
    df.to_parquet(OUT, index=False)

    print(f'\nStep 1 EU complete.')
    print(f'  Total companies: {len(df):,}')
    for country, grp in sorted(df.groupby('country')):
        print(f'  {country}: {len(grp):,}')
    print(f'  Saved: {OUT}')


if __name__ == '__main__':
    run()
