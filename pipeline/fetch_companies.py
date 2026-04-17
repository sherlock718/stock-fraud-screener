"""
Step 1: Fetch all US companies from SEC EDGAR with financials + market cap.
Uses SEC EDGAR company facts API (free, no API key needed) + yfinance for market cap.

Checkpoint/resume: saves progress to data/fetch_checkpoint.json every 100 companies.
If interrupted, re-running will resume from where it left off.
"""

import requests
import json
import time
import os

import yfinance as yf

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {'User-Agent': 'StockFraudScreener contact@example.com'}
CHECKPOINT_PATH = os.path.join(DATA_DIR, 'fetch_checkpoint.json')
OUTPUT_PATH = os.path.join(DATA_DIR, 'companies_financials.json')
CHECKPOINT_EVERY = 100


def get_all_companies():
    """Fetch full company list from SEC EDGAR."""
    print("Fetching company list from SEC EDGAR...")
    url = "https://www.sec.gov/files/company_tickers_exchange.json"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()
    companies = list(data['data'])
    print(f"Found {len(companies)} companies total")
    return companies


def get_company_facts(cik: str):
    """Fetch financial facts for a single company by CIK."""
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    response = requests.get(url, headers=HEADERS)
    if response.status_code == 404:
        return None
    if response.status_code == 429:
        raise RuntimeError("rate_limited")
    response.raise_for_status()
    return response.json()


def get_market_cap(ticker: str):
    """Fetch market cap for a ticker via yfinance."""
    try:
        info = yf.Ticker(ticker).info
        return info.get('marketCap')
    except Exception:
        return None


def extract_latest_value(facts: dict, concept: str, unit: str = 'USD'):
    """Extract the most recent annual value for a given accounting concept."""
    try:
        entries = facts['facts']['us-gaap'][concept]['units'][unit]
        annual = [e for e in entries if e.get('form') == '10-K']
        if not annual:
            return None
        latest = sorted(annual, key=lambda x: x['end'])[-1]
        return latest['val']
    except (KeyError, IndexError):
        return None


def extract_financials(facts: dict) -> dict:
    """Extract all financial fields needed for fraud signal calculation."""
    return {
        # Income Statement
        'revenue':              extract_latest_value(facts, 'Revenues') or
                                extract_latest_value(facts, 'RevenueFromContractWithCustomerExcludingAssessedTax'),
        'net_income':           extract_latest_value(facts, 'NetIncomeLoss'),
        'gross_profit':         extract_latest_value(facts, 'GrossProfit'),
        'operating_income':     extract_latest_value(facts, 'OperatingIncomeLoss'),
        'ebit':                 extract_latest_value(facts, 'OperatingIncomeLoss'),

        # Balance Sheet
        'total_assets':         extract_latest_value(facts, 'Assets'),
        'current_assets':       extract_latest_value(facts, 'AssetsCurrent'),
        'current_liabilities':  extract_latest_value(facts, 'LiabilitiesCurrent'),
        'total_liabilities':    extract_latest_value(facts, 'Liabilities'),
        'long_term_debt':       extract_latest_value(facts, 'LongTermDebt'),
        'retained_earnings':    extract_latest_value(facts, 'RetainedEarningsAccumulatedDeficit'),
        'shares_outstanding':   extract_latest_value(facts, 'CommonStockSharesOutstanding', unit='shares'),
        'receivables':          extract_latest_value(facts, 'AccountsReceivableNetCurrent'),
        'inventory':            extract_latest_value(facts, 'InventoryNet'),
        'ppe_net':              extract_latest_value(facts, 'PropertyPlantAndEquipmentNet'),

        # Cash Flow
        'operating_cash_flow':  extract_latest_value(facts, 'NetCashProvidedByUsedInOperatingActivities'),
        'capex':                extract_latest_value(facts, 'PaymentsToAcquirePropertyPlantAndEquipment'),
        'depreciation':         extract_latest_value(facts, 'DepreciationDepletionAndAmortization'),
    }


def load_checkpoint():
    """Load saved checkpoint (processed CIKs + results so far)."""
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH) as f:
            return json.load(f)
    return {'processed_ciks': [], 'results': []}


def save_checkpoint(processed_ciks: list, results: list):
    """Save progress to checkpoint file."""
    with open(CHECKPOINT_PATH, 'w') as f:
        json.dump({'processed_ciks': processed_ciks, 'results': results}, f)


def fetch_and_save_companies(limit: int = None, resume: bool = True):
    """
    Fetch all companies from SEC EDGAR, enrich with market cap, save to JSON.

    Args:
        limit:  cap the number of companies processed (None = all).
        resume: if True, load checkpoint and skip already-processed CIKs.
    """
    companies = get_all_companies()
    target = companies[:limit] if limit else companies
    print(f"Processing {len(target)} companies...")

    checkpoint = load_checkpoint() if resume else {'processed_ciks': [], 'results': []}
    processed_ciks = set(checkpoint['processed_ciks'])
    results = checkpoint['results']
    errors = []

    if processed_ciks:
        print(f"Resuming: {len(processed_ciks)} already done, {len(results)} results so far")

    new_this_run = 0

    for i, company in enumerate(target):
        cik, name, ticker, exchange = company[0], company[1], company[2], company[3]

        if str(cik) in processed_ciks:
            continue

        if new_this_run % 100 == 0 and new_this_run > 0:
            total_done = len(processed_ciks) + new_this_run
            print(f"Progress: {total_done}/{len(target)} — {name} — {len(results)} saved so far")

        # Fetch SEC financials
        facts = None
        rate_limit_retries = 0
        while True:
            try:
                facts = get_company_facts(cik)
                break
            except RuntimeError as e:
                if str(e) == "rate_limited":
                    rate_limit_retries += 1
                    wait = 5 * rate_limit_retries
                    print(f"  Rate limited. Waiting {wait}s... (attempt {rate_limit_retries})")
                    time.sleep(wait)
                    if rate_limit_retries >= 5:
                        print(f"  Giving up on CIK {cik} after 5 retries.")
                        break
                else:
                    errors.append({'cik': cik, 'name': name, 'error': str(e)})
                    break
            except Exception as e:
                errors.append({'cik': cik, 'name': name, 'error': str(e)})
                break

        if facts is not None:
            financials = extract_financials(facts)
            if financials['total_assets'] and financials['revenue']:
                market_cap = get_market_cap(ticker) if ticker else None
                results.append({
                    'cik': cik,
                    'name': name,
                    'ticker': ticker,
                    'exchange': exchange,
                    'market_cap': market_cap,
                    **financials
                })

        processed_ciks.add(str(cik))
        new_this_run += 1

        if new_this_run % CHECKPOINT_EVERY == 0:
            save_checkpoint(list(processed_ciks), results)

        time.sleep(0.12)

    # Final save
    save_checkpoint(list(processed_ciks), results)
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\nDone.")
    print(f"  Total with valid financials: {len(results)}")
    print(f"  Total processed: {len(processed_ciks)}")
    print(f"  Errors this run: {len(errors)}")
    return results


if __name__ == '__main__':
    fetch_and_save_companies()
