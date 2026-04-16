"""
Step 1: Fetch all US companies in the $150M - $1B market cap range
Uses SEC EDGAR company facts API (free, no API key needed)
"""

import requests
import json
import time
import os

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {'User-Agent': 'StockFraudScreener contact@example.com'}


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
    response.raise_for_status()
    return response.json()


def extract_latest_value(facts: dict, concept: str, unit: str = 'USD'):
    """Extract the most recent annual value for a given accounting concept."""
    try:
        entries = facts['facts']['us-gaap'][concept]['units'][unit]
        # Filter for 10-K annual filings only
        annual = [e for e in entries if e.get('form') == '10-K']
        if not annual:
            return None
        # Return most recent
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


def fetch_and_save_companies(limit: int = None):
    """
    Main function: fetch companies, pull financials, save to JSON.
    limit: for testing, set to small number like 50. None = all companies.
    """
    companies = get_all_companies()

    # SEC data: [cik, name, ticker, exchange]
    results = []
    errors = []

    target = companies[:limit] if limit else companies
    print(f"Processing {len(target)} companies...")

    for i, company in enumerate(target):
        cik, name, ticker, exchange = company[0], company[1], company[2], company[3]

        if i % 50 == 0:
            print(f"Progress: {i}/{len(target)} — {name}")

        try:
            facts = get_company_facts(cik)
            if not facts:
                continue

            financials = extract_financials(facts)

            # Skip if we can't get basic data
            if not financials['total_assets'] or not financials['revenue']:
                continue

            results.append({
                'cik': cik,
                'name': name,
                'ticker': ticker,
                'exchange': exchange,
                **financials
            })

        except Exception as e:
            errors.append({'cik': cik, 'name': name, 'error': str(e)})

        # Respect SEC rate limit: max 10 requests/second
        time.sleep(0.15)

    # Save results
    output_path = os.path.join(DATA_DIR, 'companies_financials.json')
    with open(output_path, 'w') as f:
        json.dump(results, f, indent=2)

    print(f"\nDone. Saved {len(results)} companies to {output_path}")
    print(f"Errors: {len(errors)}")
    return results


if __name__ == '__main__':
    # Start with 100 companies to test, remove limit= to run full dataset
    fetch_and_save_companies(limit=100)
