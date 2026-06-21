"""
Step 4: Auto-update — check SEC EDGAR for new 10-K/10-Q filings.
Run this daily. If new filings found, re-fetches and re-scores that company.
"""

import requests
import json
import os
import time
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')
HEADERS = {'User-Agent': 'StockFraudScreener contact@example.com'}
STATE_FILE = os.path.join(DATA_DIR, 'last_update.json')


def get_recent_filings(days_back: int = 1) -> list:
    """
    Fetch companies that filed a 10-K or 10-Q in the last N days.
    Uses SEC EDGAR full-text search API.
    """
    since = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    url = (
        f"https://efts.sec.gov/LATEST/search-index?q=%2210-K%22+%2210-Q%22"
        f"&dateRange=custom&startdt={since}&forms=10-K,10-Q"
    )
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        # Fallback to EDGAR full-text search
        url2 = f"https://efts.sec.gov/LATEST/search-index?forms=10-K,10-Q&dateRange=custom&startdt={since}"
        response = requests.get(url2, headers=HEADERS)
        if response.status_code != 200:
            print(f"Could not fetch recent filings: {response.status_code}")
            return []

    data = response.json()
    hits = data.get('hits', {}).get('hits', [])

    filings = []
    for hit in hits:
        src = hit.get('_source', {})
        filings.append({
            'cik': src.get('entity_id', '').replace('CIK', ''),
            'name': src.get('display_names', [''])[0] if src.get('display_names') else '',
            'form': src.get('file_type', ''),
            'filed': src.get('file_date', ''),
        })

    return filings


def update_company(cik: str):
    """Re-fetch and re-score a single company after a new filing."""
    from fetch_companies import get_company_facts, extract_financials
    from fraud_signals import calculate_all_signals
    from score_and_report import generate_report

    print(f"  Updating CIK {cik}...")
    facts = get_company_facts(cik)
    if not facts:
        return None

    financials = extract_financials(facts)
    if not financials['total_assets']:
        return None

    # Load existing data
    companies_path = os.path.join(DATA_DIR, 'companies_financials.json')
    if os.path.exists(companies_path):
        with open(companies_path) as f:
            companies = json.load(f)
    else:
        companies = []

    # Update or insert
    existing = next((c for c in companies if str(c['cik']) == str(cik)), None)
    if existing:
        existing.update(financials)
    else:
        companies.append({'cik': cik, **financials})

    with open(companies_path, 'w') as f:
        json.dump(companies, f, indent=2)

    return financials


def run_update(days_back: int = 1):
    """Main update loop — check for new filings and re-score."""
    print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M')}] Checking for new filings...")

    filings = get_recent_filings(days_back)
    print(f"Found {len(filings)} new filings")

    if not filings:
        print("Nothing to update.")
        return

    updated = 0
    for filing in filings:
        cik = filing['cik']
        if not cik:
            continue
        result = update_company(cik)
        if result:
            updated += 1
        time.sleep(0.15)  # respect SEC rate limit

    print(f"Updated {updated} companies.")

    # Re-run signals and scoring on full dataset
    if updated > 0:
        print("Re-running fraud signals and scoring...")
        companies_path = os.path.join(DATA_DIR, 'companies_financials.json')
        signals_path = os.path.join(DATA_DIR, 'fraud_signals.json')

        with open(companies_path) as f:
            companies = json.load(f)

        from fraud_signals import calculate_all_signals
        signals = calculate_all_signals(companies)
        with open(signals_path, 'w') as f:
            json.dump(signals, f, indent=2)

        from score_and_report import generate_report
        from reports.fraud_report import fraud_report  # noqa - just trigger save
        scored = generate_report(signals)

        report_path = os.path.join(os.path.dirname(__file__), '..', 'reports', 'fraud_report.json')
        with open(report_path, 'w') as f:
            json.dump(scored, f, indent=2)

        print(f"Report updated. {len([c for c in scored if c['risk'] == 'HIGH RISK'])} HIGH RISK companies.")

    # Save last update timestamp
    with open(STATE_FILE, 'w') as f:
        json.dump({'last_update': datetime.now().isoformat()}, f)


if __name__ == '__main__':
    run_update(days_back=1)
