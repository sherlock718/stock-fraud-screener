"""
Market cap filter — fetches market cap via yfinance and filters to $150M-$1B range.
Runs after fetch_companies, before fraud_signals.
"""

import json
import os
import time
import yfinance as yf

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data')

MIN_MARKET_CAP = 150_000_000    # $150M
MAX_MARKET_CAP = 1_000_000_000  # $1B


def get_market_cap(ticker: str):
    """Fetch market cap for a ticker via yfinance."""
    try:
        info = yf.Ticker(ticker).info
        return info.get('marketCap')
    except Exception:
        return None


def filter_by_market_cap(companies: list, batch_size: int = 50) -> list:
    """
    Filter companies to only those with market cap $150M-$1B.
    Fetches market cap in batches to avoid rate limits.
    """
    print(f"Filtering {len(companies)} companies to ${MIN_MARKET_CAP/1e6:.0f}M-${MAX_MARKET_CAP/1e6:.0f}M market cap...")

    filtered = []
    skipped_no_data = 0
    skipped_out_of_range = 0

    for i, company in enumerate(companies):
        ticker = company.get('ticker')
        if not ticker:
            skipped_no_data += 1
            continue

        if i % batch_size == 0 and i > 0:
            print(f"  Progress: {i}/{len(companies)} — {len(filtered)} in range so far")
            time.sleep(1)  # brief pause every 50 tickers

        market_cap = get_market_cap(ticker)
        company['market_cap'] = market_cap

        if market_cap is None:
            skipped_no_data += 1
            continue

        if MIN_MARKET_CAP <= market_cap <= MAX_MARKET_CAP:
            filtered.append(company)
        else:
            skipped_out_of_range += 1

    print(f"\nResults:")
    print(f"  In range ($150M-$1B): {len(filtered)}")
    print(f"  Out of range:         {skipped_out_of_range}")
    print(f"  No market cap data:   {skipped_no_data}")

    return filtered


def run_market_cap_filter():
    input_path = os.path.join(DATA_DIR, 'companies_financials.json')
    output_path = os.path.join(DATA_DIR, 'companies_filtered.json')

    with open(input_path) as f:
        companies = json.load(f)

    filtered = filter_by_market_cap(companies)

    with open(output_path, 'w') as f:
        json.dump(filtered, f, indent=2)

    print(f"\nSaved {len(filtered)} companies to {output_path}")
    return filtered


if __name__ == '__main__':
    run_market_cap_filter()
