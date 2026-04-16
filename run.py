"""
Main runner — run this to execute the full pipeline.

Usage:
  python3 run.py --full       # Full run: fetch all companies + score
  python3 run.py --update     # Quick update: check new filings only
  python3 run.py --report     # Just regenerate report from existing data
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'pipeline'))

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def run_full():
    print("=== FULL PIPELINE RUN ===\n")

    print("Step 1: Fetching company financials from SEC EDGAR...")
    from pipeline.fetch_companies import fetch_and_save_companies
    fetch_and_save_companies()  # remove limit= to fetch all companies

    print("\nStep 2: Filtering to $150M-$1B market cap...")
    from pipeline.market_cap_filter import filter_by_market_cap
    import json
    with open(os.path.join(DATA_DIR, 'companies_financials.json')) as f:
        all_companies = json.load(f)
    filtered = filter_by_market_cap(all_companies)
    with open(os.path.join(DATA_DIR, 'companies_filtered.json'), 'w') as f:
        json.dump(filtered, f, indent=2)

    print("\nStep 3: Calculating fraud signals...")
    from pipeline.fraud_signals import calculate_all_signals
    with open(os.path.join(DATA_DIR, 'companies_filtered.json')) as f:
        companies = json.load(f)
    signals = calculate_all_signals(companies)
    with open(os.path.join(DATA_DIR, 'fraud_signals.json'), 'w') as f:
        json.dump(signals, f, indent=2)

    print("\nStep 4: Scoring and generating report...")
    from pipeline.score_and_report import generate_report, print_report
    scored = generate_report(signals)
    with open(os.path.join(os.path.dirname(__file__), 'reports', 'fraud_report.json'), 'w') as f:
        json.dump(scored, f, indent=2)
    print_report(scored, top_n=25)


def run_update():
    print("=== INCREMENTAL UPDATE ===\n")
    from pipeline.auto_update import run_update
    run_update(days_back=1)


def run_report():
    print("=== REGENERATING REPORT ===\n")
    import json
    from pipeline.score_and_report import generate_report, print_report
    with open(os.path.join(DATA_DIR, 'fraud_signals.json')) as f:
        signals = json.load(f)
    scored = generate_report(signals)
    with open(os.path.join(os.path.dirname(__file__), 'reports', 'fraud_report.json'), 'w') as f:
        json.dump(scored, f, indent=2)
    print_report(scored, top_n=25)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stock Fraud Screener')
    parser.add_argument('--full', action='store_true', help='Full pipeline run')
    parser.add_argument('--update', action='store_true', help='Check new filings only')
    parser.add_argument('--report', action='store_true', help='Regenerate report only')
    args = parser.parse_args()

    if args.full:
        run_full()
    elif args.update:
        run_update()
    elif args.report:
        run_report()
    else:
        print("Usage: python3 run.py --full | --update | --report")
