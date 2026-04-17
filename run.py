"""
Main runner — run this to execute the full pipeline.

Usage:
  python3 run.py --full       # Full run: fetch all companies + score
  python3 run.py --full --fresh  # Start from scratch (ignore checkpoint)
  python3 run.py --update     # Quick update: check new filings only
  python3 run.py --report     # Just regenerate report from existing data
"""

import argparse
import os
import sys
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'pipeline'))

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
REPORTS_DIR = os.path.join(os.path.dirname(__file__), 'reports')


def run_full(args):
    print("=== FULL PIPELINE RUN ===\n")

    print("Step 1: Fetching company financials + market cap from SEC EDGAR...")
    from pipeline.fetch_companies import fetch_and_save_companies
    fetch_and_save_companies(resume=not args.fresh)

    print("\nStep 2: Calculating fraud signals...")
    from pipeline.fraud_signals import calculate_all_signals
    with open(os.path.join(DATA_DIR, 'companies_financials.json')) as f:
        companies = json.load(f)
    signals = calculate_all_signals(companies)
    with open(os.path.join(DATA_DIR, 'fraud_signals.json'), 'w') as f:
        json.dump(signals, f, indent=2)

    print("\nStep 3: Scoring and generating report...")
    from pipeline.score_and_report import generate_report, print_report
    scored = generate_report(signals)
    with open(os.path.join(REPORTS_DIR, 'fraud_report.json'), 'w') as f:
        json.dump(scored, f, indent=2)
    print_report(scored, top_n=25)


def run_update():
    print("=== INCREMENTAL UPDATE ===\n")
    from pipeline.auto_update import run_update
    run_update(days_back=1)


def run_report():
    print("=== REGENERATING REPORT ===\n")
    from pipeline.score_and_report import generate_report, print_report
    with open(os.path.join(DATA_DIR, 'fraud_signals.json')) as f:
        signals = json.load(f)
    scored = generate_report(signals)
    with open(os.path.join(REPORTS_DIR, 'fraud_report.json'), 'w') as f:
        json.dump(scored, f, indent=2)
    print_report(scored, top_n=25)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stock Fraud Screener')
    parser.add_argument('--full', action='store_true', help='Full pipeline run')
    parser.add_argument('--update', action='store_true', help='Check new filings only')
    parser.add_argument('--report', action='store_true', help='Regenerate report only')
    parser.add_argument('--fresh', action='store_true', help='Ignore checkpoint, start from scratch')
    args = parser.parse_args()

    if args.full:
        run_full(args)
    elif args.update:
        run_update()
    elif args.report:
        run_report()
    else:
        print("Usage: python3 run.py --full | --update | --report")
