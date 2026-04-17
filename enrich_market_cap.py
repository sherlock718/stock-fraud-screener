"""
Enrich companies_financials.json with market cap data for companies missing it.
Checkpointed — safe to interrupt and resume.
"""

import json
import time
import os
import yfinance as yf

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
REPORTS_DIR = os.path.join(os.path.dirname(__file__), 'reports')
FINANCIALS_PATH = os.path.join(DATA_DIR, 'companies_financials.json')
CHECKPOINT_PATH = os.path.join(DATA_DIR, 'mcap_checkpoint.json')
CHECKPOINT_EVERY = 200


def load_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH) as f:
            return set(json.load(f))
    return set()


def save_checkpoint(done_tickers):
    with open(CHECKPOINT_PATH, 'w') as f:
        json.dump(list(done_tickers), f)


def get_market_cap(ticker):
    try:
        info = yf.Ticker(ticker).info
        return info.get('marketCap')
    except Exception:
        return None


def run_enrichment():
    with open(FINANCIALS_PATH) as f:
        companies = json.load(f)

    missing = [c for c in companies if not c.get('market_cap')]
    print(f"Total companies: {len(companies)}")
    print(f"Missing market cap: {len(missing)}")

    done_tickers = load_checkpoint()
    remaining = [c for c in missing if c.get('ticker') not in done_tickers]
    print(f"Resuming: {len(done_tickers)} already done, {len(remaining)} to fetch")

    # Build lookup for fast update
    lookup = {c['cik']: c for c in companies}

    updated = 0
    for i, company in enumerate(remaining):
        ticker = company.get('ticker')
        if not ticker:
            done_tickers.add(ticker or str(company['cik']))
            continue

        mcap = get_market_cap(ticker)
        if mcap:
            lookup[company['cik']]['market_cap'] = mcap
            updated += 1

        done_tickers.add(ticker)

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{len(remaining)} — {ticker} — {updated} market caps found")

        if (i + 1) % CHECKPOINT_EVERY == 0:
            # Save data + checkpoint
            with open(FINANCIALS_PATH, 'w') as f:
                json.dump(list(lookup.values()), f)
            save_checkpoint(done_tickers)

        time.sleep(0.1)

    # Final save
    with open(FINANCIALS_PATH, 'w') as f:
        json.dump(list(lookup.values()), f)
    save_checkpoint(done_tickers)

    print(f"\nDone. Updated {updated} companies with market cap.")

    # Regenerate fraud signals and report
    print("\nRegenerating fraud signals...")
    from pipeline.fraud_signals import calculate_all_signals
    signals = calculate_all_signals(list(lookup.values()))
    with open(os.path.join(DATA_DIR, 'fraud_signals.json'), 'w') as f:
        json.dump(signals, f, indent=2)

    print("Regenerating report...")
    from pipeline.score_and_report import generate_report, print_report
    scored = generate_report(signals)
    with open(os.path.join(REPORTS_DIR, 'fraud_report.json'), 'w') as f:
        json.dump(scored, f, indent=2)

    has_mcap = sum(1 for c in scored if c.get('market_cap'))
    print(f"Report saved. {has_mcap}/{len(scored)} companies have market cap.")
    print_report(scored, top_n=10)


if __name__ == '__main__':
    run_enrichment()
