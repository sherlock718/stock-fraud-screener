"""
Enrich companies_financials.json with market-based signals:
- avg_volume_90d:      average daily trading volume over 90 days
- volume_spike_ratio:  recent 30-day avg volume / 90-day avg (>3x = spike)
- price_change_90d:    price percentage change over 90 days
- illiquid_flag:       avg daily volume < 10,000 shares (thin market = manipulation risk)
- pump_dump_flag:      volume spike >3x AND price up >50% in 30 days

Checkpointed — safe to interrupt and resume.
Run: python3 enrich_market_signals.py
"""

import json
import time
import os
import yfinance as yf
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
FINANCIALS_PATH = os.path.join(DATA_DIR, 'companies_financials.json')
CHECKPOINT_PATH = os.path.join(DATA_DIR, 'market_signals_checkpoint.json')
CHECKPOINT_EVERY = 200

VOLUME_ILLIQUID_THRESHOLD = 10_000      # avg daily shares
VOLUME_SPIKE_THRESHOLD = 3.0            # recent vol / 90d avg
PRICE_PUMP_THRESHOLD = 0.50             # 50% price gain in 30 days


def load_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH) as f:
            return set(json.load(f))
    return set()


def save_checkpoint(done_tickers):
    with open(CHECKPOINT_PATH, 'w') as f:
        json.dump(list(done_tickers), f)


def get_market_signals(ticker: str) -> dict:
    """Fetch 90-day price + volume history and compute signals."""
    try:
        hist = yf.Ticker(ticker).history(period='90d')
        if hist.empty or len(hist) < 10:
            return {}

        avg_volume_90d = float(hist['Volume'].mean())
        avg_volume_30d = float(hist['Volume'].tail(30).mean()) if len(hist) >= 30 else avg_volume_90d

        volume_spike_ratio = avg_volume_30d / avg_volume_90d if avg_volume_90d > 0 else None

        # Price change over available window
        price_start = float(hist['Close'].iloc[0])
        price_end = float(hist['Close'].iloc[-1])
        price_change_90d = (price_end - price_start) / price_start if price_start > 0 else None

        # 30-day price change
        price_start_30d = float(hist['Close'].iloc[-30]) if len(hist) >= 30 else price_start
        price_change_30d = (price_end - price_start_30d) / price_start_30d if price_start_30d > 0 else None

        illiquid_flag = avg_volume_90d < VOLUME_ILLIQUID_THRESHOLD

        pump_dump_flag = (
            volume_spike_ratio is not None and volume_spike_ratio > VOLUME_SPIKE_THRESHOLD
            and price_change_30d is not None and price_change_30d > PRICE_PUMP_THRESHOLD
        )

        return {
            'avg_volume_90d':     round(avg_volume_90d),
            'volume_spike_ratio': round(volume_spike_ratio, 3) if volume_spike_ratio else None,
            'price_change_90d':   round(price_change_90d, 4) if price_change_90d is not None else None,
            'illiquid_flag':      illiquid_flag,
            'pump_dump_flag':     pump_dump_flag,
        }
    except Exception:
        return {}


def run_enrichment():
    with open(FINANCIALS_PATH) as f:
        companies = json.load(f)

    # Only enrich companies that have a ticker and are missing market signal data
    missing = [c for c in companies if c.get('ticker') and c.get('avg_volume_90d') is None]
    print(f"Total companies: {len(companies)}")
    print(f"Need market signal enrichment: {len(missing)}")

    done_tickers = load_checkpoint()
    remaining = [c for c in missing if c.get('ticker') not in done_tickers]
    print(f"Resuming: {len(done_tickers)} already done, {len(remaining)} to fetch")

    lookup = {c['cik']: c for c in companies}

    updated = 0
    for i, company in enumerate(remaining):
        ticker = company['ticker']

        signals = get_market_signals(ticker)
        if signals:
            lookup[company['cik']].update(signals)
            updated += 1

        done_tickers.add(ticker)

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{len(remaining)} — {ticker} — {updated} enriched")

        if (i + 1) % CHECKPOINT_EVERY == 0:
            with open(FINANCIALS_PATH, 'w') as f:
                json.dump(list(lookup.values()), f)
            save_checkpoint(done_tickers)

        time.sleep(0.2)

    # Final save
    with open(FINANCIALS_PATH, 'w') as f:
        json.dump(list(lookup.values()), f)
    save_checkpoint(done_tickers)

    print(f"\nDone. Enriched {updated} companies with market signals.")
    _regenerate_report(list(lookup.values()))


def _regenerate_report(companies):
    print("\nRegenerating fraud signals...")
    from pipeline.fraud_signals import calculate_all_signals
    signals = calculate_all_signals(companies)
    with open(os.path.join(DATA_DIR, 'fraud_signals.json'), 'w') as f:
        json.dump(signals, f, indent=2)

    print("Regenerating report...")
    from pipeline.score_and_report import generate_report, print_report
    REPORTS_DIR = os.path.join(os.path.dirname(__file__), 'reports')
    scored = generate_report(signals)
    with open(os.path.join(REPORTS_DIR, 'fraud_report.json'), 'w') as f:
        json.dump(scored, f, indent=2)

    pump_dump = sum(1 for c in scored if c.get('pump_dump_flag'))
    illiquid  = sum(1 for c in scored if c.get('illiquid_flag'))
    print(f"Report saved. Pump & dump flags: {pump_dump} | Illiquid: {illiquid}")
    print_report(scored, top_n=10)


if __name__ == '__main__':
    run_enrichment()
