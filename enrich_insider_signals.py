"""
Enrich companies_financials.json with insider trading signals from SEC Form 4 filings.

For each company, fetches the last 12 months of Form 4 filings via EDGAR and computes:
- insider_sale_count:    number of open-market sale transactions
- insider_buy_count:     number of open-market purchase transactions
- net_insider_shares:    total shares sold (negative = net selling)
- insider_selling_flag:  True if net shares sold > 10,000 AND sales outnumber buys

Checkpointed — safe to interrupt and resume.
Run: python3 enrich_insider_signals.py
"""

import json
import time
import os
import re
import requests
from datetime import datetime, timedelta

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
FINANCIALS_PATH = os.path.join(DATA_DIR, 'companies_financials.json')
CHECKPOINT_PATH = os.path.join(DATA_DIR, 'insider_checkpoint.json')
REPORTS_DIR = os.path.join(os.path.dirname(__file__), 'reports')
CHECKPOINT_EVERY = 200

HEADERS = {'User-Agent': 'StockFraudScreener contact@example.com'}
NET_SELL_THRESHOLD = 10_000  # net shares sold to trigger flag


def load_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH) as f:
            return set(json.load(f))
    return set()


def save_checkpoint(done_ciks):
    with open(CHECKPOINT_PATH, 'w') as f:
        json.dump(list(done_ciks), f)


def get_recent_form4_filings(cik: str, days_back: int = 365) -> list:
    """
    Fetch list of Form 4 filings for a company in the last N days.
    Uses the SEC EDGAR submissions API.
    """
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return []
        data = resp.json()
    except Exception:
        return []

    cutoff = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')

    filings = data.get('filings', {}).get('recent', {})
    forms    = filings.get('form', [])
    dates    = filings.get('filingDate', [])
    accessions = filings.get('accessionNumber', [])

    form4_accessions = []
    for form, date, acc in zip(forms, dates, accessions):
        if form == '4' and date >= cutoff:
            form4_accessions.append(acc.replace('-', ''))

    return form4_accessions


def parse_form4_transactions(cik: str, accession: str) -> dict:
    """
    Parse a single Form 4 XML filing to extract transaction type and shares.
    Returns {'sales': int, 'purchases': int} from non-derivative open-market transactions.
    """
    cik_padded = str(cik).zfill(10)
    # Primary document URL for Form 4 XML
    doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{accession}.xml"

    try:
        resp = requests.get(doc_url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            # Try to find the filing index and get the right file
            return {'sales': 0, 'purchases': 0}
        content = resp.text
    except Exception:
        return {'sales': 0, 'purchases': 0}

    sales = 0
    purchases = 0

    # Parse non-derivative transactions (open market buys/sells)
    # Transaction code S = Sale, P = Purchase (open market)
    # D = Disposition, A = Award (not open market)
    tx_blocks = re.findall(
        r'<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>',
        content, re.DOTALL
    )

    for block in tx_blocks:
        code_match = re.search(r'<transactionCode>([^<]+)</transactionCode>', block)
        shares_match = re.search(r'<transactionShares>\s*<value>([^<]+)</value>', block)

        if not code_match or not shares_match:
            continue

        code = code_match.group(1).strip()
        try:
            shares = abs(float(shares_match.group(1).strip()))
        except ValueError:
            continue

        if code == 'S':
            sales += shares
        elif code == 'P':
            purchases += shares

    return {'sales': int(sales), 'purchases': int(purchases)}


def get_insider_signals(cik: str) -> dict:
    """Aggregate insider trading signal for a company."""
    accessions = get_recent_form4_filings(cik)

    if not accessions:
        return {}

    total_sales = 0
    total_purchases = 0
    sale_count = 0
    buy_count = 0

    for acc in accessions[:20]:  # cap at 20 most recent Form 4s
        tx = parse_form4_transactions(cik, acc)
        if tx['sales'] > 0:
            total_sales += tx['sales']
            sale_count += 1
        if tx['purchases'] > 0:
            total_purchases += tx['purchases']
            buy_count += 1
        time.sleep(0.15)

    net_shares = total_purchases - total_sales  # negative = net selling

    insider_selling_flag = (
        net_shares < -NET_SELL_THRESHOLD and
        sale_count > buy_count
    )

    return {
        'insider_sale_count':   sale_count,
        'insider_buy_count':    buy_count,
        'net_insider_shares':   int(net_shares),
        'insider_selling_flag': insider_selling_flag,
    }


def run_enrichment():
    with open(FINANCIALS_PATH) as f:
        companies = json.load(f)

    # Only process companies missing insider data
    missing = [c for c in companies if c.get('insider_sale_count') is None]
    print(f"Total companies: {len(companies)}")
    print(f"Need insider signal enrichment: {len(missing)}")

    done_ciks = load_checkpoint()
    remaining = [c for c in missing if str(c['cik']) not in done_ciks]
    print(f"Resuming: {len(done_ciks)} already done, {len(remaining)} to fetch")

    lookup = {c['cik']: c for c in companies}

    updated = 0
    for i, company in enumerate(remaining):
        cik = company['cik']

        signals = get_insider_signals(cik)
        if signals:
            lookup[cik].update(signals)
            updated += 1

        done_ciks.add(str(cik))

        if (i + 1) % 25 == 0:
            ticker = company.get('ticker', '?')
            flagged = signals.get('insider_selling_flag', False)
            print(f"  {i+1}/{len(remaining)} — {ticker} — flag={flagged} — {updated} enriched")

        if (i + 1) % CHECKPOINT_EVERY == 0:
            with open(FINANCIALS_PATH, 'w') as f:
                json.dump(list(lookup.values()), f)
            save_checkpoint(done_ciks)

        time.sleep(0.1)

    # Final save
    with open(FINANCIALS_PATH, 'w') as f:
        json.dump(list(lookup.values()), f)
    save_checkpoint(done_ciks)

    print(f"\nDone. Enriched {updated} companies with insider signals.")
    _regenerate_report(list(lookup.values()))


def _regenerate_report(companies):
    print("\nRegenerating fraud signals...")
    from pipeline.fraud_signals import calculate_all_signals
    signals = calculate_all_signals(companies)
    with open(os.path.join(DATA_DIR, 'fraud_signals.json'), 'w') as f:
        json.dump(signals, f, indent=2)

    print("Regenerating report...")
    from pipeline.score_and_report import generate_report, print_report
    scored = generate_report(signals)
    with open(os.path.join(REPORTS_DIR, 'fraud_report.json'), 'w') as f:
        json.dump(scored, f, indent=2)

    insider_flagged = sum(1 for c in scored if c.get('insider_selling_flag'))
    print(f"Report saved. Insider selling flags: {insider_flagged}")
    print_report(scored, top_n=10)


if __name__ == '__main__':
    run_enrichment()
