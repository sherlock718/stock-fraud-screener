"""
Enrich companies_financials.json with governance signals via EDGAR EFTS batch queries.

This replaces enrich_auditor_going_concern.py which used the EDGAR XBRL company-facts API —
that API only returns numeric concepts; text blocks and string DEI concepts are absent.

Signals enriched:
  going_concern:      True if company filed a 10-K (2021+) with standard going concern language.
                      Source: EDGAR EFTS full-text search for
                      "substantial doubt about its ability to continue".
  auditor_name:       None — EDGAR XBRL does not expose auditor name via the free API.
                      Will be None for all companies; auditor scoring is skipped automatically.
  big4_auditor:       False (default until a paid data source is added)
  small_auditor_flag: False (requires auditor_name to be populated)

Run time: ~30 seconds (one batch of ~46 API calls via EDGAR EFTS pagination).
No checkpoint needed — stateless batch update.

Run: python3 enrich_governance.py
"""

import json
import time
import os
import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
FINANCIALS_PATH = os.path.join(DATA_DIR, 'companies_financials.json')

HEADERS = {'User-Agent': 'StockFraudScreener contact@example.com'}
EFTS_BASE = 'https://efts.sec.gov/LATEST/search-index'
GC_QUERY  = '"substantial doubt about its ability to continue"'
PAGE_SIZE = 100


def fetch_going_concern_ciks(start_date='2021-01-01', end_date='2026-12-31'):
    """
    Batch-fetch all CIKs with going concern disclosures from EDGAR EFTS.
    Returns a set of CIK strings (leading zeros stripped).
    """
    gc_ciks = set()
    page    = 0
    total   = None

    while True:
        params = {
            'q':         GC_QUERY,
            'forms':     '10-K',
            'dateRange': 'custom',
            'startdt':   start_date,
            'enddt':     end_date,
            'size':      PAGE_SIZE,
            'from':      page * PAGE_SIZE,
        }
        resp = requests.get(EFTS_BASE, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if total is None:
            total = data['hits']['total']['value']
            print(f"EDGAR EFTS: {total} going concern 10-K filings found ({start_date} to {end_date})")

        hits = data['hits']['hits']
        if not hits:
            break

        for h in hits:
            for cik in h['_source'].get('ciks', []):
                gc_ciks.add(cik.lstrip('0'))

        page += 1
        if page % 10 == 0:
            print(f"  Page {page}/{(total // PAGE_SIZE) + 1} — {len(gc_ciks)} unique CIKs so far")

        if page * PAGE_SIZE >= total:
            break

        time.sleep(0.1)

    return gc_ciks


def main():
    with open(FINANCIALS_PATH) as f:
        companies = json.load(f)
    print(f"Loaded {len(companies)} companies")

    # ── Step 1: Batch-fetch going concern CIKs via EFTS ──────────────────────
    gc_ciks = fetch_going_concern_ciks()
    print(f"Total unique CIKs with going concern: {len(gc_ciks)}")

    # ── Step 2: Update companies_financials.json ──────────────────────────────
    flagged = 0
    for company in companies:
        cik_stripped  = str(company['cik']).lstrip('0')
        going_concern = cik_stripped in gc_ciks

        company['going_concern']      = going_concern
        company['auditor_name']        = None
        company['big4_auditor']        = False
        company['small_auditor_flag']  = False

        if going_concern:
            flagged += 1

    # ── Step 3: Save ─────────────────────────────────────────────────────────
    with open(FINANCIALS_PATH, 'w') as f:
        json.dump(companies, f)

    print(f"\nDone.")
    print(f"  Going concern flagged: {flagged}/{len(companies)}")
    print(f"  File updated: {FINANCIALS_PATH}")
    print(f"\nNext step: python3 run.py --signals")


if __name__ == '__main__':
    main()
