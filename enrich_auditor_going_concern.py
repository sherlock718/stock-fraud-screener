"""
Enrich companies_financials.json with:
  - going_concern:      True if company disclosed going concern doubts in SEC filing
  - auditor_name:       Name of the registered public accounting firm (from dei:AuditorName)
  - big4_auditor:       True if auditor is one of the Big 4 firms
  - small_auditor_flag: True if NOT a major auditor AND market cap > $50M
                        A large company using a small, unknown auditor is a classic fraud signal.

Fetches SEC EDGAR XBRL company-facts for each company.
Checkpointed — safe to interrupt and resume.
Run: python3 enrich_auditor_going_concern.py
"""

import json
import time
import os
import requests

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
FINANCIALS_PATH = os.path.join(DATA_DIR, 'companies_financials.json')
CHECKPOINT_PATH = os.path.join(DATA_DIR, 'auditor_checkpoint.json')
CHECKPOINT_EVERY = 200
LARGE_CAP_THRESHOLD = 50_000_000  # $50M market cap

HEADERS = {'User-Agent': 'StockFraudScreener contact@example.com'}

# Big 4 — checked via substring match on lowercase auditor name
BIG4_PATTERNS = ['deloitte', 'ernst & young', 'kpmg', 'pricewaterhousecoopers']

# Major / Tier-2 firms — small_auditor_flag only triggers if NOT in this list
MAJOR_FIRM_PATTERNS = BIG4_PATTERNS + [
    'bdo ', 'grant thornton', 'rsm us', 'rsm llp', 'mazars',
    'crowe', 'baker tilly', 'cohnreznick', 'moss adams', 'plante moran',
    'cherry bekaert', 'forvis', 'dixon hughes', 'eide bailly', 'marcum',
    'citrin', 'withum', 'weaver', 'cbiz', 'wipfli', 'armanino', 'friedman',
    'rkl', 'svb alliant', 'sensiba', 'mgq',
]


def load_checkpoint():
    if os.path.exists(CHECKPOINT_PATH):
        with open(CHECKPOINT_PATH) as f:
            return set(json.load(f))
    return set()


def save_checkpoint(done_ciks):
    with open(CHECKPOINT_PATH, 'w') as f:
        json.dump(list(done_ciks), f)


def get_company_facts(cik):
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    if resp.status_code == 404:
        return None
    if resp.status_code == 429:
        raise RuntimeError("rate_limited")
    resp.raise_for_status()
    return resp.json()


def extract_going_concern(facts):
    """True if the company has disclosed going concern doubts in SEC XBRL."""
    try:
        us_gaap = facts['facts'].get('us-gaap', {})
        for key in ['SubstantialDoubtAboutGoingConcernTextBlock',
                    'GoingConcernDisclosureTextBlock']:
            if key in us_gaap:
                return True
        return False
    except Exception:
        return False


def extract_auditor_name(facts):
    """Extract the most recent auditor name from dei:AuditorName XBRL."""
    try:
        entries = facts['facts']['dei']['AuditorName']['units']['']
        annual = [e for e in entries if e.get('form') in ('10-K', '10-K/A')]
        if not annual:
            return None
        latest = sorted(annual, key=lambda x: x.get('end', ''))[-1]
        return latest.get('val')
    except (KeyError, IndexError, TypeError):
        return None


def classify_auditor(name, market_cap):
    """
    Returns (big4_auditor, small_auditor_flag).
    small_auditor_flag: True when market cap > $50M and auditor is not a recognised major firm.
    """
    if not name:
        return False, False
    n = name.lower()
    is_big4  = any(p in n for p in BIG4_PATTERNS)
    is_major = any(p in n for p in MAJOR_FIRM_PATTERNS)

    # Special-case: "ey" / "e&y" short-forms
    if not is_big4 and ('ey llp' in n or n.strip() in ('ey', 'e&y')):
        is_big4  = True
        is_major = True
    if not is_major and ('pwc llp' in n or n.strip() == 'pwc'):
        is_major = True

    small_flag = (not is_major) and bool(market_cap and market_cap > LARGE_CAP_THRESHOLD)
    return is_big4, small_flag


def main():
    with open(FINANCIALS_PATH) as f:
        companies = json.load(f)
    print(f"Loaded {len(companies)} companies")

    done_ciks = load_checkpoint()
    print(f"Checkpoint: {len(done_ciks)} already enriched")

    cik_index = {str(c['cik']): i for i, c in enumerate(companies)}
    todo = [c for c in companies if str(c['cik']) not in done_ciks]
    print(f"Remaining to enrich: {len(todo)}")

    for n, company in enumerate(todo):
        cik     = str(company['cik'])
        ticker  = company.get('ticker', '')
        name    = company.get('name', '')
        mcap    = company.get('market_cap') or 0

        if n % 100 == 0:
            print(f"  {n}/{len(todo)} — {ticker} {name[:30]}")

        facts = None
        retries = 0
        while True:
            try:
                facts = get_company_facts(cik)
                break
            except RuntimeError as e:
                if str(e) == "rate_limited":
                    retries += 1
                    wait = 5 * retries
                    print(f"    Rate limited. Waiting {wait}s... (attempt {retries})")
                    time.sleep(wait)
                    if retries >= 5:
                        break
                else:
                    break
            except Exception as e:
                print(f"    Error CIK {cik}: {e}")
                break

        idx = cik_index[cik]
        if facts is not None:
            going_concern    = extract_going_concern(facts)
            auditor_name     = extract_auditor_name(facts)
            big4, small_flag = classify_auditor(auditor_name, mcap)

            companies[idx]['going_concern']      = going_concern
            companies[idx]['auditor_name']        = auditor_name
            companies[idx]['big4_auditor']        = big4
            companies[idx]['small_auditor_flag']  = small_flag
        else:
            companies[idx].setdefault('going_concern',      False)
            companies[idx].setdefault('auditor_name',        None)
            companies[idx].setdefault('big4_auditor',        False)
            companies[idx].setdefault('small_auditor_flag',  False)

        done_ciks.add(cik)

        if (n + 1) % CHECKPOINT_EVERY == 0:
            save_checkpoint(done_ciks)
            with open(FINANCIALS_PATH, 'w') as f:
                json.dump(companies, f)
            print(f"    Checkpoint saved at {n + 1}")

        time.sleep(0.12)

    # Final save
    save_checkpoint(done_ciks)
    with open(FINANCIALS_PATH, 'w') as f:
        json.dump(companies, f)

    gc_count      = sum(1 for c in companies if c.get('going_concern'))
    auditor_found = sum(1 for c in companies if c.get('auditor_name'))
    big4_count    = sum(1 for c in companies if c.get('big4_auditor'))
    small_count   = sum(1 for c in companies if c.get('small_auditor_flag'))

    print("\nDone. Summary:")
    print(f"  Going concern flagged: {gc_count}")
    print(f"  Auditor name found:    {auditor_found}/{len(companies)}")
    print(f"  Big 4:                 {big4_count}")
    print(f"  Small auditor flag:    {small_count}")


if __name__ == '__main__':
    main()
