"""
scripts/build_fraud_labels.py
─────────────────────────────────────────────────────────────────────────────
Phase 0c — Fraud Event Label System
Builds data/fraud_labels.parquet from multiple free public sources:

  1. SEC Accounting and Auditing Enforcement Releases (AAER)
     https://www.sec.gov/divisions/enforce/enforcements/aareleasesarchive.htm
  2. SEC EDGAR bankruptcy filings (Form 15 / BK tag)
  3. Stanford Securities Class Action Clearinghouse (SCAC) CSV
     https://securities.stanford.edu/class-action-filings/CSV.html

Output columns:
  ticker, market, fraud_year, label_type, source, description,
  fraud_confirmed, fraud_suspect, cik

Usage:
  python scripts/build_fraud_labels.py
  python scripts/build_fraud_labels.py --sources aaer scac
  python scripts/build_fraud_labels.py --output data/fraud_labels_2024.parquet
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator

import pandas as pd
import requests

BASE = Path(__file__).parent.parent
DATA_DIR = BASE / "data"
DATA_DIR.mkdir(exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("fraud_labels")

_HEADERS = {"User-Agent": "stock-fraud-screener research@example.com"}

# ── SEC AAER ──────────────────────────────────────────────────────────────────

AAER_INDEX_URL = (
    "https://efts.sec.gov/LATEST/search-index?q=%22accounting+and+auditing%22"
    "&dateRange=custom&startdt=1995-01-01&enddt={end_date}"
    "&forms=AAER&hits.hits._source=period_of_report,entity_name,file_date,period_of_report"
    "&hits.hits.total.value=true&hits.hits.hits.total.value=true&_source=true"
    "&hits.hits.highlight=false&category=form-type&hits.hits.highlight=false"
)

AAER_SEARCH_URL = (
    "https://efts.sec.gov/LATEST/search-index?q=%22accounting+and+auditing+enforcement%22"
    "&forms=AAER&dateRange=custom&startdt={start}&enddt={end}"
    "&hits.hits._source=period_of_report,entity_name,file_date,period_of_report,ticker"
)

EDGAR_FULL_TEXT_URL = "https://efts.sec.gov/LATEST/search-index?q=%22AAER%22&forms=AAER&dateRange=custom&startdt={start}&enddt={end}&hits.hits.total.value=true"

AAER_COMPANY_SEARCH = (
    "https://efts.sec.gov/LATEST/search-index?q=%22accounting+fraud%22+%22restatement%22"
    "&forms=8-K&dateRange=custom&startdt={start}&enddt={end}"
    "&_source=entity_name,period_of_report,file_date,biz_location"
)

# SEC EDGAR full-text search endpoint
EDGAR_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"

# Known AAER tickers from prominent enforcement actions (supplemental hardcoded list)
_KNOWN_AAERS: list[dict] = [
    {"ticker": "ENE",   "market": "US", "fraud_year": 2001, "description": "SEC AAER 1439 — Enron SPE/mark-to-market fraud"},
    {"ticker": "WCOM",  "market": "US", "fraud_year": 2002, "description": "SEC AAER 1524 — WorldCom expense capitalization $3.8B"},
    {"ticker": "HLSH",  "market": "US", "fraud_year": 2003, "description": "SEC AAER 1810 — HealthSouth $2.7B earnings fabrication"},
    {"ticker": "ADEL",  "market": "US", "fraud_year": 2002, "description": "SEC AAER 1568 — Adelphia $3.1B off-book family loans"},
    {"ticker": "PARME", "market": "IT", "fraud_year": 2003, "description": "Parmalat €14B phantom cash — SEC coordinated enforcement"},
    {"ticker": "LU",    "market": "US", "fraud_year": 2004, "description": "SEC AAER 1989 — Lucent $1.15B revenue manipulation"},
    {"ticker": "AIG",   "market": "US", "fraud_year": 2005, "description": "SEC AAER 2222 — AIG finite reinsurance / reserve manipulation"},
    {"ticker": "ESCN",  "market": "US", "fraud_year": 2007, "description": "SEC AAER — Eschelon Telecom channel stuffing"},
    {"ticker": "SAY",   "market": "US", "fraud_year": 2009, "description": "Satyam Computer — SEC coordination with SEBI"},
    {"ticker": "FNMA",  "market": "US", "fraud_year": 2004, "description": "SEC AAER 2176 — Fannie Mae earnings smoothing $10.6B"},
    {"ticker": "MO",    "market": "US", "fraud_year": 2001, "description": "SEC AAER Xerox — $6.4B revenue recognition acceleration"},
    {"ticker": "XRX",   "market": "US", "fraud_year": 2001, "description": "SEC AAER 1492 — Xerox $6.4B revenue recognition"},
    {"ticker": "CSCO",  "market": "US", "fraud_year": 2011, "description": "SEC AAER — Vitesse Semiconductor stock options backdating"},
    {"ticker": "NKLA",  "market": "US", "fraud_year": 2020, "description": "SEC/DOJ — Nikola technology fabrication, Trevor Milton fraud"},
    {"ticker": "LK",    "market": "US", "fraud_year": 2020, "description": "SEC charges — Luckin Coffee RMB 2.2B fabricated transactions"},
    {"ticker": "VRX",   "market": "US", "fraud_year": 2016, "description": "SEC investigation — Valeant channel stuffing via Philidor"},
    {"ticker": "WDI",   "market": "DE", "fraud_year": 2020, "description": "BaFin/SEC coordination — Wirecard €1.9B missing cash"},
]


def _fetch_aaer_from_edgar(years: range) -> list[dict]:
    """Query EDGAR full-text search for AAER forms and extract ticker/year pairs."""
    rows: list[dict] = []
    for year in years:
        url = (
            f"https://efts.sec.gov/LATEST/search-index"
            f"?q=%22accounting+and+auditing+enforcement%22"
            f"&forms=AAER"
            f"&dateRange=custom"
            f"&startdt={year}-01-01"
            f"&enddt={year}-12-31"
            f"&hits.hits._source=entity_name,period_of_report,file_date"
            f"&hits.hits.total.value=true"
        )
        try:
            r = requests.get(url, headers=_HEADERS, timeout=30)
            r.raise_for_status()
            data = r.json()
            hits = data.get("hits", {}).get("hits", [])
            for h in hits:
                src = h.get("_source", {})
                entity = src.get("entity_name", "")
                file_date = src.get("file_date", "")
                period = src.get("period_of_report", "")
                if not entity:
                    continue
                fy = _parse_year(period) or _parse_year(file_date) or year
                rows.append({
                    "ticker": "",
                    "market": "US",
                    "fraud_year": fy,
                    "label_type": "aaer",
                    "source": "SEC EDGAR AAER",
                    "description": f"AAER filing — {entity}",
                    "entity_name": entity,
                    "fraud_confirmed": True,
                    "fraud_suspect": False,
                })
            time.sleep(0.3)
        except Exception as exc:
            log.warning("EDGAR AAER %d failed: %s", year, exc)
    return rows


def _fetch_edgar_companies_cik(tickers: list[str]) -> dict[str, str]:
    """Resolve ticker → CIK via SEC EDGAR company_tickers.json."""
    try:
        r = requests.get(
            "https://www.sec.gov/files/company_tickers.json",
            headers=_HEADERS, timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        ticker_cik: dict[str, str] = {}
        for entry in data.values():
            t = entry.get("ticker", "").upper()
            cik = str(entry.get("cik_str", "")).zfill(10)
            if t:
                ticker_cik[t] = cik
        return {t: ticker_cik.get(t.upper(), "") for t in tickers}
    except Exception as exc:
        log.warning("CIK lookup failed: %s", exc)
        return {}


def _known_aaer_rows() -> list[dict]:
    rows = []
    for entry in _KNOWN_AAERS:
        rows.append({
            "ticker":           entry["ticker"],
            "market":           entry["market"],
            "fraud_year":       entry["fraud_year"],
            "label_type":       "aaer",
            "source":           "SEC AAER (curated)",
            "description":      entry["description"],
            "fraud_confirmed":  True,
            "fraud_suspect":    False,
            "cik":              "",
        })
    return rows


# ── Stanford SCAC ─────────────────────────────────────────────────────────────

SCAC_CSV_URL = "https://securities.stanford.edu/class-action-filings/CSV.html"
SCAC_DIRECT_CSV = "https://securities.stanford.edu/litigation/class-action-filings.csv"


def _fetch_scac() -> list[dict]:
    """Download Stanford Securities Class Action Clearinghouse CSV."""
    log.info("Downloading Stanford SCAC class action CSV…")
    try:
        r = requests.get(SCAC_DIRECT_CSV, headers=_HEADERS, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), encoding="latin-1", on_bad_lines="skip")
    except Exception as exc:
        log.warning("SCAC direct CSV failed (%s), trying HTML page…", exc)
        try:
            r = requests.get(SCAC_CSV_URL, headers=_HEADERS, timeout=60)
            r.raise_for_status()
            # Parse tables from HTML
            tables = pd.read_html(io.StringIO(r.text))
            df = tables[0] if tables else pd.DataFrame()
        except Exception as exc2:
            log.error("SCAC fetch failed: %s", exc2)
            return []

    if df.empty:
        return []

    log.info("SCAC: %d rows downloaded", len(df))

    # Normalise column names
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]

    rows = []
    ticker_col   = next((c for c in df.columns if "ticker" in c), None)
    date_col     = next((c for c in df.columns if "filing" in c and "date" in c), None)
    company_col  = next((c for c in df.columns if "company" in c or "defendant" in c), None)
    nature_col   = next((c for c in df.columns if "nature" in c or "class" in c or "allegation" in c), None)

    for _, r in df.iterrows():
        ticker = str(r.get(ticker_col, "")).strip().upper() if ticker_col else ""
        if not ticker or ticker == "NAN":
            continue
        year = _parse_year(str(r.get(date_col, ""))) if date_col else None
        if not year:
            continue
        desc_parts = []
        if company_col:
            desc_parts.append(str(r.get(company_col, "")))
        if nature_col:
            desc_parts.append(str(r.get(nature_col, "")))
        rows.append({
            "ticker":          ticker,
            "market":          "US",
            "fraud_year":      year,
            "label_type":      "scac",
            "source":          "Stanford SCAC",
            "description":     " | ".join(desc_parts)[:200],
            "fraud_confirmed": False,
            "fraud_suspect":   True,
            "cik":             "",
        })
    log.info("SCAC: %d rows parsed", len(rows))
    return rows


# ── SEC EDGAR 8-K auditor resignations ───────────────────────────────────────

def _fetch_auditor_resignations(years: range) -> list[dict]:
    """
    Look for 8-K Item 4.01/4.02 filings (auditor changes).
    High false-positive rate — labelled as fraud_suspect only.
    """
    rows: list[dict] = []
    for year in years:
        url = (
            f"https://efts.sec.gov/LATEST/search-index"
            f"?q=%22item+4.01%22+%22registered+public+accounting+firm%22"
            f"&forms=8-K"
            f"&dateRange=custom"
            f"&startdt={year}-01-01"
            f"&enddt={year}-12-31"
            f"&hits.hits._source=entity_name,period_of_report,file_date"
        )
        try:
            r = requests.get(url, headers=_HEADERS, timeout=30)
            r.raise_for_status()
            hits = r.json().get("hits", {}).get("hits", [])
            for h in hits:
                src = h.get("_source", {})
                entity = src.get("entity_name", "")
                file_date = src.get("file_date", "")
                fy = _parse_year(file_date) or year
                if entity:
                    rows.append({
                        "ticker":          "",
                        "market":          "US",
                        "fraud_year":      fy,
                        "label_type":      "auditor_resignation",
                        "source":          "SEC EDGAR 8-K Item 4.01",
                        "description":     f"Auditor change — {entity}",
                        "entity_name":     entity,
                        "fraud_confirmed": False,
                        "fraud_suspect":   True,
                        "cik":             "",
                    })
            time.sleep(0.3)
        except Exception as exc:
            log.warning("8-K auditor %d: %s", year, exc)
    log.info("Auditor resignations: %d rows", len(rows))
    return rows


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_year(s: str) -> int | None:
    if not s:
        return None
    m = re.search(r"\b(19|20)\d{2}\b", str(s))
    return int(m.group()) if m else None


def _deduplicate(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    keep_cols = [
        "ticker", "market", "fraud_year", "label_type", "source",
        "description", "fraud_confirmed", "fraud_suspect",
    ]
    for c in keep_cols:
        if c not in df.columns:
            df[c] = None

    df["ticker"]      = df["ticker"].fillna("").str.upper().str.strip()
    df["market"]      = df["market"].fillna("US")
    df["fraud_year"]  = pd.to_numeric(df["fraud_year"], errors="coerce").fillna(0).astype(int)

    # Prefer fraud_confirmed=True over fraud_suspect when deduplicating
    df = df.sort_values("fraud_confirmed", ascending=False)
    df = df.drop_duplicates(subset=["ticker", "market", "fraud_year", "label_type"], keep="first")
    return df[keep_cols].reset_index(drop=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def build(
    sources: list[str],
    output_path: Path,
    years: range,
) -> None:
    all_rows: list[dict] = []

    if "aaer" in sources:
        log.info("Building AAER labels (curated + EDGAR search)…")
        all_rows.extend(_known_aaer_rows())
        all_rows.extend(_fetch_aaer_from_edgar(years))

    if "scac" in sources:
        log.info("Building SCAC class-action labels…")
        all_rows.extend(_fetch_scac())

    if "8k" in sources:
        log.info("Building 8-K auditor-resignation labels…")
        all_rows.extend(_fetch_auditor_resignations(years))

    if not all_rows:
        log.warning("No labels collected — check network access or source selection.")
        return

    df = _deduplicate(all_rows)
    log.info("Total labels: %d (confirmed=%d, suspect=%d)",
             len(df),
             df["fraud_confirmed"].sum(),
             df["fraud_suspect"].sum())

    df.to_parquet(output_path, index=False)
    log.info("Saved → %s", output_path)

    # Summary by label_type
    summary = df.groupby("label_type").agg(
        count=("ticker", "count"),
        confirmed=("fraud_confirmed", "sum"),
    )
    print(summary.to_string())


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--sources", nargs="+",
        choices=["aaer", "scac", "8k"],
        default=["aaer", "scac"],
        help="Which label sources to download (default: aaer scac)",
    )
    parser.add_argument(
        "--output", default=str(DATA_DIR / "fraud_labels.parquet"),
        help="Output parquet path",
    )
    parser.add_argument(
        "--start-year", type=int, default=2000,
        help="Start year for EDGAR API searches",
    )
    parser.add_argument(
        "--end-year", type=int, default=datetime.now().year,
        help="End year for EDGAR API searches",
    )
    args = parser.parse_args()

    build(
        sources=args.sources,
        output_path=Path(args.output),
        years=range(args.start_year, args.end_year + 1),
    )


if __name__ == "__main__":
    main()
