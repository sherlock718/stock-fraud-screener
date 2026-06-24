"""
scripts/fetch_aaer_labels.py
─────────────────────────────────────────────────────────────────────────────
Phase 0d — AAER-based Fraud Label Construction

Builds data/aaer_labels.csv and updates the fraud_confirmed column in
data/historical_dataset_clean.parquet.

Sources (all free, no API key required):
  1. data/aaer_cache.json — pre-fetched AAER CIK/year pairs (232 unique CIKs)
  2. SEC EDGAR full-text search — 10-K filings that disclose "SEC investigation"
     AND "restatement" (Query 1: ~1,418 hits) or "accounting fraud" AND
     "restatement" (Query 2: ~521 hits).  These 10-K disclosures are the
     best publicly available proxy for confirmed accounting fraud because
     companies are legally required to disclose active SEC investigations
     in their annual reports.

Labeling window:
  fraud_confirmed = 1  when:
    company has an AAER/enforcement entry AND
    fiscal_year ∈ [fraud_year_start − 2, fraud_year_end]

  The −2 lookback captures the years when manipulation was ongoing before
  the SEC completed its investigation.

Output:
  data/aaer_labels.csv   — per-company fraud date ranges
  data/historical_dataset_clean.parquet — fraud_confirmed column updated

Usage:
  python scripts/fetch_aaer_labels.py
  python scripts/fetch_aaer_labels.py --no-update-parquet
  python scripts/fetch_aaer_labels.py --lookback 3
  python scripts/fetch_aaer_labels.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
from pathlib import Path

import pandas as pd
import requests
from scripts._root import ROOT

BASE = ROOT

# ── Paths ─────────────────────────────────────────────────────────────────────

DATA_DIR = BASE / "data"
AAER_CACHE = DATA_DIR / "aaer_cache.json"
LABELS_CSV = DATA_DIR / "aaer_labels.csv"
PARQUET = DATA_DIR / "historical_dataset_clean.parquet"

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("fetch_aaer")

_HEADERS = {"User-Agent": "stock-fraud-screener research@example.com"}

# ── EDGAR full-text search queries ────────────────────────────────────────────

EDGAR_EFTS = "https://efts.sec.gov/LATEST/search-index"

# Each tuple: (description, query_string)
# Both queries target annual filings where companies explicitly disclosed
# SEC investigations together with restatements — a strong fraud signal.
EDGAR_QUERIES: list[tuple[str, str]] = [
    (
        "SEC investigation + restatement",
        '"SEC investigation" "restatement"',
    ),
    (
        "Accounting fraud + restatement",
        '"accounting fraud" "restatement"',
    ),
]

_PER_PAGE = 100
_SLEEP = 0.25  # seconds between requests — respect SEC rate limits


# ── Helpers ───────────────────────────────────────────────────────────────────

def _norm_cik(raw: object) -> str:
    """Strip leading zeros and return integer-string CIK for comparison."""
    try:
        return str(int(str(raw).lstrip("0")))
    except (ValueError, TypeError):
        return str(raw)


def _parse_year(s: object) -> int | None:
    """Extract a 4-digit year from any string (date, filename, etc.)."""
    if not s:
        return None
    m = re.search(r"(19|20)\d{2}", str(s))
    return int(m.group()) if m else None


# ── Source 1: aaer_cache.json ─────────────────────────────────────────────────

def _load_aaer_cache() -> list[dict]:
    """
    Load pre-fetched AAER entries from data/aaer_cache.json.

    Expected format (list of objects):
      {"cik": "774055", "name": "TRANSAXIS INC  (CIK 0000774055)", "year": 2003}
    """
    if not AAER_CACHE.exists():
        log.warning("aaer_cache.json not found at %s — skipping", AAER_CACHE)
        return []

    with AAER_CACHE.open() as f:
        raw = json.load(f)

    entries = []
    for item in raw:
        cik = _norm_cik(item.get("cik", ""))
        yr = item.get("year")
        if cik and yr:
            entries.append({"cik": cik, "year": int(yr), "source": "aaer_cache"})

    log.info("aaer_cache: %d entries, %d unique CIKs",
             len(entries), len({e["cik"] for e in entries}))
    return entries


# ── Source 2: SEC EDGAR full-text search ─────────────────────────────────────

def _fetch_edgar_query(description: str, query: str, start_year: int, end_year: int) -> list[dict]:
    """
    Paginate through EDGAR EFTS full-text search results and return
    (cik, year) pairs from matching 10-K filings.
    """
    entries: list[dict] = []
    start_dt = f"{start_year}-01-01"
    end_dt = f"{end_year}-12-31"

    # First request to get total.
    # Note: omitting _source avoids a sporadic 500 from the EFTS API
    # when certain query strings interact with the field-filter param.
    # All required fields (ciks, display_names, file_date, period_ending)
    # are present in the default _source response.
    params = {
        "q": query,
        "forms": "10-K",
        "dateRange": "custom",
        "startdt": start_dt,
        "enddt": end_dt,
        "from": 0,
        "size": _PER_PAGE,
    }
    try:
        r = requests.get(EDGAR_EFTS, params=params, headers=_HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        log.warning("EDGAR query '%s' failed on first request: %s", description, exc)
        return []

    total_hits = data.get("hits", {}).get("total", {}).get("value", 0)
    log.info("EDGAR query '%s': %d total hits", description, total_hits)

    def _extract_hits(response_data: dict) -> None:
        for h in response_data.get("hits", {}).get("hits", []):
            src = h.get("_source", {})
            ciks = src.get("ciks", [])
            names = src.get("display_names", [])
            file_date = src.get("file_date", "")
            period = src.get("period_ending", "")
            yr = _parse_year(period) or _parse_year(file_date)
            if not yr:
                continue
            for i, cik_raw in enumerate(ciks):
                cik = _norm_cik(cik_raw)
                entries.append({
                    "cik": cik,
                    "year": yr,
                    "source": f"edgar:{description}",
                    "display_name": names[i] if i < len(names) else (names[0] if names else ""),
                })

    _extract_hits(data)
    time.sleep(_SLEEP)

    # Paginate remaining pages — retry once on 500 (transient EFTS errors)
    n_pages = (total_hits + _PER_PAGE - 1) // _PER_PAGE
    for page in range(1, n_pages):
        params["from"] = page * _PER_PAGE
        success = False
        for attempt in range(2):  # try twice
            try:
                r = requests.get(EDGAR_EFTS, params=params, headers=_HEADERS, timeout=30)
                r.raise_for_status()
                page_data = r.json()
                page_hits = page_data.get("hits", {}).get("hits", [])
                if not page_hits:
                    success = True
                    break
                _extract_hits(page_data)
                success = True
                break
            except Exception as exc:
                if attempt == 0:
                    log.debug("EDGAR '%s' page %d attempt 1 failed: %s — retrying", description, page, exc)
                    time.sleep(1.0)
                else:
                    log.warning("EDGAR '%s' page %d failed after retry: %s", description, page, exc)
        if not success:
            continue
        time.sleep(_SLEEP)

    log.info("EDGAR query '%s': collected %d entries", description, len(entries))
    return entries


def _fetch_edgar_sources(start_year: int, end_year: int) -> list[dict]:
    """Run all EDGAR queries and aggregate entries."""
    all_entries: list[dict] = []
    for description, query in EDGAR_QUERIES:
        all_entries.extend(_fetch_edgar_query(description, query, start_year, end_year))
    return all_entries


# ── CIK → ticker/name lookup ──────────────────────────────────────────────────

def _build_cik_lookup(df: pd.DataFrame) -> dict[str, dict]:
    """
    Build a dict of norm_cik → {ticker, name, cik_raw} from the dataset.
    Uses first occurrence per CIK.
    """
    lookup: dict[str, dict] = {}
    for _, row in df[["cik", "ticker", "name"]].drop_duplicates("cik").iterrows():
        nk = _norm_cik(row["cik"])
        if nk not in lookup:
            lookup[nk] = {
                "cik_raw": str(row["cik"]),
                "ticker": str(row["ticker"]),
                "name": str(row["name"]),
            }
    return lookup


# ── Build labels table ────────────────────────────────────────────────────────

def build_labels(
    df: pd.DataFrame,
    start_year: int,
    end_year: int,
) -> pd.DataFrame:
    """
    Aggregate all sources → per-company fraud date ranges.

    Returns a DataFrame with columns:
      cik, ticker, name, fraud_year_start, fraud_year_end, n_filings, sources
    Only companies that appear in the dataset are kept.
    """
    cik_lookup = _build_cik_lookup(df)
    ds_ciks = set(cik_lookup.keys())

    # Collect all (cik, year) pairs from every source
    all_entries: list[dict] = []
    all_entries.extend(_load_aaer_cache())
    all_entries.extend(_fetch_edgar_sources(start_year, end_year))

    # Aggregate by CIK, restricted to companies that exist in the dataset
    cik_years: dict[str, list[int]] = {}
    cik_sources: dict[str, set[str]] = {}

    for e in all_entries:
        cik = e["cik"]
        if cik not in ds_ciks:
            continue
        yr = e["year"]
        cik_years.setdefault(cik, []).append(yr)
        cik_sources.setdefault(cik, set()).add(e.get("source", "unknown"))

    log.info("Companies matched to dataset: %d", len(cik_years))

    rows = []
    for cik, yrs in cik_years.items():
        meta = cik_lookup[cik]
        rows.append({
            "cik":              meta["cik_raw"],
            "ticker":           meta["ticker"],
            "name":             meta["name"],
            "fraud_year_start": min(yrs),
            "fraud_year_end":   max(yrs),
            "n_filings":        len(yrs),
            "sources":          ";".join(sorted(cik_sources[cik])),
        })

    labels_df = pd.DataFrame(rows).sort_values("ticker").reset_index(drop=True)
    log.info(
        "Labels table: %d companies, year range %d–%d",
        len(labels_df),
        labels_df["fraud_year_start"].min() if len(labels_df) else 0,
        labels_df["fraud_year_end"].max() if len(labels_df) else 0,
    )
    return labels_df


# ── Apply labels to dataset ───────────────────────────────────────────────────

def apply_labels(
    df: pd.DataFrame,
    labels_df: pd.DataFrame,
    lookback: int,
) -> pd.DataFrame:
    """
    Set fraud_confirmed = 1 for rows where:
      fiscal_year ∈ [fraud_year_start − lookback, fraud_year_end]

    Both annual and quarterly rows are updated (consistent with the rest of
    the pipeline which trains on annual rows).
    """
    df = df.copy()
    df["fraud_confirmed"] = 0  # reset — rebuild from labels

    # Fast path: build a lookup dict from CIK (raw) → (start, end)
    label_map: dict[str, tuple[int, int]] = {}
    for _, row in labels_df.iterrows():
        label_map[str(row["cik"])] = (
            int(row["fraud_year_start"]) - lookback,
            int(row["fraud_year_end"]),
        )

    # Vectorised apply
    def _is_fraud(row: pd.Series) -> int:
        key = str(row["cik"])
        if key not in label_map:
            return 0
        lo, hi = label_map[key]
        fy = row["fiscal_year"]
        try:
            return 1 if lo <= int(fy) <= hi else 0
        except (TypeError, ValueError):
            return 0

    df["fraud_confirmed"] = df.apply(_is_fraud, axis=1)

    total = df["fraud_confirmed"].sum()
    annual_pos = df.loc[df["period_type"] == "annual", "fraud_confirmed"].sum()
    companies = df.loc[df["fraud_confirmed"] == 1, "ticker"].nunique()
    log.info(
        "Labels applied: %d total positive rows (%d annual, %d unique companies)",
        total, annual_pos, companies,
    )
    return df


# ── Coverage report ───────────────────────────────────────────────────────────

def print_coverage_report(df: pd.DataFrame, labels_df: pd.DataFrame) -> None:
    """Print a summary of label coverage to stdout."""
    annual = df[df["period_type"] == "annual"]
    pos = annual[annual["fraud_confirmed"] == 1]

    print("\n── Label Coverage Report ────────────────────────────────────────")
    print(f"  Total annual rows         : {len(annual):,}")
    print(f"  Fraud-positive annual rows: {len(pos):,}")
    print(f"  Positive rate (annual)    : {len(pos)/len(annual)*100:.3f}%")
    print(f"  Unique companies flagged  : {pos['ticker'].nunique()}")
    print(f"\n  Positives by fiscal year:")
    yr_counts = pos["fiscal_year"].value_counts().sort_index()
    for yr, n in yr_counts.items():
        bar = "█" * (n // 2)
        print(f"    {yr}: {n:4d}  {bar}")
    print("\n  Top 15 companies by positive row count:")
    top = (pos.groupby(["ticker", "name"])
              .size()
              .sort_values(ascending=False)
              .head(15)
              .reset_index(name="n"))
    for _, r in top.iterrows():
        print(f"    {r['ticker']:<10s} {r['name'][:40]:<40s}  {r['n']} rows")

    print("\n  Source breakdown (labels table):")
    if "sources" in labels_df.columns:
        src_counts: dict[str, int] = {}
        for s in labels_df["sources"]:
            for part in str(s).split(";"):
                src_counts[part] = src_counts.get(part, 0) + 1
        for src, cnt in sorted(src_counts.items(), key=lambda x: -x[1]):
            print(f"    {src:<45s}  {cnt}")
    print("─────────────────────────────────────────────────────────────────\n")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--lookback", type=int, default=2,
        help="Years before fraud_year_start to include as positive (default: 2)",
    )
    parser.add_argument(
        "--start-year", type=int, default=2000,
        help="Earliest year for EDGAR search queries (default: 2000)",
    )
    parser.add_argument(
        "--end-year", type=int, default=2024,
        help="Latest year for EDGAR search queries (default: 2024)",
    )
    parser.add_argument(
        "--labels-output", default=str(LABELS_CSV),
        help="Output path for aaer_labels.csv",
    )
    parser.add_argument(
        "--parquet", default=str(PARQUET),
        help="Path to historical_dataset_clean.parquet",
    )
    parser.add_argument(
        "--no-update-parquet", action="store_true",
        help="Build labels CSV only; do not modify the parquet",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show coverage stats but do not write any files",
    )
    args = parser.parse_args()

    # Load dataset
    log.info("Loading dataset from %s …", args.parquet)
    df = pd.read_parquet(args.parquet)
    log.info("Dataset: %d rows × %d columns", *df.shape)

    # Build labels
    labels_df = build_labels(df, args.start_year, args.end_year)

    if labels_df.empty:
        log.error("No labels built — check network access and aaer_cache.json")
        return

    # Apply labels
    df_labeled = apply_labels(df, labels_df, lookback=args.lookback)

    # Coverage report
    print_coverage_report(df_labeled, labels_df)

    if args.dry_run:
        log.info("--dry-run: no files written")
        return

    # Save labels CSV
    out_labels = Path(args.labels_output)
    labels_df.to_csv(out_labels, index=False)
    log.info("Saved → %s  (%d rows)", out_labels, len(labels_df))

    # Update parquet
    if not args.no_update_parquet:
        out_parquet = Path(args.parquet)
        df_labeled.to_parquet(out_parquet, index=False)
        log.info("Saved → %s", out_parquet)
    else:
        log.info("--no-update-parquet: parquet not modified")


if __name__ == "__main__":
    main()
