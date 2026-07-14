# Universe Coverage Report — Historical & Delisted Companies

**Date:** 2026-07-14  
**Scope:** Issue 1 — Survivorship bias from missing delisted companies

---

## 1. Current Universe Construction

The stock universe is constructed through a single source:

| Stage | Source | What it provides |
|-------|--------|-----------------|
| `pipeline/step1_fetch_tickers.py` | SEC `company_tickers.json` | ~13,000 CIKs with **current** tickers |
| (enrichment) | `company_tickers_exchange.json` | Exchange codes for listed companies |
| (enrichment) | EDGAR submissions API | SIC codes per CIK |

**No other stage adds historical or delisted CIKs.** The pipeline only processes companies present in `data/tickers.parquet`, which is a snapshot of currently-registered companies.

The survivorship adjustment in `step6_clean.py` flags companies whose last filing is ≥3 years old and imputes -50% forward returns, but this only affects companies that were in the ticker universe at download time.

---

## 2. Coverage Statistics

### CIKs by year in current dataset

| Year | Unique CIKs | Note |
|------|-------------|------|
| 2009 | 257 | Early XBRL adoption era |
| 2010 | 689 | |
| 2011 | 1,720 | |
| 2012 | 1,937 | |
| 2013 | 1,990 | |
| 2014 | 2,091 | |
| 2015 | 2,406 | |
| 2016 | 2,506 | |
| 2017 | 2,637 | |
| 2018 | 3,026 | |
| 2019 | 3,283 | |
| 2020 | 3,470 | |
| 2021 | 5,535 | |
| 2022 | 6,392 | |
| 2023 | 6,683 | |
| 2024 | 6,941 | |

The monotonic increase from 257 → 6,941 is partly XBRL adoption but also reflects that companies delisted before 2024 are absent from early years.

### Known fraud/bankruptcy companies: presence audit

**Total in `KNOWN_FRAUD_CIKS`:** 37  
**Present in dataset:** 12 (32%)  
**In tickers.parquet but no data:** 3 (Fannie Mae, Freddie Mac, Luckin Coffee)  
**Completely missing:** 22 (59%)

#### Missing companies with XBRL data available on EDGAR (recoverable)

| Company | CIK | XBRL Concepts | Fraud Period |
|---------|-----|---------------|--------------|
| Cendant | 1060349 | 434 | 1997–2001 |
| Computer Associates | 356028 | 408 | 1998–2005 |
| Global Crossing | 1085869 | 358 | 1999–2003 |
| Lernout & Hauspie | 1040570 | 166 | 1997–2002 |
| Luckin Coffee | 1767837 | 454 | 2017–2021 |
| Qwest Communications | 101830 | 600 | 1999–2003 |
| Rite Aid | 84129 | 491 | 1997–2003 |
| Sunbeam | 93859 | 315 | 1996–2002 |
| Xerox | 108772 | 757 | 1997–2002 |

These 9 companies have XBRL data on EDGAR and are **only missing because they aren't in the current `company_tickers.json`**.

#### Missing companies without XBRL data (pre-XBRL era, unrecoverable via current pipeline)

Bear Stearns, Countrywide, Delphi, Dewey & LeBoeuf, Gemstar, Hertz, MF Global, Nortel, Peregrine Financial, Satyam, Symbol Technologies, Tyco, Wirecard, WorldCom.

These companies either filed before XBRL was mandatory (pre-2009) or used non-US GAAP taxonomies.

---

## 3. Source Evaluation

### Option A: SEC EDGAR Full-Index (Recommended)

**Source:** `https://www.sec.gov/Archives/edgar/full-index/{year}/{qtr}/company.idx`

- **Coverage:** Every company that filed with SEC since 1993. Includes all delisted companies.
- **Licensing:** Public domain (SEC data).
- **Identifier mapping:** CIK → company name + filing date. No ticker mapping (ticker changes are not tracked).
- **XBRL constraint:** Only companies that filed in XBRL (mandatory from ~2009 for large accelerated filers, 2012 for all) will have data in the company-facts API.
- **Expected effect on sample size:** 
  - For 2012+ fiscal years: estimated +2,000–4,000 CIKs (companies delisted between 2012–2024)
  - For 2009–2011: modest gains (fewer XBRL filers existed)
- **Implementation:** Scan full-index for 10-K/10-K/A filings, extract unique CIKs per year, merge with current universe.

### Option B: CRSP Historical Securities Database

- **Coverage:** Comprehensive — every security listed on NYSE/AMEX/NASDAQ since 1925.
- **Licensing:** Requires institutional subscription ($$$). Not freely available.
- **Identifier mapping:** PERMNO/PERMCO → CUSIP → CIK mapping available.
- **Status:** Referenced in `docs/architecture/BACKLOG.md` item 17 as expensive. **Not available in this project.**

### Option C: SEC EDGAR Submissions Bulk Files

**Source:** `https://data.sec.gov/submissions/` (bulk download available)

- **Coverage:** All current and historical filers, with filing history.
- **Licensing:** Public domain.
- **Identifier mapping:** CIK → name, SIC, former names, filing history with form types.
- **Expected effect:** Same as Option A but provides SIC codes and entity metadata directly.
- **Implementation:** Download bulk submissions zip, extract CIKs that filed 10-K/10-K/A.

---

## 4. Recommended Approach

**Use SEC Full-Index (Option A) to build a historical CIK supplement, constrained to XBRL era (2009+).**

Rationale:
- Free, public domain, authoritative
- Covers all delisted companies that ever filed a 10-K
- Only useful where XBRL data exists (the pipeline's data source)
- Does not introduce label dependency (unlike using AAER CIKs as universe supplement)

### Architecture separations

| Concern | Implementation |
|---------|---------------|
| Historical universe construction | New: `pipeline/step0_historical_universe.py` — builds CIK list from full-index |
| Delisting date/return handling | Existing: `step6_clean.py` survivorship logic (unchanged) |
| Fraud labels | Existing: `enrich_fraud_labels.py` (unchanged, runs after universe is built) |

---

## 5. Limitations

1. **Pre-XBRL companies are unrecoverable** via the current pipeline. WorldCom (bankrupt 2002), Bear Stearns (2008), Tyco (restructured 2007) all filed before mandatory XBRL.
2. **The full-index approach adds CIKs but cannot add data** — the company-facts API must still return XBRL data for step2 to produce snapshots.
3. **Ticker mapping for delisted companies** is unreliable — the full-index provides company names, not historical tickers. This affects price matching in step3.
4. **The 14 companies without XBRL** represent the most severe fraud cases (Enron, WorldCom, Tyco, etc.) and cannot be added without an alternative data source.
5. **CIK reuse after restructuring** — EDGAR reuses CIKs for successor entities. Testing showed:
   - Xerox (CIK 108772): XBRL spans 2009–2018 — covers the original entity history
   - Rite Aid (CIK 84129): XBRL spans only 2020–2024 — successor data only, fraud-era (1997–2003) not available
   - Each recovered CIK must be validated for temporal coverage before being trusted for historical backtest periods.

---

## 6. Implementation Plan

Phase 1 (this session): Build `step0_historical_universe.py` that:
1. Downloads full-index files for 2009–2025
2. Extracts unique CIKs that filed 10-K or 10-K/A
3. Cross-references with company-facts API availability (HEAD request)
4. Produces `data/historical_ciks.parquet` with: cik, name, first_10k_year, last_10k_year, has_xbrl
5. Merges with existing `tickers.parquet` to produce expanded universe

Phase 2 (future): Add tests proving delisted companies appear in historical backtest years.
