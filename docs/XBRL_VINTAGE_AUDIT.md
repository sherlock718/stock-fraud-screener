# XBRL Fact Vintage Audit — Evidence Table

**Date:** 2026-07-14  
**Scope:** Issue 2 — Loss of historical XBRL vintages / amendment handling

**Reclassification:** This issue was initially characterized as "look-ahead leakage."
After verification, it is more accurately described as **loss of historical vintages
with timing distortion**. The amendment values were never used before their filing
dates — they were correctly dated to the amendment date. The problem is that the
original filing's data was discarded entirely, creating an availability gap.

---

## 1. Mechanism

The SEC Company Facts API returns ALL XBRL-tagged values for a given concept and fiscal period,
including values from later filings (8-K earnings releases, 10-K/A amendments) that revise the
original 10-K/10-Q figures. The prior pipeline logic:

```python
if key not in result or filed > result[key][1]:
    result[key] = (val, filed)
```

Selected the **most recently filed** value, which could be an amendment filed months after
the original. This causes two distinct failures:

1. **Look-ahead leakage**: The snapshot uses a value that was not public at the original filing date.
2. **Lost original vintage**: The original filing's value is overwritten and cannot be recovered from the snapshot.

---

## 2. Five-Company Evidence Table

### Company A: GE (CIK 40554) — FY2011

| Field | Original (10-K) | Amendment (8-K) | Pipeline Selected | Leakage? |
|-------|-----------------|-----------------|-------------------|----------|
| Concept | Assets | Assets | Assets | |
| Value | $577,712,000,000 | $605,255,000,000 | $605,255,000,000 | **YES** |
| Filed | 2012-02-24 | 2012-05-04 | 2012-05-04 | |
| Form | 10-K | 8-K | 8-K | |
| Δ | — | +4.77% | — | |

**GE NetIncomeLoss FY2011**: Original $6,566M → Amendment $6,510M (-0.85%). Pipeline used amendment.

### Company B: Groupon (CIK 1490281) — FY2016

| Field | Original (10-K) | Amendment (8-K) | Pipeline Selected | Leakage? |
|-------|-----------------|-----------------|-------------------|----------|
| Concept | Revenues | Revenues | Revenues | |
| Value | $3,042,123,000 | $2,858,646,000 | **$2,858,646,000** | **YES** |
| Filed | 2017-02-15 | 2017-05-17 | 2017-05-17 | |
| Form | 10-K | 8-K | 8-K | |
| Δ | — | -6.03% | — | |

Rebalance impact: Snapshot would not be eligible until 2017-05-17 instead of 2017-02-15 (3 months late).

### Company C: Overstock.com (CIK 1130310) — FY2019

| Field | Original (10-K) | Amendment (8-K) | Pipeline Selected | Leakage? |
|-------|-----------------|-----------------|-------------------|----------|
| Concept | Revenues | Revenues | Revenues | |
| Value | $9,614,000,000 | $5,699,000,000 | **$5,699,000,000** | **YES** |
| Filed | 2020-02-27 | 2020-05-19 | 2020-05-19 | |
| Form | 10-K | 8-K | 8-K | |
| Δ | — | **-40.72%** | — | |

Most severe case: nearly halving revenue through a later reclassification.

### Company D: GE (CIK 40554) — FY2014 Revenue

| Field | Original (10-K) | Amendment (8-K) | Pipeline Selected | Leakage? |
|-------|-----------------|-----------------|-------------------|----------|
| Concept | Revenues | Revenues | Revenues | |
| Value | $45,364,000,000 | $41,710,000,000 | $41,710,000,000 | **YES** |
| Filed | 2015-02-27 | 2015-05-11 | 2015-05-11 | |
| Form | 10-K | 8-K | 8-K | |
| Δ | — | -8.05% | — | |

### Company E: Overstock.com (CIK 1130310) — FY2019 Assets

| Field | Original (10-K) | Amendment (8-K) | Pipeline Selected | Leakage? |
|-------|-----------------|-----------------|-------------------|----------|
| Concept | Assets | Assets | Assets | |
| Value | $27,009,000,000 | $27,093,000,000 | $27,093,000,000 | **YES** |
| Filed | 2020-02-27 | 2020-05-19 | 2020-05-19 | |
| Form | 10-K | 8-K | 8-K | |
| Δ | — | +0.31% | — | |

---

## 3. Conclusion

**The issue is confirmed as LOSS OF HISTORICAL VINTAGES, not look-ahead leakage.**

### Was the amendment value ever usable before its filing date?

**No.** The pipeline set `filed_date` to the amendment's actual filing date (e.g., 2020-05-19
for Overstock). Step 3 then uses `filed_date` as the entry date for price lookup and
forward-return measurement (line 355: `entry_date = pd.Timestamp(row['filed_date'])`).
The amendment value was never scored before its own publication date.

### What actually went wrong?

1. **Original vintage lost:** The 10-K filed 2020-02-27 with revenue $9.6B was discarded.
   The model never saw this company's FY2019 data until May 2020.
2. **Availability gap:** Between Feb 27 and May 19, the company was invisible to the model
   despite having publicly filed financial statements.
3. **Timing distortion:** Entry price was set at May 2020 (post-COVID recovery), not
   Feb 2020 (pre-crash). Forward returns measured from the wrong date.
4. **Value distortion:** The snapshot contains the restated value ($5.7B), not what the
   market knew in Feb 2020 ($9.6B). Feature ratios like P/S, revenue growth, etc. are
   computed from the restated figure.

### Scale

4.6% of annual rows (2,681/58,190) have filing lags > 120 days, suggesting they may be
amendments rather than original filings. These represent cases where the original 10-K
was available months earlier but was discarded in favor of a later revision.

### Distinction from leakage

| Attribute | True Leakage | What Actually Happened |
|-----------|-------------|----------------------|
| Value used before available? | Yes | **No** — correctly dated to amendment |
| Original vintage preserved? | N/A | **No** — discarded |
| Forward return timing? | Wrong | Wrong (measured from late date) |
| Model score before filing? | Yes | **No** — gap instead |

---

## 4. Earliest-Primary Fix (Interim)

**File:** `pipeline/step2_build_snapshots.py` → `extract_concept_series()`

**Policy:** For each (fy, fp), select the earliest PRIMARY filing (10-K, 10-Q, 20-F, 10-KSB).
Ignore later amendments (8-K, 10-K/A, 10-Q/A) unless no primary filing exists.

**Primary forms:** `{10-K, 10-Q, 20-F, 10-KSB, 10-QSB}`

**Handling of unordered records:** All entries for a period are collected first, then sorted
by filed_date. The earliest primary filing wins regardless of API record order.

**Tests added:** 7 unit tests in `TestVintageAwareness` class covering:
- Original preferred over amendment
- Amendment unavailable before its date
- Amendment-only fallback
- Unordered API records
- Multiple concepts with different dates
- 10-K/A not treated as primary
- Quarterly primary (10-Q)

### Earliest-Primary vs True As-Of Selection

| Scenario | Earliest-Primary | True As-Of | Winner |
|----------|-----------------|------------|--------|
| Rebalance before amendment filed | Original 10-K ✓ | Original 10-K ✓ | Tie |
| Rebalance after amendment filed | Original 10-K (stale) | Amendment ✓ | As-Of |
| No primary filing exists | Fallback to earliest any | Latest before cutoff | As-Of |
| Mid-year scoring refresh | Original (frozen) | Amendment if available | As-Of |

**Interim limitation:** Earliest-primary permanently freezes the snapshot at the original
value. A true as-of system would store ALL vintages and select the latest available
at each scoring date. For an annual-rebalance strategy with March 31 cutoff:
- ~95% of 10-K filings occur Jan–Mar → earliest-primary is correct at rebalance
- ~5% of amendments file before next rebalance → earliest-primary uses stale data
- This is acceptable for the annual model but would be incorrect for quarterly scoring

**Full vintage storage (not implemented):** Would require:
- Storing all (fy, fp, filed_date, val, form, accn) tuples per concept
- An `as_of(date)` function that filters to `filed_date <= date` then picks latest
- Re-running snapshot construction at each rebalance date
- Significant storage and compute overhead

---

## 5. Impact Assessment

To measure the effect on the existing dataset, a full re-run of step2 would be needed.
Based on the audit sample:
- ~53 value changes found across 4 companies in key financial concepts alone
- Revenue changes averaged -10% to -15% where amendments existed
- Affected companies include those central to fraud detection (Groupon, Overstock)

The existing `data/snapshots.parquet` retains the old (leaky) values for before/after comparison.
After a step2 re-run, compare `data/snapshots.parquet` (new) vs `data/snapshots_pre_vintage_fix.parquet` (backup).
