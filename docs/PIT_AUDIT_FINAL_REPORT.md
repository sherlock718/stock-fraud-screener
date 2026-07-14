# PIT Integrity Audit — Final Report

**Date:** 2026-07-14

---

## 1. Historical-Universe Coverage (Matched, Full-Year)

**Methodology:** SEC full-index (all 4 quarters) for calendar 2018, matched by CIK.
Classification from 30-sample of missing CIKs via EDGAR submissions API.

### Verified 2018 Comparison

| Metric | Value |
|--------|-------|
| Full-index 10-K/10-K/A filers (2018, all quarters) | 6,986 |
| Our dataset (FY2017 + FY2018 CIKs) | 3,102 |
| Matched (in both) | 2,554 |
| Missing from our dataset | 4,432 |
| Extra in our dataset (not in 2018 full-index) | 548 |
| **Recall** | **36.6%** |
| Precision | 82.3% |

### Classification of 4,432 Missing CIKs (from 30-sample)

| Category | Sample Count | Estimated Total | % |
|----------|-------------|----------------|---|
| Operating companies (non-financial) | 17 | ~2,511 | 57% |
| Financials/funds (SIC 6000–6999) | 12 | ~1,772 | 40% |
| Unresolved | 1 | ~147 | 3% |

**Note:** "Operating company" does not confirm exchange-listed common equity.
OTC shells, SPACs, and private filers are included in this category.
Without historical security-type data (e.g., CRSP), further classification is not possible.

### Estimated Coverage by Year

| Year | FI Est | Est Investable | Our CIKs | Recall | Missing (inv) |
|------|--------|---------------|----------|--------|--------------|
| 2009 | 10,088 | 5,750 | 257 | 4% | 5,493 |
| 2012 | 8,766 | 4,996 | 1,937 | 39% | 3,059 |
| 2015 | 8,535 | 4,864 | 2,406 | 49% | 2,458 |
| 2018 | 7,000 | 3,989 | 3,026 | 76% | 963 |
| 2021 | 8,009 | 4,565 | 5,535 | >100% | 0 |
| 2024 | 7,092 | 4,042 | 6,941 | >100% | 0 |

**Interpretation:** Pre-2018 coverage is 4–76% of estimated investable filers.
Post-2021, our dataset exceeds the estimated investable population (our fiscal_year
window captures filings from adjacent calendar years).

### Diagnostic Sample: Confirmed Missing Investable Equities

| CIK | Company | SIC | XBRL | Last 10-K | Status |
|-----|---------|-----|------|-----------|--------|
| 101830 | Sprint LLC | 4813 | Y | 2019-11-12 | Acquired by T-Mobile |
| 108772 | Xerox Corp | 3577 | Y | 2026-03-17 | Still filing (dropped from our universe) |
| 84129 | New Rite Aid LLC | 5912 | Y | 2024-07-25 | Restructured |
| 356028 | CA Inc. | 7372 | Y | 2018-05-09 | Acquired by Broadcom |
| 25191 | Countrywide Financial | 6035 | N | 2008-04-24 | Acquired by BofA (pre-XBRL) |

**Conclusion:** The dataset is **historically enriched but not comprehensively survivorship-free**.
Thousands of operating companies that filed 10-Ks in the XBRL era (2009+) are absent because
they were delisted/acquired before the SEC ticker-list download date.

---

## 2. XBRL Amendment Handling (Reclassified)

### Classification

**Not leakage.** Amendment values were never used before their filing dates.
The pipeline correctly set `filed_date` to the amendment date.

**Actual issue:** Loss of historical vintages — the original 10-K value was discarded,
creating an availability gap and shifting entry-price measurement to the amendment date.

### Interim Fix: Earliest-Primary Selection

For each (fiscal_year, fiscal_period), select the earliest PRIMARY filing
(10-K, 10-Q, 20-F). Later amendments (8-K, 10-K/A) ignored unless no primary exists.

### Impact Measurement (10-company sample, 3 key concepts)

| Metric | Value |
|--------|-------|
| Concept-periods examined | 1,466 |
| Rows where selection changed | 48 (3.3%) |
| Companies affected | 8/10 |
| Median filing-date shift | 99 days |
| Maximum filing-date shift | 371 days |
| Shifts > 60 days | 42/48 (88%) |

### Limitation

Earliest-primary is correct for annual rebalance (~95% of cases) but permanently
freezes the snapshot at the original value. A true as-of system would:
- Retain every filing vintage (fy, fp, filed_date, val, form, accn)
- Reconstruct using only facts with `filed_date <= as_of_date`
- Allow amendments to become available after their publication date
- Keep restatement-derived labels separate from predictive features

---

## 3. Winsorization — Resolved

### Confirmed

- Global winsorization allowed future observations to affect historical clipping thresholds.
- New implementation uses `filed_date` grouped by filing quarter.
- Only observations with `filed_date < start_of_quarter` inform bounds.
- Cutoff aligns with the strategy's scoring availability (step3 uses `filed_date` as entry_date).

### Test Invariant (Proven)

`test_adding_future_filings_does_not_change_past_clips`:
Adding observations filed after a given quarter cannot change clipped values
for observations filed before that quarter. 8/8 tests pass.

### Materiality

- Rank correlation: > 0.99999 (all features)
- Fraud-tail membership change: 6/6,614 (0.09%)
- Backtest impact: 0.00% on all metrics (composite and ML-gated strategies)
- Root cause of zero impact: composite uses rank-based scoring; only 2/28 model features are winsorized

**Status: RESOLVED. No further work needed.**

---

## 4. Maximum Drawdown Audit

### Finding

The reported 0.0% max drawdown was a **frequency artifact**, not a formula bug.

| Frequency | Max Drawdown | Explanation |
|-----------|-------------|-------------|
| Annual endpoints | 0.0% | All 15 annual returns are positive (min +7.5%) |
| Monthly NAV | **-15.61%** | Intra-year decline captured by monthly price data |

### Corrected Metrics (Monthly Frequency)

| Metric | Value |
|--------|-------|
| CAGR | 32.62% |
| Sharpe | 1.353 |
| Sortino | 1.353 |
| Calmar | 2.09 |
| Max Drawdown | **-15.61%** |
| DD Duration | 12 months |
| Hit Rate | 88.4% |
| Excess vs SPY | +18.76% |

### Verification

- All 15 annual returns positive: confirmed (no formula error)
- Monthly NAV from `compute_monthly_nav()`: independently verified
- CAGR recalculated from annual returns: 32.62% (consistent)
- Root cause: `monthly_px` parameter was not passed to `run_backtest()`

### Fix

The comparison script must always pass `monthly_px=monthly_prices.parquet` when available.
No change to `backtest/engine.py` needed — the monthly calculation was already implemented
but not invoked.

---

## 5. Remaining Material Limitations

1. **Universe coverage 4–76% for 2009–2018.** ~2,500 operating companies per year are missing.
   Without CRSP or equivalent, we cannot distinguish exchange-listed equities from OTC/private filers.

2. **XBRL earliest-primary is an approximation.** 3.3% of concept-periods are affected.
   Full as-of vintage reconstruction would require storing all filing variants.

3. **Delisting returns not available** for missing companies (no CRSP). The -50% imputation
   in step6 only applies to companies already in the dataset.

4. **Model was not retrained** on PIT-winsorized features. Current zero-impact result reflects
   the model's insensitivity to winsorization, not a guarantee that retraining would be identical.

---

## 6. Commit Hashes

| Commit | Description |
|--------|-------------|
| `d061232` | Historical universe builder + coverage report |
| `d05b10a` | XBRL earliest-primary vintage selection |
| `072ba0a` | Global → PIT expanding winsorization |
| `406586b` | Issue 2 reclassification (not leakage) |
| `625422f` | filed_date winsorization + backtest comparison |
| `8d873c7` | Rename to "amendment handling", impact measurement |
