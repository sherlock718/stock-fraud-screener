# Fraud Case Study Library

Real-world fraud cases with the quantitative signals that were detectable in annual filings **before** the fraud was publicly revealed.

---

## Overview

The Case Study Library documents seven major accounting frauds across US, German, and UK markets. For each case we show:

- The fraud type and mechanism
- Which quantitative signals were elevated in the 1–3 years before revelation
- Key Beneish, Altman, and accruals metrics
- Lessons applicable to screening today

Where the company exists in the dataset, live model scores are plotted over time.

---

## Case 1 — Wirecard AG (WDI · DE · 2020)

**Fraud type:** Accounting Manipulation — Missing Cash

Wirecard reported €1.9B held in trust accounts in the Philippines that did not exist. Auditor EY signed off for years. The collapse in June 2020 wiped €12B+ in market cap within days. It remains the largest post-war German corporate fraud.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Days Sales Outstanding (DSO) | DSO grew from 58 days (2015) to 97 days (2019) — receivables growing far faster than revenue |
| Receivables Accrual Index (DSRI) | DSRI > 1.5 by 2018 — strong Beneish manipulation flag |
| Asset Quality Index (AQI) | AQI > 1.3 — rapid growth in non-current assets relative to total assets |
| Cash Flow vs Net Income | Operating cash flow persistently below reported net income — classic accrual divergence |
| Auditor | EY issued clean opinions for nine years while KPMG special review (2020) flagged the gap |

### Key Lesson

A company generating revenue almost entirely through third-party acquirers and storing cash in trust accounts in opaque jurisdictions should trigger maximum scrutiny of every cash-flow statement. Beneish DSRI and OCF/NI divergence were both elevated 2+ years before collapse.

---

## Case 2 — Luckin Coffee Inc. (LK · US · 2020)

**Fraud type:** Revenue Inflation — Fabricated Transactions

Luckin fabricated ~RMB 2.2B (~$310M) in sales in 2019 by creating phantom customer transactions through affiliated entities. The fraud was uncovered by a Muddy Waters short report in January 2020 and confirmed by internal audit in April 2020. Shares fell 75% in one session; Nasdaq delisted the stock in June 2020.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Gross Margin Index (GMI) | GMI > 1.6 in FY2019 — gross margins declining despite reported revenue growth |
| Sales Growth Index (SGI) | SGI of 4.6 in FY2019 — revenue growth far above sector peers |
| Total Accruals to Total Assets | Accruals ratio among the highest in the restaurant sector peer group |
| Revenue per store | Reported per-store revenue implausibly high versus foot-traffic data |
| Short interest | Short float exceeded 25% before the fraud announcement |

### Key Lesson

Hyper-growth combined with opaque related-party distribution channels is a classic pump scheme. The SGI of 4.6× peers and rapidly worsening GMI were measurable signals well before the short seller report.

---

## Case 3 — Enron Corporation (ENE · US · 2001)

**Fraud type:** SPV Abuse — Off-Balance Sheet Debt

Enron used hundreds of Special Purpose Entities (SPEs) to hide debt and inflate profits. Executives used mark-to-market accounting on long-term contracts to book future revenue immediately. The $63B bankruptcy in December 2001 was the largest in US history at the time.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Return on Assets trend | ROA declined from 6.5% (1996) to 1.5% (2000) while EPS grew — earnings quality deteriorating |
| Leverage vs peers | Debt-to-equity was 50% higher than pipeline peers but hidden via SPE off-balance-sheet treatment |
| Beneish DSRI | Receivables grew 2× revenue in 2000 — classic DSRI elevation |
| Total Accruals / Assets (TATA) | TATA > 0.07 in FY2000 and FY2001 — top decile for accrual manipulation |
| Altman Z-Score | Z-score fell below 1.81 distress threshold by Q2 2001 |

### Key Lesson

When earnings grow but ROA falls, ask where the return is going. Enron hid debt in SPEs that don't appear on consolidated balance sheets — but the OCF/NI divergence was visible every year from 1998 onwards.

---

## Case 4 — WorldCom Inc. (WCOM · US · 2002)

**Fraud type:** Expense Capitalization — $11B Fraud

WorldCom capitalized $3.8B in ordinary line costs as capital expenditures in 2001–2002, inflating earnings by the same amount. Total fraud eventually reached $11B. The July 2002 bankruptcy at $107B in assets displaced Enron as the largest US bankruptcy.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| CapEx / Revenue ratio | CapEx jumped from 12% to 19% of revenue in 2001 — anomalous in a contracting telecom market |
| Asset Quality Index (AQI) | AQI > 1.5 — non-current assets growing as expense lines were capitalised |
| Cash Flow from Operations | OCF fell sharply while net income was stable — the fundamental red flag |
| Gross Margin Index (GMI) | GMI rose sharply in a declining revenue environment — impossible without cost manipulation |
| Altman Z-Score | Z-score dropped below 1.0 in the 12 months before bankruptcy |

### Key Lesson

Capitalizing operating costs is the oldest fraud in the book. A sudden, unexplained jump in CapEx/Revenue during a revenue decline is the primary signal. AQI > 1.5 flagged it 18 months before the restatement.

---

## Case 5 — NMC Health plc (NMC · US · 2020)

**Fraud type:** Hidden Debt — $4B Undisclosed Liabilities

UAE-listed (London Stock Exchange) hospital group NMC Health was found to have hidden $4B+ in debt not appearing on official balance sheets. Muddy Waters published a short report in December 2019. The company collapsed into administration in April 2020.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Debt-to-EBITDA | Disclosed leverage appeared moderate at 3.5× but actual leverage was >8× |
| Accounts Payable Days | DPO expanded from 45 to 95 days — supplier financing masking cash pressure |
| Free Cash Flow Yield | FCF yield was 50% below peer median despite similar margins |
| Beneish DSRI | Elevated receivables relative to revenue growth in FY2018 |
| Governance flags | Founder-controlled board, complex cross-ownership, supplier-customer overlaps |

### Key Lesson

Companies with complex cross-ownership, high supplier concentration, and expanding accounts payable should be treated as high governance risk regardless of clean audit opinions. FCF/EBITDA ratio below 0.5 is a clear red flag.

---

## Case 6 — Steinhoff International (SNHJ · DE · 2017)

**Fraud type:** Accounting Irregularities — Multi-Year Revenue Inflation

Steinhoff (JSE + Frankfurt) disclosed "accounting irregularities" in December 2017 which turned out to be €6.5B+ in fictitious profit booked over multiple years. The share price lost 95% of its value in two days.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Gross Margin Index (GMI) | Gross margins consistently 200–300 bps above sector peers — implausibly high for a discount retailer |
| Sales Growth Index (SGI) | Revenue grew 30%+ YoY while same-store sales were flat — acquisition-masked organic inflation |
| Goodwill / Total Assets | Goodwill exceeded 40% of total assets after a series of opaque acquisitions |
| EBITDA vs OCF divergence | EBITDA-to-OCF conversion ratio below 0.6 for three consecutive years |
| Beneish M-Score | M-score above −2.22 threshold in FY2016 and FY2017 |

### Key Lesson

Goodwill-heavy acquisition strategies combined with implausibly stable margins are fertile ground for fraud. The GMI and SGI signals were anomalous relative to discount retail peers 3 years before the collapse.

---

## Case 7 — Valeant Pharmaceuticals (VRX · US · 2016)

**Fraud type:** Channel Stuffing + Price Gouging

Valeant used specialty pharmacy Philidor to stuff drug distribution channels and inflate revenue, while simultaneously raising drug prices 500–1000%. The stock fell 90% from peak to trough. SEC investigated channel-stuffing practices and revenue recognition with its specialty pharmacy network.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Receivables Growth | Accounts receivable grew 3× faster than revenue in 2015 — classic channel stuffing signal |
| DSRI (Beneish) | DSRI of 1.87 in FY2015 — well above the 1.03 manipulation threshold |
| Debt Load | Net debt exceeded $30B vs EBITDA of $5B — leverage unsustainable at any normal interest rate |
| Organic Revenue Growth | Strip out acquisitions and price hikes: organic volume growth was negative |
| Altman Z-Score | Z-score below 1.81 by early 2016 — distress zone |

### Key Lesson

Channel stuffing shows up first in the DSRI. When receivables grow faster than revenue in a pharmaceutical company, and the distribution network includes captive specialty pharmacies, the revenue number is unreliable.

---

## Cross-Case Signal Summary

| Case | DSRI | GMI | AQI | SGI | OCF/NI | Z-Score |
|------|:----:|:---:|:---:|:---:|:------:|:-------:|
| Wirecard | ⚠️ | | ⚠️ | | ⚠️ | |
| Luckin Coffee | | ⚠️ | | ⚠️ | ⚠️ | |
| Enron | ⚠️ | | | | ⚠️ | ⚠️ |
| WorldCom | | ⚠️ | ⚠️ | | ⚠️ | ⚠️ |
| NMC Health | ⚠️ | | | | ⚠️ | |
| Steinhoff | | ⚠️ | | ⚠️ | ⚠️ | |
| Valeant | ⚠️ | | | | | ⚠️ |

!!! tip "Pattern"
    No single signal catches every fraud, but elevated DSRI and OCF/NI divergence appear in 5 of the 7 cases. The Beneish M-Score combining all 8 components is the single highest-recall baseline screener across these cases.

---

## Using the Case Study Tab in the App

The **📚 Case Studies** tab in the Streamlit app lets you:

1. Select any of the 7 cases from the dropdown
2. Read the fraud summary and pre-fraud warning signals
3. View live Beneish M-Score, Altman Z-Score, and Fraud Score from the dataset if the ticker is available
4. See a score timeline chart with the fraud revelation year marked as a vertical line
5. Compare scores across all 7 cases in the overview table at the bottom
