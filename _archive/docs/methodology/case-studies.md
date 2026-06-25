# Fraud Case Study Library

Real-world fraud cases with the quantitative signals that were detectable in annual filings **before** the fraud was publicly revealed.

---

## Overview

The Case Study Library documents fifteen major accounting frauds across US, German, Italian, UK, and Japanese markets. For each case we show:

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

## Case 8 — Satyam Computer Services (SAY · US · 2009)

**Fraud type:** Balance Sheet Fabrication — $1.5B Cash Hole

Satyam chairman Ramalinga Raju confessed in January 2009 to fabricating ₹50.4B (~$1.47B) in cash and bank balances that did not exist. Fake fixed deposits, inflated receivables, and understated liabilities had been on the books for years. The Indian IT outsourcing giant collapsed overnight in what became known as "India's Enron." PricewaterhouseCoopers India signed off on the accounts for eight years.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Cash vs Operating Cash Flow | Reported cash balance was massive yet OCF was inconsistently low — cash on the balance sheet was fictitious |
| Receivables Growth (DSRI) | DSRI rose above 1.4 in FY2007–2008 — receivables growing faster than revenue |
| Return on Assets decline | ROA fell from 18% (2004) to 8% (2008) while reported margins stayed stable |
| Accruals ratio (TATA) | Total accruals to total assets climbed into the top decile of Indian IT peers |
| Auditor Independence | PwC India had a 10-year tenure; local affiliate fees were disproportionately small |

### Key Lesson

When a cash-rich company consistently generates weak operating cash flow, the cash is almost certainly fictional. DSRI elevation combined with ROA deterioration is the earliest reliable signal in balance-sheet fabrication frauds.

---

## Case 9 — Parmalat SpA (PARME · IT · 2003)

**Fraud type:** Phantom Cash — €14B Black Hole

Parmalat, the Italian dairy giant, collapsed in December 2003 after a €14B accounting hole was discovered. The company claimed €3.9B in a Bank of America account in the Cayman Islands — a document that proved to be a forgery. The fraud had been running for over a decade, funded by ever-increasing debt hidden in offshore subsidiaries. Grant Thornton and Deloitte both audited parts of the empire without detecting it.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Debt-to-Equity explosion | Reported debt grew from €2B (1997) to €14B (2003) — a 7× increase hidden across 200+ subsidiaries |
| Cash vs Debt paradox | Claimed to hold €4B+ cash while simultaneously borrowing billions — logically impossible |
| Asset Quality Index (AQI) | AQI > 1.4 — non-current assets at offshore entities ballooned without operational explanation |
| Interest Coverage ratio | Interest coverage fell below 1.0 by 2002 — debt was unpayable from operating income alone |
| Altman Z-Score | Z-score entered distress zone (< 1.81) by FY2001 — two years before collapse |

### Key Lesson

Claimed cash balances of €4B+ alongside €14B in debt should have been impossible — the interest payments alone exceeded reported operating income. Z-score distress and AQI elevation were detectable years before the forged bank document surfaced.

---

## Case 10 — Nikola Corporation (NKLA · US · 2020)

**Fraud type:** Technology Fabrication — Fake Demo / SEC Fraud

Nikola, an electric truck startup, was accused by Hindenburg Research in September 2020 of being "an intricate fraud" — most prominently staging a promotional video showing a truck driving under its own power when it had actually been pushed down a hill. Founder Trevor Milton resigned and was convicted of fraud in 2022. SEC and DOJ both brought charges. The stock fell over 80% from peak.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Revenue vs Valuation | Market cap exceeded $30B with $0 in actual revenue — valuation entirely based on unverifiable claims |
| Negative OCF with large stock issuance | Operating cash flow deeply negative while share-based compensation was the primary "asset" |
| Sales Growth Index (SGI) | SGI from forecasted to actual revenue: effectively zero — no delivery milestone met |
| Insider selling | Founder sold $70M+ in shares before fraud revelation |
| Governance flags | SPAC structure, single founder control, no audited revenue history |

### Key Lesson

SPAC-structure companies with no revenue history, single-founder governance, and stock-based dilution as the primary cash mechanism warrant maximum skepticism on every technology claim. Governance and dilution sub-scores catch this class of fraud before any accounting manipulation is visible.

---

## Case 11 — Theranos Inc. (Private · US · 2015)

**Fraud type:** Technology Fabrication — $9B Blood-Testing Fraud

Theranos, a Silicon Valley blood-testing startup, claimed its Edison device could run hundreds of diagnostic tests from a single finger-prick drop of blood at a fraction of the laboratory cost. In reality, the device could reliably perform only a handful of tests; the rest used conventional Siemens analysers. Founder Elizabeth Holmes was convicted of investor fraud in January 2022. The company raised $900M+ from investors and partners before the Wall Street Journal investigation by John Carreyrou in October 2015.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Revenue vs Technology Claims | Claimed test cost of $15 vs industry standard $100+ — mathematically impossible margin without undisclosed automation |
| Cash Burn vs Revenue | Raised $900M+ in equity but disclosed zero commercial revenue during the investment period |
| Due Diligence Restriction | Investors were contractually barred from visiting labs or verifying patient outcomes — extreme governance red flag |
| Governance Concentration | Founder held 99%+ voting control via super-voting shares; board included no medical or lab-science experts |
| Regulatory Non-Compliance | CLIA lab certification was obtained under a subsidiary brand; the CMS inspection in 2015 found immediate jeopardy conditions |

### Key Lesson

When a private company refuses audited financials, bars investors from standard diligence, and concentrates total voting control with the founder, the valuation is entirely faith-based. Governance and dilution sub-scores are the primary detection layer for this class of fraud — accounting signals require public filings to compute.

---

## Case 12 — Adelphia Communications (ADELQ · US · 2002)

**Fraud type:** Related-Party Looting — $3.1B Off-Book Family Loans

Adelphia Communications, founded by the Rigas family, concealed $3.1B in co-borrowing arrangements in which the company guaranteed debt taken out by Rigas family entities for personal use (purchase of cable systems, real estate, and a golf course). The off-book liabilities were disclosed in a footnote that most analysts missed. The company filed for Chapter 11 in June 2002; founder John Rigas was convicted of bank fraud in 2004.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Off-Balance-Sheet Liabilities | $3.1B in co-borrowing arrangements listed as a single opaque footnote — not consolidated on balance sheet |
| Related-Party Transactions | Founding family entities received company guarantees; Rigas entities purchased cable systems at above-market prices |
| Debt / EBITDA | Disclosed leverage of ~8× was already high; actual leverage post-restatement exceeded 12× |
| OCF vs Reported Earnings | Free cash flow was deeply negative while reported net income was positive — classic quality divergence |
| Altman Z-Score | Z-score fell below 1.81 in FY2001, two quarters before the June 2002 bankruptcy filing |

### Key Lesson

Related-party looting is the hardest fraud to detect from public filings alone — the key is the footnote, not the headline numbers. Z-score distress and OCF/NI divergence still give early warning even when the balance sheet is deliberately obscured.

---

## Case 13 — HealthSouth Corporation (HLSH · US · 2003)

**Fraud type:** Earnings Fabrication — $2.7B Inflated Net Income

HealthSouth CEO Richard Scrushy directed management to set quarterly earnings targets that exceeded what the business could generate, then instructed subordinates to fabricate journal entries to bridge the gap. Over fifteen quarters from 1996 to 2002 the company inflated pre-tax income by $2.7B. Sixteen members of management pleaded guilty; Scrushy himself was acquitted on the fraud counts (later convicted on unrelated bribery charges) in a controversial jury outcome.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Earnings vs OCF Gap | EPS grew steadily while OCF consistently underperformed reported net income — visible in every annual filing from 1997 |
| Asset Quality Index (AQI) | AQI > 1.6 — property and equipment were systematically overstated to absorb fabricated journal entries |
| Sloan Accruals | Accruals ratio in the top decile for healthcare services peers across the entire fraud period |
| Gross Margin Stability | Margins were suspiciously stable despite large swings in Medicare reimbursement rates affecting all competitors |
| Auditor Tenure Risk | Ernst & Young engagement ran for 17 years — long-tenure auditors are associated with reduced scepticism |

### Key Lesson

"Earnings first, entries second" manipulation produces a consistent pattern: rising EPS with rising accruals and flat or declining OCF. AQI and Sloan accruals both flagged HealthSouth 3+ years before the restatement.

---

## Case 14 — Autonomy Corporation (AUTNF · US/UK · 2012)

**Fraud type:** Revenue Inflation — $5B HP Acquisition Writedown

Hewlett-Packard acquired UK software company Autonomy for $11.1B in October 2011. One year later HP wrote down $8.8B, attributing $5B to "serious accounting improprieties" at Autonomy. HP alleged that low-margin hardware sales were classified as software revenue, that round-trip transactions were used to inflate recognized revenue, and that sales to value-added resellers were booked before any genuine end-customer demand existed. Autonomy's founder Mike Lynch was extradited to the US and acquitted in 2024 after a high-profile trial.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Revenue Misclassification | Low-margin hardware sales were recorded as high-margin software revenue — blending two incomparable revenue streams |
| Gross Margin Index (GMI) | Reported 80%+ gross margins were implausible for a business with known hardware and services activity |
| Round-Trip Transactions | Value-added resellers were allegedly paid to purchase Autonomy software, which was then returned or credited |
| DSRI (Beneish) | DSRI above 1.3 in FY2010 and FY2011 — receivables growing faster than revenue in a subscription software business |
| Revenue Growth vs Cash Collection | Revenue grew 60%+ YoY in 2010–2011 while DSO expanded from ~60 to ~110 days — cash collection did not match recognition |

### Key Lesson

Gross margins above 80% in a business with known hardware activity are a contradiction that demands explanation. DSRI elevation in a supposed subscription software company (where receivables should be short-cycle) is a direct indicator of premature or fictitious revenue recognition.

---

## Case 15 — Toshiba Corporation (TOSBF · JP · 2015)

**Fraud type:** Earnings Fabrication — ¥152B Overstated Profit (7 Years)

Toshiba disclosed in April 2015 that it had overstated cumulative pre-tax profits by ¥151.8B (~$1.2B) over seven fiscal years from FY2008 to FY2014 across its infrastructure, consumer electronics, and semiconductor divisions. The primary mechanism was manipulation of percentage-of-completion (POC) estimates on long-term infrastructure contracts. An independent investigation found that top management had created a culture of "challenges" — internal profit targets that exceeded achievable capacity — leading divisions to pull forward revenue recognition to meet targets.

### Detectable Signals

| Signal | Detail |
|--------|--------|
| Percentage-of-Completion Manipulation | Long-term infrastructure project revenue pulled forward via systematically overstated completion percentages — visible as receivables growing faster than billings |
| Operating Income vs OCF Divergence | Operating income exceeded OCF by a cumulative ¥152B over 7 years — the exact size of the fraud |
| Gross Margin Index (GMI) | Semiconductor and infrastructure division margins were 200–400 bps above peer benchmarks in years when the sector was under pricing pressure |
| Management Culture Indicators | Internal investigation cited "challenges" set above achievable capacity; CFO-level pressure documented in emails |
| Beneish M-Score Trajectory | M-score deteriorated from approximately −2.5 to −1.9 between FY2009 and FY2014, crossing the −1.78 warning threshold in FY2013 |

### Key Lesson

POC-based revenue manipulation is gradual — ¥5–10B/year for the first few years, then accelerating as prior-year overstatements must be sustained. The OCF/operating income divergence accumulates to the exact size of the fraud, making it one of the most reliable detection signals for this manipulation type.

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
| Satyam | ⚠️ | | | | ⚠️ | |
| Parmalat | | | ⚠️ | | ⚠️ | ⚠️ |
| Nikola | | | | ⚠️ | ⚠️ | |
| Theranos | | | | | ⚠️ | |
| Adelphia | | | | | ⚠️ | ⚠️ |
| HealthSouth | | ⚠️ | ⚠️ | | ⚠️ | |
| Autonomy | ⚠️ | ⚠️ | | | ⚠️ | |
| Toshiba | | ⚠️ | | | ⚠️ | |

!!! tip "Pattern"
    No single signal catches every fraud, but elevated DSRI and OCF/NI divergence appear in 10 of the 15 cases. The Beneish M-Score combining all 8 components is the single highest-recall baseline screener across these cases. For SPAC-era and private-company frauds (Nikola, Theranos), governance and dilution sub-scores are the primary detection layer where no public accounting data exists.

---

## Using the Case Study Tab in the App

The **📚 Case Studies** tab in the Streamlit app lets you:

1. Select any of the 15 cases from the dropdown
2. Read the fraud summary and pre-fraud warning signals
3. View live Beneish M-Score, Altman Z-Score, and Fraud Score from the dataset if the ticker is available
4. See a score timeline chart with the fraud revelation year marked as a vertical line
5. Compare scores across all 15 cases in the overview table at the bottom
