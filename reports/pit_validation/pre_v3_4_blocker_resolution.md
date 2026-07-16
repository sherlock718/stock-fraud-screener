# Pre-V3.4 Blocker Resolution

Date: 2026-07-17

Status: **Accepted by user — 2026-07-17**

This is a policy and bounded-collection approval only. It does not execute
V3.4, authorize performance calculation, or alter any V3.1–V3.3 artifact.

## Confirmed evidence

The accepted V3.3 manifest SHA-256 revalidated exactly as
`8bf4cf867e883764d4e25c0d61a755c02443196ceac76be2843f7ff3ebf7bea3`.
All 1,448 referenced records matched their recorded sizes and hashes. The
embedded V3.1 and V3.2 manifests matched their accepted originals, and the
production configuration was byte-identical across V3.1–V3.3.

The accepted population contains 90 vintage positions across 79 unique
issuers. Its frozen benchmark sleeves are 4 IWC, 24 IWM, and 62 MDY. No
accepted position maps to SPY, so SPY is outside this collection scope.

## Approved transaction-cost policy

`V3_4_TRANSACTION_COST_POLICY` is resolved as follows:

- Rate: 25 basis points per side; 50 basis points for a complete round trip.
- Components per side: 15 bps half-spread, 8 bps market impact and execution
  slippage, and 2 bps commissions/exchange/regulatory charges.
- Cost: `0.0025 * absolute actual traded notional`, paid from vintage cash.
- Turnover: `sum(abs(trade_notional)) / pre-cost vintage NAV`; there is no
  one-half multiplier and no capital from another overlapping vintage in the
  denominator.
- Entry: charge purchases at the accepted common entry close immediately after
  execution.
- Rebalance: a later vintage's entry does not sell, resize, or charge an earlier
  vintage. An unchanged vintage has zero annual-rebalance cost.
- Exit: charge sales at the accepted common exit close immediately after
  execution proceeds are recognized.
- Events: mandatory splits, conversions, distributions, and cash consideration
  are corporate actions, not trades. Charge only an evidence-backed actual
  market purchase or sale, at its sourced execution timestamp.

No tier, 30/60 bps default, annual-label subtraction, or invented event trade
is permitted.

## Approved risk-free policy

`V3_4_RISK_FREE_POLICY` is resolved as follows:

- Instrument: one-month US Treasury constant-maturity security.
- Economic source: Federal Reserve Board H.15 series `DGS1MO`.
- Archive: Federal Reserve-sourced ALFRED/FRED observations.
- Immutable vintage: `2026-07-17`, pinned with
  `realtime_start=2026-07-17` and `realtime_end=2026-07-17`.
- Frequency: daily on Federal Reserve business days.
- Availability: no earlier than 16:15 America/New_York on the observation date.
- Interval observation: use only the immediately preceding Federal Reserve
  business day's observation for an interval beginning at valuation timestamp
  `t0`; that observation must be released by `t0`.
- Conversion, for published percent-per-annum yield `y` and actual interval
  calendar days `d`:

  `risk_free_return = (1 + y / 200) ** (2 * d / 365.2425) - 1`

- Assign the return to the interval-ending ledger date.
- A missing, nonnumeric, late, or incomplete designated observation makes
  Sharpe and Sortino unavailable. Do not search farther back, fill, carry,
  interpolate, average, revise, or substitute it.
- Acquisition cash earns 0%; this series does not accrue portfolio cash.

## Approved external collection

`V3_4_EXTERNAL_DATA_APPROVAL` authorizes only the collection below.

### Accepted positions

Each entry is `ticker/CIK -> benchmark`.

- 2015: FSLR/0001274494->MDY, FICO/0000814547->MDY,
  ALB/0000915913->MDY, AGCO/0000880266->MDY, RHP/0001040829->MDY,
  URI/0001067701->MDY, AAT/0001500217->IWM, SFL/0001289877->IWM,
  AN/0000350698->MDY, ELS/0000895417->MDY, LFUS/0000889331->MDY,
  ASC/0001577437->IWM, UDR/0000074208->MDY, CDP/0000860546->MDY,
  ON/0001097864->MDY.
- 2016: BBSI/0000902791->IWC, BLMN/0001546417->MDY,
  PCRX/0001396814->IWM, HUN/0001307954->MDY, REX/0000744187->IWC,
  CNC/0001071739->MDY, ZD/0001084048->MDY, RIG/0001451505->MDY,
  CCK/0001219601->MDY, SUPN/0001356576->IWM, FTNT/0001262039->IWM,
  TRN/0000099780->MDY, JBL/0000898293->MDY, TNET/0000937098->IWM,
  SYNA/0000817720->IWM.
- 2017: FLEX/0000866374->MDY, LRCX/0000707549->MDY,
  ATKR/0001666138->IWM, CC/0001627223->MDY, RIG/0001451505->MDY,
  ARMK/0001584509->MDY, CMCO/0001005229->IWM, IDCC/0001405495->MDY,
  CRTO/0001576427->MDY, NGVT/0001653477->MDY, HOG/0000793952->MDY,
  BLDR/0001316835->IWM, AMKR/0001047127->MDY, KIM/0000879101->MDY,
  PPC/0000802481->MDY.
- 2018: FLEX/0000866374->MDY, VISN/0001517228->MDY,
  AN/0000350698->MDY, HAIN/0000910406->MDY, MKSI/0001049502->MDY,
  KIM/0000879101->MDY, IR/0001699150->MDY, ENOV/0001420800->MDY,
  AA/0001675149->MDY, BGS/0001278027->IWM, PTEN/0000889900->MDY,
  HOG/0000793952->MDY, SANM/0000897723->MDY, LRCX/0000707549->MDY,
  CENX/0000949157->IWM.
- 2019: FLEX/0000866374->MDY, JELD/0001674335->MDY,
  DVA/0000927066->MDY, BGSF/0001474903->IWC, RHI/0000315213->MDY,
  PEB/0001474098->MDY, MATV/0001000623->IWM, OEC/0001609804->IWM,
  HAIN/0000910406->MDY, TBI/0000768899->IWM, TRGP/0001389170->MDY,
  OLN/0000074303->MDY, LKQ/0001065696->MDY, TALO/0001724965->IWM,
  PII/0000931015->MDY.
- 2020: HOG/0000793952->MDY, LKQ/0001065696->MDY,
  GEO/0000923796->IWM, PAG/0001019849->MDY, TMHC/0001562476->IWM,
  MTX/0000891014->IWM, PDM/0001042776->MDY, VST/0001692819->MDY,
  PIPR/0001230245->IWC, WHR/0000106640->MDY, MLI/0000089439->IWM,
  MTW/0000061986->IWM, CRTO/0001576427->IWM, JLL/0001037976->MDY,
  WKC/0000789460->IWM.

The benchmark instruments are IWC (iShares Micro-Cap ETF), IWM (iShares
Russell 2000 ETF), and MDY (SPDR S&P MidCap 400 ETF Trust).

### Sources, endpoints, dates, and count

For each of the 79 unique holding symbols plus IWC, IWM, and MDY (82 market
instruments), request:

- 82 Nasdaq Data Link Sharadar SEP payloads:
  `https://data.nasdaq.com/api/v3/datatables/SHARADAR/SEP.json?ticker={symbol}&date.gte=2015-07-02&date.lte=2023-07-12`
- 82 Sharadar ACTIONS payloads:
  `https://data.nasdaq.com/api/v3/datatables/SHARADAR/ACTIONS.json?ticker={symbol}&date.gte=2015-07-02&date.lte=2023-07-12`
- 82 Sharadar TICKERS payloads:
  `https://data.nasdaq.com/api/v3/datatables/SHARADAR/TICKERS.json?ticker={symbol}&table=SEP`
- 79 SEC submissions payloads:
  `https://data.sec.gov/submissions/CIK{10-digit-cik}.json`
- Three Sharadar metadata payloads: SEP, ACTIONS, and TICKERS metadata.
- One ALFRED `DGS1MO` payload for 2015-07-01 through 2023-07-12,
  with both realtime bounds fixed to 2026-07-17.

The expected total is exactly 329 HTTP requests. No retry, pagination
expansion, additional filing download, alternate vendor, proxy, refresh,
symbol substitution, or fallback is authorized. An incomplete response stops
the collection and requires new approval.

### Required fields and adjustment semantics

- SEP: ticker, date/timestamp, adjusted close, unadjusted close, volume,
  dividends/distributions, exchange, currency, last-updated timestamp, and
  adjustment metadata.
- ACTIONS: action type, release timestamp, effective/ex/record/pay/settlement
  dates where applicable, split or conversion ratio, cash amount and currency,
  successor/counter-security, source, and last-updated timestamp.
- TICKERS: permanent identifier, dated ticker/name/exchange identity,
  listing/delisting dates, security type, CUSIP where licensed, and
  last-updated timestamp.
- SEC: CIK, entity name, ticker/exchange associations, former names, accession
  number, form, filing and acceptance timestamps, and primary document.
- Risk-free: observation date, value, units, frequency, realtime/vintage
  bounds, and retrieval metadata.

An adjusted close may be used only if retained metadata proves it embeds
splits and cash distributions. Embedded components are not posted again.
Conversions, acquisition consideration, successor securities, liquidation
recoveries, and any unembedded value require ACTIONS evidence and
reconciliation. Ambiguity fails the full affected vintage closed.

### Retrieval evidence and destination

The approved destination is
`artifacts/pit_validation/session_v3_4_market_ledger_inputs/`, with raw payloads
under `raw/nasdaq_data_link/{sep,actions,tickers,metadata}/`,
`raw/sec/submissions/`, and
`raw/fred/DGS1MO_alfred_vintage_2026-07-17.json.gz`. The request ledger is
`lineage/request_manifest.jsonl`.

Each request record must retain canonical parameters, UTC request and retrieval
timestamps, HTTP status and relevant headers, SHA-256 of the exact response
bytes, SHA-256 of the deterministic compressed file, source release/adjustment
vintage, and destination. Normalized rows must retain their raw-payload hashes.
Generated payloads remain Git-ignored.

## Execution boundary

All three pre-V3.4 named blockers are resolved. The next bounded task may
execute only V3.4 under these policies. It may not calculate performance,
begin V3.5, substitute sources or instruments, expand the request envelope, or
use any prohibited legacy or fallback treatment.
