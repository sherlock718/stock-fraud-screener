# Codex Roadmap — Linear Production Validation

## Purpose

Validate one production strategy from certified inputs to one canonical
backtest. This roadmap supersedes the former numbered and lettered recovery
chain. That history remains available in Git commit `ebed029` and the frozen
reports under `reports/pit_validation/`; it is not an active execution plan.

## Fixed scope

- US SEC annual population only.
- One annual July decision calendar and one three-year target.
- Observed evidence only; unresolved rows fail closed.
- Corrected `production_v3_ml_gates` only. It is a new certified production
  contract; legacy performance claims do not transfer to it.
- OOS decision-tree agreement gate plus OOS LightGBM three-year regression
  ranker.
- Fixed production hard gates, top 15, equal weights, $200,000 AUM, and a 1%
  position/ADTV limit.
- One security-level monthly total-return NAV and one controlled backtest.

The model contract uses observed three-year targets for both roles: binary
local-benchmark outperformance for the decision tree and observed stock return
for the LightGBM regressor. Fold-local feature selection is capped at 28 from
the V3.1 frozen candidate pool. Clean training requires certified positive ROA
and Beneish below -1.78; the uncertified `fraud_suspect` heuristic is excluded.
The tree threshold of 0.55 is a fixed policy parameter, not a newly optimized
claim.

Explicitly excluded: 6m/1y/2y/5y models, alternate strategies, legacy-result
reproduction, `OLD_RECONSTRUCTED`, `CORRECTED_PARTIAL`, policy-imputed portfolio
sensitivities, threshold optimization, and multi-market expansion.

## Reusable evidence

The following may be reused only after their recorded manifests revalidate:

- Session 8D certified US annual filing population.
- Session 8E observed three-year labels, decision/prediction/entry timestamps,
  common-session benchmark mapping, and price provenance.
- Session 8F corrected feature transformations and stable row identity.
- Fold-local eligibility, feature-selection, preprocessing, prediction-lineage,
  and canonical-NAV helpers covered by the checkpoint test suite.

Session 9 Ridge/logistic predictions and Session 9B empty holdings remain audit
evidence only. They are not production model inputs or backtest sides.

## Session V3.1 — Freeze the production table and contract

Build one immutable observed-only, three-year modeling/selection table. Carry
stable row identity plus decision, prediction, and entry timestamps from the
certified sources. Materialize every production hard-gate field with explicit
provenance. Freeze the exact decision-tree and LightGBM configurations, target,
fold calendar, missingness behavior, feature candidates, top-15/equal-weight
rules, and corrected ADTV equation. Do not train models or source new market
data. Stop with explicit blockers if any required field cannot be certified.

**Deliverable:** one manifest-backed production table, configuration, coverage
report, and focused contract tests.

**Prompt:**

> Execute Session V3.1 from docs/CODEX_ROADMAP.md as one bounded production-table and contract freeze. Revalidate only the existing certified inputs actually used; build one observed-only US annual three-year table carrying stable identity and decision/prediction/entry timestamps; materialize every fixed production hard gate with provenance; and freeze the exact OOS decision-tree, LightGBM ranker, top-15 equal-weight, $200,000 AUM, and 1%-ADTV contracts. Do not train models, source external data, select holdings, run a backtest, optimize parameters, begin V3.2, commit, or push. Fail closed with exact blockers.

## Session V3.2 — Generate exact OOS production predictions

Using only the accepted V3.1 table and configuration, fit fold-local decision
trees and LightGBM regressors. Every fitted transformation remains inside its
historical fold. Generate explicit row-level OOS tree probabilities and
three-year regression predictions with complete lineage. Do not select holdings
or measure strategy performance.

**Deliverable:** models, OOS predictions, fold coverage, manifest, and report.

## Session V3.3 — Freeze liquidity-qualified holdings

Apply all fixed non-liquidity gates and model roles, then obtain or validate
candidate-wide pre-prediction ADTV evidence. For a $200,000 equal-weight top-15
portfolio, require median 30-session dollar volume of at least
`(200000 / 15) / 0.01 = 1,333,333.333333...`. Rank eligible candidates by OOS
LightGBM prediction and freeze exactly 15 equal-weight holdings per supported
period. Missing evidence fails the candidate closed; incomplete periods form no
portfolio.

External market access, if required, needs explicit approval before collection.

**Deliverable:** candidate/gate/exclusion tables, holdings, weights, liquidity
lineage, manifest, and report.

## Session V3.4 — Freeze the canonical market ledger inputs

The only admissible selection input is the accepted V3.3 manifest with SHA-256
`8bf4cf867e883764d4e25c0d61a755c02443196ceac76be2843f7ff3ebf7bea3`.
Revalidate that file and every consumed record before execution. V3.4 must not
alter V3.1–V3.3 artifacts or reinterpret their holdings, weights, mappings, or
timestamps.

### Frozen calendar and unsupported-period behavior

- Each decision year is a separate three-year vintage. Decision is July 2 at
  00:00 UTC, prediction is July 2 at 00:01 UTC, and entry is the accepted first
  common regular-session close strictly after prediction: 2015-07-02 20:00
  UTC, 2016-07-05 20:00 UTC, 2017-07-03 17:00 UTC, 2018-07-02 20:00 UTC,
  2019-07-02 20:00 UTC, and 2020-07-02 20:00 UTC.
- The annual schedule creates overlapping vintages; it does not shorten the
  accepted three-year holding horizon. A vintage's target exit is its accepted
  entry timestamp plus exactly 36 calendar months. Exit is the first common
  regular-session total-return close on or after that target, within ten
  calendar days. No fiscal-year, December-to-December, fixed-day-count, or
  independently chosen stock/benchmark date may replace this clock.
- A later supported vintage enters only at its own accepted entry timestamp;
  this is the only permitted annual rebalance clock. It does not sell, resize,
  or relabel an earlier overlapping vintage. A combined capital-allocation rule
  across overlapping vintages is not inferred in V3.4.
- The supported 2020 vintage may continue to its own evidence-backed target
  exit even though 2021 is unsupported. The unsupported 2021 period forms no
  new portfolio and is not cash-filled, carried forward, reduced below 15,
  replaced, or bridged into a continuous strategy NAV. Any combined-series
  treatment of the resulting gap is a named blocker requiring user approval.

### Frozen valuation and benchmark contract

- Entry, every month-end, and exit valuation use total-return closes on the
  same frozen US regular-session calendar. For each vintage, the valuation
  clock is the calendar-designated final common XNYS/XNAS regular session on or
  before calendar month-end; every active security and required benchmark must
  have a valid close for that exact session. Do not step backward to a stale
  security-specific date, mix timestamps, forward-fill, use a month label as a
  price, or treat absence as zero return. A sourced event may replace only the
  economic value it explicitly resolves; otherwise the full vintage fails
  closed. Entry and exit retain the stricter common-session rules above.
- Freeze each holding's benchmark at its decision timestamp from the V3.1
  decision-available market cap: IWC below $300 million, IWM from $300 million
  to below $2 billion, MDY from $2 billion to below $10 billion, and SPY at $10
  billion or above. Missing cap, mapping, or validated benchmark coverage fails
  closed; no proxy or later remapping is allowed.
- Each holding's benchmark sleeve starts with the same vintage weight and uses
  that holding's exact entry, month-end, and exit clocks. The vintage benchmark
  is the sum of those sleeves; it is not indexed by fiscal year and is not an
  independently dated annual-return series.

### Frozen price, event, cost, and risk-free boundary

- The primary path is observed-only adjusted-price/total-return evidence with
  source, field semantics, retrieval time, release/adjustment vintage, raw
  payload hash, currency, exchange calendar, and availability timestamp. For
  every split, dividend, distribution, conversion, cash consideration, or
  replacement security, record whether the accepted price field already
  embeds it. An embedded component is not posted again to the event ledger;
  an unembedded component requires dated sourced terms and reconciliation.
  Ambiguous adjustment behavior fails closed.
- Dated entity/security/ticker/exchange mappings must preserve the same
  economic holding. A migration continues only through proven mapping terms; a
  stock merger or reorganization uses sourced conversion terms; a cash
  acquisition moves sourced consideration and distributions to 0%-earning
  cash; and bankruptcy/liquidation uses observed prices, distributions,
  recoveries, and cancellation terms, reaching zero only with evidence of zero
  recovery. Delisting without complete continuation evidence, acquisition with
  incomplete terms, ambiguous mapping, or unexplained source disappearance
  invalidates the observed-only vintage. Policy-imputed disappearance outcomes
  are outside V3.4.
- Before collection or ledger construction, user approval must freeze one
  transaction-cost rate and identify its components, whether the quoted rate is
  per side or round trip, the exact turnover denominator, and the entry,
  rebalance, exit, and event-trade charging timestamps. Costs are charged only
  on actual traded notional at those accepted timestamps and paid from cash.
  The legacy market-cap tiers, 30/60 bps defaults, and any annual-label cost
  subtraction are prohibited. **Blocker `V3_4_TRANSACTION_COST_POLICY`: no
  accepted rate or interpretation currently exists.**
- Before collection or ledger construction, user approval must freeze the
  risk-free instrument/source, immutable vintage, observation release and
  availability timestamp, source frequency, exact conversion to each ledger
  month, and missing/revision behavior. Only rates available by the applicable
  valuation timestamp may be used; incomplete coverage makes Sharpe/Sortino
  unavailable and is never filled, carried, interpolated, or replaced. The
  fixed 3% constant is prohibited. **Blocker `V3_4_RISK_FREE_POLICY`: no
  accepted source, vintage, or conversion currently exists.**

### Approval and execution boundary

No external request is authorized by this contract clarification. Before V3.4
may collect anything, the user must approve a bounded collection plan naming
the holdings and benchmark instruments, security/event/risk-free sources,
endpoints, date ranges, expected request count, fields, adjustment semantics,
retrieval/vintage timestamps, and destination. Approval authorizes only that
collection; it does not authorize a substitute vendor, proxy, refresh,
performance calculation, or fallback. **Blocker `V3_4_EXTERNAL_DATA_APPROVAL`
remains open.**

The legacy December-to-December calendar, fixed 3% risk-free constant, legacy
tiered cost assumptions, annual labels used as returns, policy-imputed
disappearance outcomes, and every unsupported fallback are explicitly
inadmissible. Any policy not fixed above remains a named blocker requiring user
approval; implementation must not guess it. Once all blockers are resolved,
V3.4 may freeze complete adjusted-price/total-return, benchmark, risk-free,
cost, mapping, and corporate-action evidence for the V3.3 holdings only and
build security-level monthly ledger inputs. It must not calculate performance
metrics or begin V3.5.

**Deliverable:** canonical input manifest, coverage tables, security ledger,
monthly NAV inputs, and report.

## Session V3.5 — Run one controlled backtest

Run the production `ml_gates` strategy once using only V3.1–V3.4 frozen
artifacts. Reconcile holdings, turnover, costs, security returns, benchmark, and
monthly NAV before computing CAGR, Sharpe, drawdown, and Calmar from the same
net NAV. Report results and limitations without optimization or legacy
performance substitution.

**Deliverable:** one reproducible backtest result, reconciliation tables,
manifest, and final validation report.

## Completion rule

The project reaches the production-validation boundary only when V3.1 through
V3.5 pass in order. A blocker stops the sequence; it does not create another
lettered session. Resolve it inside the same session contract or revise this
roadmap explicitly before continuing.

## Commit checkpoints

Each V3 session ends with one conventional commit after its scoped tests, final
verification, diff review, changelog entry, and handoff update pass. Generated
artifact payloads remain Git-ignored and are never staged. A blocked session may
commit a useful fail-closed diagnostic checkpoint, but it does not create a
lettered session. Do not accumulate work from multiple completed V3 sessions in
one commit.
