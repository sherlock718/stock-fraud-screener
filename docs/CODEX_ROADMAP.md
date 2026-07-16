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
- Production `ml_gates` only.
- OOS decision-tree agreement gate plus OOS LightGBM three-year regression
  ranker.
- Fixed production hard gates, top 15, equal weights, $200,000 AUM, and a 1%
  position/ADTV limit.
- One security-level monthly total-return NAV and one controlled backtest.

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

For the V3.3 holdings only, freeze complete adjusted-price/total-return,
benchmark, risk-free, transaction-cost, security-mapping, and corporate-action
evidence. Prove adjustments and event cash flows are not double counted. Build
the security-level monthly ledger and fail closed on unresolved coverage. Do
not calculate performance metrics.

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
