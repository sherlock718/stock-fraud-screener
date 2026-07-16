# Archived Handoff — V3 Production Validation

> Archived on 2026-07-17. This handoff records the paused validation program
> and must not be treated as the active next task.

## Current state

Sessions V3.1 through V3.3 are accepted. The V3.3 manifest-backed artifact is
under `artifacts/pit_validation/session_v3_3_liquidity_holdings/`; generated
market evidence and selection payloads are Git-ignored.

A bounded pre-V3.4 documentation clarification revalidated the accepted V3.3
manifest SHA-256
`8bf4cf867e883764d4e25c0d61a755c02443196ceac76be2843f7ff3ebf7bea3`
without altering any accepted artifact. The V3.4 roadmap contract now pins the
accepted July entry/rebalance/three-year exit clocks, the unsupported-period
gap behavior, exact common-session month-end valuation, decision-time
IWC/IWM/MDY/SPY mapping and clocks, adjustment/event double-counting controls,
and evidence-backed security/event resolution.

On 2026-07-17 the user explicitly resolved all three pre-V3.4 blockers. The
accepted transaction-cost policy is a flat 25 bps per side on actual traded
notional, with gross turnover divided by pre-cost vintage NAV and costs charged
only at evidence-backed entry, exit, or actual market-trade timestamps. The
accepted risk-free policy is Federal Reserve H.15 `DGS1MO`, immutable ALFRED
vintage 2026-07-17, using the immediately preceding Federal Reserve business
day's observation and the exact interval conversion frozen in the roadmap.
The accepted external-data envelope is 329 expected Sharadar/SEC/ALFRED
requests for the 79 unique issuers underlying the 90 holdings and required
IWC/IWM/MDY instruments. The complete accepted scope is recorded in
`reports/pit_validation/pre_v3_4_blocker_resolution.md`.

V3.3 revalidated the exact accepted V3.1 manifest SHA-256
`2b5249cdb05c7bad1759abbd281ec1c90a8a9ce2fbd72973cd4dc905c8a86e5a`
and V3.2 manifest SHA-256
`ba0e3b2d850af113c26306dbec1d9d5cab7a58aa78cafd40cefac31059899912`,
plus all 156 referenced table, configuration, prediction, model-role, model,
code, report, and lineage records before selection.

The fixed non-liquidity gates, exact OOS tree probability gate at 0.55, and
exact OOS LightGBM three-year return prediction produced 1,428
liquidity-required candidates across 2015–2020. With explicit approval, V3.3
made 1,428 candidate/window-scoped Yahoo chart requests; all returned HTTP 200.
Raw responses, parameters, retrieval times, hashes, symbol/exchange mappings,
regular-session clocks, unadjusted closes, volumes, USD currency, and no-
adjustment policy are retained.

Of those candidates, 1,174 passed the exact median 30-session dollar-volume
threshold of $1,333,333.3333333333. Another 221 failed below the threshold and
33 failed closed because at least one required session had zero volume. Ranking
occurred only after liquidity pass. Six supported decision periods (2015–2020)
contain exactly 15 holdings each at weight 1/15; every other period formed no
portfolio. No performance, NAV, backtest, post-selection liquidity collection,
Session 9 prediction, substitute model, optimization, or V3.4 work occurred.

## Commit cadence

The roadmap requires one conventional commit after every verified V3 session.
Generated artifact payloads remain Git-ignored and are never staged. Do not
combine multiple completed sessions or create lettered blocker sessions.

## Files changed in V3.3

- `portfolio/build_session_v3_3_holdings.py`
- `tests/portfolio/test_build_session_v3_3_holdings.py`
- `reports/pit_validation/v3_3_liquidity_holdings.md`
- `CHANGELOG.md`
- `docs/CODEX_HANDOFF.md`

## Files changed in the pre-V3.4 clarification

- `docs/CODEX_ROADMAP.md`
- `CHANGELOG.md`
- `docs/CODEX_HANDOFF.md`

## Files changed in the pre-V3.4 blocker resolution

- `reports/pit_validation/pre_v3_4_blocker_resolution.md`
- `docs/CODEX_ROADMAP.md`
- `CHANGELOG.md`
- `docs/CODEX_HANDOFF.md`

## Verification

- Focused V3.1–V3.3 selection boundary: 15 passed.
- Full suite: 674 passed, 4 skipped, 78 warnings.
- The artifact's internal verdict passed exact 30-session clocks/arithmetic,
  candidate-wide gate ordering, exclusions, deterministic ranks, 15 equal
  weights per supported period, incomplete-period closure, and scope claims.

Pre-V3.4 documentation clarification verification:

- Accepted V3.3 manifest SHA-256 and 11 required roadmap clauses revalidated.
- Focused V3.1–V3.3 plus canonical monthly-NAV contract suite: 25 passed, 13
  existing dependency deprecation warnings.
- Warn-only architecture sync and `git diff --check`: passed.

Pre-V3.4 blocker-resolution verification:

- Accepted V3.3 manifest SHA-256 and all 1,448 referenced records revalidated.
- V3.1/V3.2 embedded manifests and the byte-identical V3.1–V3.3 production
  configuration revalidated.
- Focused V3.1–V3.3 plus canonical monthly-NAV contract suite: 25 passed, 13
  existing dependency deprecation warnings.
- No external request, collection, refresh, accepted-artifact change, ledger,
  performance calculation, V3.4 execution, commit, or push occurred.

## Exact next task

Execute only V3.4 using the accepted V3.3 holdings, clarified calendar/ledger
contract, approved 25 bps-per-side transaction-cost policy, approved immutable
`DGS1MO` policy, and approved 329-request collection envelope. Stop and request
new approval rather than retrying, paginating, expanding filing retrieval,
substituting a vendor/instrument/symbol, or using a proxy or fallback. Do not
calculate performance or begin V3.5.
