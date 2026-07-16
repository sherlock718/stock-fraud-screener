# Codex Handoff

## Current state

The accumulated PIT correction work through Session 9B is checkpointed on
branch `codex/pit-checkpoint` in commit `ebed029`. The checkpoint includes the
validated source, tests, audit reports, and reproducibility builders; the
approximately 4 GiB payload under `artifacts/pit_validation/` remains local and
Git-ignored. Its directory sizes and manifest hashes are recorded in
`docs/PIT_ARTIFACT_ARCHIVE.md`.

The repository-wide suite passed after the checkpoint corrections: 659 passed,
4 skipped, with 78 existing warnings. The corrected ADTV position equation is
covered by focused tests. No push occurred.

## Active direction

The former Session 1–9B recovery history and post-9B letter chain are no longer
the active roadmap. `docs/CODEX_ROADMAP.md` now contains five linear production
sessions, V3.1 through V3.5, for one observed-only US annual three-year
`ml_gates` strategy.

Keep these boundaries:

- Do not reproduce legacy performance or compare stale paths.
- Do not rebuild five horizons or alternate strategies.
- Do not use Session 9 Ridge/logistic predictions as the production
  LightGBM/tree roles.
- Do not commit generated artifact payloads.
- Do not source external data, commit, or push without explicit authorization.

## Exact next task

Execute Session V3.1 only. Revalidate the minimum certified sources it consumes,
then freeze one observed-only US annual three-year production table with stable
identity, decision/prediction/entry timestamps, all hard-gate fields, and the
exact tree/LightGBM/selection/liquidity contract. Do not train models or begin
V3.2.

> Execute Session V3.1 from docs/CODEX_ROADMAP.md as one bounded production-table and contract freeze. Revalidate only the existing certified inputs actually used; build one observed-only US annual three-year table carrying stable identity and decision/prediction/entry timestamps; materialize every fixed production hard gate with provenance; and freeze the exact OOS decision-tree, LightGBM ranker, top-15 equal-weight, $200,000 AUM, and 1%-ADTV contracts. Do not train models, source external data, select holdings, run a backtest, optimize parameters, begin V3.2, commit, or push. Fail closed with exact blockers.
