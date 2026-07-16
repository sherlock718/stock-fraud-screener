# Codex Handoff

## Current state

The project has been reset around one goal: make a simple, workable stock
screener. The audit-grade V3 historical validation program is paused and its
roadmap and handoff are preserved under `docs/archive/`.

Historical V3.1–V3.3 artifacts and reports were not altered. The accepted
pre-V3.4 policy resolution remains in
`reports/pit_validation/pre_v3_4_blocker_resolution.md`, but its 329-request
collection is not an active task and no related credentials are currently
needed.

Product-facing documentation no longer presents legacy backtest metrics as
current facts. The repository still needs a read-only reality check before a
canonical implementation path is chosen.

## Simplification changes

- Replaced `README.md`, `docs/START_HERE.md`, and the active roadmap with a
  short product-first path.
- Archived the V3 validation roadmap/handoff and legacy FAQ/configuration under
  `docs/archive/`.
- Preserved historical reports and artifact paths so their lineage remains
  intact.
- Added no product code, data refresh, model training, or performance result.

## Verification

- Active-document checks: passed.
- Full test suite: 674 passed, 4 skipped, 78 existing warnings.
- `git diff --check`: passed.
- Architecture sync: no changed architecture-sensitive files detected.

## Product rule

Prefer one supported entrypoint, one input/model path, and one explainable
shortlist. Do not add another strategy or refresh external data until the
existing candidates have been inspected.

## Exact next task

Execute Product Session P1 from `docs/CODEX_ROADMAP.md`. Inspect the current
product candidates without writes or external requests, report what actually
runs and how current its inputs are, and recommend exactly one bounded
implementation task. Do not resume V3.4, retrain models, refresh data, calculate
performance, commit, or push.
