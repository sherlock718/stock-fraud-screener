# Codex Handoff

## Current state

Sessions V3.1 and V3.2 are accepted. The V3.2 manifest-backed artifact is under
`artifacts/pit_validation/session_v3_2_oos_predictions/`; its manifest SHA-256
is `ba0e3b2d850af113c26306dbec1d9d5cab7a58aa78cafd40cefac31059899912`.
The generated model and prediction payload is Git-ignored.

V3.2 revalidated the exact accepted V3.1 manifest SHA-256 plus only its consumed
table and configuration records. The row-complete output contains 87,612
row-role records for the two required `production_v3_ml_gates` roles. It has
78,492 finite OOS predictions from 26 fitted models, covering 39,246 rows per
role across decision folds 2014–2026. The eight 2010–2013 role-folds failed
closed as `fold_no_eligible_training_history` for 4,302 rows per role.

Every fitted fold uses only labels with `label_end_date < decision_timestamp`,
the certified clean-training filter, separate role-specific selection from the
frozen 119 candidates, at most 28 selected features, fold-local medians, and the
exact frozen tree or LightGBM parameters and seed. The frozen candidate pool
contains `observed_excess_return_3y`; V3.2 preserves it as a candidate but masks
its score-row value until its certified label end date, preventing future
observed outcomes from entering OOS scoring. All feature, preprocessing, model
configuration/model, target, fold, row, and prediction lineage is persisted.

The 258 source rows in 2027/2028 are retained for each role, producing 516
explicit `future_decision_after_v3_1_freeze` exclusions. V3.2 did not read
Session 9 predictions, use fallbacks, optimize parameters or thresholds, select
holdings, source market data, calculate ADTV, run a backtest, or begin V3.3.

## Commit cadence

The roadmap requires one conventional commit after every verified V3 session.
Each checkpoint includes code, tests, docs, changelog, and handoff, but never
generated artifact payloads. Do not combine multiple completed sessions into
one commit and do not create lettered blocker sessions.

## Files changed in V3.2

- `modeling/build_session_v3_2_oos.py`
- `tests/modeling/test_build_session_v3_2_oos.py`
- `reports/pit_validation/v3_2_oos_predictions.md`
- `CHANGELOG.md`
- `docs/CODEX_HANDOFF.md`

## Verification

- Focused V3.2/V3.1/fold-lineage boundary: 20 passed.
- Full suite: 669 passed, 4 skipped, 78 warnings.
- Independent artifact verification matched all 147 manifest input, artifact,
  code, and report records; row-role uniqueness, strict training clocks, finite
  predictions, feature caps, exact model parameters, complete lineage, and
  future exclusions passed.
- Generated payload: 147 artifact records, 26 model payloads, one row-complete
  prediction table, fold coverage, exclusions, configuration, and lineage.

## Exact next task

Do not begin V3.3 without explicit approval. If approved, execute only Session
V3.3 from the roadmap: apply the frozen non-liquidity and model roles, then
obtain or validate candidate-wide pre-prediction ADTV evidence before ranking.
The certified Session 8E normalized price records contain no volume, so any
external market-data collection requires explicit approval first.
