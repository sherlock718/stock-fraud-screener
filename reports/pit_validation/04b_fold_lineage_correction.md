# Session 4B — Fold-Local Feature and Preprocessing Lineage

Date: 2026-07-15  
Scope: accepted findings T19 and T21–T30 only. No dataset rebuild, artifact
generation, model training, backtest execution, or Session 4C work was performed.

## Corrected boundary

Feature candidates are now derived from the permitted training population, not
from a full panel. Static classification uses its eligible horizon-specific
training population for fill-rate, IC, and correlation decisions. PSI may use
only the explicitly declared validation/development population. Rows after the
validation end remain untouched test data and do not participate in selection.

OOF scoring, classifier walk-forward CV, regression walk-forward CV, and the
backtest walk-forward classifier/tree/regressor now repeat candidate discovery
and feature selection inside every label-eligible training fold. Appended later
rows therefore cannot change an already-defined fold. Static standalone
regression likewise selects on its eligible training split rather than loading a
full-history feature set.

## Fail-closed artifact contract

`modeling/fold_lineage.py` records deterministic content fingerprints for the
dataset, training population, selection population, and optional development
population, plus the horizon, target, cutoff, label policy, selector
configuration, and ordered features. Reuse requires exact equality for every
field. Missing or incomplete lineage, stale data/populations, cross-horizon or
cross-policy metadata, and missing artifacts raise `LineageError`.

OOF no longer falls back from a missing feature artifact to `model_meta.json` or
all full-panel candidates; it selects within each fold. Tuning accepts persisted
model metadata only after exact lineage validation and rejects legacy metadata.
Static tuned hyperparameters are not accepted by historical walk-forward CV
without fold-specific compatible lineage.

The existing local model and feature JSON artifacts predate this schema. They
were not modified or certified by inference. Any future consumer that requires
them must regenerate them in the controlled Sessions 8–9 execution boundary.

## Frozen preprocessing

- classification, OOF, tuning, and regression medians are fitted on eligible
  training rows and reused unchanged for validation/test/scoring;
- logistic `StandardScaler` remains inside the training-fitted pipeline;
- optional sector z-score parameters are now fitted once on eligible training
  rows, stored in metadata, and applied unchanged to validation/test rows; and
- standalone regression 1st/99th target bounds remain training-only and are
  reused unchanged within each fold.

The sector-normalization application defect from T26 was corrected only as
required to enforce this frozen-parameter contract. Calendar-dependent cohort
feature values and all Session 4C semantics remain untouched.

## Invariance evidence

Synthetic tests verify that:

- altering held-out test values or non-null coverage cannot change selection;
- appending later rows cannot change an existing fold's candidates, selected
  features, indices, or predictions;
- training medians, scaler means, sector parameters, and regression target
  bounds are unchanged by scoring rows;
- dataset fingerprints are row-order invariant but content sensitive;
- missing, stale, cross-horizon, cross-policy, or cutoff-incompatible lineage
  fails closed; and
- tuning rejects legacy model metadata without lineage.

## Verification

`python3 -m pytest tests/modeling tests/backtest/test_walk_forward.py
tests/research/test_ic_engine.py tests/research/test_factor_research.py -q` →
**93 passed**, with 9 pre-existing pandas `FutureWarning` messages from
`backtest/engine.py`.

Targeted modules compiled successfully with bytecode redirected to `/tmp`, and
`git diff --check` passed.

No parquet dataset, feature/model JSON, joblib/CBM model, prediction column,
feature-selection output, or backtest artifact was rebuilt, regenerated,
trained, or overwritten.
