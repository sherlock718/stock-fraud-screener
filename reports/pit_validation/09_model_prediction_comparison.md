# Session 9 — CORRECTED_8F Models and Explicit OOS Predictions

**Status:** complete (2026-07-16)  
**Artifact:** `artifacts/pit_validation/session9_corrected_8f/`  
**Manifest SHA-256:** `bb75e2be97104736786a14c2b6c435fecef5aa335fb41181fe1f1947cb094deb`

## Boundary and preflight

This was one bounded corrected-model and explicit-OOS-prediction build. Before
the first Session 9 edit, the Session 8F manifest hash and every referenced
record passed: two validated inputs, 33 input/output/checkpoint/lineage records,
seven code-lineage records, and three dirty-state records. The build read only
the two physical Session 8F feature populations and their copied Session
8E-certified gates and labels.

The corrected path is named `CORRECTED_8F`. No stale corrected-partial artifact
was read or modeled. `OLD_RECONSTRUCTED` is frozen as unavailable because no
separately certified old-feature population has stable row identity, proven
filing availability, and Session 8E-aligned labels. No old population was
constructed ad hoc.

## Fold contract

Each annual Session 8B decision timestamp is an OOS fold. A scored row requires
both Session 8F required-price-feature support and Session 8E horizon-label
support. Historical training rows require the same intersection plus strict
`label_end_date < fold decision_timestamp`; equality, null dates, and missing
provenance fail closed.

Within each historical fold:

- the latest eligible historical cohort is held out for classifier calibration;
- feature selection is fit on earlier development rows and cannot see either
  Session 8E outcome as a candidate feature;
- median imputation and standardization are fit only on the model's historical
  fit population;
- classification uses balanced logistic regression plus sigmoid calibration;
- regression uses ridge regression; and
- every feature list, preprocessor, calibration record, and model is stored and
  content-addressed separately by population, horizon, fold, and model kind.

The random seed is 42. Macro and macro-interaction features are explicitly
excluded. No prior feature list, median, preprocessor, calibration, model,
prediction, or fold cache was reused.

## Certified population counts

Both physical namespaces contain 43,806 stable annual rows and are economically
identical: their stable-row, row-horizon-gate, and label-economics fingerprints
match. They remain separate inputs, model directories, prediction tables, and
checkpoints.

| Horizon | 8E label supported | Price + label supported | Unavailable | Excluded |
|---|---:|---:|---:|---:|
| 6m | 24,127 | 23,575 | 14,514 | 5,717 |
| 1y | 24,127 | 23,575 | 14,514 | 5,717 |
| 2y | 21,492 | 21,030 | 17,059 | 5,717 |
| 3y | 19,025 | 18,613 | 19,476 | 5,717 |
| 5y | 14,514 | 14,209 | 23,880 | 5,717 |

Counts are identical in each namespace. Early supported cohorts remain explicit
but prediction-ineligible when fewer than two historical training cohorts exist.

## OOS prediction result

The two physical prediction tables contain 438,060 rows each: one record for
every stable row × horizon × model kind. Across both namespaces, 349,104 rows
have eligible OOS predictions and 527,016 retain an explicit exclusion reason.
The eligible count per model kind and per namespace is:

| Horizon | Eligible rows | Fit folds | Unavailable early folds |
|---|---:|---:|---:|
| 6m | 23,113 | 14 | 2 |
| 1y | 22,217 | 13 | 3 |
| 2y | 18,632 | 11 | 4 |
| 3y | 15,098 | 9 | 5 |
| 5y | 8,216 | 5 | 7 |

There are 208 fitted population/horizon/fold/model artifacts and 84 explicit
unavailable fold records. Selected feature counts range from 19 to 26. Every
eligible prediction has stable/entity identity, population, horizon, fold,
decision/prediction timestamps, certified label end/provenance, four artifact
IDs, prediction, and within-fold rank.

## Descriptive OOS diagnostics

These are descriptive row-level diagnostics, not an acceptance threshold,
backtest, portfolio result, or production-threshold optimization. Because
Session 8E added zero policy-only labels, values are identical in both physical
namespaces.

| Horizon | Classifier ROC AUC | Regression Spearman IC |
|---|---:|---:|
| 6m | 0.5661 | 0.0684 |
| 1y | 0.5506 | 0.0174 |
| 2y | 0.5680 | 0.0979 |
| 3y | 0.5912 | 0.0937 |
| 5y | 0.5747 | 0.2873 |

## Freeze and verification

The final manifest covers seven certified inputs, 846 configuration/checkpoint/
model/prediction/support/lineage records, four code-lineage records, and three
dirty-state references. Independent post-freeze hashing found zero failures.
Row keys are unique, eligible rows have non-null predictions/ranks/artifact IDs,
ineligible rows have null predictions and non-empty reasons, all fitted folds
have a maximum training label end strictly before their decision timestamp, and
all selected feature files contain zero macro features.

No backtest, portfolio, market-data source/refresh, production-threshold
optimization, Session 9C action, commit, or push occurred.
