# Session 6A — Historical Prediction Consumption Correction

Date: 2026-07-15

Status: Implemented; no data, prediction, model, price, or backtest artifact was
generated.

## Scope and outcome

Session 6A implements the accepted Session 5A prediction-lineage and
missing-score contracts. Historical score consumers now fail closed unless each
declared ML role has a non-null, compatible, row-level manifest whose source is
`walk_forward_oos` or `oof_oos`. Final/static scores are never selected as a
historical fallback.

The frozen historical parquet has no compatible prediction manifests. Its
persisted OOF/static columns therefore remain excluded rather than being
reconstructed, inferred, or relabeled. This is the required stop-condition
outcome, not a prediction regeneration request.

## Shared prediction contract

`modeling/prediction_lineage.py` is the single enforcement point. A score is
eligible only when:

- the canonical row identity and availability/decision evidence are complete;
- the score is non-null and agrees with its manifested raw/transformed value;
- source, model role, horizon, and label policy match the declared requirement;
- training, maximum publication, and maximum label timestamps are strictly
  before the row decision timestamp;
- dataset/population, selector/features, preprocessing, fold, model artifact,
  code state, calendar/cohort, target, score-unit, and seed fields are complete;
  and
- the canonical prediction key is not duplicated.

Rows persist `historical_score_eligible`, `historical_score_source`, and
`historical_score_exclusion_reason`. Required exclusion codes distinguish
missing predictions/manifests, missing model artifacts, missing feature or
preprocessing artifacts, non-OOS sources, label-policy/decision mismatches,
incompatible lineage, duplicate keys, and unproven identity.

## Corrected consumers

| Path | Required historical OOS roles |
|---|---|
| Engine `ml_gates` | tree agreement gate + 3y regression ranker |
| Engine `composite` | 1y + 3y classifier rankers |
| Engine `qem` | 1y classifier ranker |
| Engine `scdv` | 3y classifier ranker |
| Engine `iarb` | 3y classifier ranker |
| `alpha_fraud_risk` / alpha composite | all configured 1y, 3y, and 5y OOF factor inputs |
| Alpha/screener registries | direct OOF roles plus OOF roles inherited through fraud-risk or alpha composite |
| IC-weighted portfolio registry | every declared signal plus all direct/indirect OOF lineage |

The engine's `load_and_score` path now consumes already-generated scores only
and removes legacy static classifier columns. The former final-model load,
same-year expanding-median fallback, and private in-engine walk-forward training
generator were removed. Historical prediction generation now has no callable
backtest-engine path around the manifest contract.

## Portfolio coverage and reporting

Ranking happens only after row validation. An official period forms only when
exactly `target_n` fully scored, gate-passing rows are available. Otherwise the
selected index is empty and the period records
`insufficient_valid_score_coverage` (or the generic target coverage code for a
rule-only filter). No null row can enter through `nlargest`, and no 0–5-row
threshold can remove a required ML weight. If any decision period has a coverage
gap, the official performance result is marked unavailable rather than chaining
the remaining periods into a discontinuous CAGR or Sharpe series.

Each period reports universe candidate count, post-gate count, final valid-score
count, valid/excluded counts by required role, target and selected counts,
period exclusion, and row-level identity/source/reason records. The backtest
result preserves these records even when no official period is formed.

## Synthetic invariance and missingness coverage

Tests cover:

- changing final/static classifier or regression columns without changing
  historical `ml_gates` selection;
- adding a later same-year filing without imputing or changing earlier OOS
  scores;
- later-filing and final/static invariance through alpha composite;
- missing prediction, non-OOS source, artifact, preprocessing, and duplicate-key
  exclusion reasons;
- 0 through 5 valid regression scores, partial score families, and `top_n`
  greater than non-null coverage;
- exact five-row ML-weight behavior; and
- fail-closed `composite`, `ml_gates`, QEM, SCDV, IARB, alpha composite,
  screener registry, alpha registry, and IC-weighted portfolio registry paths.

## Explicitly unchanged and deferred

Session 6A does not change monthly returns, annual returns, transaction costs,
corporate-action/disappearance handling, `likely_delisted` gates, calendars,
market data, prediction/model artifacts, or performance results. Session 6B,
data rebuilds, prediction generation, retraining, calendar selection, and
production comparisons remain separate work.

## Verification

- Focused prediction/strategy/alpha/registry/integration tests: 104 passed.
- Full unit suite: 613 passed, 4 skipped, with 86 pre-existing pandas
  fragmentation/downcasting/date warnings.
- Targeted compilation and `git diff --check` passed.
- `python3 quality/check_sync.py --warn-only` exited successfully and reported
  no changed architecture files to check.
- Post-closeout hardening removed the remaining private legacy training
  generator and made `reports/pit_validation/*.md` trackable; the same focused
  and full verification boundaries were rerun afterward.
