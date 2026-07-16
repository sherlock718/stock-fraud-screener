# Final Codex Roadmap — Modular Repository Validation

**Status:** Active source of truth for future work.

**Scope:** Understand and correct the US production three-year path first while
applying confirmed label-availability rules consistently to every trained
horizon, then perform the full XBRL rebuild, then complete the remaining Four
Chapters.

This roadmap consolidates:

- the Four Chapters learning plan;
- the pipeline-orientation questions;
- the prior Claude end-to-end PIT plan;
- the corrected label-availability and artifact-lineage requirements;
- the repository's low-token Codex workflow.

Historical entries in `SESSION_PLAN.md` remain useful evidence, but its old
execution prompts are not the plan for this work.

## How to Use This Roadmap

Each Codex task handles exactly one numbered session.

At the beginning:

1. Read `AGENTS.md`, `docs/START_HERE.md`, `docs/CODEX_WORKFLOW.md`, and
   `docs/CODEX_HANDOFF.md`.
2. Read only the current session below and its named files.
3. Inspect actual implementation before accepting roadmap assumptions.

At the end:

1. Save the specified deliverable.
2. Run only the specified verification.
3. Update `docs/CODEX_HANDOFF.md` with evidence and the next session.
4. Do not commit or push unless explicitly requested.

Default controls: automatic model/reasoning selection, Standard speed, no
subagents, no MCPs, no external refresh, bounded output, and the low-token
execution rules below.

### Low-Token Execution Rules

- Reuse the current handoff and already-read session evidence; do not reread
  unchanged orientation documents or replay prior investigations.
- Start with one bounded `rg`/targeted-read pass. Do not load whole modules,
  reports, diffs, logs, or directory trees when line ranges answer the question.
- Keep raw command output in `/tmp` when it may be large; return only counts,
  failures, warnings, and the smallest relevant excerpts.
- During implementation, run only focused tests. Run the full suite once, after
  code and focused tests are stable. If a later edit is documentation-only, use
  `git diff --check`; if it is narrowly behavioral, rerun only affected tests
  unless shared behavior materially changed.
- Review with `git diff --stat`, `git diff --check`, and path-scoped diffs. Avoid
  emitting the cumulative dirty-worktree diff unless the task requires it.
- Do not use subagents, web/MCP/connectors, telemetry inspection, or repeated
  environment discovery unless the current session explicitly requires them.
- Stop at the numbered session boundary. Record unresolved work in the handoff
  instead of expanding the audit or implementation.

## Non-Negotiable Methodology

- Methodological correctness beats preservation of historical performance.
- Separate confirmed evidence, inference, and unresolved questions.
- Do not call the partial run full PIT validation while Step 2 snapshots remain stale.
- Do not overwrite the only dataset, model, prediction, price cache, or report.
- A training row is eligible only when its complete target was observable before
  the fold scoring date. The timestamp convention must be documented.
- Fitted preprocessing must use eligible training/as-of information only.
- Every controlled corrected or FULL-PIT run must use explicit artifacts and
  frozen canonical market data; `LEGACY_SAVED` and unavailable reconstructed
  paths are reference-only evidence, never comparison runs.
- Official risk metrics must derive from one internally consistent monthly NAV series.
- The dataset must be described as historically enriched, not comprehensively
  survivorship-free.
- Do not treat every disappearing security as a bankruptcy or apply one generic
  delisting penalty to acquisitions, restructurings, ticker changes, or migrations.

---

## Phase 0 — Roadmap and Guardrails

### Session 0 — Install the Working Structure — COMPLETE

**Outcome:** Codex defaults, workflow, handoff, and this roadmap exist.

**Evidence:**

- `AGENTS.md`
- `.codex/config.toml`
- `.codex/hooks.json`
- `docs/CODEX_WORKFLOW.md`
- `docs/CODEX_HANDOFF.md`
- `docs/CODEX_ROADMAP.md`

No production code, data, models, or backtest artifacts are changed here.

---

## Phase 1 — Establish Methodological Truth

### Session 1 — Audit Three-Year Label Availability (Read-Only) — COMPLETE

**Objective:** Determine whether historical folds trained on outcomes that were
not yet fully observable.

**Inspect:**

- `pipeline/step3_enrich_prices.py`
- `modeling/train.py`
- `modeling/train_regression_model.py`
- `modeling/score_oof.py`
- `backtest/engine.py`
- relevant target/split tests and dataset columns

**Required evidence:** target definition, entry price/date, intended and actual
exit date, return type, missing terminal-price policy, corporate-action handling,
all fold scoring dates, current training masks, affected rows by fold/year, and
whether `embargo_years=0` admits incomplete outcomes.

**Deliverable:** `reports/pit_validation/01_label_availability_audit.md`

**Result:** Confirmed that every audited three-year fold admits labels unavailable
at its scoring date. The report records lower-bound affected counts, reconstruction
limits, and the accepted Session 2 contract.

**Stop condition:** If dates cannot be reconstructed reliably, report the blocker;
do not guess, rebuild, or retrain.

**Prompt:**

> Execute Session 1 from docs/CODEX_ROADMAP.md. Read-only. Audit three-year label availability and produce the required evidence report. Do not modify code or artifacts.

### Session 2 — Implement Target Dates and Purged Eligibility — COMPLETE

**Prerequisite:** **SATISFIED 2026-07-15.** Session 1 completed and the timestamp,
provenance, and missing-label conventions below were accepted.

**Accepted implementation contract:**

- Training eligibility is strictly `label_end_date < scoring_date`; equality is
  excluded.
- Stock-return labels use the actual stock exit trading date. Benchmark-relative
  binary/excess labels use the later actual stock/benchmark exit trading date.
- Persist actual dates and explicit provenance in the canonical pipeline; do not
  infer availability from fiscal year alone.
- Missing-date, unknown-provenance, and policy-imputed labels are training-
  ineligible until provenance and availability are explicit. Do not add a silent
  legacy fallback.
- Retain the current January 1 fold scoring convention in Session 2. A broader
  filing/rebalance-calendar decision is deferred.
- Change code and focused tests only. Do not mutate the current clean dataset,
  rebuild data, retrain models, or report corrected performance.

**Objective:** Persist or deterministically derive `label_start_date` and
`label_end_date`, then enforce the documented eligibility rule in every three-year
training and walk-forward path.

**Required tests:** before/after/exact scoring-date cases, missing end date,
future-target invariance, fold maximum end-date invariant, and documented
delisted/acquired treatment.

**Deliverable:** minimal code changes, focused tests, and a fold-purge summary.

**Result:** Step 3 now persists actual three-year stock/benchmark trading dates
and provenance; Step 6 explicitly marks policy-imputed labels; and every located
three-year static, tuning, OOF, walk-forward, regression, and backtest training
path uses the shared strict eligibility rule. Synthetic tests cover all required
boundary and invariance cases. The immutable legacy parquet remains unchanged
and is deliberately training-ineligible for three-year targets until Session 8.

**Evidence:** `reports/pit_validation/02_fold_purge_summary.md`

**Verification:** targeted modeling/pipeline tests only.

**Prompt:**

> Execute Session 2 from docs/CODEX_ROADMAP.md using the accepted Session 1 findings. Implement the minimal label-date and purging correction with focused invariance tests. Do not rebuild or retrain.

### Session 2B — Generalize Horizon Eligibility and Survivorship Sensitivity — COMPLETE

**Reason for revision:** Session 2 correctly fixed the production three-year
path, but the repository also trains 6m, 1y, 2y, and 5y targets. Rebuilding with
three-year-only date columns would preserve the same leakage risk elsewhere and
force another rebuild later. Excluding policy-imputed outcomes from the primary
model is necessary for provenance correctness, but observed-only training can
itself be optimistic when missing outcomes correlate with company failure.

**Accepted implementation contract:**

- Persist horizon-qualified actual stock/benchmark end dates and provenance for
  every Step 3 horizon; keep the common actual stock entry trading date.
- Enforce strict observed-label availability for every trained horizon: actual
  target end date must be non-null and strictly before the scoring date.
- Keep disappearing companies in the historical universe.
- Keep observed-only labels as the default primary training population.
- Preserve the `-50%` likely-delisted assumption only as an explicitly selected
  sensitivity population, never silently mixed into the primary model.
- A policy-imputed label becomes sensitivity-eligible only after both its target
  horizon and the heuristic no-filing detection lag have elapsed. Its provenance
  and policy-availability date must be explicit.
- Do not claim the policy label is a true delisting return or distinguish
  acquisitions, bankruptcies, ticker changes, or restructurings without an event
  source.
- Change code and synthetic/focused tests only. Do not rebuild data, retrain
  models, or report performance.

**Objective:** Generalize Session 2 across all generated/trained horizons and
make the observed-only versus policy-sensitivity populations reproducible before
the controlled rebuild.

**Required tests:** multiple horizons, strict boundary cases, horizon-specific
stock versus relative dates, missing/unknown provenance, legacy-schema failure,
future-target invariance, policy detection/horizon availability, and proof that
policy labels are excluded by default but included only in sensitivity mode.

**Deliverable:** generalized code/tests and
`reports/pit_validation/02b_horizon_survivorship_summary.md`.

**Result:** Step 3, Step 6, and all located static/walk-forward training paths now
use horizon-qualified availability fields. Observed-only remains the default;
the explicitly requested `include_policy_imputed` mode admits heuristic `-50%`
labels only after their recorded policy availability date. No artifact was
rebuilt or retrained.

**Verification:** targeted pipeline/modeling/backtest tests only.

**Prompt:**

> Execute Session 2B from docs/CODEX_ROADMAP.md. Generalize label availability to every trained horizon and add an explicit, dated likely-delisted sensitivity population. Keep observed-only as the default. Do not rebuild or retrain.

### Session 3 — Audit All Fitted Transformations (Read-Only) — COMPLETE

**Objective:** Find every operation that may learn parameters globally or from
future observations.

**Search/classify:** quantiles, clipping, winsorization, scalers, imputers,
encoders, feature selection, normalization, ranking, deduplication, and any
full-sample transformation.

Classify each as:

1. fixed formula;
2. as-of cross-sectional;
3. training-fold fitted;
4. potentially global/future-aware.

Known starting points include `modeling/constants.py` and
`research/factor_research.py`, but the audit must not stop there.

**Deliverable:** `reports/pit_validation/03_transformation_audit.md`

**Result:** The accepted audit confirmed global loader clipping, global and
order-dependent fraud-taxonomy transforms, future/test-aware feature selection,
unproven persisted feature-set reuse, full-cohort annual transformations, a
future-derived historical `likely_delisted` gate, and static historical score
fallbacks. It also separated valid fold-fitted operations from unresolved
calendar, vintage, quarterly-availability, and record-resolution questions. No
correction was implemented.

**Prompt:**

> Execute Session 3 from docs/CODEX_ROADMAP.md. Read-only. Inventory and classify every fitted or potentially future-aware transformation. Do not fix anything yet.

### Session 4A — Correct Confirmed Global Downstream Transformations

**Prerequisites:** Session 3 findings are accepted; the current dataset and model
artifacts remain immutable; and the canonical owner of each materialized feature
is identified before editing.

**Exact scope:** Correct only T12–T15 from the accepted audit:

- remove independent full-panel `growth_yoy` clipping from the shared modeling
  and factor-research loaders so downstream reads do not refit pipeline values;
- make the active fraud-taxonomy clipping/ranking population PIT-safe; and
- make dilution EPS change ticker-local, chronologically ordered, and independent
  of dataframe row order.

Do not change annual cohort ranks, fold feature selection, backtest gates or
fallback scores, macro data, targets, model specifications, or strategy rules.
Do not rebuild data or retrain models.

**Required future-invariance tests:**

- loading an already materialized historical row preserves its growth values;
- appending later-year rows cannot change earlier loaded growth or fraud-taxonomy
  values;
- appending rows unavailable at the tested as-of cutoff cannot change earlier
  taxonomy values;
- shuffling rows cannot change taxonomy output after key-based re-alignment;
- one ticker's EPS history cannot affect another ticker's dilution change; and
- no downstream loader silently estimates new clipping bounds.

**Stop conditions:** Stop without broadening the change if the canonical feature
owner is ambiguous, the taxonomy scoring timestamp/universe is not available to
the active call, or the correction would require a dataset rebuild to test safely.
Record the blocker and required lineage field instead of inventing a fallback.

**Deferred questions:** Historical dataset/model effects are measured only in
Sessions 8–10. Macro vintages, duplicate/amendment precedence, quarterly-source
availability, annual cohort semantics, and research/test-tuned strategy choices
remain outside this session.

**Deliverable:** minimal code/tests and
`reports/pit_validation/04a_global_transform_correction.md`.

**Verification:** focused fraud, pipeline, modeling-loader, and factor-research
tests only.

**Prompt:**

> Execute Session 4A from docs/CODEX_ROADMAP.md. Correct only accepted audit findings T12–T15 and add the required future- and order-invariance tests. Do not rebuild or retrain.

### Session 4B — Make Feature Selection and Preprocessing Lineage Fold-Local

**Prerequisites:** Session 4A is complete; the Session 2/2B label-eligibility
contract remains authoritative; and each static, OOT, OOF, tuning, regression,
and walk-forward path has an explicit training/development/scoring boundary.

**Exact scope:** Correct T19 and T21–T30 only where needed to ensure that candidate
fill-rate checks, IC/correlation/PSI selection, imputation/scaling/target bounds,
and reusable feature metadata are learned from the permitted training or explicit
development population and then frozen for scoring. OOF and walk-forward folds
must select within each fold or load an artifact whose recorded dataset, cutoff,
eligibility policy, selector configuration, and training population prove exact
compatibility. Held-out test rows cannot choose features or preprocessing.

Do not change model families, hyperparameter objectives, target definitions,
calendar-dependent feature values, static historical score fallback behavior, or
strategy thresholds. Do not regenerate feature/model artifacts or retrain models.

**Required future-invariance tests:**

- changing held-out test values or coverage cannot change selected features;
- appending later rows cannot change an already defined fold's candidates,
  selected features, preprocessing parameters, or predictions;
- validation data affects only explicitly permitted development decisions and
  never masquerades as untouched test data;
- each OOF/walk-forward fold fits or loads only lineage-compatible feature and
  preprocessing state;
- missing, stale, cross-horizon, or label-policy-incompatible lineage fails
  closed rather than falling back to `model_meta.json` or another external set;
  and
- medians, scalers, sector-normalization parameters, and regression target bounds
  are estimated on eligible training rows and reused unchanged for scoring.

**Stop conditions:** Stop if a consumer lacks a stable fold/scoring identifier,
artifact compatibility cannot be proven from available metadata, or the smallest
safe correction would require regenerating persisted features/models. Document
the required schema or regeneration step for Sessions 8–9; do not certify legacy
artifacts by inference.

**Deferred questions:** The separate sector-normalization application defect may
be fixed only if required to enforce the frozen-parameter contract; otherwise log
it for a bounded follow-up. Full-sample research/strategy selection (T32–T34),
model-performance effects, and static backtest fallbacks remain deferred.

**Deliverable:** minimal code/tests and
`reports/pit_validation/04b_fold_lineage_correction.md`.

**Verification:** focused modeling, feature-selection, OOF, tuning, regression,
and synthetic walk-forward tests only.

**Prompt:**

> Execute Session 4B from docs/CODEX_ROADMAP.md. Make feature selection and preprocessing lineage fold-local using the accepted audit boundary. Fail closed on unproven artifacts. Do not rebuild or retrain.

### Session 4C0 — Accept the Calendar and Eligible-Cohort Contract

**Status:** accepted by the user on 2026-07-15 in
`reports/pit_validation/04c_calendar_contract_proposal.md`.

**Accepted boundary:** the rebalance calendar is deliberately deferred because it
may vary by horizon. Session 4C uses actual row publication time, requires
row-level proven availability provenance with fail-closed legacy/estimated dates,
and retains earliest-primary/no-amendment-switching.

**Prompt:**

> Review `reports/pit_validation/04c_calendar_contract_proposal.md`. Confirm the three filing-materialization rules or identify exact revisions. Do not choose a rebalance schedule, implement Session 4C, rebuild data, regenerate artifacts, or retrain models in the acceptance task.

### Session 4C — Correct Calendar-Dependent Cohort Transformations

**Status:** complete on 2026-07-15. See
`reports/pit_validation/04c_cohort_transform_correction.md`.

**Prerequisites:** Sessions 4A and 4B are complete, and Session 4C0 has explicitly
accepted the filing-materialization portion of
`reports/pit_validation/04c_calendar_contract_proposal.md` (or a revised
replacement). Rebalance/decision-snapshot calendars remain deferred by horizon.

**Exact scope:** Correct the cohort-dependent parts of T05–T09 and T16: Step 5
sparse-history winsorization fallbacks; Step 5 fiscal-year/market and sector ranks,
percentiles, and rank-derived interactions; Step 6 accrual winsorization and
size-category imputation; and filing-time materialized alpha factor ranks. Each
transform must use only the accepted as-of cohort or training population. Sparse groups must not
fall back to a later same-quarter, complete-year, or full-dataframe population.
Add only the minimal source availability-provenance field and validation needed
to enforce the accepted contract; do not otherwise change upstream extraction.

Do not integrate production, portfolio, research, or historical common-date
snapshot consumers until their horizon-specific calendars are accepted. Do not
change fixed row/trailing formulas, quarterly aggregation lineage (T10),
macro vintages, duplicate resolution, strategy-time ranks already computed on a
frozen eligible slice, backtest gates/fallback scores, or factor definitions and
weights. Do not rebuild data or retrain models.

**Required future-invariance tests:**

- adding a later filing in the same fiscal year/quarter cannot change a value
  already computed at an earlier scoring timestamp;
- adding later years cannot change historical cohort values;
- early- and late-filer fixtures use exactly the peers available by their accepted
  scoring cutoff;
- row order cannot change cohort ranks, winsorization, or imputation;
- sparse market/sector groups follow the documented training/as-of fallback and
  never global or complete-cohort bounds; and
- upstream Step 5/6 and event-time alpha materialization agree on the same
  eligible-cohort contract.

**Stop conditions:** Do not implement until the filing-materialization contract is accepted.
Stop if required filing/accession timestamps are absent, amendment precedence
changes cohort membership, or one calendar cannot represent materially different
consumers. Split the affected consumer into a later bounded session rather than
using fiscal year as a proxy for availability.

**Deferred questions:** horizon-specific rebalance/decision calendars, FRED
vintage/release lag, adjusted-price vintage, quarterly source-row availability,
duplicate/amendment precedence, and whether
portfolio builders need a separately enforced caller-side filing gate remain
unresolved.

**Deliverable:** minimal code/tests and
`reports/pit_validation/04c_cohort_transform_correction.md`.

**Verification:** focused Step 5, Step 6, event-time alpha, and PIT-quality tests
only.

**Prompt:**

> Execute Session 4C from docs/CODEX_ROADMAP.md only after the filing-materialization contract is accepted. Correct the bounded event-time cohort transformations and add same-cohort future-invariance tests. Do not choose or integrate a rebalance calendar, rebuild data, or retrain.

### Session 5 — Audit Monthly NAV and Strategy Consumption (Read-Only)

**Prerequisites:** Sessions 4A–4C are complete, or 4C has a documented blocker
that does not prevent tracing the backtest boundary. Session 3 findings T11 and
T31 are accepted inputs to this audit.

**Exact scope:** Prove how predictions flow into each tested strategy; identify
every use of final/static models, expanding same-year medians, or fallback scores
in historical periods; trace the future-derived `likely_delisted` value into every
historical gate/filter; and prove how monthly portfolio returns become CAGR,
Sharpe, drawdown, and Calmar.

**Establish:**

- whether `composite` consumes model predictions;
- the exact production `ml_gates`/regression score path;
- which years/rows use walk-forward predictions versus final/static scores and
  whether any missing score fails closed;
- every historical `likely_delisted` gate, including screener/filter call paths,
  and the as-of information actually available at that decision date;
- missing monthly-return policy;
- treatment of total losses and disappearing securities;
- annual/monthly reconciliation;
- whether every official metric uses the same monthly NAV.

**Required future-invariance tests/evidence:** This session is read-only. Specify
synthetic tests proving that adding future filings cannot change historical gate
eligibility, adding/changing a final model cannot change historical OOS scores,
same-year later filings cannot change historical imputation, and a missing
walk-forward prediction cannot silently activate a static fallback.

**Stop conditions:** Stop before proposing a fix if a score cannot be tied to a
model/fold/training cutoff, if the intended missing-score policy is unknown, or if
removing the delisting gate would implicitly decide unresolved corporate-action
return policy. Report affected rows/years and the decision required.

**Deferred questions:** Evidence-backed corporate-action classification and true
delisting returns remain separate from removing future knowledge. Performance
effects belong to Sessions 9–10, not this audit.

**Deliverable:** `reports/pit_validation/05_backtest_path_audit.md`

**Prompt:**

> Execute Session 5 from docs/CODEX_ROADMAP.md. Read-only. Trace explicit model predictions through strategy selection and monthly NAV metrics. Report every ambiguity before proposing changes.

### Session 5A — Accept Backtest Lineage, Missingness, and Return Contracts

**Status:** Complete — all four contracts explicitly accepted on 2026-07-15,
including the amended personal small-cap sensitivities and point-in-time
listing/filing-staleness/tradability gate wording.

**Prerequisite:** Session 5 audit complete. This is a policy-design and acceptance
session, not an implementation or performance session.

**Exact scope:** Explicitly accept four contracts before any Session 6 code:

1. row-level historical prediction lineage, including score source, model/fold
   artifact ID, training cutoff, label policy, feature/preprocessing lineage,
   decision timestamp, and exclusion reason;
2. fail-closed missing-score behavior for every strategy, including the minimum
   score coverage required to form a portfolio and a prohibition on silent
   final/static fallback;
3. corporate-action and disappearance treatment for acquisitions, bankruptcy,
   ticker changes, migrations, restructurings, source-coverage loss, missing
   prices, and policy-imputed sensitivity returns; and
4. the canonical return stream, including monthly total-return NAV, transaction
   costs, partial price coverage, total-loss bounds, annual/monthly reconciliation,
   and metric-frequency conventions.

The contract may leave a corporate-action class unresolved, but that class must
then be explicitly excluded from official performance claims rather than assigned
an inferred outcome. Do not choose a rebalance calendar in this session.

**Stop conditions:** Stop if a policy choice lacks sufficient evidence, silently
changes the investment strategy, or would relabel the aggregate legacy result as
reproducible. Record alternatives and the exact downstream exclusion instead.

**Deliverable:** `reports/pit_validation/05a_backtest_contract.md` plus explicit
user acceptance before Session 6A or 6B begins.

**Prompt:**

> Execute Session 5A from docs/CODEX_ROADMAP.md. Propose and freeze the row-level prediction-lineage, missing-score, corporate-action/disappearance, and canonical-return contracts. Do not implement code, choose a rebalance calendar, run a backtest, or treat the saved legacy result as reproducible.

### Session 6A — Correct Historical Prediction Consumption

**Status:** Complete — implemented and verified on 2026-07-15
(`613 passed, 4 skipped`).

**Prerequisite:** The Session 5A prediction-lineage and missing-score contracts
are explicitly accepted.

**Exact scope:** Apply the smallest corrections needed for explicit OOS-only
prediction consumption, row-level fail-closed missing-score behavior,
training/as-of-only historical imputation, and removal of every silent
final/static historical fallback. Persist row-level score source and exclusion
reason. Keep observed-only and policy-imputed model populations separate. Do not
change monthly return accounting, invent corporate-action outcomes, regenerate
predictions/models, or run production comparisons.

**Required tests:** final-model invariance for historical OOS scores; same-year
later-filing invariance for imputation; 0–5 and partially missing score coverage;
`top_n` greater than non-null scores; and fail-closed behavior for `composite`,
`ml_gates`, `qem`, `scdv`, `iarb`, alpha composite, and registry paths.

**Stop conditions:** Stop if an affected historical score lacks sufficient fold
lineage to replace safely or a correction would regenerate predictions/models.
An unprovable score must remain excluded with a recorded reason; it must not be
reconstructed silently.

**Verification:** targeted prediction/strategy tests, followed by the full unit
suite because score consumption is shared.

**Deliverable:** bounded code/tests and
`reports/pit_validation/06a_prediction_consumption_correction.md`.

**Prompt:**

> Execute Session 6A from docs/CODEX_ROADMAP.md. Implement the accepted OOS-only score-consumption and fail-closed missing-score contract with synthetic invariance tests. Do not change monthly returns, rebuild data, regenerate predictions, retrain, or run production comparisons.

### Session 6B — Correct Monthly NAV and Historical Gate Behavior

**Status:** Complete — implemented and verified on 2026-07-15
(`627 passed, 4 skipped`).

**Prerequisite:** The Session 5A corporate-action/disappearance and canonical-
return contracts are explicitly accepted. Session 6A need not be complete when
the code changes are independent, but Gate A requires both sessions.

**Exact scope:** Apply the smallest accepted corrections for future-derived
historical `likely_delisted` gates, missing monthly prices, disappearing
securities, transaction costs, total losses, partial coverage, annual/monthly
reconciliation, and internally consistent monthly performance metrics. Keep the
observed-only primary and explicitly dated policy-imputed sensitivity paths
separate. Do not infer an outcome for an unresolved corporate-action class.

**Required tests:** monotonic NAV, known drawdown, missing-return policy,
total-loss case, positive annual endpoints hiding monthly drawdowns, future-row
invariance for historical gate eligibility, no/partial ticker and month coverage,
disappearance mid-year, transaction costs, annual/monthly reconciliation, and
proof that CAGR, Sharpe, drawdown, and Calmar consume the same canonical NAV.

**Stop conditions:** Stop if gate removal requires choosing an unaccepted return
policy, price coverage cannot distinguish missing data from a true disappearance,
or the correction would regenerate prices, predictions, or models. Preserve the
row-level exclusion reason and defer artifact generation.

**Deferred questions:** Threshold/strategy optimization and measured performance
changes remain outside this correction session.

**Verification:** targeted backtest tests, followed by the full unit suite because
the engine is shared.

**Deliverable:** bounded code/tests and
`reports/pit_validation/06b_monthly_nav_correction.md`.

**Prompt:**

> Execute Session 6B from docs/CODEX_ROADMAP.md. Implement the accepted historical-gate and canonical monthly-NAV contracts with synthetic tests. Do not infer corporate-action outcomes, regenerate artifacts, or run production comparisons.

**Gate A:** Sessions 1–3, 4A–4C, 5, 5A, 6A, and 6B must pass before any dataset
rebuild or model retraining. A documented 4C calendar blocker does not permit a
rebuild that would materialize unresolved cohort transformations. An unresolved
corporate-action class must remain excluded from official performance claims.

---

## Phase 2 — Freeze and Run the Corrected-Partial Comparison

### Session 7 — Freeze Legacy Evidence and Add Lineage Validation

**Status: COMPLETE (2026-07-15).** Frozen as evidentiary-only
`LEGACY_SAVED`; the saved performance claim remains non-reproducible.

**Objective:** Preserve all available evidence for the saved legacy claim before
any data/model mutation. Do not describe the aggregate saved result as
reproducible: its holdings, weights, folds, and score sources are absent.

Use:

```text
artifacts/pit_validation/
    legacy_saved/{dataset,models,backtest,manifest.json}
    old_reconstructed/{dataset,models,predictions,backtest,manifest.json}
    corrected_partial/{dataset,models,predictions,backtest,manifest.json}
    full_pit/{dataset,models,predictions,backtest,manifest.json}
```

Trace and hash the clean dataset, all trained-horizon models, preprocessing/feature metadata,
predictions/scores, monthly price cache, benchmarks, strategy/cost configuration,
reports, code commit, dirty state, Python, and important dependencies.

The limitations manifest must also record the complete dirty patch relative to
the baseline commit; state explicitly that saved legacy artifacts predate the
Sessions 1–6B corrections; and identify missing canonical benchmark NAV,
time-aligned monthly risk-free returns, corporate-action/security-mapping
evidence, and adjusted-price provenance. Hash the Session 6B NAV/event schema
and explicit return-policy configuration without treating them as inputs to the
older saved result.

`legacy_saved` is an evidentiary snapshot of what exists, including explicit
missing artifacts and incompatibilities. `old_reconstructed` is a future clean
rerun on frozen old data under accepted contracts and must never be presented as
the original run.

Implement a real compatibility validator using streaming hashes. Do not require
manifest commit equality with every later documentation commit.

**Deliverable:** frozen `LEGACY_SAVED` evidence set, limitations manifest,
compatibility validator, and focused tests. The `OLD_RECONSTRUCTED` directory may
be reserved but is not populated in this session.

**Prompt:**

> Execute Session 7 from docs/CODEX_ROADMAP.md. Freeze and hash the available LEGACY_SAVED evidence, including the baseline commit and complete dirty patch; record that saved artifacts predate the corrected code and list missing benchmark-NAV, monthly risk-free, adjusted-price, and corporate-action evidence. Implement the compatibility validator. Do not call the legacy result reproducible, reconstruct predictions, rebuild, or retrain. Follow the roadmap's low-token execution rules.

### Session 7A — Freeze CORRECTED-PARTIAL Pipeline Inputs — COMPLETE

**Status: COMPLETE (2026-07-15).** The stale pre-fix Step 2 snapshots, daily
Step 3 price cache, and macro enrichment were frozen read-only under
`artifacts/pit_validation/corrected_partial_inputs/`. The preflight manifest
SHA-256 is
`31c4f4e289ac49ef7c01f333370b4e3a35a3e786c0b289200bdc95e9c5c2f4d5`.

The audit found 47 absent company symbols covering 1,726 Korean snapshot rows,
33 empty-cache symbols covering 9,816 rows, and two absent Korean benchmarks
(`^KS11`, `^KQ11`). None of the affected company rows has a saved observed
forward return. Missing or empty cache observations must remain unavailable/null;
no fetch, substitution, or policy imputation is permitted.

This freeze is corrected-pipeline input evidence only. It does not modify
`LEGACY_SAVED`, materialize `CORRECTED_PARTIAL`, or reproduce the saved legacy
run. See `reports/pit_validation/08_preflight_input_freeze.md`.

### Session 8 — Build CORRECTED-PARTIAL Dataset and Compare Data

**Status: COMPLETE (2026-07-15).** The audited four-stage offline lineage was
materialized under `artifacts/pit_validation/corrected_partial/`, with separate
pre-taxonomy and final datasets, streaming-hash lineage, and exhaustive
LEGACY_SAVED data-level comparison. The result is explicitly
`CORRECTED_PARTIAL`: it uses stale pre-fix Step 2 snapshots, is conditioned on
the frozen incomplete daily cache, and does not reproduce the saved legacy run.
See `reports/pit_validation/08_data_comparison.md`.

**Prerequisites:** Validate the Session 7A input manifest and all frozen payload
hashes before writing. Freeze the current `data/prices.parquet` as reference-only
evidence with expected SHA-256
`ead68437e62752d38b8fa73c145ee360e24f39c254b03b971b49f6743b62c595`;
it is evidence for the cache-coverage audit, not a pipeline input.

**Objective:** Run only the local stages required by accepted Sessions 2/2B and
4A–4C. The audited lineage is offline Step 3, Step 5, Step 6, then fraud-taxonomy
enrichment. Step 3 must open the frozen daily cache read-only, never call the
network, and preserve missing or empty series as unavailable/null. Implement a
bounded tested runner or explicit offline/cache/output arguments rather than
using the current network-capable CLI unchanged.

Every checkpoint, working copy, intermediate, and final output must remain under
`artifacts/pit_validation/corrected_partial/`. Do not mutate frozen inputs. Keep
both the pre-taxonomy clean dataset and the final taxonomy-enriched dataset so
that enrichment does not overwrite the only intermediate copy.

This dataset still uses stale pre-fix Step 2 snapshots and must be labeled
`CORRECTED-PARTIAL`.

**Compare:** rows/year, unique keys, duplicates, columns/schema, missingness,
filing dates, horizon-qualified label dates/targets, observed versus
policy-sensitivity eligibility counts, class balance, changed features/rows, and
every unexpected addition/removal. Disappearing companies remain in the universe.

**Deliverable:** corrected-partial dataset, manifest, and
`reports/pit_validation/08_data_comparison.md`.

**Prompt:**

> Execute Session 8 from docs/CODEX_ROADMAP.md. Validate the Session 7A manifest and hashes, freeze the current prices parquet as reference-only evidence, implement and test a strictly offline/read-only Step 3 path, then run offline Step 3, Step 5, Step 6, and fraud-taxonomy enrichment entirely under artifacts/pit_validation/corrected_partial/. Preserve pre-taxonomy and final datasets, keep missing cache observations null, and report every frozen LEGACY_SAVED-versus-CORRECTED_PARTIAL data difference without implying that the saved legacy backtest is reproducible. Do not use external data, retrain, predict, backtest, begin Session 8B, commit, or push.

### Session 8B — Accept Horizon-Specific Decision and Rebalance Calendars

**Status: COMPLETE (2026-07-15).** Calendar contracts are frozen for the five
declared modeled horizons (`6m`, `1y`, `2y`, `3y`, `5y`); all six other
materialized horizons are explicitly excluded because they have no modeling/OOS
path. The contract uses a common July decision, proven filing availability,
strict label completion before decision, horizon-matched common stock/benchmark
windows, and separate observed-only/policy-sensitivity populations. Current
corrected-partial artifacts omit the required row-level filing provenance and
entity identity, so all accepted horizons remain unsupported pending Session
8C and Session 9 is blocked. See
`reports/pit_validation/08b_horizon_calendar_contract.md`.

**Prerequisites:** The corrected-partial dataset and its row-level availability
fields from Session 8 exist. This is a policy-design session, not a performance
search.

**Exact scope:** define the decision/rebalance schedule separately for each
investment horizon before generating OOS predictions. Every schedule must name
exact decision timestamps/trading-day rules, filing cutoff, late-filing
treatment, eligible fiscal cohorts, model-training cutoff, prediction timestamp,
holding interval, benchmark interval, and mapping to forward-return measurement.

Explicitly resolve the current mismatch among fiscal-year labels,
`fiscal_year + 1` monthly holding windows, filing-date-based forward returns, and
benchmarks indexed by fiscal year. Do not select a schedule because it produces
the best historical Sharpe/CAGR.

**Deliverable:** accepted horizon-to-calendar mapping and frozen configuration
for Session 8C and Session 9. If a horizon remains undecided, exclude it from
prediction and official backtest claims rather than applying another horizon's
calendar.

**Prompt:**

> Execute Session 8B from docs/CODEX_ROADMAP.md. Define and freeze horizon-specific decision, prediction, holding, return, and benchmark calendars before retraining, without optimizing on test performance. Exclude unresolved horizons.

### Session 8C — Validate Training-Label Market Inputs

**Status: COMPLETE (2026-07-15) — NO-GO.** All five accepted horizons have
zero supported rows. Each excludes all 43,906 official-scope US annual rows
because the frozen lineage lacks stable entity identity and proven filing
availability; independently, the cache cannot certify accepted common-session,
benchmark-assignment, trading-calendar, total-return, or adjustment-vintage
semantics. Session 9 remains blocked. The smallest next task is a clean,
provenance-preserving US Step 2 rebuild in Session 8D. See
`reports/pit_validation/08c_training_label_market_inputs.md`.

**Prerequisites:** Session 8 has materialized corrected label dates and
provenance, and Session 8B has accepted the horizon-specific calendars.

**Objective:** Before any retraining, validate and freeze the price, benchmark,
trading-date, adjustment, and availability semantics used to create training
targets. Prove row-level coverage for each trained horizon and distinguish
observed stock labels, observed relative labels, unavailable labels, and explicit
policy-imputed sensitivity labels.

This is a selection-independent label-source gate. It does not require future
selected holdings and does not replace Session 9B's selection freeze or Session
9C's selected-holding prices, benchmark NAV, risk-free returns, costs, and
corporate-action coverage. If adjustment vintage or benchmark provenance cannot
be established, either exclude the affected horizon or label Session 9 output
as a frozen-cache-conditioned diagnostic; do not present it as certified OOS
performance.

**Deliverable:** frozen training-label market-input manifest, horizon-level
coverage and provenance tables, exclusions/limitations, and
`reports/pit_validation/08c_training_label_market_inputs.md`.

**Prompt:**

> Execute Session 8C from docs/CODEX_ROADMAP.md. Before retraining, validate and freeze the selection-independent prices, benchmarks, trading dates, adjustments, and availability used to create every trained-horizon target. Produce row-level coverage and provenance, exclude unsupported horizons or label them frozen-cache-conditioned diagnostics, and do not train models, generate predictions, run backtests, refresh external data without approval, begin Session 9, commit, or push.

### Session 8D — Rebuild and Freeze Provenance-Preserving US Step 2

**Status: COMPLETE (2026-07-16).** The frozen 8,021-CIK US universe was rebuilt
from official SEC Company Facts with immutable raw responses, explicit
failures/exclusions, stable entity identity, and proven primary-filing
availability. The certified output contains 163,216 period rows across 4,937
entities; 1,086 entities are excluded-only and 1,998 are unavailable. Every
stored raw response rehashed successfully. This repairs Step 2 provenance only
and does not unblock Session 9. See
`reports/pit_validation/08d_step2_provenance_rebuild.md`.

**Prerequisites:** Validate the Session 8C validation manifest and its referenced
Session 8B/8 artifacts before relying on them. Freeze and hash the current
`data/tickers.parquet` as the issuer-universe input. The current local file has
8,021 unique US CIKs; this count is an expected preflight assertion, not a
reason to silently accept later drift.

**Exact scope:** Perform a read-only evidence pass over the US Step 2 extraction,
earliest-primary selection, equal-time collision behavior, entity construction,
availability timestamp, provenance, checkpoint, and output paths before editing.
Then add only the artifact-scoped input, raw-response, checkpoint, and output
controls required for a clean rebuild without overwriting `data/snapshots.parquet`
or `data/snapshots_checkpoint.json`.

The only authorized external source is the official SEC Company Facts API for
the frozen 8,021-CIK universe. State the expected request/runtime/storage scope
before fetching. Preserve every successful raw response immutably (compressed
is acceptable) with request URL, CIK, retrieval timestamp, HTTP result, byte
size, and streaming SHA-256; record failures explicitly and never substitute,
infer, or reuse an unmanifested response. Keep all inputs, raw payloads,
checkpoints, output parquets, logs, and manifests under
`artifacts/pit_validation/corrected_step2/`.

Every certified output row must persist stable `entity_id`, fiscal year, period
type, market, source filing date, effective `availability_timestamp`, and
`availability_provenance = sec_primary_filing`. Use only supported primary SEC
filings. Later amendments, unsupported forms, missing dates, ambiguous source
records, and unresolved equal-time collisions fail closed with explicit row- or
entity-level exclusion reasons; do not infer accession precedence. Preserve
unproven raw extraction rows only in a separately named diagnostic output if
needed, never in the certified population.

**Validation:** Add focused synthetic tests for artifact scoping, stable entity
IDs, primary/amendment ordering, equal-time ambiguity, date-only availability,
raw-response hashing, resume behavior, and failure recording. Validate output
schema, unique entity-period keys, null/provenance consistency, annual versus
quarterly counts, source coverage, excluded reasons, raw-response hashes, code
lineage, configuration, and dirty state. Compare against the stale Step 2
snapshot only as a diagnostic; do not treat row-count similarity as proof.

**Deliverable:** a frozen corrected Step 2 evidence set and manifest under
`artifacts/pit_validation/corrected_step2/`, plus
`reports/pit_validation/08d_step2_provenance_rebuild.md` with explicit proven,
unavailable, and excluded counts. Update the changelog, roadmap, and handoff.

**Do not:** run Step 3 or later stages; generate or rewrite labels; source price
or benchmark data; train/retrain models; generate predictions; run backtests or
production comparisons; optimize thresholds; begin Session 8E or Session 9;
overwrite existing datasets; commit; or push.

**Prompt:**

> Execute Session 8D from docs/CODEX_ROADMAP.md as one bounded provenance-preserving US Step 2 rebuild. Use one agent, Standard speed, concise commands, and bounded reasoning. Validate the Session 8C manifest chain first, freeze the current 8,021-CIK US ticker universe, make Step 2 artifact-scoped, fetch only official SEC Company Facts, preserve and hash every raw response, and write all checkpoints/outputs/manifests under artifacts/pit_validation/corrected_step2/. Fail closed on missing or ambiguous filing provenance and report explicit proven, unavailable, and excluded counts. Do not overwrite existing datasets, run Step 3+, generate labels, source market prices, train, predict, backtest, optimize, begin Session 8E/9, commit, or push.

### Session 8E — Build and Revalidate Contract-Aligned Label Market Inputs

**Status: COMPLETE (2026-07-16).** Frozen provider-adjusted stock/benchmark
inputs, XNYS/XNAS calendars, common sessions, decision-time benchmark
assignment, event/adjustment evidence, retrieval vintage, separate population
artifacts, and a superseding support gate are stored under
`artifacts/pit_validation/contract_aligned_label_inputs/`. Observed support is
24,127/24,127/21,492/19,025/14,514 rows for 6m/1y/2y/3y/5y respectively. The
policy namespace adds zero unsupported inferred outcomes. See
`reports/pit_validation/08e_contract_aligned_label_market_inputs.md`.

**Deliverable:** frozen contract-aligned label-source payloads, labels, manifest,
and a superseding support verdict. Session 9 remains blocked unless at least one
accepted horizon has nonzero certified support.

### Session 8F — Materialize Corrected Feature Populations

**Status: COMPLETE (2026-07-16).** The corrected annual feature, cleaning, and
fraud-taxonomy lineage is frozen under
`artifacts/pit_validation/corrected_feature_population/`. Both physical
population namespaces contain the same 43,806 stable rows because Session 8E
certified zero policy-only additions. Required pre-decision price features are
supported for 26,232 rows, unavailable for 11,857, and excluded for 5,717.
Certified accounting and taxonomy lineage is present for all 43,806 rows.
Macro features are explicitly unavailable because no certified vintage and
release-lag input exists. See
`reports/pit_validation/08f_corrected_feature_population.md`.

**Prerequisites and input boundary:** Before any build edit, revalidate the
complete Session 8E manifest chain and every referenced hash, including stored
and decompressed 8D SEC and 8E market payloads. Use only
`corrected_step2/outputs/certified_snapshots.parquet` plus Session 8E-certified
security mappings, calendars, regular-session prices, gates, and labels. Never
join a corrected-partial feature, stale Step 2 row, legacy macro input, or old
preprocessing output.

**Execution contract:** Construct a deterministic stable row ID from entity,
fiscal period, and proven availability. Compute the required market-cap,
momentum, volatility, and 52-week-high inputs only from Session 8E closes
strictly before the Session 8B decision timestamp. Null the entire required
price family when any member is missing. Run Step 5, Step 6, and taxonomy in
separate `observed_only` and `include_policy_imputed` directories; disable the
legacy quarterly/size and survivorship-policy imputations. Taxonomy transforms
use proven filing availability rather than row order or filing-date-only
fallback. No missing macro value or interaction may be synthesized.

**Frozen result:** Step 5, Step 6, and taxonomy each preserved all 43,806 stable
row IDs in both namespaces. Every horizon retains Session 8E's exact
supported/unavailable/excluded partition. The final manifest SHA-256 is
`9c1e4b82c2e7bc2a85228adf6668b797acec827e2bd1ff7c58d20cfb6a9ac01a`.

**Do not:** train or tune models; select features across future folds; generate
predictions; run backtests; optimize thresholds; begin Session 9; reuse stale
corrected-partial features; commit; or push.

### Session 9 — Retrain All Horizon Models and Generate Explicit OOS Predictions

**Status: COMPLETE (2026-07-16).** The bounded `CORRECTED_8F` build is frozen
under `artifacts/pit_validation/session9_corrected_8f/`. The complete Session 8F
manifest and all 2 validated inputs, 33 artifact records, seven code-lineage
records, and three hash-covered dirty-state records passed revalidation before
the first Session 9 edit. The final Session 9 manifest SHA-256 is
`bb75e2be97104736786a14c2b6c435fecef5aa335fb41181fe1f1947cb094deb`.

**Prerequisites:** Sessions 8D–8F are accepted, Session 8E has produced at least
one Session 8B-accepted horizon with nonzero certified support, and Session 8F
has frozen the corresponding corrected feature populations. These prerequisites
are met for all five accepted horizons.

**Objective:** Retrain classification and regression paths from scratch using
the Session 8F corrected feature populations, accepted Session 8B decision
timestamps, Session 8E-certified target horizons, eligible rows, and
training-only fitted parameters. Produce separate
observed-only primary and `include_policy_imputed` sensitivity runs; never blend
their training populations or artifacts.

Do not reuse old selected features, medians, preprocessing, predictions, or fold
caches. Record actual configuration and results; do not enforce a performance
acceptance threshold.

Call the only certified corrected path `CORRECTED_8F`; `CORRECTED_PARTIAL` is a
stale, prohibited input and output name. For each 6m/1y/2y/3y/5y fold, intersect
Session 8F required-price-feature support with Session 8E horizon-label support.
Training eligibility additionally requires strict
`label_end_date < decision_timestamp`; equality and missing dates fail closed.
Keep macro features excluded. Fit feature selection, imputation, preprocessing,
calibration, and models only within the historical training fold. Keep the
physical `observed_only` and `include_policy_imputed` namespaces separate even
when their rows are identical, and record that identity explicitly.

Generate explicit `CORRECTED_8F` row-level OOS prediction tables containing
stable row ID, entity ID, horizon, population, fold, decision timestamp, label
end date/provenance, eligibility or exclusion reason, feature/preprocessing/
model artifact IDs, prediction, and rank. Mark `OLD_RECONSTRUCTED` unavailable
unless a separately certified old-feature population with stable row identity,
filing availability, and Session 8E-aligned labels already exists. Never build
that population ad hoc and never fabricate an explicit legacy prediction table.

**Deliverable:** corrected models, explicit prediction tables, manifests, and
`reports/pit_validation/09_model_prediction_comparison.md`.

**Frozen result:** Both physical namespaces contain 438,060 row/model records;
174,552 per namespace have eligible OOS predictions. There are 208 fitted fold
models and 84 explicit early-fold unavailability records. `OLD_RECONSTRUCTED`
is unavailable, macro features remain excluded, and no stale corrected-partial
artifact was read. No backtest, portfolio, market refresh, threshold
optimization, Session 9B/9C action, commit, or push occurred.

**Prompt:**

> Execute Session 9 from docs/CODEX_ROADMAP.md as one bounded corrected-model and explicit-OOS-prediction build. Revalidate Session 8F first; use only its physically separate populations and Session 8E-certified labels; call the corrected path CORRECTED_8F; keep macro excluded and every fitted operation fold-local; mark OLD_RECONSTRUCTED unavailable unless a separately certified old population already exists; and do not use corrected-partial artifacts, run backtests, begin Session 9B/9C, commit, or push.

### Session 9B — Freeze Compatible OOS Selection Paths and Holdings

**Status: COMPLETE — FROZEN UNAVAILABLE (2026-07-16).** The exact Session 9
manifest SHA-256 and all 7 validated inputs, 846 artifact records, four
code-lineage records, and three dirty-state references passed before the first
Session 9B artifact write. The final Session 9B manifest SHA-256 is
`2b17030e554b8f663f48e7f44299107f781dbdadc4d73054361b3ebe1ab492b8`.

**Frozen result:** No compatible explicitly named path is complete. Production
`ml_gates` lacks the required OOS tree-agreement probability, and the Session 9
calibrated logistic classifier was not substituted or evaluated at the tree
threshold. The engine composite/QEM/SCDV paths lack complete frozen non-model
ranking/gate inputs and canonical liquidity eligibility; IARB additionally has
no compatible non-US population. Both physical population namespaces retain
separate 219,030-row candidate/exclusion tables and empty holdings/weight
tables. The accepted entry timestamp is also absent from the Session 9
prediction lineage and is explicitly unavailable rather than inferred. All five
paths are unavailable, no smaller portfolio was formed, and Session 9C remains
blocked.

**Prerequisites:** Session 9 is complete and its exact manifest, prediction
tables, model artifacts, population identity, code lineage, and dirty-state
records revalidate before any selection artifact is written.

**Objective:** Convert only explicit eligible `CORRECTED_8F` OOS predictions
into deterministic row-level candidate, gate, rank, selected-holding, weight,
and exclusion records for every supported decision period. Keep
`observed_only` and `include_policy_imputed` physically separate and record their
identity when it persists. Do not fit, calibrate, tune, or rescore a model.

First reconcile the Session 9 model roles against the accepted Session 5A
strategy contract and current frozen configuration. The production `ml_gates`
path requires a compatible OOS tree-agreement probability plus a 3y regression
ranker. Session 9 produced a calibrated logistic classifier, not that tree
probability; never substitute it silently or apply the old tree threshold to it.
Freeze only separately named selection paths whose required scores, gates,
ranking direction, `target_n`, weighting, liquidity rule, and missing-score
behavior are already accepted and compatible. If no complete selection rule is
supported, mark that path unavailable and report the exact missing role or
policy instead of inventing one.

For every decision period, retain the full candidate population and explicit
exclusion codes. An official portfolio requires all frozen `target_n` holdings;
do not shrink the portfolio, renormalize around missing inputs, optimize a
threshold, or select on realized outcomes. Preserve decision/prediction/entry
timestamps and artifact IDs through the selection lineage.

**Deliverable:** frozen compatible-strategy inventory, row-level selection and
exclusion tables, selected-holding/weight tables, configuration, checkpoints,
manifest, and `reports/pit_validation/09b_oos_selection_freeze.md`. Session 9C
remains blocked unless at least one explicitly named path produces complete
holdings under the accepted contract.

**Prompt:**

> Execute Session 9B from docs/CODEX_ROADMAP.md as one bounded selection-freeze task. Revalidate the complete Session 9 manifest first. Using only explicit eligible CORRECTED_8F OOS predictions and already-accepted strategy parameters, reconcile model roles, gates, ranking, target_n, weights, and exclusions; keep both population namespaces physically separate; preserve full row-level lineage; and freeze holdings only for compatible explicitly named paths. Do not substitute the Session 9 logistic classifier for the production tree-agreement role, invent or optimize thresholds, refit models, source market data, run backtests, begin Session 9C, commit, or push. If no path is complete, freeze the unavailable verdict and exact blocker instead.

### Post-Session-9B Production `ml_gates` Dependency Correction

**Status: CONTRACT FROZEN; EXECUTION NOT STARTED (2026-07-16).** The frozen
Session 9B verdict was independently revalidated without rewriting it. Its
manifest SHA-256 remains
`2b17030e554b8f663f48e7f44299107f781dbdadc4d73054361b3ebe1ab492b8`;
all 1 validated input, 16 artifact records, and 2 code-lineage records match
their recorded sizes and hashes. Each physical namespace still contains
219,030 path-candidate rows, zero non-null entry timestamps, and zero holdings.
The unavailable verdict therefore remains correct. Session 9C is still
blocked.

The blocker order is corrected below. The original Session 9B artifacts are
historical evidence and must not be edited in place. Every successor must use a
new manifest and retain both physical population namespaces.

#### Session 9B.1 — Restore Selection Lineage and Materialize Hard-Gate Inputs

**Prerequisites:** Revalidate the complete Session 8E, 8F, 9, and 9B manifest
chain before writing a successor artifact.

**Objective:** Preserve, rather than recompute, Session 8E's certified
`entry_session` and `entry_timestamp` through a hash-addressed 8E→8F→9
selection-lineage bridge. Join only on the stable certified entity/year row
identity and verify that all supported horizons for an entity/year agree on one
entry timestamp. Retain decision and prediction timestamps, security mapping,
population, horizon, fold, label, feature, preprocessing, model, calibration,
and prediction IDs. A missing, duplicate, conflicting, or unmatched timestamp
fails closed. Do not recalculate an entry/exit session, return, benchmark, or
market outcome.

Materialize or block every accepted production hard-gate input before any model
fit is authorized:

| Production condition | Corrected source/status |
|---|---|
| US market | Session 8D/8F `market`; certified input, null/non-US fails |
| $50M–$10B decision market cap | Session 8E/8F `decision_market_cap`; certified where non-null, otherwise fails |
| Beneish M-score `< -1.78` | **Blocker:** `beneish_m_score` is absent from the final 8F outputs; components alone are not the accepted gate |
| Piotroski F-score `>= 3` | Session 8F `piotroski_f_score`; certified input |
| Positive ROA | Session 8F `piotroski_roa_pos == 1`; certified input |
| Altman Z-score `> 1.0` | **Blocker:** `altman_z_score` is absent from the final 8F outputs; components alone are not the accepted gate |
| Sector P/S percentile `<= 0.70` | **Blocker:** `ps_ratio_sector_pct` is absent from the final 8F outputs; `sales_to_price` is not a silent substitute |
| 12-month momentum `> -0.40` | Session 8F `momentum_12m_prior`; certified only on required-price-supported rows |
| Tree agreement probability `>= 0.55` | **Blocker until Session 9B.2:** Session 9 logistic probability is incompatible |
| 3y regression rank descending | **Compatibility blocker:** Session 9 supplies Ridge; `docs/PRODUCTION_CONFIG.md` names LightGBM. Do not call Ridge the production ranker without an explicit contract reconciliation |
| Equal weight, `target_n=15` | Frozen policy input; full 15 required or the period is unavailable |
| Candidate-wide 1%-ADTV gate | **Blocker until Session 9B.3** under the corrected contract below |

For the three missing deterministic gate fields, a successor may materialize
them only from Session 8F certified corrected inputs using a frozen formula,
cohort/sector boundary, missingness policy, code hash, and tests. It must not
read a stale corrected-partial or legacy feature value. If the accepted formula
or required corrected component is incomplete, retain the named blocker.

**Deliverable:** a new immutable selection-lineage/hard-gate evidence set,
coverage and blocker matrix, manifest, and report. No model fit, market-outcome
recomputation, liquidity calculation, selection, holdings, or backtest.

#### Session 9B.2 — Fit the Frozen Fold-Local Tree-Agreement Role

**Prerequisites:** Session 9B.1 has preserved accepted entry timestamps and
resolved or explicitly retained every non-model hard-gate blocker. The exact
tree specification below is frozen before this fit is authorized.

**Frozen model specification:** one `sklearn.tree.DecisionTreeClassifier` per
held-out decision period and physical population, with role
`tree_agreement_gate`, horizon `3y`, binary target
`outperformed_benchmark`, and positive-class probability persisted as
`tree_prob`. Train only on Session 8F rows with Session 8E-certified 3y support
and strict `label_end_date < held_out_decision_timestamp`; require at least five
distinct historical decision cohorts and at least five selected features.
Select up to 35 features inside each training fold by the legacy fold-local
absolute ICIR rule against Session 8E `stock_return` (the corrected counterpart
of legacy `forward_return_3y`): calculate cross-sectional Spearman IC separately
per historical decision cohort with at least 30 non-null feature/target rows,
require at least three valid cohort ICs, score
`abs(mean(IC) / (population_std(IC) + 1e-8))`, and sort descending with stable
canonical-feature order as the tie break. Use only the certified Session 8F
feature universe and exclude metadata, targets, scores, macro fields, and all
held-out rows. Impute with training-fold medians; do not scale or calibrate.
Use `criterion="gini"`, `splitter="best"`, `max_depth=4`,
`min_samples_split=2`, `min_samples_leaf=30`, `min_weight_fraction_leaf=0.0`,
`max_features=None`, `random_state=42`, `max_leaf_nodes=None`,
`min_impurity_decrease=0.0`, `class_weight=None`, and `ccp_alpha=0.0`.
Persist ordered features, medians, complete estimator parameters/classes,
training rows and maximum label date, fold IDs, code/dirty lineage, and OOS
probabilities. Missing features, one-class training, insufficient cohorts, or
missing positive class make that fold unavailable. Apply the already accepted
`tree_prob >= 0.55` only during the later selection rerun, never during fitting.

**Deliverable:** separately manifested fold-local tree models and explicit OOS
prediction rows joined to the Session 9B.1 lineage. No threshold optimization,
regression fit, liquidity work, selection, holdings, backtest, Session 9C,
commit, or push.

#### Session 9B.3 — Freeze Candidate-Wide Preselection Liquidity Evidence

**Prerequisites:** Sessions 9B.1 and 9B.2 are accepted. Revalidate their exact
manifests before use. This session occurs before ranking and final holdings,
not as selected-holding evidence in Session 9C.

**Frozen ADTV contract:** production AUM is `$200,000`; `target_n=15`;
equal-weight planned position size is `AUM / target_n = $13,333.333333...`;
and the maximum position is `1%` of ADTV. Therefore a candidate passes only
when
`median_30_session_dollar_volume >= (AUM / target_n) / 0.01 = $1,333,333.333333...`.
The earlier engine expression `AUM * 0.01` is not this contract and must not be
used.

For every candidate that passes the certified non-liquidity hard gates and has
both required OOS model roles, calculate daily dollar volume as unadjusted
regular-session close times regular-session volume for exactly the 30 valid
sessions whose `market_close < prediction_timestamp`; the evidence timestamp is
the candidate's `prediction_timestamp`, and the preserved Session 8E entry
timestamp remains the later execution timestamp. Take the median of those 30
daily values. Use only a hash-frozen, dated security mapping and volume payload
already certified or separately approved for this evidence session. Fewer than
30 valid observations, missing/nonpositive close or volume, ambiguous security
identity, missing timestamp, or absent payload fails the candidate closed with
an explicit reason. Compute this for the full eligible candidate set before
regression ranking; never compute liquidity only for a provisional top 15,
backfill from a later window, or silently disable the gate.

**Deliverable:** candidate-wide ADTV evidence/coverage/exclusion tables,
configuration, manifest, and report. No holdings, market-outcome calculation,
backtest, Session 9C, commit, or push.

#### Session 9B-R — Rerun Production `ml_gates` and Freeze Final Holdings

**Prerequisites:** Sessions 9B.1–9B.3 are complete and revalidate; every
production hard gate and both exact model roles are certified, or the path
retains an explicit blocker. The Ridge-versus-LightGBM ranking-family conflict
must be resolved by an explicit accepted contract before production holdings
can be named.

**Objective:** Rerun Session 9B into a new successor namespace. Apply all
non-liquidity hard gates, require the exact fold-local tree role, apply
`tree_prob >= 0.55`, apply the candidate-wide ADTV gate, rank remaining rows by
the accepted 3y regression role descending, then take exactly 15 and assign
`1/15` weights. Preserve the full candidate/exclusion lineage and both physical
populations. If fewer than 15 rows survive or any required evidence is missing,
freeze that period/path unavailable; do not shrink or renormalize.

**Deliverable:** successor Session 9B inventory, full candidate/exclusion
tables, complete holdings/weights if supported, manifest, and report. Only a
successful complete result unblocks Session 9C.

### Session 9C — Freeze Canonical Market Inputs and Prove Price Coverage

**Prerequisites:** Session 9B-R, after Sessions 9B.1–9B.3, has frozen at least
one compatible, explicitly named production `CORRECTED_8F` path with complete
15-row holdings, weights, exclusions, preserved entry timestamps, and
candidate-wide liquidity evidence; the Session 5A corporate-action and
canonical-return contracts are accepted. The original empty Session 9B freeze
does not satisfy this prerequisite.

**Exact scope:** freeze the canonical market inputs required for a later
controlled evaluation: prices/total returns, benchmarks, trading calendars,
time-aligned monthly risk-free returns, corporate-action/security-mapping
evidence, liquidity, turnover, and cost inputs. Produce row- and month-level
coverage for every Session 9B-R selected holding and benchmark in each physically
separate `CORRECTED_8F` population/strategy path.

Validate the adjusted/total-return source and adjustment vintage, prove that
dividends or corporate-action consideration cannot be counted both in adjusted
prices and the event ledger, and define each event return's price basis,
effective/settlement date, conversion terms, and post-event cash/security
treatment. Costs must map to actual buys, sells, retained positions, turnover,
and configured fees rather than an unexplained annual deduction.

The existing 448-ticker monthly cache was collected through static
`composite`/`qem`/`scdv` dry runs, excludes the official `ml_gates` collection
path, and is not presumed canonical. Partial coverage must fail closed or follow
the accepted explicit exclusion/sensitivity rule; it must not become zero return
silently. Preserve the existing cache and do not overwrite it.

**Stop conditions:** Stop if required market data would need an unapproved
external refresh, corporate-action evidence is insufficient for an affected
holding, monthly risk-free or benchmark coverage is incomplete for a claimed
metric, adjusted-price/event double counting cannot be ruled out, or coverage
cannot support annual/monthly reconciliation. Report exact tickers/months and
exclude the affected metric or comparison rather than filling silently.

**Deliverable:** frozen canonical-market-input manifest, holding/benchmark/
risk-free month coverage matrix, event-adjustment and transaction-cost
reconciliation, exclusions, and
`reports/pit_validation/09c_market_input_coverage.md`.

**Prompt:**

> Execute Session 9C from docs/CODEX_ROADMAP.md only after Sessions 9B.1–9B.3 and Session 9B-R freeze a complete compatible production CORRECTED_8F selection path. Revalidate the successor Session 9B manifest, then freeze canonical post-selection inputs and prove complete holding price, benchmark, monthly risk-free, turnover-aware cost, adjusted-price, and corporate-action coverage for every physical population/strategy namespace. Do not redo the candidate-wide preselection ADTV gate in 9C. Prove events are not double counted, preserve existing caches, fail closed on partial coverage, and do not run performance comparisons, refresh external data without explicit approval, begin Session 10, commit, or push.

### Session 10 — Controlled CORRECTED_8F Backtests

**Prerequisites:** Sessions 9B.1–9B.3, 9B-R, and 9C have frozen at least one
compatible production selection path and complete canonical market inputs for
every required holding, benchmark, risk-free month, cost, and event record.

**Objective:** Evaluate only the supported `CORRECTED_8F` selection paths using
the accepted Session 8B calendars, Session 9B holdings/weights, and Session 9C
canonical market inputs. Use identical frozen prices, benchmarks, universe,
dates, costs, filters, weighting, missing-price policy, and corporate-action
treatment across the physical population namespaces. `OLD_RECONSTRUCTED` is
unavailable and `CORRECTED_PARTIAL` is stale/prohibited; neither is a backtest
side. Present `LEGACY_SAVED` headline numbers only as an unreproducible
historical reference, never as a controlled comparator.

Report rule-based and ML-based strategies separately when appropriate. Calculate
monthly/annual returns, CAGR, Sharpe, drawdown dates/duration, Calmar, volatility,
downside deviation, best/worst month, negative months, turnover, gross/net return,
costs, benchmark comparison, and exact differences between supported explicitly
named strategy/population paths. Because Session 8E added zero policy-only rows,
record identical sensitivity results when proven; do not invent policy-50 or
policy-100 portfolios.

**Stop conditions:** Do not publish Sharpe or Sortino without complete aligned
monthly risk-free coverage; do not publish benchmark-relative metrics without a
complete canonical benchmark NAV; and do not publish a continuous observed-only
headline result across any unresolved selected-holding price or event period.
Never substitute a sensitivity or reduced-signal path for observed-only
performance, and never bridge an unavailable period with cash or a smaller
portfolio unless a separately predeclared sensitivity contract permits it.

**Deliverable:** explicit backtest artifacts and
`reports/pit_validation/10_corrected_8f_backtest.md`.

**Prompt:**

> Execute Session 10 from docs/CODEX_ROADMAP.md only after Sessions 9B.1–9B.3, 9B-R, and 9C are complete. Evaluate the frozen supported CORRECTED_8F selection paths using explicit predictions, holdings, accepted calendars, complete canonical market inputs, and one reconciled monthly NAV. Fail closed per metric on missing risk-free, benchmark, price, cost, or event evidence; keep every physical population and explicitly named strategy separate; do not run OLD_RECONSTRUCTED or CORRECTED_PARTIAL; and show LEGACY_SAVED only as an unreproducible reference. Report exact, unrounded results, differences, and limitations without threshold optimization.

### Session 11 — Partial PIT Report and Documentation Corrections

**Objective:** Consolidate Sessions 1–10, including 5A, 6A/6B, 7A, 8B, 8C,
8D–8F, 9B, and 9C,
into the official partial validation.

Update methodology wording for universe coverage, XBRL vintage loss,
winsorization, label availability, monthly NAV, and performance. Recompute metrics;
do not copy prior numbers. Clearly state stale Step 2 snapshots and incomplete
survivorship coverage.

**Deliverable:** updated `docs/PIT_AUDIT_FINAL_REPORT.md`, supporting methodology
docs, and a concise executive summary.

**Prompt:**

> Execute Session 11 from docs/CODEX_ROADMAP.md. Consolidate verified artifacts into the official PARTIAL PIT report, correct the methodology documentation, and state all remaining limitations plainly.

**Gate B:** Do not begin the SEC rebuild until the partial comparison is
reproducible and its limitations are accepted.

---

## Phase 3 — Full End-to-End PIT Validation

### Session 12 — SEC/XBRL Rebuild Preflight

**Objective:** Prove the earliest-primary Step 2 code, cache behavior, rate limits,
resume/checkpoint logic, storage needs, expected runtime, and validation sample
before starting the multi-hour crawl.

Do not perform the full crawl in this session. Produce a runbook with abort/resume
criteria and immutable source/output paths.

**Deliverable:** `reports/pit_validation/12_step2_rebuild_runbook.md`

**Prompt:**

> Execute Session 12 from docs/CODEX_ROADMAP.md. Perform a read-only/dry-run preflight for the SEC Step 2 rebuild and produce an executable runbook. Do not start the full crawl.

### Session 13 — Rebuild Step 2 Snapshots

**Objective:** Execute the approved SEC crawl with checkpoints. Preserve the old
snapshot, record API/cache behavior and hashes, and validate earliest-primary
selection using audited examples.

**Deliverable:** rebuilt snapshot under `full_pit`, manifest, crawl log, coverage
summary, and OLD-versus-new snapshot comparison.

**Prompt:**

> Execute Session 13 from docs/CODEX_ROADMAP.md using the approved runbook. Rebuild Step 2 with checkpoints, preserve all old artifacts, and validate the new XBRL snapshot evidence.

### Session 14 — Full Downstream Dataset Rebuild and Quality Gates

**Objective:** Run every required downstream stage from rebuilt snapshots through
the clean dataset, using corrected PIT transformations, horizon-qualified label
eligibility, and explicit observed/policy populations.

Run feature-contract, dataset-quality, bias, schema, and lineage checks. Fix
vacuous checks rather than accepting a pass that evaluated zero rows.

**Deliverable:** FULL-PIT dataset, manifest, quality reports, and controlled
`CORRECTED_8F`/`FULL_PIT` data comparison, with stale `CORRECTED_PARTIAL`,
unavailable `OLD_RECONSTRUCTED`, and `LEGACY_SAVED` shown only as separately
identified reference evidence.

**Prompt:**

> Execute Session 14 from docs/CODEX_ROADMAP.md. Build and validate the FULL-PIT dataset from rebuilt snapshots, run non-vacuous quality gates, compare it with the certified CORRECTED_8F feature population, and keep stale or unavailable historical paths reference-only.

### Session 15 — Full Retraining, OOS Predictions, and Backtest

**Objective:** Repeat the clean retraining, explicit prediction, and controlled
monthly backtest process on FULL-PIT data with frozen comparison inputs. Keep the
observed-only primary and policy-imputed sensitivity runs separate.

Report deterioration honestly. Do not optimize thresholds to recover the old
headline result during this validation.

**Deliverable:** full models/predictions/backtests/manifests plus controlled
`CORRECTED_8F`/`FULL_PIT` model, portfolio, and performance comparison.

**Prompt:**

> Execute Session 15 from docs/CODEX_ROADMAP.md. Retrain and evaluate FULL-PIT without threshold optimization, then compare the certified CORRECTED_8F and FULL_PIT paths explicitly while keeping stale CORRECTED_PARTIAL, unavailable OLD_RECONSTRUCTED, and LEGACY_SAVED reference-only.

### Session 16 — Final PIT Methodology Report

**Objective:** Produce the final evidence-backed report and update all public
claims to match the fully rebuilt artifacts.

Separate resolved issues from structural limitations such as historical security
master coverage, delisting returns, and earliest-primary versus true accession-level
as-of reconstruction.

**Deliverable:** final PIT report, limitations register, artifact index, and commit
plan. Commit/push only on explicit approval.

**Prompt:**

> Execute Session 16 from docs/CODEX_ROADMAP.md. Produce the final full-PIT methodology report from verified artifacts and prepare, but do not execute, the commit/push plan.

**Gate C:** The core US three-year validation is complete only after Session 16.

---

## Phase 4 — Complete the Four Chapters

These sessions build understanding and improve maintainability after the core
methodology is trustworthy. They must not silently change validated behavior.

### Session 17 — Chapter 1: Pipeline Spine, Columns, and Filing Behavior

**Objective:** Update the pipeline orientation with complete column inventories and
representative samples for every stage. Explain winsorization, near-zero YoY bases,
annual filing-month variation, survivorship alternatives, historical-universe
limits, and XBRL vintages in plain language.

**Deliverable:** updated pipeline orientation plus machine-generated column
dictionary/schema artifacts.

**Prompt:**

> Execute Session 17 from docs/CODEX_ROADMAP.md. Complete Chapter 1 with full pipeline columns, representative samples, and plain-language answers to the filing, winsorization, and survivorship questions. Documentation only.

### Session 18 — Chapter 2: Feature Inventory and Economic Rationale

**Objective:** Trace every production feature from raw source to transformation,
missingness/imputation, model eligibility, selected horizons, and economic rationale.
Flag aliases, duplicates, fragile formulas, near-zero denominator risks, and
features lacking a defensible story.

**Deliverable:** canonical feature registry and updated modeling orientation.

**Prompt:**

> Execute Session 18 from docs/CODEX_ROADMAP.md. Complete Chapter 2 by producing a canonical end-to-end feature registry with lineage, transformations, usage, and economic rationale. Do not change features.

### Session 19 — Chapter 3: Gates, Models, and Production Notebook

**Objective:** Trace all eight hard gates, decision-tree agreement, regression
ranking, ADTV filter, top-15 selection, configuration sources, and notebook behavior.
Identify duplicated thresholds, stale notebook logic, and any difference between
documented and executed production behavior.

**Deliverable:** gate/model execution map and notebook consistency report.

**Prompt:**

> Execute Session 19 from docs/CODEX_ROADMAP.md. Complete Chapter 3 by tracing every gate, model score, liquidity rule, and notebook step to its source of truth. Report inconsistencies without refactoring.

### Session 20 — Chapter 4: Trace Picks and Rejections

**Objective:** Select representative chosen stocks, near-misses, gate failures,
fraud/distress exclusions, and missing-data exclusions. Trace each from raw filing
through features, eligibility, scores, gates, ranking, portfolio inclusion, and
realized target.

**Deliverable:** auditable stock-level trace report with no investment recommendation.

**Prompt:**

> Execute Session 20 from docs/CODEX_ROADMAP.md. Complete Chapter 4 by tracing representative picks and rejected stocks end-to-end with exact data, gates, scores, ranks, and exclusion reasons.

### Session 21 — Controlled Architecture Cleanup

**Objective:** Revisit dead code, archive candidates, naming inconsistencies,
duplicated logic, stale docs, incomplete packages, and CI warnings using the now
validated maps.

Create a proposed change list first. Apply only low-risk, behavior-preserving
cleanup that has explicit approval and tests. Keep methodological changes out.

**Deliverable:** cleanup proposal, accepted minimal changes, and updated architecture docs.

**Prompt:**

> Execute Session 21 from docs/CODEX_ROADMAP.md. Review validated architecture evidence, propose dead-code and naming cleanup, and stop for approval before behavior-affecting changes.

---

## Completion Definition

The roadmap is complete when:

- every three-year training row obeys the accepted label-availability rule;
- no unintended global/future-aware fitted transformation remains;
- `LEGACY_SAVED`, stale `CORRECTED_PARTIAL`, and unavailable
  `OLD_RECONSTRUCTED` are preserved as explicitly non-comparable evidence,
  while `CORRECTED_8F` and `FULL_PIT` artifacts are reproducible and
  compatibility-validated;
- model predictions used by each backtest are explicit;
- all official performance claims use complete canonical market inputs and one
  annual/monthly-reconciled NAV;
- the full Step 2 rebuild has flowed through training and backtesting;
- structural universe/delisting/vintage limitations are stated honestly;
- all Four Chapters have auditable documentation;
- cleanup occurs only after methodological validation;
- relevant tests and non-vacuous real-data checks pass.
