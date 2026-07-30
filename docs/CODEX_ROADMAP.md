# Codex Roadmap — Canonical Product Consolidation

## Purpose

Consolidate the repository's existing work into one understandable,
reproducible dataset-to-screening product. Do not rebuild what already exists
or resume historical-performance certification.

## Product boundary

- One manifest-backed bronze/source layer.
- One cleaned longitudinal point-in-time dataset contract.
- One leakage-safe temporal research and model-evaluation route.
- Interpretable and sophisticated model options under the same split contract.
- One out-of-sample backtest and portfolio route with small-cap, liquidity,
  cost, and survivorship limitations explicit.
- One explainable ranked shortlist and report.
- Fraud risk remains one factor or safety gate.

## Session P1 — Read-only canonicalization decision

Map the existing implementation before changing it:

1. Identify canonical bronze/source artifacts and their manifests.
2. Trace the six-stage pipeline to cleaned-dataset outputs.
3. Map later PIT corrections against the main pipeline and identify true
   supersession, duplication, or unresolved conflicts.
4. Compare existing cleaned-dataset candidates on schema, time semantics,
   coverage, targets, provenance, and downstream compatibility.
5. Identify the single best existing temporal split, fold-local feature
   selection, model, backtest, liquidity, portfolio, and reporting route.
6. Classify relevant paths as canonical candidates, required dependencies,
   historical evidence, duplicates, unknowns, or future archive candidates.
7. Recommend exactly one bounded implementation task for P2.

Do not download or refresh data, call external services, edit or archive files,
retrain models, calculate performance, or revive V3.4.

**Deliverable:** a concise dependency/canonical-path map, evidence-backed
recommendation, blockers, archive candidates that require later confirmation,
and exactly one P2 implementation task.

## Session P2 — Establish one canonical data route

Implement only the task selected by P1. Prefer connecting or promoting existing
corrected components over creating new versions. Establish one canonical
source-to-cleaned-dataset command or contract, explicit validation failures,
lineage, and focused tests.

No model training or performance calculation unless P1 proves that the only P2
blocker belongs at that boundary.

**Deliverable:** one reproducible, documented canonical data route and an exact
statement of its coverage and remaining survivorship limitations.

## Session P3 — Establish one leakage-safe research/model route

Use the canonical dataset contract. Confirm target availability, temporal
splits, fold-local preprocessing and feature selection, and comparable
interpretable/sophisticated model interfaces. Reuse existing implementations.

**Deliverable:** one reproducible research/model command with OOS-only outputs
and focused leakage tests.

## Session P4 — Establish one product route

Connect OOS predictions to the existing backtest, small-cap/liquidity gates,
portfolio construction, and explainable report. Make costs and unresolved
survivorship boundaries visible. Do not claim future performance.

Only after the route is confirmed may clearly superseded files be archived in
one reviewed, dependency-safe change.

**Deliverable:** one usable dataset-to-shortlist product path, concise usage
documentation, and an honest limitations section.

## Deferred work

The archived V3.4/V3.5 canonical-market-ledger program is not an active
dependency. New data vendors, multi-market expansion, strategy proliferation,
threshold tuning, new feature families, new fraud models, and broad repository
reorganization remain deferred until the canonical product path works.

## Post-P4 continuation

P2-P4 are complete as the local reproducible product baseline. The approved
continuation is a controlled refactor, not a whole-repository rewrite. The
ordered consolidation, private publication, US refresh, free-source
survivorship/event ledger, backtest, nested tuning, international restoration,
and archive plan is frozen in
`docs/CANONICAL_REFACTOR_COMPLETION_PLAN.md`.

The exact next bounded implementation is Session C1 from that plan. It must
preserve the legacy international code structure and existing dirty worktree,
and it must not collect, publish, archive, branch, commit, or push without
separate authorization.
