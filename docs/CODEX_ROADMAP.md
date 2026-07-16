# Codex Roadmap — Workable Product

## Purpose

Turn the existing repository into one understandable, reproducible stock
screener. Product usability comes before additional research or historical
performance certification.

## Product boundary

- One supported entrypoint.
- One documented input dataset and model/factor path.
- One explainable ranked shortlist.
- Clear input date, eligibility rules, scores, and exclusions.
- Fraud risk remains one factor or safety gate, not the product identity.
- No performance claim unless it is separately validated later.

## Session P1 — Read-only product readiness

Inspect the existing notebook, application, portfolio, model, and data paths.
Identify the smallest route that is closest to producing a real shortlist.
Verify file presence, loadability, input dates, dependencies, and expected
outputs without changing repository or external state.

Do not download data, call external services, retrain models, edit files, run a
new backtest, or revive the V3 validation program.

**Deliverable:** a short readiness report containing confirmed facts, blockers,
and exactly one recommended implementation task.

## Session P2 — Make one path run

Implement only the task selected by P1. Establish one reproducible command or
notebook flow, explicit validation errors, and focused tests. Retire competing
entrypoints only after their useful behavior is accounted for.

**Deliverable:** one locally runnable screener path and usage instructions.

## Session P3 — Make inputs current enough

Define the minimum freshness contract for the supported product. Refresh only
the sources required by the P2 path, with bounded scope and clear provenance.
Do not rebuild unrelated markets, datasets, or research artifacts.

**Deliverable:** dated inputs, freshness checks, and a reproducible refresh
command.

## Session P4 — Product release boundary

Run the supported path, review the shortlist and explanations, execute focused
and full verification as justified, and remove or archive obsolete active
instructions. Historical research remains available but cannot compete with
the product entrypoint.

**Deliverable:** a usable screener, concise documentation, and an honest list of
limitations.

## Deferred work

The archived V3.4/V3.5 canonical-ledger and backtest program is not an active
dependency. Multi-market expansion, strategy comparison, parameter tuning,
new performance claims, and broad repository reorganization remain deferred
until the product works.
