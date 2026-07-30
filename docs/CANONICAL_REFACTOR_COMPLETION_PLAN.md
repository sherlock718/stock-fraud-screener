# Canonical Refactor Completion Plan

Status: approved planning baseline after Product Sessions P2-P4
Date: 2026-07-30

## Decision

Continue the refactor. Do not restart the repository from scratch.

P2-P4 already provide tested, manifest-backed contracts for the hardest
correctness boundaries: point-in-time availability, observed-only labels,
fold-local feature selection and preprocessing, row-complete out-of-sample
predictions, candidate-wide liquidity, portfolio construction, and
prediction-to-report lineage. A full rewrite would have to reproduce and
re-audit all of those contracts, while doing nothing by itself to solve the
actual external-evidence gaps in survivorship, corporate actions, historical
membership, and risk-free vintages.

The chosen approach is a controlled canonical-spine refactor:

1. keep P2-P4 as the reproducible baseline;
2. extract reused implementations out of historical `session_*` modules;
3. add one canonical orchestrator and versioned artifact publisher;
4. replace the legacy refresh route one boundary at a time;
5. archive superseded material only after dependency proof and replacement.

A component may be rewritten in isolation when its contract cannot be safely
extracted or tested. That is not authorization for a whole-repository rewrite.

## Fixed user decisions

1. Build and validate the canonical refresh for the US first.
2. Preserve the legacy international/per-market code structure and evidence so
   later markets can be restored without reconstructing the old design from
   memory.
3. Use free data sources for now. Design source adapters so paid security,
   membership, corporate-action, and news providers can be added later.
4. Design both historical and live event/M&A handling, beginning with the
   historical evidence ledger.
5. Preserve retired tracked code and documentation on a separate Git archive
   branch.
6. Continue using the prior Hugging Face dataset destination
   `ekrash718/stock-screener-data`, subject to a pre-publish visibility check.
   Canonical artifacts must be private, versioned, and non-overwriting.

No additional product decision is required to begin Session C1 below.
Commits, branch creation, external collection, and Hugging Face publication
still require their normal explicit authorization at the action boundary.

## What “free-only survivorship handling” means

The legacy inferred-delisting method was not useless. A hypothetical negative
return can be useful as a stress or policy-sensitivity scenario. It is not an
observed return and must not be mixed into the canonical observed-label table
or presented as certified performance.

Until stronger evidence exists:

- the primary dataset remains observed-only;
- unsupported exits remain explicitly unavailable;
- no disappearance is silently converted to a return;
- a named sensitivity namespace may test explicit assumptions, including the
  historical minus-50-percent scenario;
- sensitivity results must remain separate from primary labels, model fitting,
  and official performance;
- free historical membership, identity, delisting, and corporate-action
  evidence will be collected and coverage gaps quantified;
- paid adapters may later replace or supplement the same ledger contract
  without changing downstream interfaces.

This preserves the useful part of the earlier policy while removing the claim
that an assumed outcome is observed truth.

## Target active workflow

```text
external free sources
        |
        v
immutable raw payloads and source indexes
        |
        v
US universe + security/event ledger + filing availability
        |
        v
canonical P2-version dataset
        |
        v
canonical P3-version fold-local selection/models/OOS predictions
        |
        v
canonical P4-version gates/liquidity/portfolio/report
        |
        +--> live event review and cited LLM summary
        |
        +--> evidence-backed backtest when ledger/rate inputs pass
```

Deterministic reconstruction of an existing version must remain separate from
external refresh. A refresh always creates a new version and requires coverage,
schema, lineage, and artifact-hash comparison before promotion.

## Target repository architecture

The existing package boundaries remain useful:

- `pipeline/`: source contracts, corrected transformations, and canonical P2
  construction;
- `modeling/`: neutral fold, feature-selection, preprocessing, model, and
  prediction-lineage implementations plus the P3 builder;
- `portfolio/`: gates, candidate-wide liquidity, construction, and P4 builder;
- `backtest/`: security/event consumption, vintage accounting, costs, and
  monthly NAV;
- `data_io/`: versioned canonical Hugging Face publication and retrieval;
- `workflows/`: separate external-refresh and deterministic canonical
  reconstruction orchestrators;
- `quality/`: manifest, PIT, coverage, drift, and archive-dependency checks;
- `docs/archive/` or the archive branch: historical documentation after active
  replacements exist.

International modules, market mappings, calendars, and per-market snapshot
contracts must remain intact while the active route is US-first. They may be
moved or clearly marked legacy, but not deleted.

Large generated and raw artifacts do not belong in Git merely because the code
archive uses a Git branch. They remain in private artifact storage with exact
hash manifests. The archive branch preserves tracked code, configuration,
tests, and documentation.

## Ordered bounded sessions

### C1 — Canonical repository consolidation

Objective: make the completed P2-P4 route the obvious active spine without
changing its accepted behavior.

- [x] Review the complete dirty-worktree diff and generated P2-P4 records.
- [x] Produce a tracked dependency inventory classifying paths as canonical,
      source evidence, shared implementation, international legacy,
      historical evidence, cache, or unresolved.
- [x] Extract reusable P3/P4 helpers from `session_v3_*` modules into neutral
      modules.
- [x] Update canonical builders and focused tests to use the neutral modules.
- [x] Add one deterministic `workflows/run_canonical.py` entrypoint for
      P2 -> P3 -> P4 reconstruction from pinned inputs.
- [x] Preserve the old six-step and international entrypoints as explicitly
      labeled legacy code; do not delete them.
- [x] Update active FAQ, production configuration, workflow, and architecture
      documentation so they no longer advertise old performance or stale
      readiness claims.
- [x] Add an archive inventory with dependencies, replacements, sizes, hashes
      where applicable, and proposed destinations.
- [x] Run focused lineage/model/portfolio tests during extraction.
- [x] Run the full suite once at the final boundary.
- [x] Update `CHANGELOG.md`, `docs/START_HERE.md`, and
      `docs/CODEX_HANDOFF.md`.
- [x] After explicit approval, create a checkpoint commit that includes the
      reviewed P2-P4 work and C1 consolidation.
- [x] After that checkpoint, create the separate archive branch
      `codex/legacy-archive` before later tracked-file retirement.

Done when a new session can find and run the canonical local route without
reading historical V3 documents or importing active behavior from
historically named modules.

### C2 — Private canonical artifact publication

Objective: make the current P2-P4 baseline recoverable from a clean checkout.

Local evidence confirms the previous mechanism:

- dataset repository: `ekrash718/stock-screener-data`;
- dataset-type Hugging Face repository;
- authentication through `HF_TOKEN`;
- `data_io/push_to_hf.py` creates a private repository by default;
- `data_io/pull_from_hf.py` uses the same repository by default;
- legacy files were uploaded individually at repository-root paths.

The existing repository's actual current visibility has not been externally
verified. Publication must fail closed if it is public or cannot be verified
as private.

- [ ] Add a canonical publisher and retriever; do not extend the legacy root
      upload list silently.
- [ ] Verify the existing repository is private before uploading.
- [ ] Use immutable versioned paths such as
      `canonical/<artifact-name>/<manifest-sha256>/...`.
- [ ] Upload P2, P3, and P4 manifests plus every referenced generated record
      needed for reconstruction or consumption.
- [ ] Preserve existing legacy root files unchanged.
- [ ] Record repository, repository type, revision, relative path, byte size,
      and SHA-256 in small tracked pointer manifests.
- [ ] Download into a temporary target and independently verify every record.
- [ ] Add CI verification that does not fall back to mutable `latest`.
- [ ] Publish only after explicit approval and without exposing `HF_TOKEN`.

Done when a clean checkout can retrieve the exact current P2-P4 baseline and
reconcile it byte for byte.

### D1 — US canonical raw-refresh replacement

Objective: replace the legacy six-stage US refresh without losing the legacy
international structure.

- [ ] Preserve exact raw responses and collection timestamps before parsing.
- [ ] Version the US universe rather than overwriting a ticker list.
- [ ] Reuse corrected Step 2 filing materialization and availability rules.
- [ ] Build explicit price, benchmark, calendar, decision, and label-support
      contracts.
- [ ] Keep macro fields unavailable until release-vintage evidence is
      certified; never insert current FRED values into historical rows.
- [ ] Reuse corrected PIT Step 5 feature transformations.
- [ ] Run Step 6 observed-only, with inferred delisting returns disabled.
- [ ] Generate a new non-overwriting P2 version.
- [ ] Compare row identity, schema, feature coverage, label support, missingness,
      gates, and source drift against the pinned P2 baseline.
- [ ] Require explicit promotion before new P3/P4 versions consume it.
- [ ] Retain international collectors, mappings, and per-market artifact
      contracts behind inactive/legacy entrypoints.

Done when fresh free US data can create a reviewable new P2 version without
mutating the reproducible baseline.

### S1 — Free historical security and survivorship ledger

Objective: improve the survivorship boundary as far as free evidence permits.

- [ ] Define stable issuer, security, ticker, exchange, and effective-date
      schemas with provider-neutral interfaces.
- [ ] Collect free historical SEC identity/submission evidence and available
      exchange/security-list evidence with raw payload preservation.
- [ ] Record ticker and exchange changes, mergers, bankruptcies, suspensions,
      delistings, and security-type changes when evidenced.
- [ ] Record source, retrieval time, effective time, and confidence for every
      event.
- [ ] Define price-adjustment and corporate-action semantics.
- [ ] Reconcile every P4 holding and required benchmark instrument.
- [ ] Report matched, ambiguous, and unsupported population coverage.
- [ ] Keep unsupported exits unavailable in primary results.
- [ ] Add separately labeled sensitivity scenarios, including the legacy
      minus-50-percent policy, without feeding them into observed-label model
      training.
- [ ] Preserve a paid-provider adapter boundary for later adoption.

Done when the exact free-source coverage and unresolved survivorship boundary
are measurable. Free-only evidence may still be insufficient for official
performance; that outcome is acceptable and must fail closed.

### E1 — Historical then live event/M&A layer

Objective: replace the old ungrounded LLM warning with cited evidence.

- [ ] Begin with the historical security/event ledger and effective-time
      eligibility.
- [ ] Define warn/exclude policies for pending acquisitions, bankruptcy,
      suspension, delisting, and other material events.
- [ ] Preserve dated source documents and their hashes.
- [ ] Apply deterministic rules before any LLM call.
- [ ] Add a live current-shortlist evidence collector using the same schema.
- [ ] Allow an LLM to summarize only the retrieved, cited evidence.
- [ ] Preserve human review for ambiguous live cases.
- [ ] Prohibit current model knowledge or uncited claims in historical
      backtests.

Done when every event warning can be traced to a dated source and reproduced,
and the historical and live modes are clearly separated.

### B1 — Evidence-backed backtest

Objective: calculate performance only after required inputs pass.

- [ ] Complete or explicitly fail the S1 market-ledger coverage contract.
- [ ] Acquire the immutable required `DGS1MO` ALFRED vintage from a free
      authoritative source and preserve its raw evidence.
- [ ] Validate entries, exits, adjustments, benchmarks, common sessions, and
      unresolved events for every vintage.
- [ ] Run annual July 2 decisions with independent overlapping 36-month
      vintages.
- [ ] Apply 25 bps per side to absolute actual traded notional.
- [ ] Produce monthly gross and net NAV with the canonical fail-closed engine.
- [ ] Report evidence coverage before CAGR, volatility, drawdown, Sharpe, or
      comparisons.
- [ ] Keep all old V3 performance claims historical and non-transferable.

Done when every reported metric is reproducible from versioned evidence. If
free data cannot satisfy the ledger contract, official performance remains
unavailable and the exact blocker is documented.

### M1 — Nested temporal tuning and gate-order comparison

Objective: optimize only after a valid evaluation framework exists.

- [ ] Preserve current fixed-parameter P3 as the untouched baseline.
- [ ] Add inner temporal train/validation folds inside outer walk-forward
      evaluation folds.
- [ ] Refit feature selection, preprocessing, and tuning inside each inner
      fold.
- [ ] Tune decision-tree and LightGBM roles independently.
- [ ] Compare broad-universe training with post-model gates,
      gate-eligible-universe training, and broad training with gate values as
      features.
- [ ] Keep structural eligibility and PIT availability before training.
- [ ] Keep model thresholds, safety constraints, liquidity, and portfolio rules
      after OOS scoring unless a predeclared experiment says otherwise.
- [ ] Compare coverage, stability, turnover, costs, and performance across
      regimes.
- [ ] Publish any accepted result as a new version, never as an overwrite of
      P3.

Done when the model/gate choice is selected by predeclared nested temporal
criteria rather than reuse of outer OOS results.

### I1 — Restore international markets incrementally

Objective: use the preserved legacy structure to add validated markets after
the US route is stable.

- [ ] Select one additional market as the first adapter test.
- [ ] Validate local filing availability, identities, exchange calendars,
      benchmarks, currencies, accounting comparability, and corporate actions.
- [ ] Implement the common source/P2 contract through a market adapter.
- [ ] Add currency-aware targets, liquidity, costs, and portfolio semantics.
- [ ] Repeat P2-P4 validation for that market.
- [ ] Expand market by market; do not assume the US contract transfers
      unchanged.

Done when each restored market has its own evidence-backed contract. No legacy
market code or mapping is deleted before its replacement is proven.

### A1 — Dependency-safe archive and retirement

Objective: make the active repository unambiguous while retaining recoverable
history.

- [ ] Use `codex/legacy-archive` for tracked legacy code, tests,
      configuration, notebooks, and documents.
- [ ] Keep large/gitignored artifacts in private Hugging Face artifact storage,
      not in the Git branch.
- [ ] Archive the first confirmed PIT candidates:
      `corrected_partial`, `corrected_partial_inputs`, and the Session 9B
      zero-holdings freeze.
- [ ] Archive superseded V3 generated artifacts after pointer and dependency
      verification.
- [ ] Archive historical models, backtests, reports, and notebooks only after
      active consumers have replacements.
- [ ] Rewrite active FAQ/configuration documents rather than merely moving
      their stale claims.
- [ ] Hash and manifest every material archive package.
- [ ] Test recovery before removing an active-tree copy.
- [ ] Never archive canonical P2-P4 artifacts, corrected source evidence, or
      still-imported shared implementations.

Done when the primary branch exposes one canonical workflow and every retired
component has a recoverable location and named replacement.

### REL1 — Consolidated release boundary

- [ ] Verify a clean P2 -> P3 -> P4 reconstruction.
- [ ] Verify private Hugging Face recovery and hashes.
- [ ] Run focused verification and the full suite once.
- [ ] Review the diff and archive inventory.
- [ ] Update changelog, onboarding, handoff, limitations, and next task.
- [ ] Commit, push, and tag only after explicit approval.
- [ ] Retain clear statements that the product is research, free-source
      survivorship coverage may remain incomplete, and no future performance
      is promised.

## Completion definition

The refactor is complete only when:

1. one documented US raw-refresh path creates versioned source evidence;
2. one documented deterministic path reconstructs P2-P4;
3. active canonical code no longer imports historically named session modules;
4. P2-P4 are recoverable from private immutable Hugging Face references;
5. survivorship/event coverage is measured, with assumptions isolated as
   sensitivity analysis;
6. performance either passes the evidence contract or remains explicitly
   unavailable;
7. tuning uses nested temporal validation and a new artifact version;
8. international legacy structure remains recoverable for later adapters;
9. tracked legacy material is preserved on the archive branch and large
   artifacts are hash-manifested in private storage;
10. active documentation names one workflow and does not transfer old
    performance claims.

## Exact next-session task

Execute Session C1 only. Start with `docs/START_HERE.md`,
`docs/CODEX_HANDOFF.md`, this plan, and the three canonical P2-P4 manifests.
Preserve the dirty worktree and all canonical/legacy artifacts. Build the
dependency classification, extract active shared functions out of historical
`session_v3_*` names, add the deterministic canonical orchestrator, update
active documentation, and materialize a non-destructive archive inventory.
Run focused tests during the work and the full suite once at the final
boundary. Do not collect external data, publish to Hugging Face, move archive
files, create a branch, commit, or push unless separately and explicitly
authorized.
