# Canonical Refactor Completion Plan

Status: Session REL1 clean-checkout verification complete; release blocked pending durable sources and explicit authorization
Date: 2026-07-31

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

The product sequence is now explicit. V1 uses the best reproducible free data
available and reports survivorship-aware historical performance with observed
evidence, unresolved coverage, and policy scenarios physically separated. V2
may later replace ambiguous or assumed V1 inputs with paid authoritative data
through the same provider-neutral contracts. Paid data no longer blocks V1,
but V1 must not be described as survivorship-complete or provider-certified.

## Fixed user decisions

1. Build and validate the canonical refresh for the US first.
2. Preserve the legacy international/per-market code structure and evidence so
   later markets can be restored without reconstructing the old design from
   memory.
3. Build V1 with the best available free data, including a transparent
   survivorship-aware historical backtest and performance report. Design source
   adapters so paid security, membership, corporate-action, price, and news
   providers can be added for V2 without changing downstream interfaces.
4. Design both historical and live event/M&A handling, beginning with the
   historical evidence ledger.
5. Preserve retired tracked code and documentation on a separate Git archive
   branch.
6. Continue using the prior Hugging Face dataset destination
   `ekrash718/stock-screener-data`, subject to a pre-publish visibility check.
   Canonical artifacts must be private, versioned, and non-overwriting.
7. Calculate CAGR, annualized volatility, Sharpe, Sortino, maximum drawdown,
   Calmar, benchmark-relative metrics, turnover, and costs for V1 only after
   the free-data evidence tiers, survivorship scenarios, and P4-compatible NAV
   clock are frozen and tested. Every metric must identify its evidence and
   scenario namespace.

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
- unsupported exits remain explicitly unavailable in the observed namespace;
- no disappearance is silently converted to a return;
- named V1 performance namespaces may test explicit assumptions, including the
  historical minus-50-percent scenario and a separately predeclared terminal-
  loss stress; the exact scenario set and timing rules must be frozen before
  calculation;
- observed, best-free-evidence, and policy-sensitivity results must remain
  physically separate from primary labels and model fitting;
- no holding may be silently dropped from NAV because its identity, price, or
  terminal outcome is unresolved;
- every V1 performance table must lead with holding/capital coverage, unresolved
  exposure, scenario-imputed exposure, benchmark coverage, and rate coverage;
- a zero-risk-free Sharpe may be reported only under an explicit name when the
  exact free `DGS1MO` vintage remains unavailable; an excess-return Sharpe
  requires the hash-pinned vintage specified by the contract;
- free historical membership, identity, delisting, and corporate-action
  evidence will be collected and coverage gaps quantified;
- paid adapters may later replace or supplement the same ledger contract
  without changing downstream interfaces.

This preserves the useful part of the earlier policy while removing the claim
that an assumed outcome is observed truth. V1 performance is therefore a
reproducible free-source research result with explicit scenario dependence;
V2 is the later paid-data certification and comparison layer.

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
        +--> free-data V1 observed/scenario backtest and performance report
        |
        +--> paid-data V2 replacement and certification through same contracts
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

- [x] Add a canonical publisher and retriever; do not extend the legacy root
      upload list silently.
- [x] Verify the existing repository is private before uploading.
- [x] Use immutable versioned paths such as
      `canonical/<artifact-name>/<manifest-sha256>/...`.
- [x] Upload P2, P3, and P4 manifests plus every referenced generated record
      needed for reconstruction or consumption.
- [x] Preserve existing legacy root files unchanged.
- [x] Record repository, repository type, revision, relative path, byte size,
      and SHA-256 in small tracked pointer manifests.
- [x] Download into a temporary target and independently verify every record.
- [x] Add CI verification that does not fall back to mutable `latest`.
- [x] Publish only after explicit approval and without exposing `HF_TOKEN`.

Done when a clean checkout can retrieve the exact current P2-P4 baseline and
reconcile it byte for byte.

C2 completion: after explicit approval and informed re-authorization, the user
ran the guarded publisher directly because Codex's environment prohibited the
export. The exact 202-file, 481,666,707-byte P2-P4 baseline was published
privately at immutable revision
`aaf056ea115067e42ef9abf9fa93ade75cdd4052`. The command completed its
temporary byte/hash recovery before materializing all three pointers.
Independent metadata reconciliation found all 202 expected paths present,
zero missing, and private visibility retained. No legacy root path was
overwritten. The three exact pointer files are materialized, locally
validated, and included in the separately authorized C2 checkpoint, completing
strict clean-checkout recovery.

### D1 — US canonical raw-refresh replacement

Objective: replace the legacy six-stage US refresh without losing the legacy
international structure.

- [x] Preserve exact raw responses and collection timestamps before parsing.
- [x] Version the US universe rather than overwriting a ticker list.
- [x] Reuse corrected Step 2 filing materialization and availability rules.
- [x] Build explicit price, benchmark, calendar, decision, and label-support
      contracts.
- [x] Keep macro fields unavailable until release-vintage evidence is
      certified; never insert current FRED values into historical rows.
- [x] Reuse corrected PIT Step 5 feature transformations.
- [x] Run Step 6 observed-only, with inferred delisting returns disabled.
- [x] Generate a new non-overwriting P2 version.
- [x] Compare row identity, schema, feature coverage, label support, missingness,
      gates, and source drift against the pinned P2 baseline.
- [x] Require explicit promotion before new P3/P4 versions consume it.
- [x] Retain international collectors, mappings, and per-market artifact
      contracts behind inactive/legacy entrypoints.

Done when fresh free US data can create a reviewable new P2 version without
mutating the reproducible baseline.

Completed 2026-07-30 with review-only refresh version
`20260730T110301Z`. The pinned P2-P4 manifests and private Hugging Face
revision remain unchanged; the new P2 candidate is not promoted.

### S1 — Free historical security and survivorship ledger

Objective: improve the survivorship boundary as far as free evidence permits.

- [x] Define stable issuer, security, ticker, exchange, and effective-date
      schemas with provider-neutral interfaces.
- [x] Collect free historical SEC identity/submission evidence and available
      exchange/security-list evidence with raw payload preservation.
- [x] Record ticker and exchange changes, mergers, bankruptcies, suspensions,
      delistings, and security-type changes when evidenced.
- [x] Record source, retrieval time, effective time, and confidence for every
      event.
- [x] Define price-adjustment and corporate-action semantics.
- [x] Reconcile every P4 holding and required benchmark instrument.
- [x] Report matched, ambiguous, unsupported, and conflicting coverage.
- [x] Keep unsupported exits unavailable in primary results.
- [x] Add separately labeled sensitivity scenarios, including the legacy
      minus-50-percent policy, without feeding them into observed-label model
      training.
- [x] Preserve a paid-provider adapter boundary for later adoption.

Done when the exact free-source coverage and unresolved survivorship boundary
are measurable. Free-only evidence may still be insufficient for official
performance; that outcome is acceptable and must fail closed.

Completed 2026-07-30 with offline ledger version
`artifacts/security_ledger/us/20260730T141429Z-s1-final/`, built only from the
exact SEC index and submissions responses already frozen by D1. The 184
requirements comprise all 180 P4 holding rows and four benchmark instruments:
0 matched, 135 ambiguous, 49 unsupported, and 0 conflicting. The ledger
records 49 Form 25 delisting indicators, 14 registration-termination
indicators, and 10 explicit Item 1.03 bankruptcy indicators, all unresolved
where primary terms or effective times are absent. No unsupported ticker,
exchange, merger, suspension, security-type, action, or return event was
invented. Official performance remains unavailable.

### E1 — Historical then live event/M&A layer

Objective: replace the old ungrounded LLM warning with cited evidence.

- [x] Begin with the historical security/event ledger and effective-time
      eligibility.
- [x] Define warn/exclude/unresolved/human-review policies for pending
      acquisitions, completed mergers, bankruptcy, suspension, delisting,
      registration termination, exchange noncompliance, and other material
      events.
- [x] Preserve dated source documents and their hashes.
- [x] Apply deterministic rules before any LLM call.
- [x] Add a live current-shortlist evidence collector using the same schema.
- [x] Allow an LLM to summarize only the retrieved, cited evidence.
- [x] Preserve human review for ambiguous live cases.
- [x] Prohibit current model knowledge or uncited claims in historical
      backtests.

Done when every event warning can be traced to a dated source and reproduced,
and the historical and live modes are clearly separated.

Completed 2026-07-30 under
`artifacts/event_review/us/20260730T144043Z-e1-final/`, manifest SHA-256
`e44ff9aa9d2ac3be2ae66e5fb006bba435cdda3eef4c4521b4d1e3362a58caa6`.
All 180 historical holdings remain unresolved because S1 has no exact event
effective times or proven security scope; no filing indicator was upgraded to
an event. The current 15-name contract routes every name to human review and
freezes an exact, non-overwriting 47-request SEC primary-document plan. No
external request or LLM call was made, and official performance remains
unavailable.

The later separately authorized continuation exhausted the exact 47-request
plan, preserved and extracted the responses, and completed deterministic
citation adjudication under
`artifacts/event_review/us/20260730T173110Z-e1-adjudication-v2/`, manifest
SHA-256
`dcbf95776b50139797410674d7f0ca410cfdaa1401917dea4fb47f7b00e9c7c6`.
No document or current name satisfies the complete contract. The subsequent
presentation derivative is frozen under
`artifacts/final_shortlist/us/20260731T000054Z-final-shortlist-v2/`, manifest
SHA-256
`04965561433188242307574b7238866aa2a436828b630bce29d64a6544f9ee95`.
Neither continuation changes S1 coverage or unlocks performance.

### B1A — Existing-local-evidence decision — completed 2026-07-31

Objective: decide whether the existing local evidence can support B1 without
collecting data or calculating performance.

- [x] Reverify the exact P4, S1, E1-adjudication, and final-shortlist manifest
      hashes.
- [x] Inspect the P4 backtest-input contract and the current monthly-NAV
      fail-closed implementation.
- [x] Classify every B1 input as complete, ambiguous, potentially obtainable
      for free, paid-provider-only, or methodologically unresolved.
- [x] Test whether any preserved S1 or E1 evidence can improve historical
      security/action coverage without inference.
- [x] Verify the exact required `DGS1MO` provenance, release clock, and ALFRED
      vintage and search preserved local artifacts for a qualifying copy.
- [x] Separate official-performance blockers from inputs usable only in a
      clearly labeled sensitivity analysis.

The decision is `unavailable_fail_closed` for current local/free evidence:

- S1 covers 184 requirements as 0 matched, 135 ambiguous, 49 unsupported, and
  0 conflicting. The 49 unsupported requirements are 45 still-open 2024-2026
  holdings plus four benchmark requirements; the 135 matured holdings remain
  ambiguous.
- E1 adjudication upgrades none of the historical ledger requirements: 0/47
  documents and 0/15 current names satisfy its complete deterministic
  contract.
- No preserved artifact contains the required Federal Reserve H.15 `DGS1MO`
  ALFRED vintage with `realtime_start=realtime_end=2026-07-17` and the
  contract's release-time eligibility.
- Frozen Yahoo payloads remain liquidity evidence only. They cannot establish
  security continuity, action terms, delisting returns, benchmark identity, or
  official performance.
- `backtest/monthly_nav.py` supplies useful fail-closed monthly arithmetic but
  does not yet implement P4's independent July, overlapping 36-month,
  gross-and-net vintage contract.

This closes only the no-new-evidence B1 decision. It does not claim that a
future evidence-backed backtest is impossible.

### Q1 — Frozen P3 predictive-quality audit — completed 2026-07-31

Objective: determine whether the already-frozen OOS model outputs contain
stable predictive signal worth funding with better market-history evidence.

- [x] Reverify P2-P4 lineage and use only existing P3 OOS predictions and
      observed labels.
- [x] Calculate regression diagnostics by decision year: Spearman information
      coefficient, Pearson correlation, MAE, RMSE, and predeclared
      quantile/top-15 lift.
- [x] Calculate decision-tree diagnostics by decision year: ROC AUC, PR AUC,
      Brier score, calibration, and the frozen 0.55-threshold confusion table.
- [x] Compare against simple decision-time baselines without fitting or tuning
      on the outer OOS rows.
- [x] Report stability by year and, where existing fields support it, sector
      and size; report label coverage, missing-label bias, and concentration.
- [x] Classify the evidence as no, weak, moderate, or strong signal and issue
      one go/no-go recommendation for a paid-provider fit-gap task.
- [x] Do not retrain, tune thresholds, form new portfolios, calculate trading
      performance, or promote an artifact.

Done when the value of obtaining backtest-grade data can be judged from model
evidence rather than assumed from shortlist construction.

Q1 classified the evidence as moderate predictive signal. On 16,597 labeled
OOS rows, LightGBM produced pooled Spearman IC 0.138 and positive annual IC in
all ten labeled decision years, while top-15 target lift was positive in only
five. The tree produced pooled ROC AUC 0.569 and median annual AUC 0.556, but
its calibration and Brier score were weak. Mature-year label coverage was
62.4 percent and strongly conditional on decision-time size/price support.
This supports continuing to a disciplined V1 backtest, not a claim of proven
tradable performance.

After Q1, the user selected free-data V1 first and paid-data V2 later. The
original Q1 recommendation to inspect paid-provider fit is therefore deferred
to V2; it is not a prerequisite for the V1 sessions below.

### B1B — Free-data V1 performance contract — completed 2026-07-31

Objective: freeze the most defensible free-data evidence, survivorship,
scenario, NAV, metric, and disclosure contract before collecting more data,
changing backtest code, or calculating performance.

- [x] Reverify P2-P4, Session 8E, S1, E1, and Q1 facts before relying on any
      price, identity, event, label, or prediction field.
- [x] Inspect the legacy survivorship modes and current monthly-NAV code without
      executing performance, and identify behavior that can be reused only
      after it satisfies the P4 clock and separation contracts.
- [x] Map every one of the 180 P4 holdings and four benchmark requirements to
      exact free-source price, identity, action, and terminal-outcome evidence;
      distinguish observed, document-supported but incomplete, ambiguous,
      unsupported, and open-vintage states.
- [x] Freeze separate namespaces for observed-available diagnostics, the
      best-free-evidence full-accounting scenario, the legacy minus-50-percent
      sensitivity, and a conservative terminal-loss stress. Define exact
      event dates, missing-price rules, recovery assumptions, and prohibitions
      before any metric is calculated.
- [x] Define how unresolved capital remains visible in NAV and prohibit silent
      row deletion, forward filling across terminal events, current-ticker
      substitution without lineage, and mixing assumed returns into observed
      labels.
- [x] Freeze the P4 annual July decision, first-common-session entry, separate
      overlapping 36-month vintages, 25-bps-per-side actual-traded-notional
      cost policy, benchmark sleeves, and completed-vintage boundary.
- [x] Freeze formulas and minimum inputs for gross/net NAV, CAGR, annualized
      volatility, maximum drawdown, Sharpe, Sortino, Calmar, beta, alpha,
      tracking error, information ratio, turnover, hit rate, and coverage.
- [x] Define an exact free `DGS1MO` acquisition path and a separately named
      zero-risk-free Sharpe fallback if that exact vintage cannot be preserved.
- [x] Specify the V1 report language, with free-source and scenario dependence
      adjacent to every headline metric and no `survivorship-complete`,
      `certified`, or future-performance claim.
- [x] End with one bounded B1C implementation task and its focused tests.

This session is read-only. No model run, data collection, performance
calculation, portfolio change, provider trial, or purchase is authorized.

### B1C — Free-data V1 evidence assembly — completed 2026-07-31

Objective: after explicit authorization, build one versioned performance-input
artifact from the free sources and policies approved by B1B.

- [x] Reuse and independently reverify eligible frozen Session 8E Yahoo and
      SEC evidence before making any new request.
- [x] Collect only the targeted free identity, action, terminal-outcome, price,
      benchmark, and calendar evidence separately authorized after B1B; retain
      exact raw responses, timestamps, failures, sizes, and hashes.
- [x] Collect and hash the exact free authoritative `DGS1MO` ALFRED vintage if
      available under the frozen provenance contract; otherwise preserve the
      explicit failure and keep the zero-risk-free namespace separate.
- [x] Build provider-neutral security, listing, action, price, benchmark, and
      rate tables for every holding requirement without silently replacing an
      unresolved identity or event.
- [x] Reconcile every matured P4 holding and required benchmark to an observed
      evidence state or an explicit B1B scenario state.
- [x] Keep 2024-2026 vintages explicitly open until their 36-month horizons
      mature.
- [x] Produce holding-count, capital-weight, session, action, benchmark, and
      rate coverage evidence before any performance metric.
- [x] Materialize a non-overwriting manifest-backed artifact; do not calculate
      performance in this session.

### B1D — P4-compatible monthly-NAV implementation — completed 2026-08-01

Objective: implement and test the frozen P4 portfolio clock and metric engine
against fixtures without running the historical study.

- [x] Support annual July 2 decisions and independent overlapping 36-month
      vintages.
- [x] Apply 25 bps per side to absolute actual traded notional.
- [x] Produce monthly gross and net portfolio and benchmark NAV.
- [x] Account for every holding; unresolved identity, session, adjustment,
      event, price, benchmark, or terminal outcome must either fail the
      observed namespace closed or use only an explicitly selected B1B
      scenario resolver.
- [x] Never rebalance an earlier vintage because a later vintage is formed;
      calculate both separate-vintage and aggregate-strategy NAV without
      double-counting capital.
- [x] Calculate gross and net CAGR, annualized volatility, maximum drawdown,
      Sharpe, Sortino, Calmar, beta, alpha, tracking error, information ratio,
      turnover, hit rate, and coverage from monthly series.
- [x] Keep incomplete vintages, observed-available diagnostics, best-free-
      evidence results, and each sensitivity scenario physically separate.
- [x] Add focused tests for missing prices, terminal events, ticker changes,
      cash/stock mergers, bankruptcies, delistings, costs, overlapping
      vintages, benchmark alignment, risk-free alignment, and metric formulas.

### B1E — Controlled free-data V1 backtest and product report — completed 2026-08-01

Objective: calculate and present the best reproducible free-data V1 performance
only after B1C and B1D pass.

- [x] Reverify all evidence and implementation hashes.
- [x] Require complete accounting before calculating metrics: every holding
      must resolve to observed evidence, an explicit scenario outcome, or a
      reported fail-closed state; silent deletion is prohibited.
- [x] Run only matured 2015-2023 three-year vintages; keep 2024-2026 outside
      completed-vintage performance.
- [x] Report holding and capital coverage, ambiguous/unsupported exposure,
      scenario-imputed exposure, benchmark coverage, and rate coverage before
      CAGR, volatility, drawdown, Sharpe, Sortino, or benchmark comparisons.
- [x] Report gross and net results for the observed-available diagnostic,
      best-free-evidence scenario, legacy minus-50-percent sensitivity, and
      conservative terminal-loss stress without blending namespaces.
- [x] Report separate-vintage and aggregate overlapping-strategy results, with
      transaction costs, benchmark comparisons, and uncertainty attributable
      to unresolved survivorship evidence.
- [x] Keep every assumption-based result explicitly labeled and place its
      assumptions beside the metric rather than only in an appendix.
- [x] Keep all old V3 performance claims historical and non-transferable.
- [x] Produce a concise V1 product report that states these are free-source
      historical research results, not survivorship-complete certification,
      personalized advice, or a future-performance promise.

Done when every V1 metric is reproducible from versioned evidence and a named
scenario, every unresolved exposure is quantified, and no missing holding is
silently removed. Paid-data certification remains a V2 task.

### V2A — Paid-provider fit-gap and certification decision — user-deferred 2026-08-01

Objective: after V1, determine whether paid data can materially reduce
uncertainty and justify a certified V2 rerun before spending money.

- [ ] Map candidate providers to the exact provider-neutral V1 contracts for
      historical membership, identity, ticker/exchange history, security type,
      corporate actions, delisting terms and returns, adjusted/unadjusted
      prices, benchmark identity, timestamps, revisions, export/API access,
      licensing, cost, and platform compatibility.
- [ ] Test a predeclared set of the hardest V1 ambiguous and unsupported cases
      using documentation, samples, or a trial only when separately authorized.
- [ ] Obtain exact prices and terms and distinguish subscription cost from
      integration and recurring operating cost.
- [ ] Estimate how much V1 capital/scenario exposure each provider would
      convert to observed evidence.
- [ ] Recommend exactly one provider, a mixed-source design, or no purchase.
- [ ] If later authorized, collect a non-overwriting V2 artifact, rerun the
      unchanged B1D engine, and report the exact metric divergence from V1.

No purchase, trial activation, external request, or collection is authorized
by this plan entry. The user has explicitly deferred paid-data exploration in
favor of making the free-source route as strong as possible. V2A remains a
preserved optional future task and is not the current next session.

### M1 — Nested temporal tuning and gate-order comparison

Objective: improve feature selection and the existing free-data LightGBM and
interpretable-tree roles only through predeclared nested temporal validation.
Preserve P3 and B1E as untouched baselines; never tune against B1E, a scenario
choice, CAGR, Sharpe, drawdown, or another outer-OOS headline result.

#### M1A — Freeze the experiment contract

- [x] Independently verify the frozen P2, P3, P4, B1D, and B1E hashes and the
      current fold-lineage, feature-selection, preprocessing, and model code.
- [x] Preserve the exact P3 outer walk-forward folds as the final OOS boundary;
      use no random split and no reuse of an outer-OOS row or label.
- [x] Define expanding-window inner folds wholly inside each outer-training
      population. Every inner-training label must end before its validation
      decision, and every label used for tuning must be observable before the
      corresponding outer decision. Purge any crossing three-year horizon.
- [x] Freeze fold-local fitting for missingness rules, winsorization,
      imputation, encoding, scaling, feature ranking, redundancy pruning,
      stability selection, and hyperparameter choice. Refit on the complete
      outer-training fold only after the inner choice is frozen.
- [x] Start only from the existing P3-eligible point-in-time feature universe.
      Exclude targets, target support, label-availability fields, row identity,
      timestamps, future prices, model outputs, policy-imputed outcomes, and
      uncertified macro fields.
- [x] Compare the current feature-selection baseline with one bounded
      stability-selection and redundancy-pruning variant. Record selection
      frequency, sign/direction stability, fold-level IC stability,
      missingness, redundancy, and selected-set size.
- [x] Keep only the existing LightGBM regression and interpretable decision-
      tree roles. Freeze small bounded grids and deterministic seeds before
      execution; do not add another model family.
- [x] Select LightGBM by predeclared inner-fold rank-prediction evidence,
      including median Spearman IC, fold dispersion, and positive-fold
      frequency. Select the tree by inner-fold ROC AUC with Brier/calibration
      diagnostics and an explicit simplicity preference.
- [x] Freeze tie-breakers that prefer lower dispersion, fewer features, and
      lower model complexity. The winner rule must be executable without any
      outer-OOS or portfolio-performance value.
- [x] Freeze three regimes: existing broad-universe training with downstream
      gates; gate-eligible training using only PIT-available gates; and broad
      training with gate values as features only when they satisfy the feature
      contract.
- [x] Treat 30% aggregate net CAGR and 1.0 zero-risk-free diagnostic Sharpe
      only as final reporting thresholds. They are prohibited selection or
      tuning objectives, and failure may not trigger retuning on the same OOS
      history.
- [x] Materialize one non-overwriting manifest-backed contract artifact with
      exact outer/inner fold tables, feature eligibility/exclusion rules,
      candidate methods and grids, objectives, tie-breakers, deterministic
      environment, prohibited inputs, and M1B-M1D execution specifications.
- [x] Add deterministic contract tests for temporal ordering, label maturity,
      horizon purging, fold locality, prohibited-feature exclusion, bounded
      grids, and the prohibition on B1E performance inputs. Do not execute a
      model or calculate performance in M1A.

M1A completed 2026-08-01 under the non-overwriting artifact
`artifacts/modeling/nested_walk_forward/20260801T000000Z-m1a/`, manifest
SHA-256 `a9d4d2eeb06543206e1f2f7d1a9c3000599b69ee5b22db7c89de21fd5330cabc`.
The artifact contains 34 exact outer role records, 306 inner
role/regime/fold records, a 102-record label-maturity ledger, the frozen
feature/exclusion contract, bounded grids, tie-breakers, evidence hashes, and
the M1B-M1D execution specification. Focused M1A tests pass 7 tests. No
historical model or portfolio execution occurred.

#### M1B — Implement and test without historical execution

- [x] Implement the frozen M1A nested-fold, selection, preprocessing, tuning,
      and lineage interfaces without changing the contract after seeing data.
- [x] Add synthetic tests that deliberately attempt label overlap, transformer
      leakage, global feature selection, outer-fold reuse, and prohibited
      metric consumption; every attempt must fail closed.
- [x] Do not run historical fitting, tuning, scoring, or performance in M1B.

M1B completed 2026-08-01 in `modeling/nested_walk_forward.py` and
`tests/modeling/test_nested_walk_forward.py`. The implementation verifies the
frozen M1A manifest and every generated record, retains the exact P3 outer and
inner boundary, implements the baseline and bounded stability/redundancy
selectors, all three regimes and seeded grids, fold-local imputation and
preprocessing, inner-only objectives, simplicity tie-breakers, one-shot tuning
state, M1C outer-refit hooks, and explicit lineage. Twenty-one synthetic tests
fail label/row/transformer/selector/feature/metric/adaptive-retry leakage
closed; the broader focused boundary passes 67 tests. No historical model,
score, portfolio, performance value, or artifact was produced.

#### M1C — One controlled nested walk-forward model run

- [x] Run the frozen M1A/M1B implementation once on the exact free-source P2
      dataset and existing P3 roles.
- [x] Choose features and parameters for each outer fold using only its inner
      evidence, then materialize row-complete outer-OOS predictions,
      exclusions, selected features, configurations, and predictive metrics in
      a new non-overwriting artifact. Preserve P3 unchanged.
- [x] Keep 2024-2026 unlabeled/open outcomes outside tuning and historical
      evaluation; they may be scored only under the frozen production rule.

M1C completed 2026-08-01 under the non-overwriting artifact
`artifacts/modeling/nested_walk_forward/20260801T121426Z-m1c/`, manifest
SHA-256 `125cb5d6a8012c3b03ee6eab5f00ac944135c39996e992ecf40d8816acdacc58`.
The complete preflight was persisted before fitting and covers all frozen
folds, populations, candidates, features, targets, regimes, selectors, grid
points, seeds, hashes, and prohibited inputs. Exact-population caching reduces
138 available inner-population occurrences to 52 unique selector/preprocessor
population computations without caching an estimator, prediction, metric, or
winner. The artifact contains 87,612 row-complete records, 20,142 matured
metric-eligible OOS predictions, 24,566 open 2024-2026 production scores
outside metrics, 516 explicit 2027-2028 future exclusions, full normalized
lineage and diagnostics, and 16 model records. Pooled predictive-only evidence
over 10,071 rows per role is LightGBM Spearman IC 0.335 and tree ROC AUC 0.654.
Every manifest record and row/model/diagnostic reference independently
verified. P3/P4/B1D/B1E/M1A remain unchanged; no portfolio or performance
calculation ran.

#### M1D — One locked portfolio comparison

- [x] Freeze one M1C route before performance, then consume it through the
      unchanged P4 portfolio rules and unchanged B1D/B1E evaluation contract.
- [x] Run one versioned comparison against B1E with gross/net NAV, benchmark,
      costs, turnover, drawdown, coverage, all physical namespaces, and the
      exact risk-free limitation. Do not select a configuration from the M1D
      result or retune on the same history.
- [x] Report the 30% CAGR and 1.0 zero-rate Sharpe thresholds as met or not met,
      alongside drawdown, coverage, turnover, stability, and evidence limits.
      A miss is an honest final result, not authorization for adaptive retries.

M1D completed 2026-08-01 under
`artifacts/performance/m1d/20260801T162953Z-m1d/`. The route was frozen before
performance under lock manifest SHA-256
`757e19cd9e35290a6b339f79e2c44a0f1ddb47c03b913930b1abda84f0bf74bc`;
the final manifest SHA-256 is
`b04cea8236da6cd92749410f6186360be5f29dbbbeb00a64f2ed07c180cc72ab`.
Exactly one execution consumed the 16 M1A/M1C inner-evidence winners through
unchanged P4 construction and B1D/B1E evaluation rules. It materializes 120
holdings over 2019-2026, uses only the 75 matured 2019-2023 holdings for
completed metrics, and keeps all 45 open 2024-2026 holdings outside them. The
primary free-evidence namespace records 19.75% aggregate net CAGR, 0.854
zero-rate diagnostic Sharpe, 24.50% annualized volatility, -29.46% maximum
drawdown, and 2.0 turnover. The 30% CAGR and 1.0 Sharpe thresholds are both
not met. Exact DGS1MO metrics remain unavailable, identities and adjusted-
close semantics remain uncertified, and the B1E aggregate comparison is not
like-for-like because B1E begins in 2015 while M1D begins in 2019. The missed
thresholds did not authorize a retry or retune.

M1 is complete: the model/gate choice came only from predeclared inner-fold
evidence, outer-OOS predictions remained untouched until evaluation, the
final backtest was one-shot, and every artifact is a new version rather than
an overwrite of P3, P4, or B1E. I1, A1, REL1, and deferred V2A remain outside
this completed session.

### I1 — Restore international markets incrementally

Objective: use the preserved legacy structure to add validated markets after
the US route is stable.

- [x] Select one additional market as the first adapter test.
- [ ] Validate local filing availability, identities, exchange calendars,
      benchmarks, currencies, accounting comparability, and corporate actions.
- [x] Implement the common source/P2 contract through a market adapter.
- [x] Add currency-aware targets, liquidity, costs, and portfolio semantics.
- [x] Repeat P2-P4 validation for that market with unsupported requirements
      failing closed and without downstream performance.
- [ ] Expand market by market; do not assume the US contract transfers
      unchanged.

I1 completed only the first adapter for Canada (`CA`, TSX/TSXV). The fresh
artifact is `artifacts/international/i1/20260801T180000Z-i1-ca/`, manifest
SHA-256 `d3f17854cf0839713163e8dda99aefedd6ba064a14a042a886770818a472d9f6`.
It contains the preflight/selection record, source lineage, frozen contract,
and 9,207 row-complete core P2 records. Certified filing timestamps,
calendar/benchmark vintage, actions/delistings, dated FX, survivorship-
complete targets, and P3/P4 downstream compatibility remain unsupported.
No legacy market code or mapping was deleted or overwritten. Stop here.

### US1A — Final US free-data product contract and offline candidate — completed 2026-08-01

Objective: choose the data baseline using data-only gates, then connect the
accepted M1A/M1C route to unchanged P4 product rules without collecting event
evidence or rerunning performance.

- [x] Rehash P2, P3, P4, B1D, B1E, M1A, M1C, M1D, I1, and all five partial
      M1C attempts before materialization.
- [x] Assess D1 only on source lineage, schema, stable identities,
      availability timestamps, row coverage, target support, feature
      compatibility, and deterministic reconstruction.
- [x] Retain accepted P2 because D1 has 242 missing and 76 new stable IDs and
      cannot satisfy M1C's pinned P2 manifest/identity contract without
      retraining or a contract change.
- [x] Freeze the accepted M1A/M1C inner-evidence route before product output;
      prohibit M1D performance and all outer/portfolio metrics as selection
      inputs.
- [x] Apply unchanged P4 eligibility, candidate-wide liquidity, 15-name
      equal-weight, explanation, rank, and stable-ID tie-break rules.
- [x] Materialize a fresh row-complete artifact under
      `artifacts/product/us_free_v1/20260801T183000Z-us1a/`, manifest SHA-256
      `f27773c0e1fb92a25707e0fec363e13afa7ccb10f8ace7537187c6be58575edf`.
- [x] Map exact existing E1 stable-row/security evidence for five names and
      record `event_evidence_not_collected` for the other ten.
- [x] Preserve complete P2/M1C/P4 manifests and code lineage, stock-level
      explanations and limitations, deterministic verification, diagnostics,
      hashes, and manifest records.
- [x] Pass 89 focused product/prediction/P4/shortlist/lineage tests and the
      final full suite with 853 passed, 4 skipped, and 78 existing warnings.
- [x] Calculate no performance and make no external request, promotion,
      overwrite, archive, publication, commit, or push.

### US1B — Frozen-shortlist evidence — completed 2026-08-01

- [x] Freeze the exact 15-name identity/rank/CIK/decision/weight boundary and
      exact 36-URL request plan before the first request; require approval token
      `APPROVE-US1B-f1bcfc5d2c740d1626c3d6c11148ee1711ec5d70911165b897daf84f90f9bfbc`.
- [x] Reuse 15 D1 submissions, one D1 SEC exchange index, and 13 exact E1
      primary-document responses without recollection or retry.
- [x] Retrieve all 36 approved URLs in one attempt each with HTTP 200 and
      preserve 3,681,377 exact response-body bytes plus complete request,
      response, timestamp, status, size, hash, and retry lineage.
- [x] Materialize
      `artifacts/product/us_free_v1_evidence/20260801T193322Z-us1b/`, manifest
      SHA-256
      `f802ec26358d27afa21c99490e598e13c98a6ad60229705769f3fae1818d9dcd`,
      with 95 records and 2,653,162 recorded bytes.
- [x] Produce 15 market/exchange identity rows, 47 separate document
      adjudications, 46 accession-bound cited claims, 15 name adjudications,
      and one evidence-only US1A report derivative.
- [x] Keep 14 names unresolved and HPK failed-request because its preserved E1
      Form 25-NSE response remains HTTP 503; retain unresolved action and human
      review for all 15 names.
- [x] Verify every artifact/raw response/claim locator, the exact unchanged
      US1A shortlist columns, and all preserved boundaries; pass the 39-test
      focused evidence/lineage/failure boundary and the final full suite with
      859 passed, 4 skipped, and 78 existing warnings.
- [x] Change no baseline, route, model, score, gate, liquidity decision,
      holding, rank, weight, performance output, or preserved artifact.

### US1C — Local release consolidation — completed 2026-08-01

- [x] Freeze `docs/US1C_RELEASE_CONSOLIDATION_CONTRACT.json` before output,
      SHA-256
      `d7098153542afa77ff09e850980362eb957b3e24cd609a4984451289595f1d47`.
- [x] Expose `python3 -m workflows.run_us_free_v1` as the single local command
      for P2 -> P3 -> P4 -> US1A -> US1B plus all named supporting boundaries.
- [x] Independently rehash every record and manifest entry in US1A and US1B,
      reconcile the exact shortlist and evidence states, and verify D1 remains
      unsupported for frozen M1C because of 242 P2-only and 76 D1-only IDs.
- [x] Rehash P2, P3, P4, B1D, B1E, M1A, M1C, the M1D lock and final artifact,
      I1, and all five partial M1C attempts without modifying them.
- [x] Produce two fresh non-overwriting, byte-identical reconstructions.
- [x] Recover the exact three-artifact, 202-file, 481,666,707-byte P2/P3/P4
      baseline read-only from private immutable Hugging Face revision
      `aaf056ea115067e42ef9abf9fa93ade75cdd4052`, with no mutable fallback or
      remote mutation.
- [x] Materialize and independently verify
      `artifacts/product/us_free_v1_release_candidate/20260801T210000Z-us1c/`,
      manifest SHA-256
      `a639e7bdd40eae8d8b28b0ee802c8de09beb8c05569a20f3996d172db9a7b466`,
      with 24 records and 1,039,444 recorded bytes.
- [x] Pass 37 focused consolidation/reconstruction/recovery/immutability/
      non-overwrite/failure-path tests with 13 existing warnings and the
      single final full suite with 871 passed, 4 skipped, and 78 existing
      warnings.
- [x] Consolidate active documentation without deleting or rewriting
      historical evidence. Perform no release, archive, remote mutation,
      methodology change, evidence collection, model/performance execution,
      branch, commit, push, or tag.

### A1 — Dependency-safe archive and retirement

Objective: make the active repository unambiguous while retaining recoverable
history.

- [x] Use `codex/legacy-archive` for tracked legacy code, tests,
      configuration, notebooks, and documents.
- [x] Keep large/gitignored A1 packages out of Git; preserve the unchanged
      private Hugging Face pointers for canonical P2-P4. Under the explicit
      no-publish boundary, the A1 packages remain local and make no remote-
      durability claim.
- [x] Archive the first confirmed PIT candidates:
      `corrected_partial`, `corrected_partial_inputs`, and the Session 9B
      zero-holdings freeze, plus its required Session 9 parent.
- [x] Fail closed on V3.1-V3.3 retirement after dependency verification found
      active parity-test imports and an M1A V3.1 pin; retain all three.
- [x] Inventory historical models, backtests, reports, and notebooks and retain
      every item whose active consumer/replacement remains unresolved.
- [x] Rewrite active FAQ/configuration documents rather than merely moving
      their stale claims.
- [x] Hash and manifest every material archive package.
- [x] Test recovery before removing an active-tree copy.
- [x] Never archive canonical P2-P4 artifacts, corrected source evidence, or
      still-imported shared implementations.

Done when the primary branch exposes one canonical workflow and every retired
component has a recoverable location and named replacement.

A1 completed 2026-08-01. The frozen contract SHA-256 is
`1b8174bac5181b68f6e3913c2e6bf73a7271acce9f1421063e40b39db2a63f3a`;
the local archive manifest SHA-256 is
`892cf0991b9e1a5a651728a1329bd91422037c62de6d57ee27adcc1e919399da`.
Four packages preserve 887 files and 1,954,687,841 source bytes. Four exact
temporary recoveries and the 11-file archive-branch recovery all passed before
retirement. Original artifact roots now retain only their byte-identical
manifest and an archive pointer. The focused boundary passes 47 tests with 13
existing warnings, the post-retirement US1C route passes, and the final full
suite passes 869 tests with 4 skips and 78 existing warnings. The tracked A1
result pointer SHA-256 is
`c41b675092c66ddb3609fd16cfd82bd5b9ea1876c3528278cff5e5d789fe177f`.
No release or remote mutation occurred.

### REL1 — Consolidated release boundary

- [x] Verify checkpoint `0b6fb150ede821eacc7b03f9769fd543390b3c8e` in a clean
      detached worktree without switching or resetting the active checkout.
- [x] Verify the authoritative route and deterministic P2 -> P3 -> P4 -> US1A
      -> US1B chain; reconstruct US1C twice with candidate manifest SHA-256
      `a639e7bdd40eae8d8b28b0ee802c8de09beb8c05569a20f3996d172db9a7b466`.
- [x] Verify read-only immutable recovery at
      `aaf056ea115067e42ef9abf9fa93ade75cdd4052`: 202 files and 481,666,707
      bytes, with no mutable fallback or remote mutation.
- [x] Recover all four A1 packages and all 11 tracked retired files; contract
      and local archive manifest hashes are
      `1b8174bac5181b68f6e3913c2e6bf73a7271acce9f1421063e40b39db2a63f3a` and
      `892cf0991b9e1a5a651728a1329bd91422037c62de6d57ee27adcc1e919399da`.
- [x] Run focused checks and review the diff/archive inventory; update all
      required REL1 documentation.
- [x] Run the single final full suite after these documentation edits: 828
      passed, 5 skipped, 24 failed, 16 errors, and 78 warnings in 119.07s.
      Failures are missing or drifted ignored Session 8E/B1C, corrected-Step-2,
      and E1 lineage inputs in the clean checkout.
- [ ] Commit, push, tag, publish, promote, or release; each requires explicit
      authorization and remains prohibited in this task.

REL1 is verified but not release-ready. P2-P4 have immutable recovery, while
US1A/US1B/supporting ignored artifacts, US1C, and A1 packages are local-only
without immutable recoverable sources; A1 explicitly claims no remote
durability. US1B remains 14 unresolved names plus failed-request HPK, all 15
requiring human review. Provider-certified/survivorship-complete performance
remains blocked by the S1 ledger gap and missing immutable `DGS1MO` vintage.

## Completion definition

The refactor is complete only when:

1. one documented US raw-refresh path creates versioned source evidence;
2. one documented deterministic path reconstructs P2-P4;
3. active canonical code no longer imports historically named session modules;
4. P2-P4 are recoverable from private immutable Hugging Face references;
5. survivorship/event coverage is measured, with assumptions isolated as
   sensitivity analysis;
6. free-data V1 performance is reproducible under separate observed and named
   scenario namespaces, with coverage and assumption exposure adjacent to
   every headline metric;
7. tuning uses nested temporal validation and a new artifact version;
8. international legacy structure remains recoverable for later adapters;
9. tracked legacy material is preserved on the archive branch and large
   artifacts are hash-manifested in private storage;
10. active documentation names one workflow and does not transfer old
    performance claims;
11. paid-data V2 can replace V1 evidence through the same interfaces and
    report exact metric divergence without changing the frozen V1 artifact.

## Exact next-session task

Stop at the REL1 release-decision boundary. No commit, push, tag, publication,
promotion, upload, or release is authorized until the documentation diff is
approved and a durable source decision is made for local US1A/US1B/supporting
and A1 artifacts. Deferred V2A and new evidence/performance work remain out of
scope.
