# Frozen Canonical Architecture

Status: Session DUR1 immutable artifact durability complete; release not performed

## Supported spine

```text
corrected Step 2 + frozen Session 8E evidence
    -> P2 corrected US annual observed-only features
    -> P3 fold-local selection, preprocessing, models, OOS predictions
    -> P4 gates, candidate-wide liquidity, portfolio, shortlist, report
    -> accepted M1A/M1C model route through unchanged P4 product rules (US1A)
    -> frozen-shortlist evidence with unresolved human review (US1B)
    -> local release-candidate consolidation and recovery proof (US1C)
    -> immutable recovery of every required artifact group (DUR1)
```

The single local entrypoint is:

```bash
python3 -m workflows.run_us_free_v1
```

It verifies P2 -> P3 -> P4 -> US1A -> US1B plus the named supporting frozen
boundaries without external access. `workflows/run_canonical.py` remains the
narrower P2 -> P3 -> P4 diagnostic/reconstruction command.

## Frozen modules

- `pipeline/build_corrected_feature_population.py`: P2 construction.
- `modeling/build_canonical_research_model.py`: P3 construction.
- `modeling/oos_modeling.py`: neutral fold-local fitting shared by active P3.
- `portfolio/build_canonical_product.py`: P4 construction.
- `portfolio/selection_contract.py`: neutral fixed gate/liquidity/portfolio
  constants shared by active P4.
- `workflows/run_canonical.py`: deterministic local orchestration.
- `workflows/run_us_free_v1.py`: complete offline US free-data V1
  verification and US1C candidate verification.
- `data_io/us_free_v1_durability.py`: DUR1 contract validation and immutable,
  fail-closed recovery of all required ignored artifacts.
- `portfolio/build_us_free_product.py`, `portfolio/us1b_frozen_evidence.py`,
  and `portfolio/us1c_release_consolidation.py`: US1A-US1C product,
  evidence, and consolidation boundaries.
- `data_io/canonical_hf.py`: private, content-addressed publication,
  immutable pointer validation, and byte-verified recovery contracts.
- `data_io/publish_canonical_to_hf.py`: local preparation, authenticated
  visibility preflight, and approval-bound atomic publication.
- `data_io/retrieve_canonical_from_hf.py`: clean-checkout recovery pinned to
  full Hugging Face commit revisions.
- `scripts/publish_canonical`: location-independent publication wrapper.
- `backtest/free_data_v1_nav.py`: accepted B1D free-data accounting engine;
  its results remain research, not provider-certified performance.

Active canonical builders do not import historically named `session_v3_*`
modules.

## Preserved legacy boundary

The old six-step workflow, per-market orchestrators, market mappings,
international evidence, and still-referenced V3.1-V3.3 builders remain in
place. They are not active canonical entrypoints and preserve the design and
lineage needed by later adapters and frozen M1A parity.

Session A1 retired only four confirmed superseded payload groups: corrected
partial output, its partial inputs, alternative Session 9 OOS output, and the
Session 9B zero-holdings freeze. Their original roots retain the exact
historical manifests and archive pointers. DUR1 makes the required payloads
and tracked archive-branch dependencies byte-recoverable from the immutable
private artifact revision.

The older orientation documents under `docs/architecture/orientation_*.md`
describe the legacy multi-market implementation unless they explicitly say
otherwise. The dependency and archive classifications are tracked in
`docs/CANONICAL_DEPENDENCY_INVENTORY.md` and
`docs/ARCHIVE_INVENTORY.md`.

## Boundaries

- The canonical primary dataset is observed-only and US annual.
- Certified macro vintages are unavailable and no historical macro value is
  synthesized.
- Free-source coverage is historically enriched, not comprehensively
  survivorship-free.
- Frozen Yahoo payloads support liquidity only.
- Provider-certified or survivorship-complete performance remains unavailable;
  B1E/M1D are explicitly free-source research results and the exact risk-free
  namespace remains unavailable.
- External refresh, approval-bound Hugging Face publication, archive moves,
  and performance execution remain outside deterministic verification.
- C2 published the exact P2-P4 baseline privately at immutable revision
  `aaf056ea115067e42ef9abf9fa93ade75cdd4052`. The three pointer manifests pin
  all 202 files and 481,666,707 bytes; no mutable revision is accepted.
- DUR1 published the complete 21-group, 56,092-path, 7,082,517,721-byte
  durability contract at immutable private revision
  `a282a1023f321b9bad84ec6f12e5d846345ff833`; no required artifact depends on
  local-only or mutable state.
- This architecture is frozen as a research reference. Product v2 belongs in
  a separate repository.
