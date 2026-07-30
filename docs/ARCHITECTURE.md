# Active Canonical Architecture

Status: Session C1 consolidated local baseline

## Supported spine

```text
corrected Step 2 + frozen Session 8E evidence
    -> P2 corrected US annual observed-only features
    -> P3 fold-local selection, preprocessing, models, OOS predictions
    -> P4 gates, candidate-wide liquidity, portfolio, shortlist, report
```

The single local entrypoint is:

```bash
python3 -m workflows.run_canonical
```

It verifies the accepted manifests in P2 -> P3 -> P4 order. With
`--build-missing`, it may invoke an absent stage's non-overwriting builder.
Existing roots are never reused or replaced.

## Active modules

- `pipeline/build_corrected_feature_population.py`: P2 construction.
- `modeling/build_canonical_research_model.py`: P3 construction.
- `modeling/oos_modeling.py`: neutral fold-local fitting shared by active P3.
- `portfolio/build_canonical_product.py`: P4 construction.
- `portfolio/selection_contract.py`: neutral fixed gate/liquidity/portfolio
  constants shared by active P4.
- `workflows/run_canonical.py`: deterministic local orchestration.
- `backtest/monthly_nav.py`: accepted accounting implementation, inactive for
  official performance until required evidence exists.

Active canonical builders do not import historically named `session_v3_*`
modules.

## Preserved legacy boundary

The old six-step workflow, historical `session_*` builders, per-market
orchestrators, market mappings, and international evidence remain in place.
They are not active canonical entrypoints and are not deleted or archived by
C1. They preserve the design needed for later market adapters.

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
- Official performance remains fail-closed without the accepted
  security/action ledger and immutable risk-free vintage.
- External refresh, Hugging Face publication, archive moves, and performance
  calculation are outside deterministic reconstruction.
