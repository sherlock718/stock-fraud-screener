# Production Configuration

Status: **canonical local baseline verified; external refresh inactive**

The supported active configuration is frozen by the three canonical manifests:

- P2: `artifacts/canonical/corrected_us_annual/manifest.json`
- P3: `artifacts/canonical/corrected_us_annual_3y_research_model/manifest.json`
- P4: `artifacts/canonical/corrected_us_annual_3y_product/manifest.json`

Verify the complete local route with:

```bash
python3 -m workflows.run_canonical
```

The workflow is non-overwriting and fail-closed. `--build-missing` may invoke
only an absent stage, after all upstream accepted manifests validate.

## Fixed active contract

- Output: an explainable ranked stock shortlist.
- Market/period: US annual.
- Population: observed-only primary labels.
- Horizon: three years.
- Models: fold-local decision tree and LightGBM regression under one temporal
  split contract.
- Portfolio: fixed hard gates, candidate-wide 30-session liquidity, 15
  equal-weight holdings.
- Inputs: explicitly dated, hash-pinned, and validated.
- Risk framing: fraud risk is one factor or safety gate.
- Claims: no certified historical or future-performance claim.
- Performance: unavailable until the security/action ledger and immutable
  `DGS1MO` ALFRED vintage satisfy the accepted contract.

The scheduled `.github/workflows/refresh_data.yml` route is now explicitly
legacy and manual opt-in only. It is not the canonical refresh and must not be
used to promote or publish canonical artifacts. US-first external refresh and
private immutable publication are deferred to later approved sessions.

The historical Session 47b configuration is preserved at
`docs/archive/LEGACY_PRODUCTION_CONFIG.md`.
