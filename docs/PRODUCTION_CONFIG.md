# Production Configuration

Status: **US free-data V1 candidate verified locally; no release performed**

The supported active configuration preserves the three canonical manifests and
the unchanged US1A/US1B product/evidence derivatives:

- P2: `artifacts/canonical/corrected_us_annual/manifest.json`
- P3: `artifacts/canonical/corrected_us_annual_3y_research_model/manifest.json`
- P4: `artifacts/canonical/corrected_us_annual_3y_product/manifest.json`
- US1A: `artifacts/product/us_free_v1/20260801T183000Z-us1a/manifest.json`
- US1B:
  `artifacts/product/us_free_v1_evidence/20260801T193322Z-us1b/manifest.json`
- US1C local candidate:
  `artifacts/product/us_free_v1_release_candidate/20260801T210000Z-us1c/manifest.json`

Verify the complete local route with:

```bash
python3 -m workflows.run_us_free_v1
```

The command is offline, read-only by default, and fail-closed. The narrower
`python3 -m workflows.run_canonical` command verifies only P2 -> P3 -> P4 and
is not the complete free-data V1 route.

## Fixed active contract

- Output: an explainable ranked stock shortlist.
- Market/period: US annual.
- Population: observed-only primary labels.
- Horizon: three years.
- Models: the accepted M1A/M1C inner-evidence-selected decision-tree and
  LightGBM roles under the frozen temporal contract; P3 remains the unchanged
  canonical baseline.
- Portfolio: fixed hard gates, candidate-wide 30-session liquidity, 15
  equal-weight holdings.
- Inputs: explicitly dated, hash-pinned, and validated.
- Risk framing: fraud risk is one factor or safety gate.
- Claims: no certified historical or future-performance claim.
- Performance: B1E and M1D are separately labeled free-source historical
  research results. Provider-certified or survivorship-complete performance
  and exact `DGS1MO`-dependent metrics remain unavailable.

The `.github/workflows/refresh_data.yml` route is explicitly legacy and manual
opt-in only. It must not promote or publish canonical artifacts. D1 is the
versioned, separately approval-gated US refresh; C2 already froze the accepted
P2-P4 bytes at immutable private revision
`aaf056ea115067e42ef9abf9fa93ade75cdd4052`.

Session A1 retired only the four dependency-safe partial/Session-9 payload
groups named in `docs/A1_ARCHIVE_CONTRACT.json`. Its packages are local and
gitignored; no remote archive upload or release occurred.

The historical Session 47b configuration is preserved at
`docs/archive/LEGACY_PRODUCTION_CONFIG.md`.
