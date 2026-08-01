# Canonical Dependency Inventory

Status: Session A1 post-retirement inventory

## Active route

| Boundary | Paths | Dependency / disposition |
|---|---|---|
| Authoritative local route | `workflows/run_us_free_v1.py` | Offline P2 -> P3 -> P4 -> US1A -> US1B verification plus frozen supporting boundaries and optional US1C candidate verification. |
| Canonical P2-P4 | `pipeline/build_corrected_feature_population.py`, `modeling/build_canonical_research_model.py`, `portfolio/build_canonical_product.py`, `workflows/run_canonical.py` | Preserved canonical dataset/research/product baseline and narrower diagnostic reconstruction route. |
| US product | `portfolio/build_us_free_product.py`, `portfolio/us1b_frozen_evidence.py`, `portfolio/us1c_release_consolidation.py` | US1A product, US1B evidence, and US1C local consolidation. No release performed. |
| Shared implementation | `pipeline/event_time_cohorts.py`, `pipeline/step5_compute_features.py`, `pipeline/step6_clean.py`, `pipeline/enrich_fraud_taxonomy.py`, `modeling/fold_lineage.py`, `modeling/oos_modeling.py`, `modeling/nested_walk_forward.py`, `modeling/prediction_lineage.py`, `portfolio/selection_contract.py`, `backtest/free_data_v1_nav.py` | Still imported or hash-pinned; preserve. |
| Immutable recovery | `data_io/canonical_artifact_pointers/{p2,p3,p4}.json` | Exact private revision `aaf056ea115067e42ef9abf9fa93ade75cdd4052`; 202 files and 481,666,707 bytes. |
| International legacy | per-market Step 1/2 modules, `workflows/run_pipeline_{br,ca,eu,jp,kr}.py`, mappings/calendars, I1 | Preserve for later adapters; inactive in the US-first route. |
| Legacy six-step | `workflows/run_pipeline.py`, `workflows/refresh_data.py`, `.github/workflows/refresh_data.yml` | Manual historical route; not canonical and not retired because international/refresh comparisons still reference it. |

## A1 dependency resolution

The complete pre-action inventory at
`docs/A1_REPOSITORY_INVENTORY_BEFORE.json` hashes all 303 tracked and 53
non-ignored untracked files. The archive contract maps imports, workflows, CI,
documentation, and artifact lineage before retirement.

- Partial-PIT reconstruction had no workflow/CI importer. Its one-off tracked
  builders and reports were retired with it. The corrected Step 2 manifest's
  prior-chain dependency remains resolvable because the exact partial manifest
  stays at the original pointer-only root.
- Session 9 and Session 9B had no workflow/CI consumer. Their only importers
  were the two retired focused tests; P3 and P4 are their tested active
  replacements.
- V3.1-V3.3 failed the retirement gate: active parity tests import V3.2/V3.3,
  and the M1A contract pins V3.1. Their code and artifacts remain unchanged.
- No candidate appeared in either active CI workflow.

## Preserved boundaries

Canonical P2-P4, corrected Step 2, Session 8E, corrected feature evidence,
D1, S1, E1, B1C-B1E, M1A-M1D, I1, US1A-US1C, all partial M1C attempts,
international/per-market code, shared implementations, datasets, models,
historical backtests not proven replaceable, notebooks, and the unrelated
`.jupyter_ystore.db` were excluded from retirement.

The four A1 packages contain 887 files and 1,954,687,841 source bytes. Their
tracked result pointer is `docs/A1_ARCHIVE_MANIFEST.json`; exact package
details and recovery commands are in `docs/ARCHIVE_INVENTORY.md`.
