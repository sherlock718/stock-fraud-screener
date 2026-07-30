# Canonical Dependency Inventory

Status: Session C1 tracked inventory
Scope: current dirty worktree and accepted P2-P4 local artifacts

No path was moved, deleted, published, or regenerated for this inventory.

## Classification

| Classification | Paths | Dependency / disposition |
|---|---|---|
| Canonical | `pipeline/build_corrected_feature_population.py`, `modeling/build_canonical_research_model.py`, `portfolio/build_canonical_product.py`, `workflows/run_canonical.py` | Active P2 -> P3 -> P4 spine. |
| Canonical artifacts | `artifacts/canonical/corrected_us_annual/`, `artifacts/canonical/corrected_us_annual_3y_research_model/`, `artifacts/canonical/corrected_us_annual_3y_product/` | Accepted non-overwriting local baseline; preserve exact manifests and records. |
| Source evidence | `artifacts/pit_validation/corrected_step2/`, `artifacts/pit_validation/contract_aligned_label_inputs/`, `data/snapshots.parquet`, `data/prices.parquet`, `data/ARTIFACT_MANIFEST.json` | P2 and P4 source/bronze dependencies or broader historical source evidence. |
| Shared implementation | `pipeline/event_time_cohorts.py`, `pipeline/step5_compute_features.py`, `pipeline/step6_clean.py`, `pipeline/enrich_fraud_taxonomy.py`, `modeling/fold_lineage.py`, `modeling/oos_modeling.py`, `modeling/constants.py`, `portfolio/selection_contract.py`, `backtest/monthly_nav.py` | Neutral or still-valid implementations used at canonical boundaries. |
| International legacy | `workflows/run_pipeline_{br,ca,eu,jp,kr}.py`, per-market Step 1/2 modules, market mappings/calendars, suffixed Parquet contracts | Preserve intact for later adapters; inactive in the US-first canonical route. |
| Legacy six-step | `workflows/run_pipeline.py`, `pipeline/step1_*` through `pipeline/step6_clean.py`, `.github/workflows/refresh_data.yml` | Preserved and explicitly labeled legacy; not the canonical refresh. |
| Historical evidence | `modeling/freeze_session_v3_1.py`, `modeling/build_session_v3_2_oos.py`, `portfolio/build_session_v3_3_holdings.py`, `modeling/build_session9_oos.py`, `reports/pit_validation/`, `docs/archive/` | Retain until archive-branch checkpoint and dependency proof. No active canonical imports. |
| Cache | `.jupyter_ystore.db`, Python caches, local price caches, logs | Not canonical inputs. Preserve existing files during C1; exclude from archive decisions until ownership is clear. |
| Unresolved | Other `artifacts/pit_validation/` namespaces and historical reports not named in the archive inventory | Keep in place pending path-level dependency proof. |

## Accepted P2-P4 generated records

| Stage | Root size | Files | Manifest SHA-256 | Generated records | Validated inputs | Code-lineage records |
|---|---:|---:|---|---:|---:|---:|
| P2 | 436,854,784 bytes | 34 | `40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3` | 33 | 3 | 8 |
| P3 | 14,163,968 bytes | 142 | `8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2` | 141 | 5 | 5 |
| P4 | 31,723,520 bytes | 26 | `28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7` | 25 | 8 | 5 |

Sizes above are filesystem allocation measured by `du` during C1; exact
per-record byte sizes and hashes remain authoritative in each manifest.

## Dirty-worktree preservation review

The pre-C1 dirty state contained the completed P2-P4 implementation and
documentation: changes to P2 availability/materialization, historical lineage
reconciliation, canonical P3/P4 builders and tests, the roadmap/handoff/start
documents, and the untracked completion plan. The unrelated untracked
`.jupyter_ystore.db` cache was also present.

C1 adds neutral shared modules, the canonical orchestrator, focused tests,
active documentation, and inventories. It does not revert, overwrite, stage,
commit, publish, archive, or remove any pre-existing dirty-worktree path.
