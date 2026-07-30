# Non-Destructive Archive Inventory

Status: proposed destinations only; no C1 move or deletion is authorized

Archive prerequisites for every tracked candidate are: a reviewed checkpoint
commit containing P2-P4 and C1, creation of the separately authorized
`codex/legacy-archive` branch, exact dependency confirmation, and proof that an
active replacement exists. Large generated artifacts remain in private
artifact storage rather than Git.

| Candidate | Class | Dependencies | Active replacement | Size / manifest SHA-256 | Proposed destination |
|---|---|---|---|---|---|
| `artifacts/pit_validation/corrected_partial/` | Historical partial artifact | Historical PIT reports/tests; no canonical dependency | P2 corrected US annual artifact | 565,977,088 allocated bytes; `10b648c581ed56d6b6acb7f16d786be00409f4dfc96f0e92073693276e51b6ee` | Private legacy artifact namespace after consumer search |
| `artifacts/pit_validation/corrected_partial_inputs/` | Historical partial inputs | `corrected_partial` and possible audit evidence | Corrected Step 2 + Session 8E pinned inputs | 1,316,864,000 allocated bytes; `31c4f4e289ac49ef7c01f333370b4e3a35a3e786c0b289200bdc95e9c5c2f4d5` | Private legacy artifact namespace after consumer search |
| `artifacts/pit_validation/session9_corrected_8f/` | Alternative research output | Session 9 research/tests | P3 accepted tree/LightGBM OOS route for active product | 33,591,296 allocated bytes; `bb75e2be97104736786a14c2b6c435fecef5aa335fb41181fe1f1947cb094deb` | Private alternative-research namespace |
| `artifacts/pit_validation/session9b_oos_selection_freeze/` | Zero-holdings historical freeze | Session 9B reports/tests | P4 candidate-wide liquidity and holdings route | 40,947,712 allocated bytes; `2b17030e554b8f663f48e7f44299107f781dbdadc4d73054361b3ebe1ab492b8` | Private historical-validation namespace |
| `modeling/build_session_v3_2_oos.py` | Tracked historical code | Historical V3.2 manifest, V3.3 reconciliation, focused tests | `modeling/oos_modeling.py` plus canonical P3 builder | Hash must be frozen at archive checkpoint | `codex/legacy-archive`; later `legacy/` move only after dependency rewrite |
| `portfolio/build_session_v3_3_holdings.py` | Tracked historical code | Historical V3.3 manifest/tests | `portfolio/selection_contract.py` plus canonical P4 builder | Hash must be frozen at archive checkpoint | `codex/legacy-archive`; later `legacy/` move only after dependency rewrite |
| `modeling/freeze_session_v3_1.py` | Tracked historical contract builder | Historical V3.1/V3.2 tests and manifests | Canonical P3 contract freezes the accepted active roles | Hash must be frozen at archive checkpoint | `codex/legacy-archive` |
| `docs/archive/` and historical V3 reports | Historical documentation | Audit/history references | `docs/START_HERE.md`, `docs/ARCHITECTURE.md`, FAQ, production config, handoff | Tracked text; checkpoint commit supplies exact Git hashes | Remain in place until archive branch exists |
| `workflows/run_pipeline.py` and per-market runners | Legacy/international code | Future US refresh comparison and later market adapters | No complete external-refresh replacement yet | Not archiveable in C1 | Preserve in main tree, clearly labeled legacy |
| `.github/workflows/refresh_data.yml` | Legacy workflow | Manual historical multi-market refresh only | D1 external US refresh does not exist yet | Not archiveable in C1 | Preserve as disabled-schedule/manual-opt-in legacy workflow |

## Explicit exclusions

The P2-P4 canonical roots, corrected Step 2 evidence, Session 8E evidence,
international/per-market structures, `data/historical_dataset_clean.parquet`,
saved models, and all unresolved artifact namespaces are not approved archive
targets. No cache is deleted by this inventory.
