# Session A1 Archive Inventory

Status: completed local dependency-safe retirement; no remote upload or release

The frozen contract is `docs/A1_ARCHIVE_CONTRACT.json`, SHA-256
`1b8174bac5181b68f6e3913c2e6bf73a7271acce9f1421063e40b39db2a63f3a`.
The ignored local archive manifest is
`artifacts/archive/a1/20260801T220130Z-a1/manifest.json`, SHA-256
`892cf0991b9e1a5a651728a1329bd91422037c62de6d57ee27adcc1e919399da`.
Every package was extracted into an absent temporary target and every recovered
path, byte size, and SHA-256 matched before an active-tree payload was retired.

## Archived artifact packages

| Original root | Files / bytes | Original manifest | Package SHA-256 | Active replacement |
|---|---:|---|---|---|
| `artifacts/pit_validation/corrected_partial/` | 19 / 565,934,344 | `10b648c581ed56d6b6acb7f16d786be00409f4dfc96f0e92073693276e51b6ee` | `e07f41abf149b9f4b23c47c21f6fad4f535d73feaf158ac1d2beec7af9a70dd6` | Canonical P2 corrected US annual artifact |
| `artifacts/pit_validation/corrected_partial_inputs/` | 4 / 1,316,859,187 | `31c4f4e289ac49ef7c01f333370b4e3a35a3e786c0b289200bdc95e9c5c2f4d5` | `0992283d399780624c11a3488d6134f744f823f630a9afd1d0eda7c546530de3` | Corrected Step 2 + Session 8E + canonical P2 inputs |
| `artifacts/pit_validation/session9_corrected_8f/` | 847 / 30,992,893 | `bb75e2be97104736786a14c2b6c435fecef5aa335fb41181fe1f1947cb094deb` | `1a66ff970d7fb0f1f197ee4a0e7eed1cd6c98d5c5e140d7c6c3724d0132674ce` | Canonical P3 OOS research/model route |
| `artifacts/pit_validation/session9b_oos_selection_freeze/` | 17 / 40,901,417 | `2b17030e554b8f663f48e7f44299107f781dbdadc4d73054361b3ebe1ab492b8` | `8f3ab2976f79f1a05767e524809b43153f30f6cb234ebf1fad1cffd67bc3ae2e` | Canonical P4 candidate-wide liquidity/holdings route |

Each original root is now pointer-only: its exact historical `manifest.json`
remains byte-identical and `ARCHIVED_POINTER.json` names the package, package
hash, inventory hash, archive manifest, and recovery command. The packages are
local and Git-ignored. No private Hugging Face path was added or changed, so A1
makes no remote-durability claim for these packages.

## Retired tracked material

The following exact files are absent from the active tree and recoverable from
`codex/legacy-archive` commit
`ed53232cb45cf8b82cfd8941ead5f9a88016e507`:

- `quality/compare_corrected_partial.py`
- `quality/freeze_corrected_partial_evidence.py`
- `quality/freeze_corrected_step2.py`
- `reports/pit_validation/08_data_comparison.md`
- `reports/pit_validation/08_preflight_input_freeze.md`
- `modeling/build_session9_oos.py`
- `modeling/freeze_session9b_selection.py`
- `tests/modeling/test_build_session9_oos.py`
- `tests/modeling/test_freeze_session9b_selection.py`
- `reports/pit_validation/09_model_prediction_comparison.md`
- `reports/pit_validation/09b_oos_selection_freeze.md`

The contract records the SHA-256 of every file. `git archive` recovery into
`/private/tmp/a1-tracked-recovery` reproduced all 11 with zero mismatches before
retirement. No branch was switched or modified.

## Fail-closed exclusions

The V3.1-V3.3 artifacts/builders remain because parity tests still import the
historical implementations and M1A still pins
`modeling/freeze_session_v3_1.py`. Canonical P2-P4, corrected Step 2 and
Session 8E evidence, D1, S1, E1, B1, M1, I1, US1A-US1C, immutable Hugging Face
pointers, international/per-market code, shared implementations, datasets,
models, and unrelated `.jupyter_ystore.db` are unchanged and excluded.
