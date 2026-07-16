# PIT Artifact Archive Index

Frozen: 2026-07-16

The payloads under `artifacts/pit_validation/` are local validation evidence,
not Git source. They occupy approximately 4 GiB and are excluded by
`.gitignore`. Do not delete or overwrite them without first copying the complete
directory and verifying the manifest hashes below.

| Artifact set | Local size (KiB) | Manifest SHA-256 |
|---|---:|---|
| `contract_aligned_label_inputs` | 966,016 | `0ab15685a445f09919b19393e074b8b5903afef7e12defc7c44aeac624f4581a` |
| `corrected_feature_population` | 334,576 | `9c1e4b82c2e7bc2a85228adf6668b797acec827e2bd1ff7c58d20cfb6a9ac01a` |
| `corrected_partial` | 576,936 | `10b648c581ed56d6b6acb7f16d786be00409f4dfc96f0e92073693276e51b6ee` |
| `corrected_partial_inputs` | 1,286,000 | `31c4f4e289ac49ef7c01f333370b4e3a35a3e786c0b289200bdc95e9c5c2f4d5` |
| `corrected_step2` | 809,240 | `899cffd7a9d1dc3395a08bee5c65ad4a5e8a109a83c63346ac54c891fe706e08` |
| `legacy_saved` | 106,364 | `00b237943c47700ba311c330da7bf5b1a13eb078eb19a669dd62dd1659f69aae` |
| `session9_corrected_8f` | 32,804 | `bb75e2be97104736786a14c2b6c435fecef5aa335fb41181fe1f1947cb094deb` |
| `session9b_oos_selection_freeze` | 39,988 | `2b17030e554b8f663f48e7f44299107f781dbdadc4d73054361b3ebe1ab492b8` |

`calendar_contract` and `training_label_market_inputs` are small supporting
directories without top-level manifests. The manifest-backed directories above
contain their own record-level hashes and dirty-state lineage where applicable.

This index preserves discovery metadata only. It is not a substitute for the
local payloads and does not make absent artifacts reproducible from Git alone.
