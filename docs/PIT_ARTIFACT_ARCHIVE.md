# PIT Artifact Archive Index

Updated: 2026-08-01, Session A1

Large PIT evidence is Git-ignored. Canonical/source boundaries remain in place;
only the four rows marked `A1 archived` were packaged and retired after exact
recovery proof.

| Artifact set | Manifest SHA-256 | A1 status |
|---|---|---|
| `contract_aligned_label_inputs` | `0ab15685a445f09919b19393e074b8b5903afef7e12defc7c44aeac624f4581a` | Retained Session 8E source evidence |
| `corrected_feature_population` | `9c1e4b82c2e7bc2a85228adf6668b797acec827e2bd1ff7c58d20cfb6a9ac01a` | Retained frozen evidence |
| `corrected_partial` | `10b648c581ed56d6b6acb7f16d786be00409f4dfc96f0e92073693276e51b6ee` | A1 archived; exact manifest + pointer retained |
| `corrected_partial_inputs` | `31c4f4e289ac49ef7c01f333370b4e3a35a3e786c0b289200bdc95e9c5c2f4d5` | A1 archived; exact manifest + pointer retained |
| `corrected_step2` | `899cffd7a9d1dc3395a08bee5c65ad4a5e8a109a83c63346ac54c891fe706e08` | Retained corrected source evidence |
| `legacy_saved` | `00b237943c47700ba311c330da7bf5b1a13eb078eb19a669dd62dd1659f69aae` | Retained; consumers/replacement unresolved |
| `session9_corrected_8f` | `bb75e2be97104736786a14c2b6c435fecef5aa335fb41181fe1f1947cb094deb` | A1 archived; exact manifest + pointer retained |
| `session9b_oos_selection_freeze` | `2b17030e554b8f663f48e7f44299107f781dbdadc4d73054361b3ebe1ab492b8` | A1 archived; exact manifest + pointer retained |
| `session_v3_1_production_contract` | `2b5249cdb05c7bad1759abbd281ec1c90a8a9ce2fbd72973cd4dc905c8a86e5a` | Retained: M1A pin/parity dependency |
| `session_v3_2_oos_predictions` | `ba0e3b2d850af113c26306dbec1d9d5cab7a58aa78cafd40cefac31059899912` | Retained: parity-test dependency |
| `session_v3_3_liquidity_holdings` | `8bf4cf867e883764d4e25c0d61a755c02443196ceac76be2843f7ff3ebf7bea3` | Retained: parity-test dependency |

`calendar_contract` and `training_label_market_inputs` remain small supporting
directories without top-level manifests. Exact A1 package hashes, per-file
inventory hashes, source byte counts, recovery commands, and exclusions are in
`docs/A1_ARCHIVE_MANIFEST.json` and `docs/A1_ARCHIVE_CONTRACT.json`.

The A1 packages are local and Git-ignored. No Hugging Face upload, publication,
or remote durability claim was made for them; the accepted P2-P4 immutable
private pointers remain unchanged.
