# Session 7 — Frozen Legacy Evidence and Compatibility Validation

Date: 2026-07-15

Scope: freeze the locally available `LEGACY_SAVED` evidence and add manifest
compatibility validation. No dataset, price, prediction, model, backtest, or
production result was rebuilt or regenerated.

## Outcome

`artifacts/pit_validation/legacy_saved/` is an evidentiary snapshot, not a
reproducible run. Its manifest explicitly sets `reproducible=false`. The saved
aggregate performance result lacks holdings, weights, fold/model identity,
training cutoffs, feature lineage, fallback choices, and score sources, so this
session does not describe the legacy performance claim as reproducible.

The snapshot contains 39 files totaling 108,828,661 bytes. The manifest has 45
artifact records: 35 present and 10 explicitly missing. It preserves the active
3-year classifier and other locally available model evidence, including archive
candidates, but records the expected active 1-year and 5-year saved classifiers
as missing. Archive files are not treated as substitutes without lineage.
The frozen manifest SHA-256 is
`00b237943c47700ba311c330da7bf5b1a13eb078eb19a669dd62dd1659f69aae`.

`OLD_RECONSTRUCTED` is reserved in the manifest and remains unpopulated.
`CORRECTED_PARTIAL` and `FULL_PIT` are neither built nor populated.

## Frozen lineage and dirty state

- Required baseline commit:
  `3f706e3e10d2b354c6e8b9407760fa2074749c0a`.
- The complete pre-Session-7 status relative to that baseline contains 74
  entries: 44 tracked modifications and 30 untracked files.
- Tracked binary patch SHA-256:
  `ce49eeb7228e874a6210936c16d4d698ada506c2b33c37180c6af9293a68bc10`.
- Untracked binary patch SHA-256:
  `4e81dc241a38dcb380b4609f37e7e48573a0717e3af4d09751b609c4531c34ac`.
- Status-file SHA-256:
  `0120e61ee8ff1212ada1bb2a2168f5cb5078dc71d1e97a6fb0c924a76b928f61`.
- `.codex/config.toml` is recorded by path, size, and hash, but its content is
  deliberately excluded from the patch snapshot because it may contain
  credentials. No credentials or secrets were archived.
- The manifest states that saved legacy artifacts predate the corrected code
  from Sessions 1–6B.

## Key artifact evidence

| Evidence | Status | SHA-256 / limitation |
|---|---|---|
| Clean dataset | present | `520a9b52e2a63d013a3527abbcde32c484a226c2739450d2a6a48ab175144dae` |
| Monthly price cache | present | `9c7ad56e835d50f3cd121d55341d9b6d0ffc07f5aaee2b30708a50031266461e` |
| Saved backtest result | present | `4806317b6329a32c42b679b7eb87091378aff27d431a207988b0e41145afd683` |
| Active 3-year classifier | present | `f527d7dd616727b9488bb43b8a2c42efafd42b3afca6eeaf2a6f827f9ad02c7c` |
| Active 1-year classifier | missing | expected `models/model_1y.joblib` |
| Active 5-year classifier | missing | expected `models/model_5y.joblib` |
| Row-level predictions/lineage | missing | no holdings, folds, score sources, or compatible sidecar |
| Canonical benchmark NAV | missing | annual benchmark CSVs are not monthly NAV |
| Monthly risk-free returns | missing | no frozen time-aligned series |
| Adjusted-price provenance | missing | no source/vintage/split/dividend evidence |
| Corporate-action evidence | missing | no dated event terms for selected securities |
| Security mapping evidence | missing | no point-in-time ticker/security mapping |

The actual frozen monthly-cache hash matches the Session 5 report. It does not
match `d0e7c3ee...`, which appears in the later handoff verification summary;
the manifest records both values and the discrepancy rather than substituting a
historical summary for the available bytes.

## Corrected-code evidence, not legacy inputs

The Session 6B NAV/event schema and explicit return policies are frozen only as
corrected-code evidence:

- `backtest/monthly_nav.py`:
  `75379adb4006020b081682eed4b506219d39f91888e0b78c7241dd8da9e6955d`;
- `backtest/engine.py` return-policy wiring:
  `0a95bcc7f01b49bfc0d448519fb2a9de84fd467f15708b14b5b99706509f376f`;
- Session 6B report:
  `9249faf39ea87413a2b8e3260e73225af74578e0c8a4f541e80e5dacc5edb2dd`.

The compatibility validator rejects any manifest that marks these corrected
artifacts as legacy inputs. Commit equality with later documentation or
validator commits is diagnostic only and does not invalidate frozen bytes.

## Verification

- `python3 -m pytest tests/quality/test_artifact_compatibility.py -q`:
  5 passed.
- `python3 -m quality.artifact_compatibility
  artifacts/pit_validation/legacy_saved/manifest.json --check-sources`:
  compatible, 0 errors, 0 warnings.
- Streaming hashes were used for all copied payloads, including the 88,494,877
  byte clean dataset.
- No full test suite was repeated because this bounded validator does not alter
  shared production behavior.

## Explicit limitations

The snapshot cannot establish the saved portfolio constituents, OOS prediction
lineage, monthly benchmark-relative path, risk-adjusted metrics, adjusted-price
semantics, or disappearing-security outcomes. It therefore cannot reproduce or
methodologically certify the saved performance claim. Session 6B corrected
schemas do not repair the old artifact after the fact and are not treated as
inputs to it.
