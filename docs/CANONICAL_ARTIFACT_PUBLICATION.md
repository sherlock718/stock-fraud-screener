# Canonical Artifact Publication

Status: C2 complete. The exact private P2-P4 publication, temporary
byte-verification, immutable pointers, remote reconciliation, and authorized
Git checkpoint succeeded.

## Fixed destination and content

Canonical artifacts use the existing private dataset repository
`ekrash718/stock-screener-data`. The legacy root-level upload and download
utilities remain unchanged. C2 adds a separate content-addressed namespace:

```text
canonical/<artifact-name>/<manifest-sha256>/<artifact-relative-path>
```

The published and locally verified revision is
`aaf056ea115067e42ef9abf9fa93ade75cdd4052`:

| Stage | Artifact | Manifest SHA-256 | Files | Bytes |
|---|---|---:|---:|---:|
| P2 | `corrected_us_annual` | `40e7c716ce98dfece7caf4dfc42739425660b83b7c1ac73d1cbdadfee7a3c2b3` | 34 | 436,240,397 |
| P3 | `corrected_us_annual_3y_research_model` | `8ed9e4a514a06ab1b542886abb1d41e727400df711c7f89be8f71cbc549b80f2` | 142 | 13,758,028 |
| P4 | `corrected_us_annual_3y_product` | `28ecc946c5c1c3c75ee6e13013fdb1a7eda1e1fd73d70e5d915f3d1edd1aabc7` | 26 | 31,668,282 |
| Total | P2-P4 | — | 202 | 481,666,707 |

Each stage count is its manifest plus every `records` entry. Local preparation
fails if a manifest hash, declared size, declared hash, path boundary, symlink
policy, or complete artifact-root file set differs.

Generate the local-only plan with:

```bash
python3 -m data_io.publish_canonical_to_hf --prepare
```

An optional `--plan-output <new-path>` writes all 202 exact source,
repository, size, and hash records without overwriting an existing plan.

## Fail-closed remote contract

Authenticated remote actions use `HF_TOKEN` when set, otherwise the standard
Hugging Face user credential store. The canonical utilities never print the
token. Before any upload, the publisher:

1. requires authenticated repository metadata to report `private is True`;
2. requires a full 40-character repository-head commit SHA;
3. lists repository files at that exact SHA;
4. refuses the complete operation if any planned destination already exists.

Publication uses one dataset-repository commit with the verified head as
`parent_commit`. A concurrent repository change therefore fails instead of
silently rebasing or overwriting. No repository is created, no visibility is
changed, and no legacy root path is included.

The read-only authenticated preflight is:

```bash
python3 -m data_io.publish_canonical_to_hf --check-visibility
```

Authentication is saved outside the repository in the Hugging Face user
credential directory with owner-only file permissions. The authenticated
read-only preflight verified:

- repository visibility: private;
- repository type: dataset;
- checked parent revision:
  `0b8f3baac4c823e0ef89d8a73da11c3f0e88c9db`;
- all 202 planned destination paths: absent;
- uploads performed: false.

The completed publication repeated this preflight, parent-pinned one commit,
and retained private visibility.

## Approval-bound publication and recovery

Do not run this command without explicit upload approval:

```bash
python3 -m data_io.publish_canonical_to_hf \
  --publish \
  --confirm-repo ekrash718/stock-screener-data
```

The user supplied explicit approval and informed re-authorization after
disclosure that the 481,666,707-byte payload includes frozen source-diff and
inventory lineage. Codex's execution environment denied the transfer, so the
user ran the exact guarded command directly. It succeeded and created revision
`aaf056ea115067e42ef9abf9fa93ade75cdd4052`.

After the single commit, the command downloads all 202 records at the returned
immutable commit SHA into a temporary target and independently checks every
byte size and SHA-256. Only then does it create the three non-overwriting
tracked pointer manifests under `data_io/canonical_artifact_pointers/`.

If the remote commit succeeds but verification is interrupted, the error
reports the new revision. Resume without another upload:

```bash
python3 -m data_io.publish_canonical_to_hf \
  --finalize-revision <full-40-character-commit-sha>
```

Each pointer records the repository, repository type, immutable revision,
artifact-relative path, repository path, byte size, and SHA-256 for every
file. P2-P4 pointers must share one repository and revision.

Authenticated post-publication metadata reconciliation confirmed:

- repository visibility: private;
- repository revision:
  `aaf056ea115067e42ef9abf9fa93ade75cdd4052`;
- expected paths: 202;
- present paths: 202;
- missing paths: 0.

## Smooth future operation

The repository includes a location-independent wrapper. It resolves and enters
the repository root automatically, so it works when launched from any current
directory:

```bash
/Users/mhoque/Desktop/stock-fraud-screener-main/scripts/publish_canonical \
  --prepare
```

For a separately approved future version, replace `--prepare` with the guarded
`--publish --confirm-repo ekrash718/stock-screener-data` flags. Saved
authentication, local reconciliation, private visibility, collision checks,
parent-pinned upload, temporary download verification, and pointer creation
are automatic.

## Clean-checkout retrieval and CI

After pointer creation, a clean checkout can populate the absent canonical
root with:

```bash
python3 -m data_io.retrieve_canonical_from_hf
```

The retriever refuses an existing target, rechecks private visibility, rejects
mutable or shortened revisions, pins every download to the pointer commit,
verifies every file in a staging directory, and exposes the target only after
complete success.

`.github/workflows/verify_canonical_artifacts.yml` provides the same
secret-backed recovery check through manual `workflow_dispatch`. It has no
`main`, `latest`, or other mutable-revision fallback.

The authorized C2 checkpoint includes the publisher/retriever code, workflow,
wrapper, tests, and pointer manifests. A clean checkout with the saved
credential can therefore discover and recover the exact published revision.
