"""
Pull dataset artifacts from HuggingFace Hub into local data/.

Downloads the final dataset and/or intermediate support artifacts so the pipeline
can resume from Step 3+ without expensive Step 1-2 rebuilds.

Usage:
    python3 scripts/pull_from_hf.py --all
    python3 scripts/pull_from_hf.py --final
    python3 scripts/pull_from_hf.py --snapshots
    python3 scripts/pull_from_hf.py --manifest
    python3 scripts/pull_from_hf.py --all --repo your-username/stock-screener-data

Artifacts:
  --final      historical_dataset_clean.parquet (main end product)
  --snapshots  snapshots.parquet + prices.parquet + per-market snapshots
  --manifest   ARTIFACT_MANIFEST.json (checksums + metadata)
  --all        all of the above

Requirements:
  - pip install huggingface_hub pandas
  - HF_TOKEN env var (or ~/.huggingface/token) for private repos
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path
from scripts._root import ROOT

BASE = ROOT

DATA = BASE / 'data'

DEFAULT_REPO = 'ekrash718/stock-screener-data'

FINAL_ARTIFACTS = [
    'historical_dataset_clean.parquet',
]

SNAPSHOT_ARTIFACTS = [
    'snapshots.parquet',
    'prices.parquet',
    'snapshots_kr.parquet',
    'snapshots_ca.parquet',
    'snapshots_eu.parquet',
    'snapshots_jp.parquet',
    'snapshots_br.parquet',
]

MANIFEST_FILE = 'ARTIFACT_MANIFEST.json'


def _get_token() -> str | None:
    token = os.environ.get('HF_TOKEN')
    if token:
        return token
    hf_cache = Path.home() / '.huggingface' / 'token'
    if hf_cache.exists():
        return hf_cache.read_text().strip()
    return None


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def _load_manifest() -> dict | None:
    manifest_path = DATA / MANIFEST_FILE
    if manifest_path.exists():
        try:
            return json.loads(manifest_path.read_text())
        except Exception:
            pass
    return None


def _check_manifest_match(filename: str, manifest: dict | None) -> bool:
    if not manifest:
        return False
    local_path = DATA / filename
    if not local_path.exists():
        return False
    for art in manifest.get('artifacts', []):
        if art.get('name') == filename:
            expected_sha = art.get('sha256')
            if expected_sha:
                actual_sha = _sha256(local_path)
                return actual_sha == expected_sha
    return False


def _report_parquet(path: Path) -> None:
    try:
        import pandas as pd
        df = pd.read_parquet(path)
        parts = []
        parts.append(f'{len(df):,} rows')
        parts.append(f'{len(df.columns)} cols')
        if 'period_type' in df.columns:
            annual = (df['period_type'] == 'annual').sum()
            parts.append(f'{annual:,} annual')
        if 'market' in df.columns:
            markets = sorted(df['market'].unique().tolist())
            parts.append(f'{len(markets)} markets')
        print(f'    {" | ".join(parts)}')
    except Exception as e:
        print(f'    (could not read: {e})')


def pull(repo_id: str, artifacts: list[str], verify: bool = True) -> None:
    try:
        from huggingface_hub import hf_hub_download
    except ImportError:
        print('ERROR: huggingface_hub not installed — run: pip install huggingface_hub')
        sys.exit(1)

    token = _get_token()
    manifest = _load_manifest() if verify else None

    DATA.mkdir(exist_ok=True)
    downloaded = 0
    skipped = 0

    for filename in artifacts:
        local_path = DATA / filename

        if verify and _check_manifest_match(filename, manifest):
            size_mb = local_path.stat().st_size / 1_048_576
            print(f'  SKIP {filename} ({size_mb:.1f} MB — checksum matches manifest)')
            skipped += 1
            continue

        print(f'  GET  {filename}...', end=' ', flush=True)
        try:
            kwargs = dict(
                repo_id=repo_id,
                filename=filename,
                repo_type='dataset',
            )
            if token:
                kwargs['token'] = token
            cached_path = hf_hub_download(**kwargs)
            shutil.copy(cached_path, local_path)
            size_mb = local_path.stat().st_size / 1_048_576
            print(f'done ({size_mb:.1f} MB)')
            if filename.endswith('.parquet'):
                _report_parquet(local_path)
            downloaded += 1
        except Exception as e:
            err_msg = str(e)
            if '404' in err_msg or 'EntryNotFound' in err_msg:
                print(f'not found on HF (skipping)')
            else:
                print(f'FAILED: {e}')

    print(f'\n  Downloaded: {downloaded} | Skipped (up-to-date): {skipped}')


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Pull dataset artifacts from HuggingFace Hub.')
    parser.add_argument('--repo', default=os.environ.get('HF_REPO', DEFAULT_REPO),
                        help=f'HuggingFace repo ID (default: {DEFAULT_REPO})')
    parser.add_argument('--all', action='store_true',
                        help='Download final dataset + snapshots + manifest')
    parser.add_argument('--final', action='store_true',
                        help='Download historical_dataset_clean.parquet only')
    parser.add_argument('--snapshots', action='store_true',
                        help='Download snapshots.parquet + prices.parquet + per-market')
    parser.add_argument('--manifest', action='store_true',
                        help='Download ARTIFACT_MANIFEST.json only')
    parser.add_argument('--no-verify', action='store_true',
                        help='Skip checksum verification (re-download everything)')
    args = parser.parse_args()

    if not any([args.all, args.final, args.snapshots, args.manifest]):
        parser.print_help()
        print('\nSpecify at least one of: --all, --final, --snapshots, --manifest')
        sys.exit(1)

    artifacts: list[str] = []
    if args.all or args.manifest:
        artifacts.append(MANIFEST_FILE)
    if args.all or args.final:
        artifacts.extend(FINAL_ARTIFACTS)
    if args.all or args.snapshots:
        artifacts.extend(SNAPSHOT_ARTIFACTS)

    print(f'Pulling from: {args.repo}')
    print(f'Artifacts requested: {len(artifacts)}')
    print()
    pull(args.repo, artifacts, verify=not args.no_verify)


if __name__ == '__main__':
    main()
