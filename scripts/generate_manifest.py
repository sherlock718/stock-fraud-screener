"""
Generate ARTIFACT_MANIFEST.json for the local data/ directory.

Computes metadata (rows, columns, markets, checksums) for all pipeline artifacts
that exist locally. The manifest is uploaded to HuggingFace (not committed to git)
and used by pull_from_hf.py for checksum verification.

Usage:
    python3 scripts/generate_manifest.py
    python3 scripts/generate_manifest.py --dry-run   # print to stdout, don't write
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).parent.parent
DATA = BASE / 'data'
OUT  = DATA / 'ARTIFACT_MANIFEST.json'

TRACKED_ARTIFACTS = [
    'historical_dataset_clean.parquet',
    'snapshots.parquet',
    'prices.parquet',
    'snapshots_kr.parquet',
    'snapshots_ca.parquet',
    'snapshots_eu.parquet',
    'snapshots_jp.parquet',
    'snapshots_br.parquet',
]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def _git_commit() -> str:
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            capture_output=True, text=True, cwd=str(BASE))
        return result.stdout.strip() if result.returncode == 0 else 'unknown'
    except Exception:
        return 'unknown'


def _parquet_meta(path: Path) -> dict:
    meta: dict = {}
    try:
        import pandas as pd
        df = pd.read_parquet(path)
        meta['rows'] = len(df)
        meta['columns'] = len(df.columns)
        if 'period_type' in df.columns:
            meta['annual_rows'] = int((df['period_type'] == 'annual').sum())
        if 'market' in df.columns:
            meta['markets'] = sorted(df['market'].unique().tolist())
    except Exception:
        pass
    return meta


def generate(dry_run: bool = False) -> dict:
    manifest = {
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'generated_by': 'scripts/generate_manifest.py',
        'source_commit': _git_commit(),
        'artifacts': [],
    }

    for name in TRACKED_ARTIFACTS:
        path = DATA / name
        if not path.exists():
            continue

        entry = {
            'name': name,
            'path': f'data/{name}',
            'size_bytes': path.stat().st_size,
            'sha256': _sha256(path),
            'storage': 'huggingface',
        }

        if name.endswith('.parquet'):
            entry.update(_parquet_meta(path))

        manifest['artifacts'].append(entry)

    if dry_run:
        print(json.dumps(manifest, indent=2))
    else:
        OUT.write_text(json.dumps(manifest, indent=2))
        print(f'Manifest written: {OUT}')
        print(f'  Artifacts: {len(manifest["artifacts"])}')
        print(f'  Commit: {manifest["source_commit"]}')

    for art in manifest['artifacts']:
        size_mb = art['size_bytes'] / 1_048_576
        rows = art.get('rows', '?')
        cols = art.get('columns', '?')
        print(f'  {art["name"]}: {size_mb:.1f} MB | {rows} rows × {cols} cols')

    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Generate ARTIFACT_MANIFEST.json for local data artifacts.')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print manifest to stdout without writing file')
    args = parser.parse_args()
    generate(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
