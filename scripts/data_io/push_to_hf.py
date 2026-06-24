"""
Push dataset, snapshots, and models to HuggingFace Hub.

Files uploaded:
  data/historical_dataset_clean.parquet  →  datasets/{HF_REPO}/historical_dataset_clean.parquet
  data/snapshots.parquet                 →  datasets/{HF_REPO}/snapshots.parquet
  data/prices.parquet                    →  datasets/{HF_REPO}/prices.parquet
  data/snapshots_{market}.parquet        →  datasets/{HF_REPO}/snapshots_{market}.parquet
  data/ARTIFACT_MANIFEST.json            →  datasets/{HF_REPO}/ARTIFACT_MANIFEST.json
  models/model_meta.json                 →  datasets/{HF_REPO}/models/model_meta.json
  models/model_{h}.joblib                →  datasets/{HF_REPO}/models/model_{h}.joblib  (h in 6m/1y/2y/3y/5y)
  models/baseline_lr_{h}.joblib          →  datasets/{HF_REPO}/models/baseline_lr_{h}.joblib
  models/model_3y_regression.joblib      →  datasets/{HF_REPO}/models/model_3y_regression.joblib
  models/model_3y_regression_meta.json   →  datasets/{HF_REPO}/models/model_3y_regression_meta.json

Requirements:
  - HF_TOKEN env var (or ~/.huggingface/token)
  - pip install huggingface_hub

Usage:
    python3 scripts/push_to_hf.py --repo your-username/stock-screener-data
    python3 scripts/push_to_hf.py --repo your-username/stock-screener-data --data-only
    python3 scripts/push_to_hf.py --repo your-username/stock-screener-data --models-only
    python3 scripts/push_to_hf.py --repo your-username/stock-screener-data --snapshots-only
    python3 scripts/push_to_hf.py --repo your-username/stock-screener-data --all-data-artifacts
    python3 scripts/push_to_hf.py --repo your-username/stock-screener-data --manifest-only
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from scripts._root import ROOT

BASE = ROOT

DATA_PATH  = BASE / 'data' / 'historical_dataset_clean.parquet'
MODELS_DIR = BASE / 'models'
DATA_DIR   = BASE / 'data'
HORIZONS   = ['6m', '1y', '2y', '3y', '5y']

SNAPSHOT_FILES = [
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


def push(repo_id: str, push_data: bool, push_models: bool,
         push_snapshots: bool, push_manifest: bool,
         private: bool, commit_message: str) -> None:
    try:
        from huggingface_hub import HfApi, create_repo
    except ImportError:
        print('ERROR: huggingface_hub not installed — run: pip install huggingface_hub')
        sys.exit(1)

    token = _get_token()
    if not token:
        print('ERROR: HF_TOKEN not set. Set it in .env or export HF_TOKEN=your_token')
        sys.exit(1)

    api = HfApi(token=token)

    # Create repo if it doesn't exist
    try:
        create_repo(repo_id=repo_id, repo_type='dataset', private=private,
                    token=token, exist_ok=True)
        print(f'Repository: https://huggingface.co/datasets/{repo_id}')
    except Exception as e:
        print(f'WARNING: Could not create repo ({e}) — attempting upload anyway')

    files_to_upload: list[tuple[Path, str]] = []

    if push_data:
        if DATA_PATH.exists():
            files_to_upload.append((DATA_PATH, 'historical_dataset_clean.parquet'))
            size_mb = DATA_PATH.stat().st_size / 1_048_576
            print(f'  + dataset ({size_mb:.1f} MB)')
        else:
            print(f'  WARNING: {DATA_PATH} not found — skipping dataset')

    if push_snapshots:
        for name in SNAPSHOT_FILES:
            p = DATA_DIR / name
            if p.exists():
                files_to_upload.append((p, name))
                size_mb = p.stat().st_size / 1_048_576
                print(f'  + {name} ({size_mb:.1f} MB)')

    if push_manifest:
        manifest_path = DATA_DIR / MANIFEST_FILE
        if manifest_path.exists():
            files_to_upload.append((manifest_path, MANIFEST_FILE))
            print(f'  + {MANIFEST_FILE}')
        else:
            print(f'  WARNING: {manifest_path} not found — run generate_manifest.py first')

    if push_models:
        meta = MODELS_DIR / 'model_meta.json'
        if meta.exists():
            files_to_upload.append((meta, 'models/model_meta.json'))
            print('  + model_meta.json')
        for h in HORIZONS:
            for prefix in ('model', 'baseline_lr'):
                p = MODELS_DIR / f'{prefix}_{h}.joblib'
                if p.exists():
                    files_to_upload.append((p, f'models/{p.name}'))
                    size_mb = p.stat().st_size / 1_048_576
                    print(f'  + {p.name} ({size_mb:.1f} MB)')
                else:
                    print(f'  WARNING: {p} not found — skipping')
        for name in ('model_3y_regression.joblib', 'model_3y_regression_meta.json'):
            p = MODELS_DIR / name
            if p.exists():
                files_to_upload.append((p, f'models/{name}'))
                size_mb = p.stat().st_size / 1_048_576
                print(f'  + {name} ({size_mb:.1f} MB)')
            else:
                print(f'  WARNING: {p} not found — skipping')

    if not files_to_upload:
        print('Nothing to upload.')
        return

    print(f'\nUploading {len(files_to_upload)} file(s) to {repo_id}...')
    for local_path, repo_path in files_to_upload:
        print(f'  {repo_path}...', end=' ', flush=True)
        try:
            api.upload_file(
                path_or_fileobj=str(local_path),
                path_in_repo=repo_path,
                repo_id=repo_id,
                repo_type='dataset',
                commit_message=commit_message,
                token=token,
            )
            print('done')
        except Exception as e:
            print(f'FAILED: {e}')

    print(f'\n✓ Upload complete → https://huggingface.co/datasets/{repo_id}')


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', required=True,
                        help='HuggingFace repo ID, e.g. your-username/stock-screener-data')
    parser.add_argument('--data-only', action='store_true',
                        help='Only upload the final parquet dataset')
    parser.add_argument('--models-only', action='store_true',
                        help='Only upload models')
    parser.add_argument('--snapshots-only', action='store_true',
                        help='Only upload snapshots.parquet + prices.parquet + per-market')
    parser.add_argument('--all-data-artifacts', action='store_true',
                        help='Upload final dataset + snapshots + manifest (no models)')
    parser.add_argument('--manifest-only', action='store_true',
                        help='Only upload ARTIFACT_MANIFEST.json')
    parser.add_argument('--private', action='store_true', default=True,
                        help='Create as private repo (default: True)')
    parser.add_argument('--public', action='store_true',
                        help='Create as public repo')
    parser.add_argument('--message', default='Auto-update from push_to_hf.py',
                        help='Commit message')
    args = parser.parse_args()

    # Determine what to push
    if args.snapshots_only:
        push_data, push_models, push_snapshots, push_manifest = False, False, True, False
    elif args.manifest_only:
        push_data, push_models, push_snapshots, push_manifest = False, False, False, True
    elif args.all_data_artifacts:
        push_data, push_models, push_snapshots, push_manifest = True, False, True, True
    elif args.data_only:
        push_data, push_models, push_snapshots, push_manifest = True, False, False, False
    elif args.models_only:
        push_data, push_models, push_snapshots, push_manifest = False, True, False, False
    else:
        push_data, push_models, push_snapshots, push_manifest = True, True, False, True

    private = not args.public

    print(f'HuggingFace push → {args.repo}')
    print(f'  private={private}  data={push_data}  models={push_models}'
          f'  snapshots={push_snapshots}  manifest={push_manifest}')
    push(args.repo, push_data, push_models, push_snapshots, push_manifest,
         private, args.message)


if __name__ == '__main__':
    main()
