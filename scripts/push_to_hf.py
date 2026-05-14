"""
Push dataset and models to HuggingFace Hub.

Files uploaded:
  data/historical_dataset_clean.parquet  →  datasets/{HF_REPO}/historical_dataset_clean.parquet
  models/model_meta.json                 →  datasets/{HF_REPO}/models/model_meta.json
  models/model_{h}.joblib                →  datasets/{HF_REPO}/models/model_{h}.joblib  (h in 1y/3y/5y)
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
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

BASE       = Path(__file__).parent.parent
DATA_PATH  = BASE / 'data' / 'historical_dataset_clean.parquet'
MODELS_DIR = BASE / 'models'
HORIZONS   = ['1y', '3y', '5y']


def _get_token() -> str | None:
    token = os.environ.get('HF_TOKEN')
    if token:
        return token
    hf_cache = Path.home() / '.huggingface' / 'token'
    if hf_cache.exists():
        return hf_cache.read_text().strip()
    return None


def push(repo_id: str, push_data: bool, push_models: bool,
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
                        help='Only upload the parquet dataset')
    parser.add_argument('--models-only', action='store_true',
                        help='Only upload models')
    parser.add_argument('--private', action='store_true', default=True,
                        help='Create as private repo (default: True)')
    parser.add_argument('--public', action='store_true',
                        help='Create as public repo')
    parser.add_argument('--message', default='Auto-update from push_to_hf.py',
                        help='Commit message')
    args = parser.parse_args()

    push_data   = not args.models_only
    push_models = not args.data_only
    private     = not args.public

    print(f'HuggingFace push → {args.repo}')
    print(f'  private={private}  push_data={push_data}  push_models={push_models}')
    push(args.repo, push_data, push_models, private, args.message)


if __name__ == '__main__':
    main()
