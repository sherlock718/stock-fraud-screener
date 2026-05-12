"""
score_historical.py — Apply trained LightGBM models to the full historical dataset.

Loads model_{1y,3y,5y}.joblib and model_meta.json, scores every row in
historical_dataset_clean.parquet, and writes ml_1y / ml_3y / ml_5y columns
back to the same parquet file.

Usage:
  python3 scripts/score_historical.py [--parquet PATH] [--models-dir DIR] [--dry-run]

Flags:
  --parquet    Path to dataset parquet (default: data/historical_dataset_clean.parquet)
  --models-dir Directory containing model_*.joblib + model_meta.json (default: models/)
  --dry-run    Score but do NOT write parquet; print summary stats only
"""

import argparse
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd


HORIZONS = ["1y", "3y", "5y"]


def load_meta(models_dir: Path) -> dict:
    meta_path = models_dir / "model_meta.json"
    if not meta_path.exists():
        sys.exit(f"ERROR: {meta_path} not found")
    with open(meta_path) as f:
        return json.load(f)


def load_model(models_dir: Path, horizon: str):
    model_path = models_dir / f"model_{horizon}.joblib"
    if not model_path.exists():
        sys.exit(f"ERROR: {model_path} not found")
    return joblib.load(model_path)


def score_horizon(df: pd.DataFrame, model, meta: dict, horizon: str) -> np.ndarray:
    features = meta[horizon]["features"]
    medians = meta[horizon]["train_medians"]

    missing = [f for f in features if f not in df.columns]
    if missing:
        print(f"  [{horizon}] WARNING: {len(missing)} features missing from parquet, will fill with median: {missing[:5]}{'...' if len(missing) > 5 else ''}")

    X = df.reindex(columns=features)
    for col in X.columns:
        if X[col].isna().any():
            fill_val = medians.get(col, 0.0)
            X[col] = X[col].fillna(fill_val)

    scores = model.predict_proba(X)[:, 1]
    return scores


def run(parquet_path: Path, models_dir: Path, dry_run: bool) -> None:
    print(f"Loading dataset: {parquet_path}")
    df = pd.read_parquet(parquet_path)
    print(f"  Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

    meta = load_meta(models_dir)

    for horizon in HORIZONS:
        col = f"ml_{horizon}"
        print(f"\nScoring horizon: {horizon}")
        model = load_model(models_dir, horizon)
        scores = score_horizon(df, model, meta, horizon)
        df[col] = scores.astype(np.float32)
        pct = np.percentile(scores, [10, 25, 50, 75, 90])
        print(f"  {col}: min={scores.min():.4f} p10={pct[0]:.4f} p25={pct[1]:.4f} "
              f"p50={pct[2]:.4f} p75={pct[3]:.4f} p90={pct[4]:.4f} max={scores.max():.4f}")

    if dry_run:
        print("\n[dry-run] Parquet NOT written.")
        return

    df.to_parquet(parquet_path, index=False)
    print(f"\nWritten: {parquet_path}  ({df.shape[0]:,} rows × {df.shape[1]} columns)")
    print(f"New columns added: {', '.join(f'ml_{h}' for h in HORIZONS)}")


def main():
    parser = argparse.ArgumentParser(description="Score historical dataset with trained LightGBM models")
    parser.add_argument("--parquet", default="data/historical_dataset_clean.parquet")
    parser.add_argument("--models-dir", default="models")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    parquet_path = Path(args.parquet)
    models_dir = Path(args.models_dir)

    if not parquet_path.exists():
        sys.exit(f"ERROR: {parquet_path} not found")

    run(parquet_path, models_dir, args.dry_run)


if __name__ == "__main__":
    main()
