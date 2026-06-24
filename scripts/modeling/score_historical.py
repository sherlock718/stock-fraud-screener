"""
score_historical.py — Apply trained LightGBM models to the full historical dataset.

Loads model_{1y,3y,5y}.joblib and model_meta.json, scores every row in
historical_dataset_clean.parquet, and writes ml_1y / ml_3y / ml_5y columns
back to the same parquet file.

Also loads model_3y_regression.joblib (Huber regressor) if present and writes
ml_pred_excess_3y — the predicted magnitude of 3y excess return — to the parquet.
This column is used by leverage_strategy.py for Stage 3 magnitude ranking.

Usage:
  python3 scripts/score_historical.py [--parquet PATH] [--models-dir DIR] [--dry-run]
  python3 scripts/score_historical.py --skip-regression   # skip regression scoring

Flags:
  --parquet          Path to dataset parquet (default: data/historical_dataset_clean.parquet)
  --models-dir       Directory containing model_*.joblib + model_meta.json (default: models/)
  --dry-run          Score but do NOT write parquet; print summary stats only
  --skip-regression  Skip loading model_3y_regression.joblib (faster if not needed)
"""

import argparse
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd


HORIZONS = ["1y", "3y", "5y"]
REGRESSION_MODEL_FILE = "model_3y_regression.joblib"
REGRESSION_META_FILE  = "model_3y_regression_meta.json"
REGRESSION_COL        = "ml_pred_excess_3y"


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


def score_regression(df: pd.DataFrame, model, meta: dict) -> np.ndarray:
    """Score all rows with the Huber regression model; returns predicted excess returns."""
    features = meta["features"]
    medians  = meta["train_medians"]

    missing = [f for f in features if f not in df.columns]
    if missing:
        print(f"  [regression] WARNING: {len(missing)} features missing, filling with median: "
              f"{missing[:5]}{'...' if len(missing) > 5 else ''}")

    X = df.reindex(columns=features)
    for col in X.columns:
        if X[col].isna().any():
            X[col] = X[col].fillna(medians.get(col, 0.0))

    return model.predict(X).astype(np.float32)


def run(parquet_path: Path, models_dir: Path, dry_run: bool,
        skip_regression: bool) -> None:
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

    # ── Regression model (magnitude) ─────────────────────────────────────────
    reg_model_path = models_dir / REGRESSION_MODEL_FILE
    reg_meta_path  = models_dir / REGRESSION_META_FILE
    if not skip_regression and reg_model_path.exists() and reg_meta_path.exists():
        print(f"\nScoring regression model: {REGRESSION_COL}")
        reg_model = joblib.load(reg_model_path)
        reg_meta  = json.loads(reg_meta_path.read_text())
        preds = score_regression(df, reg_model, reg_meta)
        df[REGRESSION_COL] = preds
        pct = np.percentile(preds, [10, 25, 50, 75, 90])
        print(f"  {REGRESSION_COL}: min={preds.min():.4f} p10={pct[0]:.4f} p25={pct[1]:.4f} "
              f"p50={pct[2]:.4f} p75={pct[3]:.4f} p90={pct[4]:.4f} max={preds.max():.4f}")
    elif not skip_regression:
        print(f"\n  [regression] {reg_model_path.name} not found — skipping "
              f"(run train_regression_model.py first)")

    if dry_run:
        print("\n[dry-run] Parquet NOT written.")
        return

    df.to_parquet(parquet_path, index=False)
    new_cols = [f"ml_{h}" for h in HORIZONS]
    if REGRESSION_COL in df.columns:
        new_cols.append(REGRESSION_COL)
    print(f"\nWritten: {parquet_path}  ({df.shape[0]:,} rows × {df.shape[1]} columns)")
    print(f"New columns added: {', '.join(new_cols)}")


def main():
    parser = argparse.ArgumentParser(description="Score historical dataset with trained LightGBM models")
    parser.add_argument("--parquet", default="data/historical_dataset_clean.parquet")
    parser.add_argument("--models-dir", default="models")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-regression", action="store_true",
                        help="Skip regression model scoring (ml_pred_excess_3y)")
    args = parser.parse_args()

    parquet_path = Path(args.parquet)
    models_dir = Path(args.models_dir)

    if not parquet_path.exists():
        sys.exit(f"ERROR: {parquet_path} not found")

    run(parquet_path, models_dir, args.dry_run, args.skip_regression)


if __name__ == "__main__":
    main()
