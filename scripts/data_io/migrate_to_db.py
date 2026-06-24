"""
Migrate the local parquet dataset into TimescaleDB (company_scores table).

The table is created as a TimescaleDB hypertable partitioned on fiscal_year
so time-range queries are fast.

Usage:
    # With docker-compose running:
    DATABASE_URL=postgresql://screener:screener@localhost:5432/screener \\
        python3 scripts/migrate_to_db.py

Options:
    --chunk-size  Rows per INSERT batch (default: 1000)
    --if-exists   What to do if table already exists:
                    replace  — drop and recreate   (default)
                    append   — append new rows only
                    fail     — abort if table exists
    --dry-run     Print row counts and schema without writing
"""
from __future__ import annotations

import argparse
import sys
import os
from pathlib import Path

import pandas as pd
from scripts._root import ROOT

BASE = ROOT

_DATA_PATHS = [
    BASE / "data" / "historical_dataset_clean.parquet",
    BASE / "data" / "historical_dataset.parquet",
]
_TABLE = "company_scores"


def load_parquet() -> pd.DataFrame:
    for p in _DATA_PATHS:
        if p.exists():
            print(f"Loading: {p}")
            return pd.read_parquet(p)
    raise FileNotFoundError(f"No parquet file found. Checked: {_DATA_PATHS}")


def _clean_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce column types to be PostgreSQL-compatible."""
    df = df.copy()
    # Ensure fiscal_year is integer (hypertable partition key)
    if "fiscal_year" in df.columns:
        df["fiscal_year"] = pd.to_numeric(df["fiscal_year"], errors="coerce").astype("Int64")
    # Downcast nullable integers
    for col in df.select_dtypes("Int64").columns:
        df[col] = df[col].astype("float64")
    # Convert object columns that are really dates
    for col in ["filed_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def create_hypertable(engine, table: str) -> None:
    """Create the TimescaleDB hypertable for company_scores."""
    from sqlalchemy import text

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        ticker          TEXT        NOT NULL,
        fiscal_year     INTEGER     NOT NULL,
        period_type     TEXT,
        market          TEXT,
        exchange        TEXT,
        name            TEXT,
        filed_date      DATE,
        fraud_score_composite DOUBLE PRECISION,
        beneish_m_score       DOUBLE PRECISION,
        altman_z_score        DOUBLE PRECISION,
        piotroski_f_score     INTEGER,
        composite_score       DOUBLE PRECISION,
        ml_score_1y           DOUBLE PRECISION,
        ml_score_3y           DOUBLE PRECISION,
        ml_score_5y           DOUBLE PRECISION,
        data_confidence       TEXT,
        PRIMARY KEY (ticker, fiscal_year)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
        # Make it a hypertable on fiscal_year — ignore if already done
        try:
            conn.execute(text(
                f"SELECT create_hypertable('{table}', 'fiscal_year', "
                f"if_not_exists => TRUE, migrate_data => TRUE);"
            ))
        except Exception as exc:
            print(f"  Hypertable creation skipped ({exc}) — continuing with regular table.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate parquet → TimescaleDB")
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--if-exists", choices=["replace", "append", "fail"], default="replace")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL environment variable not set.")
        print("  Example: DATABASE_URL=postgresql://screener:screener@localhost:5432/screener")
        sys.exit(1)

    print("Loading parquet dataset...")
    df = load_parquet()
    print(f"  {len(df):,} rows × {len(df.columns)} columns")

    df = _clean_for_db(df)

    if args.dry_run:
        print("\nDry run — schema preview:")
        print(df.dtypes.to_string())
        print(f"\nWould write {len(df):,} rows to table '{_TABLE}' (if-exists={args.if_exists})")
        return

    print(f"\nConnecting to: {db_url[:40]}...")
    try:
        from sqlalchemy import create_engine
        engine = create_engine(db_url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(__import__("sqlalchemy").text("SELECT 1"))
        print("  Connection OK.")
    except Exception as exc:
        print(f"ERROR: Cannot connect to database: {exc}")
        sys.exit(1)

    if args.if_exists == "replace":
        print(f"Creating hypertable '{_TABLE}'...")
        create_hypertable(engine, _TABLE)

    print(f"Writing {len(df):,} rows in chunks of {args.chunk_size}...")
    written = 0
    for start in range(0, len(df), args.chunk_size):
        chunk = df.iloc[start : start + args.chunk_size]
        chunk.to_sql(
            _TABLE,
            engine,
            if_exists="append" if start > 0 or args.if_exists == "append" else args.if_exists,
            index=False,
            method="multi",
        )
        written += len(chunk)
        print(f"  {written:,} / {len(df):,}", end="\r", flush=True)

    print(f"\nDone. {written:,} rows written to '{_TABLE}'.")
    print(f"Verify: psql {db_url} -c 'SELECT COUNT(*) FROM {_TABLE};'")


if __name__ == "__main__":
    main()
