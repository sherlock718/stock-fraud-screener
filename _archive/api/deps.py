"""
Shared dependencies and lazy-loaded singletons for the API.

Data source priority:
  1. TimescaleDB / PostgreSQL  (DATABASE_URL env var)
  2. Parquet files              (local fallback)
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_DATA_PATHS = [
    Path("data") / "historical_dataset_clean.parquet",
    Path("data") / "historical_dataset.parquet",
]

_SCORE_TABLE = "company_scores"


def _load_from_db() -> pd.DataFrame | None:
    """Try to load all company scores from TimescaleDB/PostgreSQL."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    try:
        from sqlalchemy import create_engine, text

        engine = create_engine(
            url,
            pool_pre_ping=True,
            connect_args={"connect_timeout": 5},
        )
        with engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {_SCORE_TABLE}")).scalar()
            if not count:
                logger.warning("TimescaleDB table %s is empty — falling back to parquet.", _SCORE_TABLE)
                return None
        df = pd.read_sql(f"SELECT * FROM {_SCORE_TABLE}", engine)
        logger.info("Loaded %d rows from TimescaleDB.", len(df))
        return df
    except Exception as exc:
        logger.warning("TimescaleDB unavailable (%s) — falling back to parquet.", exc)
        return None


def _load_from_parquet() -> pd.DataFrame | None:
    for p in _DATA_PATHS:
        if p.exists():
            logger.info("Loading dataset from %s", p)
            return pd.read_parquet(p)
    return None


@lru_cache(maxsize=1)
def get_dataset() -> pd.DataFrame | None:
    """Return the full dataset, preferring TimescaleDB over local parquet files."""
    df = _load_from_db()
    if df is not None:
        return df
    return _load_from_parquet()


def get_db_engine():
    """Return a SQLAlchemy engine when DATABASE_URL is set, else None."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    try:
        from sqlalchemy import create_engine
        return create_engine(url, pool_pre_ping=True)
    except Exception as exc:
        logger.warning("Could not create DB engine: %s", exc)
        return None
