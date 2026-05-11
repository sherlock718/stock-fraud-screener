"""
Shared dependencies and lazy-loaded singletons for the API.
"""
from __future__ import annotations

import pandas as pd
from pathlib import Path
from functools import lru_cache

_DATA_PATHS = [
    Path("data") / "historical_dataset_clean.parquet",
    Path("data") / "historical_dataset.parquet",
]


@lru_cache(maxsize=1)
def get_dataset() -> pd.DataFrame | None:
    for p in _DATA_PATHS:
        if p.exists():
            return pd.read_parquet(p)
    return None
