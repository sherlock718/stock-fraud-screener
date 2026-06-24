"""
/screener — list top fraud risks or fraud-safe opportunities.
"""
from __future__ import annotations

from typing import Annotated, Literal

import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class ScreenerRow(BaseModel):
    ticker:              str
    name:                str | None = None
    market:              str
    exchange:            str | None = None
    sector:              str | None = None
    fiscal_year:         int
    fraud_score:         float | None = None
    beneish_m_score:     float | None = None
    altman_z_score:      float | None = None
    piotroski_f_score:   int   | None = None
    composite_score:     float | None = None
    data_confidence:     str   | None = None


class ScreenerResult(BaseModel):
    mode:       str
    market:     str
    total:      int
    offset:     int
    limit:      int
    rows:       list[ScreenerRow]
    disclaimer: str = "Not financial advice. Scores are model outputs only."


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _f(row, col: str) -> float | None:
    v = row.get(col)
    return round(float(v), 4) if v is not None and pd.notna(v) else None


def _i(row, col: str) -> int | None:
    v = row.get(col)
    return int(v) if v is not None and pd.notna(v) else None


_CONF_ORDER = {"low": 0, "medium": 1, "high": 2}


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/",
    response_model=ScreenerResult,
    summary="Screen for top fraud risks or fraud-safe opportunities",
    responses={422: {"description": "Validation error — check query parameter bounds"}},
)
def screen(
    mode: Annotated[
        Literal["top_risks", "fraud_safe"],
        Query(description="top_risks: highest fraud scores | fraud_safe: lowest fraud scores"),
    ] = "top_risks",
    market: Annotated[
        str,
        Query(description="Market code (e.g. US, DE, KR) or 'all'", max_length=10),
    ] = "all",
    exchange: Annotated[
        str,
        Query(description="Exchange filter (e.g. NYSE, NASDAQ) or 'all'", max_length=20),
    ] = "all",
    sector: Annotated[
        str,
        Query(description="SIC code prefix or 'all' (e.g. '28' for chemicals)", max_length=10),
    ] = "all",
    min_piotroski: Annotated[
        int,
        Query(ge=0, le=9, description="Minimum Piotroski F-score (0–9)"),
    ] = 0,
    max_beneish: Annotated[
        float,
        Query(ge=-10.0, le=10.0, description="Maximum Beneish M-score (lower = safer; threshold ≈ -2.22)"),
    ] = 10.0,
    min_confidence: Annotated[
        Literal["", "low", "medium", "high"],
        Query(description="Minimum data confidence level"),
    ] = "",
    offset: Annotated[
        int,
        Query(ge=0, le=10_000, description="Pagination offset"),
    ] = 0,
    limit: Annotated[
        int,
        Query(ge=1, le=200, description="Number of results to return (max 200)"),
    ] = 25,
) -> ScreenerResult:
    from api.deps import get_dataset

    df = get_dataset()
    if df is None or df.empty:
        raise HTTPException(503, detail="Dataset not available")

    # Annual filings only; latest per ticker
    sub = df.copy()
    if "period_type" in sub.columns:
        sub = sub[sub["period_type"] == "annual"]

    # Market filter
    if market != "all":
        if "market" not in sub.columns:
            raise HTTPException(400, detail="'market' column not present in dataset")
        sub = sub[sub["market"].str.upper() == market.upper()]

    # Exchange filter
    if exchange != "all" and "exchange" in sub.columns:
        sub = sub[sub["exchange"].str.upper() == exchange.upper()]

    # Sector (SIC code prefix) filter
    if sector != "all" and "sic_code" in sub.columns:
        sub = sub[sub["sic_code"].astype(str).str.startswith(sector)]

    # Confidence filter
    if min_confidence and "data_confidence" in sub.columns:
        threshold = _CONF_ORDER.get(min_confidence.lower(), 0)
        sub = sub[sub["data_confidence"].str.lower().map(
            lambda x: _CONF_ORDER.get(x, 0) >= threshold
        )]

    # Piotroski filter
    if min_piotroski > 0 and "piotroski_f_score" in sub.columns:
        sub = sub[sub["piotroski_f_score"].fillna(-1) >= min_piotroski]

    # Beneish filter
    if max_beneish < 10.0 and "beneish_m_score" in sub.columns:
        sub = sub[sub["beneish_m_score"].fillna(999) <= max_beneish]

    # Choose score column
    score_col = next(
        (c for c in ["fraud_score_composite", "composite_score"] if c in sub.columns),
        None,
    )
    if score_col is None:
        raise HTTPException(500, detail="No fraud/composite score column in dataset")

    # Latest filing per ticker
    sub = (sub.sort_values("fiscal_year", ascending=False)
             .drop_duplicates("ticker", keep="first")
             .dropna(subset=[score_col]))

    ascending = mode == "fraud_safe"
    sub = sub.sort_values(score_col, ascending=ascending)

    total = len(sub)
    page  = sub.iloc[offset : offset + limit]

    rows = [
        ScreenerRow(
            ticker=str(r.get("ticker", "")),
            name=(str(r.get("name", ""))[:60] or None),
            market=str(r.get("market", "")),
            exchange=str(r.get("exchange", "")) or None,
            sector=str(r.get("sic_code", "")) or None,
            fiscal_year=int(r.get("fiscal_year", 0)),
            fraud_score=_f(r, score_col),
            beneish_m_score=_f(r, "beneish_m_score"),
            altman_z_score=_f(r, "altman_z_score"),
            piotroski_f_score=_i(r, "piotroski_f_score"),
            composite_score=_f(r, "composite_score"),
            data_confidence=str(r.get("data_confidence", "")) or None,
        )
        for _, r in page.iterrows()
    ]

    return ScreenerResult(
        mode=mode,
        market=market if market != "all" else "all",
        total=total,
        offset=offset,
        limit=limit,
        rows=rows,
    )
