"""
/companies — look up fraud scores for a specific ticker.
"""
from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()


class CompanyScore(BaseModel):
    ticker: str
    market: str
    fiscal_year: int
    composite_score: float | None
    ml_score_1y: float | None
    ml_score_3y: float | None
    ml_score_5y: float | None
    beneish_m_score: float | None
    altman_z_score: float | None
    piotroski_f_score: int | None
    data_confidence: str | None


class CompanyScoreList(BaseModel):
    ticker: str
    market: str
    history: list[CompanyScore]
    disclaimer: str = (
        "Scores are probabilistic model outputs, not fraud determinations. "
        "Not financial advice."
    )


@router.get("/{ticker}", response_model=CompanyScoreList, summary="Get fraud scores for a ticker")
def get_company_scores(
    ticker: str,
    market: Annotated[str, Query(description="Market code, e.g. US, DE, KR")] = "US",
) -> CompanyScoreList:
    """
    Return the full score history for a ticker.
    Data is loaded from the parquet dataset on startup; DB integration pending.
    """
    from api.deps import get_dataset
    df = get_dataset()
    if df is None or df.empty:
        raise HTTPException(503, "Dataset not available")

    sub = df[(df["ticker"].str.upper() == ticker.upper()) & (df["market"] == market)]
    if sub.empty:
        raise HTTPException(404, f"Ticker {ticker} not found in market {market}")

    rows = []
    for _, r in sub.sort_values("fiscal_year").iterrows():
        rows.append(CompanyScore(
            ticker=r["ticker"],
            market=r["market"],
            fiscal_year=int(r["fiscal_year"]),
            composite_score=_f(r, "composite_score") or _f(r, "fraud_score_composite"),
            ml_score_1y=_f(r, "ml_score_1y"),
            ml_score_3y=_f(r, "ml_score_3y"),
            ml_score_5y=_f(r, "ml_score_5y"),
            beneish_m_score=_f(r, "beneish_m_score"),
            altman_z_score=_f(r, "altman_z_score"),
            piotroski_f_score=_i(r, "piotroski_f_score"),
            data_confidence=str(r.get("data_confidence", "")) or None,
        ))
    return CompanyScoreList(ticker=ticker.upper(), market=market, history=rows)


def _f(row, col: str) -> float | None:
    import pandas as pd
    v = row.get(col)
    return round(float(v), 4) if v is not None and pd.notna(v) else None


def _i(row, col: str) -> int | None:
    import pandas as pd
    v = row.get(col)
    return int(v) if v is not None and pd.notna(v) else None
