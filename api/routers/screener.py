"""
/screener — list top fraud risks or fraud-safe opportunities.
"""
from __future__ import annotations

from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

router = APIRouter()


class ScreenerRow(BaseModel):
    ticker: str
    name: str | None
    market: str
    fiscal_year: int
    composite_score: float | None
    data_confidence: str | None


class ScreenerResult(BaseModel):
    mode: str
    market: str
    count: int
    rows: list[ScreenerRow]
    disclaimer: str = "Not financial advice. Scores are model outputs only."


@router.get("/", response_model=ScreenerResult, summary="Screen for top fraud risks or safe picks")
def screen(
    mode: Annotated[
        Literal["top_risks", "fraud_safe"],
        Query(description="top_risks: highest fraud scores | fraud_safe: lowest fraud scores")
    ] = "top_risks",
    market: Annotated[str, Query(description="Market code or 'all'")] = "all",
    top_n: Annotated[int, Query(ge=5, le=100)] = 25,
    min_confidence: Annotated[
        Literal["Low", "Medium", "High", ""],
        Query(description="Minimum data confidence level")
    ] = "",
) -> ScreenerResult:
    from api.deps import get_dataset
    df = get_dataset()
    if df is None or df.empty:
        raise HTTPException(503, "Dataset not available")

    score_col = "composite_score" if "composite_score" in df.columns else "fraud_score_composite"
    if score_col not in df.columns:
        raise HTTPException(500, "No composite score column in dataset")

    sub = df.copy()
    if market != "all":
        sub = sub[sub["market"] == market]

    if min_confidence:
        conf_order = {"Low": 0, "Medium": 1, "High": 2}
        threshold = conf_order.get(min_confidence, 0)
        if "data_confidence" in sub.columns:
            sub = sub[sub["data_confidence"].map(
                lambda x: conf_order.get(str(x), 0) >= threshold
            )]

    # Latest year per ticker
    sub = (sub.sort_values("fiscal_year", ascending=False)
              .drop_duplicates("ticker", keep="first")
              .dropna(subset=[score_col]))

    ascending = mode == "fraud_safe"
    sub = sub.sort_values(score_col, ascending=ascending).head(top_n)

    rows = []
    for _, r in sub.iterrows():
        rows.append(ScreenerRow(
            ticker=str(r.get("ticker", "")),
            name=str(r.get("name", ""))[:40] if r.get("name") else None,
            market=str(r.get("market", "")),
            fiscal_year=int(r.get("fiscal_year", 0)),
            composite_score=round(float(r[score_col]), 4),
            data_confidence=str(r.get("data_confidence", "")) or None,
        ))

    return ScreenerResult(
        mode=mode,
        market=market,
        count=len(rows),
        rows=rows,
    )
