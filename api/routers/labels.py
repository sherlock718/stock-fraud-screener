"""
/labels — fraud event labels from SEC AAER and other sources.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()


class FraudLabel(BaseModel):
    ticker: str
    market: str
    fraud_year: int
    label_type: str
    source: str | None
    description: str | None
    fraud_confirmed: bool
    fraud_suspect: bool


class FraudLabelList(BaseModel):
    ticker: str
    labels: list[FraudLabel]


@router.get("/{ticker}", response_model=FraudLabelList, summary="Get fraud event labels for a ticker")
def get_labels(ticker: str, market: str = "US") -> FraudLabelList:
    """
    Return known fraud event labels (AAER, restatements, bankruptcies).
    Labels are sourced from SEC AAER, Stanford SCAC, and SEC EDGAR BK tags.
    Run `scripts/build_fraud_labels.py` to refresh.
    """
    import pandas as pd
    from pathlib import Path
    labels_path = Path("data") / "fraud_labels.parquet"
    if not labels_path.exists():
        raise HTTPException(
            404,
            detail=(
                "fraud_labels.parquet not found. "
                "Run scripts/build_fraud_labels.py to generate it."
            ),
        )

    df = pd.read_parquet(labels_path)
    sub = df[(df["ticker"].str.upper() == ticker.upper()) & (df["market"] == market)]
    if sub.empty:
        return FraudLabelList(ticker=ticker.upper(), labels=[])

    labels = []
    for _, r in sub.iterrows():
        labels.append(FraudLabel(
            ticker=r["ticker"],
            market=r["market"],
            fraud_year=int(r["fraud_year"]),
            label_type=r["label_type"],
            source=r.get("source"),
            description=r.get("description"),
            fraud_confirmed=bool(r.get("fraud_confirmed", False)),
            fraud_suspect=bool(r.get("fraud_suspect", False)),
        ))
    return FraudLabelList(ticker=ticker.upper(), labels=labels)
