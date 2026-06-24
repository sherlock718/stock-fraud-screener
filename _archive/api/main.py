"""
FastAPI backend for the Stock Fraud Screener.
Provides REST endpoints consumed by the Streamlit app and external clients.
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import companies, screener, labels

app = FastAPI(
    title="Stock Fraud Screener API",
    version="1.0.0",
    description=(
        "REST API for fraud score retrieval, company screening, and fraud event labels. "
        "NOT financial advice — see /disclaimer."
    ),
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(companies.router, prefix="/companies", tags=["Companies"])
app.include_router(screener.router,  prefix="/screener",  tags=["Screener"])
app.include_router(labels.router,    prefix="/labels",    tags=["Fraud Labels"])


@app.get("/", include_in_schema=False)
def root() -> dict:
    return {"status": "ok", "docs": "/docs"}


@app.get("/disclaimer", tags=["Meta"])
def disclaimer() -> dict:
    return {
        "disclaimer": (
            "This API provides quantitative fraud-risk signals derived from public filings. "
            "It does NOT constitute financial advice, investment recommendations, or legal opinions. "
            "Scores are probabilistic model outputs — not definitive fraud determinations. "
            "Users are solely responsible for any investment decisions made using this data."
        ),
        "methodology": "https://github.com/sherlock718/stock-fraud-screener/blob/main/docs/methodology/",
        "model_version": "v4",
    }


@app.get("/health", tags=["Meta"])
def health() -> dict:
    return {"status": "healthy"}
