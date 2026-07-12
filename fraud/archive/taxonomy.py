"""
Fraud taxonomy — maps fraud indicators to 5 sub-categories.

Extracts taxonomy logic from pipeline/enrich_fraud_taxonomy.py into a
standalone module for reuse by the fraud scoring engine.

Sub-categories:
  1. Earnings manipulation (accruals, revenue recognition)
  2. Asset quality (intangibles inflation, impairment avoidance)
  3. Cash flow divergence (earnings vs OCF mismatch)
  4. Leverage/solvency risk (debt coverage deterioration)
  5. Disclosure anomalies (restatements, auditor changes)

Populated in future session.
"""
from __future__ import annotations


TAXONOMY_CATEGORIES = [
    "earnings_manipulation",
    "asset_quality",
    "cash_flow_divergence",
    "leverage_solvency",
    "disclosure_anomaly",
]


def classify_fraud_type(indicators: dict) -> dict[str, float]:
    """Map raw indicators to taxonomy sub-scores (stub)."""
    raise NotImplementedError("Populated in future session")
