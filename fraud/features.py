"""
Fraud feature registry — documents all fraud-related features.

Lists features used in the fraud risk factor, their formulas,
expected IC, and data sources.

Populated in future session.
"""
from __future__ import annotations

FRAUD_FEATURES = [
    "beneish_m_score",
    "altman_z_score",
    "piotroski_f_score",
    "wc_accruals_to_assets",
    "sloan_accrual_ratio",
    "days_receivable_index",
    "days_sales_index",
    "gross_margin_index",
    "leverage_index",
    "sgai_index",
    "total_accruals_to_total_assets",
]
