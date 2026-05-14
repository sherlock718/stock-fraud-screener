"""
5-factor alpha package.

Each module exposes a single function: compute(df) -> pd.Series of cross-sectional
rank scores (0–1, higher = better for all factors including fraud_risk where higher
means LESS fraud risk / safer). Group: (fiscal_year, market).

Factors:
  value       — P/B, EV/EBITDA, FCF yield, earnings yield composite
  quality     — ROE, ROA, Piotroski F-score, accruals composite
  momentum    — 12m/6m/3m return rank composite
  growth      — Revenue CAGR, EPS growth, OCF growth composite
  fraud_risk  — Beneish, Altman, Ohlson, ML scores composite (high = safer)

composite   — weighted blend of all five factors
"""

from .value import compute as value
from .quality import compute as quality
from .momentum import compute as momentum
from .growth import compute as growth
from .fraud_risk import compute as fraud_risk
from .composite import compute as composite

__all__ = ["value", "quality", "momentum", "growth", "fraud_risk", "composite"]
