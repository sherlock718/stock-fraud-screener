"""
Fraud rules engine — threshold-based scoring.

Implements Beneish M-Score, Altman Z-Score, and Piotroski F-Score thresholds
as rules-based fraud/distress indicators.

Populated in Session 17+.
"""
from __future__ import annotations


# Beneish M-Score threshold (> -1.78 suggests manipulation)
BENEISH_THRESHOLD = -1.78

# Altman Z-Score zones
ALTMAN_SAFE = 2.99
ALTMAN_GREY = 1.81
ALTMAN_DISTRESS = 1.81

# Piotroski F-Score (0-9, lower = weaker fundamentals)
PIOTROSKI_WEAK = 3


def score_beneish(m_score: float) -> float:
    """Return fraud probability estimate from Beneish M-Score (stub)."""
    raise NotImplementedError("Populated in future session")


def score_altman(z_score: float) -> float:
    """Return distress probability from Altman Z-Score (stub)."""
    raise NotImplementedError("Populated in future session")
