"""
Feature selection engine — PSI + IC + ICIR + dedup pipeline.

Orchestrates the four-stage selection process:
  1. PSI filter (distribution stability)
  2. IC screen (predictive power)
  3. ICIR rank (signal-to-noise)
  4. Spearman dedup (remove near-duplicates)

This module re-exports the pipeline logic from modeling.run_feature_selection
and provides a programmatic API for research notebooks.
"""
from __future__ import annotations

from modeling.run_feature_selection import (
    CORR_THRESHOLD,
    IC_MIN_ABS,
    MIN_FILL,
    PSI_THRESHOLD,
    TOP_K_ICIR,
    get_candidates,
    ic_icir_filter,
    psi_filter,
    run_selection,
)
from research.ic_engine import bh_fdr_correction, compute_yearly_ic, newey_west_tstat

__all__ = [
    "run_selection",
    "psi_filter",
    "ic_icir_filter",
    "get_candidates",
    "newey_west_tstat",
    "bh_fdr_correction",
    "compute_yearly_ic",
    "PSI_THRESHOLD",
    "IC_MIN_ABS",
    "TOP_K_ICIR",
    "CORR_THRESHOLD",
    "MIN_FILL",
]
