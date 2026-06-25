"""
IC/ICIR computation engine.

Core statistical functions for Information Coefficient analysis:
  - Newey-West HAC t-statistics for IC persistence
  - Benjamini-Hochberg FDR correction for multiple comparisons
  - Yearly IC computation with optional sector neutralization
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def newey_west_tstat(ic_series: pd.Series, max_lags: int = 4) -> float:
    """Newey-West HAC t-statistic for mean IC."""
    x = ic_series.dropna().values
    n = len(x)
    if n < 3:
        return np.nan
    mu = x.mean()
    gamma0 = np.mean((x - mu) ** 2)
    hac_var = gamma0
    for lag in range(1, min(max_lags + 1, n)):
        cov = np.mean((x[lag:] - mu) * (x[:-lag] - mu))
        hac_var += 2 * (1 - lag / (max_lags + 1)) * cov
    hac_var = max(hac_var, 1e-12)
    return mu / np.sqrt(hac_var / n)


def bh_fdr_correction(pvalues: pd.Series, alpha: float = 0.05) -> pd.Series:
    """Benjamini-Hochberg FDR correction — returns boolean Series (True = reject H0)."""
    n = len(pvalues)
    if n == 0:
        return pd.Series(dtype=bool)
    sorted_idx = pvalues.argsort()
    sorted_pvals = pvalues.iloc[sorted_idx].values
    bh_thresholds = np.arange(1, n + 1) / n * alpha
    reject_sorted = sorted_pvals <= bh_thresholds
    if reject_sorted.any():
        last_reject = np.where(reject_sorted)[0].max()
        reject_sorted[:last_reject + 1] = True
    result = pd.Series(False, index=pvalues.index)
    result.iloc[sorted_idx] = reject_sorted
    return result


def _sic_to_sector(sic: pd.Series) -> pd.Series:
    """Map SIC codes to broad sector labels."""
    s = pd.to_numeric(sic, errors="coerce").fillna(0).astype(int)
    sector = pd.Series("Other", index=s.index)
    sector[s.between(100, 999)] = "Agriculture/Mining"
    sector[s.between(1000, 1499)] = "Mining/Resources"
    sector[s.between(1500, 1999)] = "Construction"
    sector[s.between(2000, 3999)] = "Manufacturing"
    sector[s.between(4000, 4999)] = "Utilities/Transport"
    sector[s.between(5000, 5999)] = "Trade"
    sector[s.between(6000, 6799)] = "Finance/Insurance/RE"
    sector[s.between(7000, 7999)] = "Services/Hospitality"
    sector[s.between(8000, 8999)] = "Services/Professional"
    return sector


def compute_yearly_ic(
    df: pd.DataFrame,
    feat: str,
    ret_col: str,
    sector_neutral: bool = True,
) -> pd.Series:
    """Compute IC per fiscal year, optionally sector-neutral."""
    sic_col = "sic_sector" if "sic_sector" in df.columns else (
              "sic_code" if "sic_code" in df.columns else None)

    ics = {}
    for yr, grp in df.groupby("fiscal_year"):
        sub = grp[[feat, ret_col]].dropna()
        if len(sub) < 6:
            continue
        if sector_neutral and sic_col is not None:
            sub = sub.copy()
            sectors = _sic_to_sector(grp.loc[sub.index, sic_col])
            for col in [feat, ret_col]:
                demeaned = sub[col].copy().astype(float)
                for sec in sectors.unique():
                    mask = (sectors == sec) & sub[col].notna()
                    if mask.sum() >= 5:
                        demeaned[mask] -= sub.loc[mask, col].median()
                sub[col] = demeaned
        ics[yr] = sub[feat].corr(sub[ret_col], method="spearman")
    return pd.Series(ics)
