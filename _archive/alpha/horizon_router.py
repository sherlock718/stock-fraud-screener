"""
horizon_router.py — Maps a user's requested investment horizon (in months)
to the nearest trained discrete model key.

Routing table (conservative: on tie, use longer model):
    3 – 9  months  → '6m'  model
    9 – 18 months  → '1y'  model
   18 – 30 months  → '2y'  model
   30 – 48 months  → '3y'  model
   48+ months      → '5y'  model

Usage:
    from alpha.horizon_router import HorizonRouter
    key = HorizonRouter.route(18)   # → '1y'
    key = HorizonRouter.route(24)   # → '2y'
    key = HorizonRouter.route(60)   # → '5y'
"""
from __future__ import annotations

# Lower bound in months for each model key (inclusive).
# Upper bound = start of next key (exclusive).
_BREAKPOINTS: list[tuple[int, str]] = [
    (3,  '6m'),
    (9,  '1y'),
    (18, '2y'),
    (30, '3y'),
    (48, '5y'),
]

# Human-readable model labels (for UI display)
MODEL_LABELS: dict[str, str] = {
    '6m': '6-month model',
    '1y': '1-year model',
    '2y': '2-year model',
    '3y': '3-year model',
    '5y': '5-year model',
}

# Factor group mapping for top feature attribution display
FEATURE_FACTOR_GROUPS: dict[str, str] = {
    # Value
    'pe_ratio': 'Value', 'pb_ratio': 'Value', 'ev_ebitda': 'Value',
    'earnings_yield': 'Value', 'fcf_yield': 'Value', 'price_to_sales': 'Value',
    'ev_revenue': 'Value', 'value_composite': 'Value',
    # Quality
    'gross_margin': 'Quality', 'operating_margin': 'Quality', 'net_margin': 'Quality',
    'roe': 'Quality', 'roa': 'Quality', 'roic': 'Quality',
    'piotroski_f_score': 'Quality', 'altman_z_score': 'Quality',
    'current_ratio': 'Quality', 'debt_to_equity': 'Quality',
    'interest_coverage': 'Quality', 'quality_composite': 'Quality',
    'asset_quality': 'Quality', 'earnings_stability': 'Quality',
    # Momentum
    'momentum_12m_prior': 'Momentum', 'momentum_6m': 'Momentum',
    'momentum_3m': 'Momentum', 'vol_prior_12m': 'Momentum',
    'price_to_52w_high': 'Momentum', 'price_to_52w_low': 'Momentum',
    # Growth
    'revenue_growth_yoy': 'Growth', 'eps_growth_yoy': 'Growth',
    'earnings_growth_3y': 'Growth', 'revenue_growth_3y': 'Growth',
    'fcf_growth_yoy': 'Growth', 'asset_growth': 'Growth',
    'equity_growth': 'Growth',
    # Fraud Risk
    'beneish_m_score': 'Fraud Risk', 'accruals_to_assets': 'Fraud Risk',
    'sloan_accrual': 'Fraud Risk', 'sloan_wc_accruals': 'Fraud Risk',
    'sloan_lt_accruals': 'Fraud Risk', 'montier_c_score': 'Fraud Risk',
    'fraud_score_composite': 'Fraud Risk', 'zmijewski_score': 'Fraud Risk',
    'discretionary_accruals': 'Fraud Risk',
}


class HorizonRouter:
    """Maps requested investment horizon (months) to nearest trained model key."""

    @staticmethod
    def route(months: int) -> str:
        """Return the model key for a given investment horizon in months.

        On tie at a boundary, the longer model is preferred (conservative bias).
        Clamps: below 3 months → '6m'; above 60 months → '5y'.
        """
        months = max(3, int(months))
        key = '5y'
        for lower, model_key in _BREAKPOINTS:
            if months < lower:
                break
            key = model_key
        return key

    @staticmethod
    def months_to_label(months: int) -> str:
        if months < 12:
            return f'{months}m'
        years = months / 12
        return f'{years:.0f}y' if years == int(years) else f'{years:.1f}y'

    @staticmethod
    def available_keys(meta: dict) -> list[str]:
        """Return ordered list of model keys present in model_meta.json."""
        order = ['6m', '1y', '2y', '3y', '5y']
        return [k for k in order if k in meta]

    @staticmethod
    def wf_auc(meta: dict, key: str) -> float | None:
        """Return walk-forward AUC for a model key, or None if not available."""
        if key not in meta:
            return None
        return meta[key].get('wf_mean_auc') or meta[key].get('val_auc')

    @staticmethod
    def factor_group(feature_name: str) -> str:
        """Return the factor group label for a feature name."""
        return FEATURE_FACTOR_GROUPS.get(feature_name, 'Other')
