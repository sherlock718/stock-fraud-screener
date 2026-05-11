from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from src.features import _BENEISH_LABELS

_FRAUD_TAXONOMY_LABELS: dict[str, str] = {
    'fraud_score_accounting': 'Accounting\nManipulation',
    'fraud_score_dilution':   'Dilution\nFraud',
    'fraud_score_quality':    'Earnings\nQuality',
    'fraud_score_distress':   'Financial\nDistress',
    'fraud_score_governance': 'Governance\nFraud',
}


def beneish_radar(row: pd.Series) -> go.Figure | None:
    """Polar radar chart of the 8 Beneish components normalised to [0, 2]."""
    components = list(_BENEISH_LABELS.keys())
    vals = [row.get(c, np.nan) for c in components]
    labels = list(_BENEISH_LABELS.values())
    if all(np.isnan(v) for v in vals):
        return None
    vals_clamped = [max(0.0, min(float(v) if not np.isnan(v) else 1.0, 2.0)) for v in vals]
    vals_clamped.append(vals_clamped[0])
    labels.append(labels[0])
    fig = go.Figure(go.Scatterpolar(
        r=vals_clamped, theta=labels, fill='toself',
        fillcolor='rgba(239, 83, 80, 0.25)',
        line=dict(color='#EF5350', width=2),
        name='Beneish Components',
    ))
    fig.add_trace(go.Scatterpolar(
        r=[1.0] * len(labels), theta=labels,
        line=dict(color='grey', width=1, dash='dot'),
        name='Baseline (1.0)', showlegend=False,
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 2.2])),
        showlegend=False,
        margin=dict(t=30, b=20, l=40, r=40),
        height=320,
    )
    return fig


def fraud_taxonomy_radar(row: pd.Series) -> go.Figure | None:
    """Spider chart of the 5 fraud taxonomy sub-scores (0–1 scale)."""
    keys   = list(_FRAUD_TAXONOMY_LABELS.keys())
    labels = list(_FRAUD_TAXONOMY_LABELS.values())
    vals   = [row.get(c, np.nan) for c in keys]
    # need at least 3 valid values to draw a meaningful radar
    valid = [v for v in vals if not np.isnan(v)]
    if len(valid) < 3:
        return None
    # replace NaN with 0 (unknown = no signal)
    vals_clean = [float(v) if not np.isnan(v) else 0.0 for v in vals]
    # close the polygon
    vals_closed  = vals_clean  + [vals_clean[0]]
    labels_closed = labels     + [labels[0]]

    # colour by max risk
    max_score = max(vals_clean)
    fill_colour = (
        'rgba(239,83,80,0.25)'  if max_score > 0.65 else
        'rgba(255,167,38,0.25)' if max_score > 0.35 else
        'rgba(102,187,106,0.20)'
    )
    line_colour = '#EF5350' if max_score > 0.65 else '#FFA726' if max_score > 0.35 else '#66BB6A'

    fig = go.Figure(go.Scatterpolar(
        r=vals_closed, theta=labels_closed, fill='toself',
        fillcolor=fill_colour,
        line=dict(color=line_colour, width=2),
        name='Fraud Taxonomy',
        hovertemplate='%{theta}: %{r:.3f}<extra></extra>',
    ))
    fig.add_trace(go.Scatterpolar(
        r=[0.5] * len(labels_closed), theta=labels_closed,
        line=dict(color='grey', width=1, dash='dot'),
        name='Midpoint (0.5)', showlegend=False,
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1.0])),
        showlegend=False,
        margin=dict(t=30, b=20, l=40, r=40),
        height=320,
    )
    return fig
