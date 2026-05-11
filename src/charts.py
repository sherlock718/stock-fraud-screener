from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go

from src.features import _BENEISH_LABELS


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
