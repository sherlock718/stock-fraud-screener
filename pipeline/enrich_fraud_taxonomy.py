"""
P0d — Fraud Taxonomy: 5 Sub-Score System

Adds five fraud-type sub-scores to historical_dataset_clean.parquet,
each capturing a distinct fraud mechanism. Scores are 0.0–1.0 (higher = riskier).

Sub-scores:
  fraud_score_accounting   — Accounting manipulation / earnings management
    Signals: Beneish M-score, Sloan accruals, accruals_to_assets, OCF-to-NI divergence
    Core mechanism: inflate earnings via accruals or channel-stuffing

  fraud_score_dilution     — Dilution fraud / equity issuance abuse
    Signals: shares_growth (rapid issuance), shares_dilution, EPS erosion vs net income
    Core mechanism: repeated share issuance to insiders at below-market prices

  fraud_score_quality      — Earnings quality / cash flow divergence
    Signals: OCF-to-NI ratio, FCF-to-assets, gross_margin_trend, OCF growth vs earnings growth
    Core mechanism: GAAP earnings but no cash generation ("phantom profits")

  fraud_score_distress     — Financial distress / going-concern risk
    Signals: Altman Z-score components, Piotroski F-score low, leverage, liquidity
    Core mechanism: concealing impending bankruptcy via window-dressing

  fraud_score_governance   — Governance / auditor risk
    Signals: small_auditor_flag, going_concern flag, market_cap / auditor mismatch
    Core mechanism: weak oversight enables fraud

  fraud_score_composite    — Weighted average of the five sub-scores

Usage:
    python3 pipeline/enrich_fraud_taxonomy.py
    python3 pipeline/enrich_fraud_taxonomy.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

BASE = Path(__file__).parent.parent
DATA = BASE / 'data'
OUT  = DATA / 'historical_dataset_clean.parquet'


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pct_rank_clip(series: pd.Series, clip_lo: float = 0.01, clip_hi: float = 0.99) -> pd.Series:
    """Percentile-rank within non-null values, clipped to avoid extreme outliers."""
    vals = pd.to_numeric(series, errors='coerce')
    lo = vals.quantile(clip_lo)
    hi = vals.quantile(clip_hi)
    clipped = vals.clip(lo, hi)
    return clipped.rank(pct=True, na_option='keep')


def _fillna_zero(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors='coerce').fillna(0)


# ── Sub-Score Builders ────────────────────────────────────────────────────────

def build_accounting_score(df: pd.DataFrame) -> pd.Series:
    """
    Accounting manipulation score.
    Components (equally weighted):
      - Beneish M-score percentile (high score = manipulation risk)
      - Sloan accruals percentile (high = more accruals relative to assets)
      - accruals_to_assets percentile
      - OCF-to-NI: low ratio = earnings not backed by cash (inverted)
    """
    components = []

    if 'beneish_m_score' in df.columns:
        # High M-score = bad → rank ascending so high rank = high fraud risk
        components.append(_pct_rank_clip(df['beneish_m_score']))

    if 'sloan_accruals' in df.columns:
        components.append(_pct_rank_clip(df['sloan_accruals']))

    if 'accruals_to_assets' in df.columns:
        components.append(_pct_rank_clip(df['accruals_to_assets']))

    if 'ocf_to_ni' in df.columns:
        # Low OCF/NI = cash not backing earnings = bad → invert rank
        pct = _pct_rank_clip(df['ocf_to_ni'])
        components.append(1.0 - pct)
    elif 'ocf_to_ni_sector_pct' in df.columns:
        pct = _pct_rank_clip(df['ocf_to_ni_sector_pct'])
        components.append(1.0 - pct)

    if not components:
        return pd.Series(np.nan, index=df.index)

    stacked = pd.concat(components, axis=1)
    return stacked.mean(axis=1)


def build_dilution_score(df: pd.DataFrame) -> pd.Series:
    """
    Dilution fraud score.
    Components (equally weighted):
      - shares_growth percentile (rapid share count growth)
      - shares_dilution percentile (dilution from options/convertibles)
      - EPS delta vs net income delta divergence (net income up, EPS down = dilution)
    """
    components = []

    if 'shares_growth' in df.columns:
        sg = pd.to_numeric(df['shares_growth'], errors='coerce')
        # Rapid share issuance is a red flag (positive values = more shares)
        components.append(_pct_rank_clip(sg))

    if 'shares_dilution' in df.columns:
        sd = pd.to_numeric(df['shares_dilution'], errors='coerce')
        components.append(_pct_rank_clip(sd))

    # EPS-vs-NI divergence: if net_margin growing but EPS declining, that's dilution
    if 'net_margin_change' in df.columns and 'eps_diluted' in df.columns:
        nm_change = pd.to_numeric(df['net_margin_change'], errors='coerce').fillna(0)
        # Proxy: rank negative EPS growth (declining EPS despite possible earnings)
        # Here we use shares_growth as the primary driver; EPS divergence is secondary
        eps = pd.to_numeric(df['eps_diluted'], errors='coerce')
        eps_growth = eps.pct_change(fill_method=None).fillna(0).clip(-2, 2)
        # High NM change but low EPS growth = dilution
        divergence = nm_change - eps_growth
        components.append(_pct_rank_clip(divergence))

    if not components:
        return pd.Series(np.nan, index=df.index)

    stacked = pd.concat(components, axis=1)
    return stacked.mean(axis=1)


def build_quality_score(df: pd.DataFrame) -> pd.Series:
    """
    Earnings quality / cash flow divergence score.
    High score = poor cash quality = elevated fraud risk.
    Components (equally weighted):
      - OCF margin (low = bad, inverted)
      - FCF_to_assets (low = bad, inverted)
      - gross_margin_trend_3y (declining = bad, inverted)
      - OCF growth vs earnings growth divergence
    """
    components = []

    if 'ocf_margin' in df.columns:
        pct = _pct_rank_clip(df['ocf_margin'])
        components.append(1.0 - pct)  # low margin = high risk

    if 'ocf_to_assets' in df.columns:
        pct = _pct_rank_clip(df['ocf_to_assets'])
        components.append(1.0 - pct)

    # FCF yield or FCF to assets
    for col in ['fcf_yield', 'fcf_to_assets']:
        if col in df.columns:
            pct = _pct_rank_clip(df[col])
            components.append(1.0 - pct)
            break

    if 'gross_margin_trend_3y' in df.columns:
        pct = _pct_rank_clip(df['gross_margin_trend_3y'])
        components.append(1.0 - pct)  # declining trend = bad

    if 'accruals_avg_3y' in df.columns:
        components.append(_pct_rank_clip(df['accruals_avg_3y']))

    if not components:
        return pd.Series(np.nan, index=df.index)

    stacked = pd.concat(components, axis=1)
    return stacked.mean(axis=1)


def build_distress_score(df: pd.DataFrame) -> pd.Series:
    """
    Financial distress / bankruptcy risk score.
    High score = near-distress / possible concealment.
    Components:
      - Altman Z-score (low = bad, inverted)
      - Piotroski F-score (low = bad, inverted)
      - Altman X1 (working capital / assets; low = bad, inverted)
      - Net debt to EBITDA (high = leveraged, direct rank)
    """
    components = []

    if 'altman_z_score' in df.columns:
        pct = _pct_rank_clip(df['altman_z_score'])
        components.append(1.0 - pct)  # low Z = high distress

    if 'piotroski_f_score' in df.columns:
        pct = _pct_rank_clip(df['piotroski_f_score'])
        components.append(1.0 - pct)  # low F = weak fundamentals

    if 'altman_x1' in df.columns:
        # Working capital / total assets; lower = worse liquidity
        pct = _pct_rank_clip(df['altman_x1'])
        components.append(1.0 - pct)

    if 'net_debt_to_ebitda' in df.columns:
        # High = over-levered
        components.append(_pct_rank_clip(df['net_debt_to_ebitda']))

    if 'current_ratio' in df.columns:
        pct = _pct_rank_clip(df['current_ratio'])
        components.append(1.0 - pct)

    if not components:
        return pd.Series(np.nan, index=df.index)

    stacked = pd.concat(components, axis=1)
    return stacked.mean(axis=1)


def build_governance_score(df: pd.DataFrame) -> pd.Series:
    """
    Governance / auditor-quality risk score.
    High score = weak oversight.
    Primary signals (when available):
      - small_auditor_flag (hard binary, weighted 0.5)
      - going_concern flag (hard binary, weighted 0.5)
    Proxy signals used when primary columns are absent (works for all markets):
      - altman_z_score < 1.81  → financial distress / going-concern proxy
      - piotroski_f_score <= 2 → fundamentally weak, governance risk proxy
    """
    score = pd.Series(0.0, index=df.index)
    weight_total = 0.0

    if 'small_auditor_flag' in df.columns:
        saf = df['small_auditor_flag'].fillna(0).astype(bool).astype(float)
        score += saf * 0.5
        weight_total += 0.5

    if 'going_concern' in df.columns:
        gc = df['going_concern'].fillna(0).astype(bool).astype(float)
        score += gc * 0.5
        weight_total += 0.5

    # Large-cap + small auditor = additional risk flag
    if 'small_auditor_flag' in df.columns and 'market_cap_at_filing' in df.columns:
        saf = df['small_auditor_flag'].fillna(0).astype(bool)
        mkt = pd.to_numeric(df['market_cap_at_filing'], errors='coerce').fillna(0)
        mismatch = (saf & (mkt > 1e8)).astype(float)
        score += mismatch * 0.2
        weight_total += 0.2

    # Proxy signals — used when primary governance columns are missing
    if weight_total == 0:
        if 'altman_z_score' in df.columns:
            z = pd.to_numeric(df['altman_z_score'], errors='coerce')
            distress = (z < 1.81).fillna(False).astype(float)
            score += distress * 0.5
            weight_total += 0.5

        if 'piotroski_f_score' in df.columns:
            f = pd.to_numeric(df['piotroski_f_score'], errors='coerce')
            weak = (f <= 2).fillna(False).astype(float)
            score += weak * 0.5
            weight_total += 0.5

    if weight_total == 0:
        return pd.Series(np.nan, index=df.index)

    return (score / weight_total).clip(0.0, 1.0)


def build_fraud_suspect(df: pd.DataFrame) -> pd.Series:
    """
    Signal-based fraud suspect flag (no AAER required).
    Set to 1 if 2+ of: Beneish > -1.78, Piotroski ≤ 2, Altman < 1.0.
    """
    signal_count = pd.Series(0, index=df.index, dtype='int8')

    if 'beneish_m_score' in df.columns:
        b = pd.to_numeric(df['beneish_m_score'], errors='coerce')
        signal_count += (b > -1.78).fillna(False).astype('int8')

    if 'piotroski_f_score' in df.columns:
        pf = pd.to_numeric(df['piotroski_f_score'], errors='coerce')
        signal_count += (pf <= 2).fillna(False).astype('int8')

    if 'altman_z_score' in df.columns:
        az = pd.to_numeric(df['altman_z_score'], errors='coerce')
        signal_count += (az < 1.0).fillna(False).astype('int8')

    return (signal_count >= 2).astype('int8')


def build_composite_fraud_score(df: pd.DataFrame) -> pd.Series:
    """
    Weighted composite of the five sub-scores.
    Weights reflect empirical importance based on AAER research literature:
      accounting  0.30  (most predictive of detected SEC fraud)
      quality     0.25  (earnings quality widely cited)
      distress    0.20  (bankruptcy fraud)
      dilution    0.15  (equity issuance fraud)
      governance  0.10  (auditor flags have lower recall in this dataset)
    """
    weights = {
        'fraud_score_accounting': 0.30,
        'fraud_score_quality':    0.25,
        'fraud_score_distress':   0.20,
        'fraud_score_dilution':   0.15,
        'fraud_score_governance': 0.10,
    }
    score = pd.Series(0.0, index=df.index)
    total_w = 0.0
    for col, w in weights.items():
        if col in df.columns and df[col].notna().sum() > 100:
            score += df[col].fillna(0.5) * w  # fill missing sub-scores with neutral 0.5
            total_w += w

    if total_w == 0:
        return pd.Series(np.nan, index=df.index)
    return (score / total_w).clip(0.0, 1.0)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(dry_run: bool = False) -> None:
    if not OUT.exists():
        print(f'ERROR: {OUT} not found — run step 6 first')
        sys.exit(1)

    print('P0d — Fraud Taxonomy: 5 Sub-Score System')
    df = pd.read_parquet(OUT)
    print(f'  Loaded {len(df):,} rows × {len(df.columns)} columns')

    print('  Building accounting manipulation score...')
    df['fraud_score_accounting'] = build_accounting_score(df)

    print('  Building dilution fraud score...')
    df['fraud_score_dilution'] = build_dilution_score(df)

    print('  Building earnings quality score...')
    df['fraud_score_quality'] = build_quality_score(df)

    print('  Building financial distress score...')
    df['fraud_score_distress'] = build_distress_score(df)

    print('  Building governance risk score...')
    df['fraud_score_governance'] = build_governance_score(df)

    print('  Building composite fraud score...')
    df['fraud_score_composite'] = build_composite_fraud_score(df)

    print('  Building fraud_suspect flag...')
    df['fraud_suspect'] = build_fraud_suspect(df)
    if 'fraud_confirmed' in df.columns:
        df.loc[df['fraud_confirmed'] == 1, 'fraud_suspect'] = 0

    # ── Report ────────────────────────────────────────────────────────────────
    score_cols = [
        'fraud_score_accounting',
        'fraud_score_dilution',
        'fraud_score_quality',
        'fraud_score_distress',
        'fraud_score_governance',
        'fraud_score_composite',
    ]
    print(f'\n  Taxonomy Score Summary (mean ± std across all rows):')
    for col in score_cols:
        s = df[col]
        fill = s.notna().mean()
        mean = s.mean()
        std  = s.std()
        p90  = s.quantile(0.90)
        print(f'    {col:<30s}: fill={fill:.0%}  mean={mean:.3f}  std={std:.3f}  p90={p90:.3f}')

    # Show scores for confirmed fraud vs clean
    if 'fraud_confirmed' in df.columns:
        print('\n  Mean scores — fraud_confirmed=1 vs clean:')
        fraud_rows = df[df['fraud_confirmed'] == 1]
        clean_rows = df[(df['fraud_confirmed'] == 0) & (df['fraud_suspect'] == 0)] if 'fraud_suspect' in df.columns else df[df['fraud_confirmed'] == 0]
        for col in score_cols:
            f_mean = fraud_rows[col].mean()
            c_mean = clean_rows[col].mean()
            diff = f_mean - c_mean
            print(f'    {col:<30s}: fraud={f_mean:.3f}  clean={c_mean:.3f}  diff={diff:+.3f}')

    # Top composite fraud score tickers (excluding confirmed)
    if 'fraud_confirmed' in df.columns:
        suspect_only = df[(df['fraud_confirmed'] == 0) & (df['fraud_score_composite'].notna())]
        top = (suspect_only.nlargest(20, 'fraud_score_composite')
               [['ticker', 'fiscal_year', 'fraud_score_composite',
                 'fraud_score_accounting', 'fraud_score_distress',
                 'beneish_m_score', 'altman_z_score']])
        print(f'\n  Top 20 composite fraud score (non-confirmed):')
        print(top.to_string(index=False))

    if dry_run:
        print('\n  [DRY RUN] — file not modified')
        return

    n_suspect = int(df['fraud_suspect'].sum()) if 'fraud_suspect' in df.columns else 0
    print(f'\n  fraud_suspect: {n_suspect:,} rows flagged ({100*n_suspect/len(df):.2f}%)')

    df.to_parquet(OUT, index=False)
    print(f'\n  Saved: {OUT}')
    print(f'  Columns added: {", ".join(score_cols)}, fraud_suspect')


def main() -> None:
    parser = argparse.ArgumentParser(description='Fraud taxonomy sub-scores (P0d)')
    parser.add_argument('--dry-run', action='store_true', help='Print stats without saving')
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == '__main__':
    main()
