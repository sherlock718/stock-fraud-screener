"""
P0d — Fraud Taxonomy: 5 Sub-Score System

Adds six fraud-taxonomy columns to historical_dataset_clean.parquet:
five fraud-type sub-scores (0.0–1.0, higher = riskier) plus a weighted composite.

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

Note: fraud_suspect is NOT owned by this module. It is owned by
enrich_fraud_labels.py which uses a broader 5-signal definition.

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

class _FenwickCounts:
    """Integer order-statistic tree used by expanding as-of percentile ranks."""

    def __init__(self, size: int) -> None:
        self.tree = np.zeros(size + 1, dtype=np.int64)

    def add(self, position: int) -> None:
        i = position + 1
        while i < len(self.tree):
            self.tree[i] += 1
            i += i & -i

    def prefix(self, end: int) -> int:
        """Return the count in compressed positions ``[0, end)``."""
        total = 0
        i = end
        while i:
            total += int(self.tree[i])
            i -= i & -i
        return total

    def kth(self, rank: int) -> int:
        """Return the compressed position of the one-based ``rank`` value."""
        position = 0
        bit = 1 << (len(self.tree).bit_length() - 1)
        while bit:
            candidate = position + bit
            if candidate < len(self.tree) and self.tree[candidate] < rank:
                position = candidate
                rank -= int(self.tree[candidate])
            bit >>= 1
        return position


def _pct_rank_clip(
    series: pd.Series,
    as_of_dates: pd.Series,
    clip_lo: float = 0.01,
    clip_hi: float = 0.99,
) -> pd.Series:
    """Return a clipped percentile rank using only values available as of each row.

    Rows sharing an availability timestamp enter the comparison population as one
    batch. This makes the result independent of dataframe order while preventing
    later filings from changing an earlier row's clipping bounds or rank.
    """
    if not 0 <= clip_lo <= clip_hi <= 1:
        raise ValueError('clip bounds must satisfy 0 <= clip_lo <= clip_hi <= 1')
    if len(series) != len(as_of_dates):
        raise ValueError('series and as_of_dates must have the same length')

    vals = pd.to_numeric(series, errors='coerce').to_numpy(dtype=float)
    dates = pd.to_datetime(as_of_dates, errors='coerce').to_numpy()
    if pd.isna(dates).any():
        raise ValueError('fraud taxonomy requires a non-null filed_date for every row')

    valid = np.isfinite(vals)
    result = np.full(len(vals), np.nan, dtype=float)
    if not valid.any():
        return pd.Series(result, index=series.index, name=series.name)

    coordinates = np.unique(vals[valid])
    positions = np.searchsorted(coordinates, vals[valid])
    compressed = np.full(len(vals), -1, dtype=np.int64)
    compressed[valid] = positions
    tree = _FenwickCounts(len(coordinates))

    # Stable sorting is not relied upon for results: every equal-date batch is
    # inserted before any member is ranked.
    order = np.argsort(dates, kind='mergesort')
    start = 0
    count = 0
    while start < len(order):
        end = start + 1
        while end < len(order) and dates[order[end]] == dates[order[start]]:
            end += 1
        batch = order[start:end]
        for row in batch:
            if valid[row]:
                tree.add(int(compressed[row]))
                count += 1

        lower_position = (count - 1) * clip_lo if count else 0.0
        upper_position = (count - 1) * clip_hi if count else 0.0

        def quantile(position: float) -> float:
            below = int(np.floor(position))
            above = int(np.ceil(position))
            lower_value = coordinates[tree.kth(below + 1)]
            upper_value = coordinates[tree.kth(above + 1)]
            return float(lower_value + (upper_value - lower_value) * (position - below))

        lower = quantile(lower_position) if count else np.nan
        upper = quantile(upper_position) if count else np.nan
        for row in batch:
            if not valid[row]:
                continue
            value = vals[row]
            if lower == upper:
                less = 0
                equal = count
            elif value <= lower:
                less = 0
                equal = tree.prefix(int(np.searchsorted(coordinates, lower, side='right')))
            elif value >= upper:
                less = tree.prefix(int(np.searchsorted(coordinates, upper, side='left')))
                equal = count - less
            else:
                left = int(np.searchsorted(coordinates, value, side='left'))
                right = int(np.searchsorted(coordinates, value, side='right'))
                less = tree.prefix(left)
                equal = tree.prefix(right) - less
            result[row] = (less + (equal + 1) / 2) / count
        start = end

    return pd.Series(result, index=series.index, name=series.name)


def _taxonomy_as_of_dates(df: pd.DataFrame) -> pd.Series:
    if 'availability_timestamp' in df.columns:
        dates = pd.to_datetime(df['availability_timestamp'], utc=True, errors='coerce')
        if dates.isna().any():
            raise ValueError('fraud taxonomy requires non-null availability_timestamp when present')
        if 'availability_provenance' in df.columns:
            valid = df['availability_provenance'].eq('sec_primary_filing')
            if not valid.all():
                raise ValueError('fraud taxonomy requires proven SEC-primary availability')
        return dates
    if 'filed_date' not in df.columns:
        raise ValueError('fraud taxonomy requires filed_date; no undated ranking fallback is allowed')
    dates = pd.to_datetime(df['filed_date'], errors='coerce')
    if dates.isna().any():
        raise ValueError('fraud taxonomy requires a non-null filed_date for every row')
    return dates


def _ticker_local_eps_growth(df: pd.DataFrame, as_of_dates: pd.Series) -> pd.Series:
    """Compute EPS change within ticker after chronological filing-date ordering."""
    entity_key = 'entity_id' if 'entity_id' in df.columns else 'ticker'
    if entity_key not in df.columns:
        raise ValueError('dilution EPS history requires entity_id or ticker')
    eps_history = pd.DataFrame({
        '_position': np.arange(len(df)),
        'entity': df[entity_key].to_numpy(),
        'filed_date': as_of_dates.to_numpy(),
        'eps': pd.to_numeric(df['eps_diluted'], errors='coerce').to_numpy(),
    })
    if 'fiscal_year' in df.columns:
        eps_history['fiscal_year'] = pd.to_numeric(df['fiscal_year'], errors='coerce').to_numpy()
    chronology = ['entity', 'filed_date']
    if 'fiscal_year' in eps_history.columns:
        chronology.append('fiscal_year')
    eps_history = eps_history.sort_values(chronology, kind='mergesort')
    eps_history['eps_growth'] = (
        eps_history.groupby('entity', sort=False)['eps']
        .pct_change(fill_method=None)
        .fillna(0)
        .clip(-2, 2)
    )
    eps_growth = eps_history.sort_values('_position')['eps_growth']
    eps_growth.index = df.index
    return eps_growth


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
    as_of_dates = _taxonomy_as_of_dates(df)

    if 'beneish_m_score' in df.columns:
        # High M-score = bad → rank ascending so high rank = high fraud risk
        components.append(_pct_rank_clip(df['beneish_m_score'], as_of_dates))

    if 'sloan_accruals' in df.columns:
        components.append(_pct_rank_clip(df['sloan_accruals'], as_of_dates))

    if 'accruals_to_assets' in df.columns:
        components.append(_pct_rank_clip(df['accruals_to_assets'], as_of_dates))

    if 'ocf_to_ni' in df.columns:
        # Low OCF/NI = cash not backing earnings = bad → invert rank
        pct = _pct_rank_clip(df['ocf_to_ni'], as_of_dates)
        components.append(1.0 - pct)
    elif 'ocf_to_ni_sector_pct' in df.columns:
        pct = _pct_rank_clip(df['ocf_to_ni_sector_pct'], as_of_dates)
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
    as_of_dates = _taxonomy_as_of_dates(df)

    if 'shares_growth' in df.columns:
        sg = pd.to_numeric(df['shares_growth'], errors='coerce')
        # Rapid share issuance is a red flag (positive values = more shares)
        components.append(_pct_rank_clip(sg, as_of_dates))

    if 'shares_dilution' in df.columns:
        sd = pd.to_numeric(df['shares_dilution'], errors='coerce')
        components.append(_pct_rank_clip(sd, as_of_dates))

    # EPS-vs-NI divergence: if net_margin growing but EPS declining, that's dilution
    if 'net_margin_change' in df.columns and 'eps_diluted' in df.columns:
        nm_change = pd.to_numeric(df['net_margin_change'], errors='coerce').fillna(0)
        # Proxy: rank negative EPS growth (declining EPS despite possible earnings)
        # Here we use shares_growth as the primary driver; EPS divergence is secondary
        eps_growth = _ticker_local_eps_growth(df, as_of_dates)
        # High NM change but low EPS growth = dilution
        divergence = nm_change - eps_growth
        components.append(_pct_rank_clip(divergence, as_of_dates))

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
    as_of_dates = _taxonomy_as_of_dates(df)

    if 'ocf_margin' in df.columns:
        pct = _pct_rank_clip(df['ocf_margin'], as_of_dates)
        components.append(1.0 - pct)  # low margin = high risk

    if 'ocf_to_assets' in df.columns:
        pct = _pct_rank_clip(df['ocf_to_assets'], as_of_dates)
        components.append(1.0 - pct)

    # FCF yield or FCF to assets
    for col in ['fcf_yield', 'fcf_to_assets']:
        if col in df.columns:
            pct = _pct_rank_clip(df[col], as_of_dates)
            components.append(1.0 - pct)
            break

    if 'gross_margin_trend_3y' in df.columns:
        pct = _pct_rank_clip(df['gross_margin_trend_3y'], as_of_dates)
        components.append(1.0 - pct)  # declining trend = bad

    if 'accruals_avg_3y' in df.columns:
        components.append(_pct_rank_clip(df['accruals_avg_3y'], as_of_dates))

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
    as_of_dates = _taxonomy_as_of_dates(df)

    if 'altman_z_score' in df.columns:
        pct = _pct_rank_clip(df['altman_z_score'], as_of_dates)
        components.append(1.0 - pct)  # low Z = high distress

    if 'piotroski_f_score' in df.columns:
        pct = _pct_rank_clip(df['piotroski_f_score'], as_of_dates)
        components.append(1.0 - pct)  # low F = weak fundamentals

    if 'altman_x1' in df.columns:
        # Working capital / total assets; lower = worse liquidity
        pct = _pct_rank_clip(df['altman_x1'], as_of_dates)
        components.append(1.0 - pct)

    if 'net_debt_to_ebitda' in df.columns:
        # High = over-levered
        components.append(_pct_rank_clip(df['net_debt_to_ebitda'], as_of_dates))

    if 'current_ratio' in df.columns:
        pct = _pct_rank_clip(df['current_ratio'], as_of_dates)
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

def run(
    dry_run: bool = False,
    *,
    input_path: Path | None = None,
    output_path: Path | None = None,
) -> None:
    in_path = input_path or OUT
    out_path = output_path or OUT
    if not in_path.exists():
        print(f'ERROR: {in_path} not found — run step 6 first')
        sys.exit(1)

    print('P0d — Fraud Taxonomy: 5 Sub-Scores + Composite')
    df = pd.read_parquet(in_path)
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

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    print(f'\n  Saved: {out_path}')
    print(f'  Columns added: {", ".join(score_cols)}')


def main() -> None:
    parser = argparse.ArgumentParser(description='Fraud taxonomy sub-scores (P0d)')
    parser.add_argument('--dry-run', action='store_true', help='Print stats without saving')
    parser.add_argument('--input', type=Path, default=None, help='Pre-taxonomy input parquet')
    parser.add_argument('--out', type=Path, default=None, help='Taxonomy-enriched output parquet')
    args = parser.parse_args()
    run(dry_run=args.dry_run, input_path=args.input, output_path=args.out)


if __name__ == '__main__':
    main()
