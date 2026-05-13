"""
Bias audit for historical_dataset_clean.parquet.

Checks systematic biases that can invalidate ML model performance:

  1. Look-ahead bias (HARD FAIL in CI)
     - Confirm filed_date >= period_end_date for every annual row.
     - Any row where filed_date < period_end implies look-ahead leakage.

  2. Survivorship bias (WARN only)
     - What fraction of training rows belong to companies that later delisted?
     - If delisted companies are under-represented, model will be too optimistic.

  3. Overfitting audit (WARN only)
     - Compares train AUC (from model_meta.json) to walk-forward mean AUC.
     - Flags if overfit_gap > 0.15 for any horizon.
     - Writes overfit_gap to model_meta.json.

  4. FX-adjusted returns (cross-market comparability)
     - forward_return_* is in local currency. Adds forward_return_{h}_usd columns.

  5. Multiple testing correction (INFO)
     - Documents expected false discoveries across 5 horizons × strategies.

Exit codes (CI mode --ci):
  0 — all checks pass (or only warn-level issues found)
  1 — HARD FAIL: look-ahead violations found

Usage:
    python3 scripts/bias_audit.py              # full report, exit 0
    python3 scripts/bias_audit.py --ci         # exit 1 if look-ahead violations
    python3 scripts/bias_audit.py --fix        # add FX-adjusted columns to parquet
    python3 scripts/bias_audit.py --fix --out data/historical_dataset_fx.parquet
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
warnings.filterwarnings('ignore')

from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

BASE      = Path(__file__).parent.parent
DATA_PATH = BASE / 'data' / 'historical_dataset_clean.parquet'

# Horizons that have forward_return columns (must match train_models.py)
HORIZONS = {'1y': 365, '3y': 1095, '5y': 1825}

# USD FX ticker for each non-USD market country code.
# Format: "{base}USD=X" fetches base-currency-per-USD pairs via yfinance.
FX_MAP = {
    'KR': 'KRWUSD=X',
    'JP': 'JPYUSD=X',
    'DE': 'EURUSD=X',
    'GB': 'GBPUSD=X',
    'FR': 'EURUSD=X',
    'NL': 'EURUSD=X',
    'SE': 'SEKUSD=X',
    'NO': 'NOKUSD=X',
    'DK': 'DKKUSD=X',
    'FI': 'EURUSD=X',
    'BR': 'BRLUSD=X',
    'CA': 'CADUSD=X',
    'CN': 'CNYUSD=X',
    'IN': 'INRUSD=X',
    'AU': 'AUDUSD=X',
    'HK': 'HKDUSD=X',
    'SG': 'SGDUSD=X',
    'BD': 'BKTUSD=X',  # Bangladeshi Taka — often unavailable; will skip gracefully
}

TRAIN_CUTOFF = 2021


# ── Helpers ───────────────────────────────────────────────────────────────────

def _period_end_date(row: pd.Series) -> pd.Timestamp | None:
    """Infer period end date from fiscal_year + fiscal_quarter."""
    try:
        fy = int(row['fiscal_year'])
        fq = row.get('fiscal_quarter', None)
        if pd.isna(fq) or str(fq) == 'nan':
            fq = 4
        fq = int(fq)
        month_end = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}
        m, d = month_end.get(fq, (12, 31))
        return pd.Timestamp(year=fy, month=m, day=d)
    except Exception:
        return None


def _fetch_fx_series(ticker: str) -> pd.Series | None:
    """Return daily Close price series for a yfinance FX ticker, or None."""
    try:
        raw = yf.download(ticker, start='2005-01-01', auto_adjust=True, progress=False)
        if raw is None or raw.empty:
            return None
        close = raw['Close']
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        close.index = pd.to_datetime(close.index).tz_localize(None)
        return close.sort_index().dropna()
    except Exception:
        return None


def _forward_fx_return(fx_series: pd.Series, entry_date: pd.Timestamp,
                        horizon_days: int) -> float | None:
    """FX return from entry_date to entry_date + horizon_days."""
    exit_date = entry_date + timedelta(days=horizon_days)
    try:
        s_entry = fx_series.asof(entry_date)
        s_exit  = fx_series.asof(exit_date)
        if pd.isna(s_entry) or pd.isna(s_exit) or s_entry == 0:
            return None
        return float(s_exit / s_entry - 1)
    except Exception:
        return None


# ── Audit 1: Survivorship bias ────────────────────────────────────────────────

def audit_survivorship(df: pd.DataFrame) -> None:
    print('\n── Audit 1: Survivorship Bias ──────────────────────────────────────')

    # Check if a 'likely_delisted' column exists
    if 'likely_delisted' not in df.columns:
        print('  likely_delisted column not present — skipping detailed check.')
        print('  Proxy check: companies with ≤1 annual row (never updated) in dataset.')
        row_counts = df[df['period_type'] == 'annual'].groupby('ticker').size()
        single_row = (row_counts == 1).sum()
        total_cos  = row_counts.shape[0]
        print(f'  Companies with exactly 1 annual row: {single_row:,} / {total_cos:,} '
              f'({100*single_row/max(total_cos,1):.1f}%)')
        print('  NOTE: A high % of single-row companies may indicate many early-delisted '
              'companies that left the dataset, causing survivorship bias.')
        return

    ann = df[df['period_type'] == 'annual'].copy()
    train = ann[ann['fiscal_year'] <= TRAIN_CUTOFF]

    delisted_in_train = train[train['likely_delisted'] == True]
    total_train_rows  = len(train)
    del_rows          = len(delisted_in_train)
    del_cos           = delisted_in_train['ticker'].nunique()
    total_cos         = train['ticker'].nunique()

    print(f'  Training rows (fiscal_year ≤ {TRAIN_CUTOFF}): {total_train_rows:,}')
    print(f'  Rows from likely_delisted companies : {del_rows:,} '
          f'({100*del_rows/max(total_train_rows,1):.1f}%)')
    print(f'  Unique companies (train)            : {total_cos:,}')
    print(f'  Delisted companies in train         : {del_cos:,} '
          f'({100*del_cos/max(total_cos,1):.1f}%)')

    if del_rows / max(total_train_rows, 1) < 0.05:
        print('  ⚠  WARNING: < 5% of training rows are from delisted companies.')
        print('     Survivorship bias likely — model will be optimistic on held-out data.')
    else:
        print('  ✓  Delisted companies well-represented in training set.')


# ── Audit 2: Filing date lag ──────────────────────────────────────────────────

def audit_filing_lag(df: pd.DataFrame) -> None:
    print('\n── Audit 2: Filing Date Lag (look-ahead check) ─────────────────────')

    ann = df[(df['period_type'] == 'annual') & df['filed_date'].notna()].copy()
    ann['filed_date'] = pd.to_datetime(ann['filed_date'], errors='coerce')
    ann['period_end'] = ann.apply(_period_end_date, axis=1)
    ann = ann[ann['period_end'].notna()]

    lag_days = (ann['filed_date'] - ann['period_end']).dt.days
    leaking  = ann[lag_days < 0]
    same_day = ann[lag_days == 0]
    fast_30  = ann[(lag_days > 0) & (lag_days <= 30)]
    normal   = ann[lag_days > 30]

    print(f'  Rows analysed          : {len(ann):,}')
    print(f'  filed_date < period_end: {len(leaking):,}  '
          f'({100*len(leaking)/max(len(ann),1):.2f}%)  ← LEAKAGE')
    print(f'  filed_date = period_end: {len(same_day):,}  '
          f'({100*len(same_day)/max(len(ann),1):.2f}%)  ← SUSPICIOUS')
    print(f'  Lag 1–30 days          : {len(fast_30):,}  '
          f'({100*len(fast_30)/max(len(ann),1):.2f}%)  ← FAST FILERS')
    print(f'  Lag > 30 days (normal) : {len(normal):,}  '
          f'({100*len(normal)/max(len(ann),1):.2f}%)')

    if len(leaking):
        print(f'\n  ⚠  LOOK-AHEAD LEAK: {len(leaking):,} rows where filed_date < period_end')
        sample = leaking[['ticker', 'fiscal_year', 'filed_date', 'period_end']].head(10)
        print(sample.to_string(index=False))
    else:
        print('\n  ✓  No look-ahead leakage detected.')

    median_lag = lag_days[lag_days >= 0].median()
    p95_lag    = lag_days[lag_days >= 0].quantile(0.95)
    print(f'\n  Median filing lag (days): {median_lag:.0f}')
    print(f'  95th pct filing lag     : {p95_lag:.0f}')


# ── Audit 3: FX-adjusted returns ─────────────────────────────────────────────

def audit_fx(df: pd.DataFrame, fix: bool = False,
             out_path: Path | None = None) -> pd.DataFrame:
    print('\n── Audit 3: FX-Adjusted Returns ────────────────────────────────────')

    ann = df[df['period_type'] == 'annual'].copy()
    if 'country' not in ann.columns:
        print('  country column not present — cannot determine FX pairs. Skipping.')
        return df

    countries = ann['country'].dropna().unique()
    non_usd   = [c for c in countries if c in FX_MAP]
    usd_cos   = [c for c in countries if c not in FX_MAP and c != 'US']
    print(f'  Countries in dataset     : {sorted(countries)}')
    print(f'  Non-USD countries w/ FX  : {sorted(non_usd)}')
    if usd_cos:
        print(f'  Countries without FX map : {sorted(usd_cos)} (will be skipped)')

    if not non_usd:
        print('  No non-USD countries require adjustment. Dataset is US-only or all mapped.')
        return df

    if not fix:
        print('\n  Run with --fix to compute and append forward_return_{h}_usd columns.')
        return df

    # Download FX series
    fx_cache: dict[str, pd.Series | None] = {}
    needed_pairs = set(FX_MAP[c] for c in non_usd)
    print(f'\n  Downloading {len(needed_pairs)} FX series from yfinance...')
    for pair in sorted(needed_pairs):
        print(f'    {pair}...', end=' ', flush=True)
        series = _fetch_fx_series(pair)
        fx_cache[pair] = series
        print('ok' if series is not None else 'FAILED (skipping)')

    df = df.copy()
    ann_idx = df[df['period_type'] == 'annual'].index
    ann_sub = df.loc[ann_idx].copy()
    ann_sub['filed_date'] = pd.to_datetime(ann_sub['filed_date'], errors='coerce')

    for h, days in HORIZONS.items():
        ret_col  = f'forward_return_{h}'
        usd_col  = f'forward_return_{h}_usd'
        if ret_col not in df.columns:
            continue

        usd_returns = ann_sub[ret_col].copy().astype(float)

        for country in non_usd:
            pair   = FX_MAP[country]
            series = fx_cache.get(pair)
            if series is None:
                continue

            mask = (ann_sub['country'] == country) & ann_sub['filed_date'].notna()
            idx  = ann_sub[mask].index

            fx_rets = ann_sub.loc[idx, 'filed_date'].apply(
                lambda d: _forward_fx_return(series, d, days)
            )
            local_rets = ann_sub.loc[idx, ret_col]

            # USD return = (1 + local_ret) * (1 + fx_ret) - 1
            usd = (1 + local_rets) * (1 + fx_rets) - 1
            usd_returns.loc[idx] = usd.values

        df.loc[ann_idx, usd_col] = usd_returns.values
        pct_filled = usd_returns.notna().mean()
        print(f'  {usd_col}: {pct_filled:.1%} rows filled')

    if out_path is None:
        out_path = DATA_PATH
    df.to_parquet(out_path, index=False)
    print(f'\n  ✓ FX-adjusted columns written → {out_path}')

    # Comparison summary
    print('\n  FX impact summary (median return difference, non-US rows only):')
    non_us = df[(df['period_type'] == 'annual') & (df.get('country', pd.Series()) != 'US')]
    for h in HORIZONS:
        local_col = f'forward_return_{h}'
        usd_col   = f'forward_return_{h}_usd'
        if local_col in non_us.columns and usd_col in non_us.columns:
            diff = (non_us[usd_col] - non_us[local_col]).dropna()
            print(f'    {h}: median FX impact = {diff.median():+.3f}  '
                  f'(std={diff.std():.3f}, n={len(diff):,})')

    return df


# ── Audit 3 (new): Overfitting audit ────────────────────────────────────────

def audit_overfitting(meta_path: Path) -> dict[str, float]:
    """Compare train AUC to walk-forward mean AUC. Flag if gap > 0.15.

    Reads model_meta.json (val_auc proxy for train-set AUC) and
    reports/walk_forward_auc_{h}.csv for WF mean AUC.

    Returns dict of overfit_gaps per horizon. Writes gaps to model_meta.json.
    """
    print('\n── Audit 3: Overfitting Audit ──────────────────────────────────────')

    if not meta_path.exists():
        print(f'  model_meta.json not found at {meta_path} — skip.')
        return {}

    meta = json.loads(meta_path.read_text())
    reports_dir = meta_path.parent.parent / 'reports'
    gaps: dict[str, float] = {}

    for h, m in meta.items():
        val_auc = m.get('val_auc')
        if val_auc is None or str(val_auc) == 'nan':
            print(f'  [{h}] no val_auc in model_meta — skip')
            continue

        wf_path = reports_dir / f'walk_forward_auc_{h}.csv'
        if wf_path.exists():
            wf_df = pd.read_csv(wf_path)
            wf_mean = float(wf_df['auc'].mean()) if 'auc' in wf_df.columns else None
        else:
            wf_path2 = reports_dir / f'oof_auc_{h}.csv'
            if wf_path2.exists():
                wf_df = pd.read_csv(wf_path2)
                wf_mean = float(wf_df['auc'].mean()) if 'auc' in wf_df.columns else None
            else:
                wf_mean = None

        if wf_mean is None:
            print(f'  [{h}] no walk-forward AUC file found — skip')
            continue

        gap = round(float(val_auc) - wf_mean, 4)
        gaps[h] = gap
        flag = '  ⚠  OVERFIT' if abs(gap) > 0.15 else '  ✓'
        print(f'  [{h}] val_auc={val_auc:.4f}  wf_mean_auc={wf_mean:.4f}  '
              f'overfit_gap={gap:+.4f}{flag}')

    # Write gaps back to model_meta.json
    if gaps:
        for h, gap in gaps.items():
            if h in meta:
                meta[h]['overfit_gap'] = gap
        meta_path.write_text(json.dumps(meta, indent=2))
        print(f'\n  overfit_gap written to {meta_path}')

    return gaps


# ── Audit 4 (new): Multiple testing note ────────────────────────────────────

def audit_multiple_testing() -> None:
    """Document expected false discoveries across 5 horizons × strategies."""
    print('\n── Audit 4: Multiple Testing Correction (INFO) ─────────────────────')
    n_horizons    = 5   # 6m/1y/2y/3y/5y
    n_strategies  = 4   # composite/1y/3y/5y
    n_tests       = n_horizons * n_strategies
    alpha         = 0.05
    bonferroni    = alpha / n_tests
    expected_fp   = n_tests * alpha

    print(f'  Backtest comparisons : {n_tests} ({n_horizons} horizons × {n_strategies} strategies)')
    print(f'  Naive α=0.05 expected false positives : {expected_fp:.1f}')
    print(f'  Bonferroni-corrected α : {bonferroni:.4f}  (use when reporting p-values)')
    print(f'  Recommendation: require Sharpe p < {bonferroni:.4f} for any single-horizon claim.')
    print(f'  Feature selection uses BH FDR (q<0.05) per horizon — already corrected.')


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--fix', action='store_true',
                        help='Compute and write FX-adjusted return columns to parquet')
    parser.add_argument('--out', type=Path, default=None,
                        help='Output parquet path when --fix is set '
                             '(default: overwrites historical_dataset_clean.parquet)')
    parser.add_argument('--ci', action='store_true',
                        help='CI mode: exit 1 if look-ahead violations found (hard fail); '
                             'survivorship/overfitting warnings do not fail CI')
    args = parser.parse_args()

    if not DATA_PATH.exists():
        print(f'ERROR: {DATA_PATH} not found — run the pipeline first.')
        sys.exit(1)

    meta_path = BASE / 'models' / 'model_meta.json'

    print(f'Loading {DATA_PATH}...')
    df = pd.read_parquet(DATA_PATH)
    print(f'  {len(df):,} rows × {len(df.columns)} columns')
    print(f'  fiscal_year range: {df["fiscal_year"].min()} – {df["fiscal_year"].max()}')
    print(f'  markets: {sorted(df["market"].dropna().unique()) if "market" in df.columns else "N/A"}')

    audit_survivorship(df)

    # Audit 1 (re-ordered) / Audit 2: Look-ahead — hard fail
    leakage_count = _count_lookahead(df)
    if leakage_count > 0:
        print(f'\n  ✗  HARD FAIL: {leakage_count:,} look-ahead violations (filed_date < period_end)')
        if args.ci:
            sys.exit(1)
    else:
        print('\n  ✓  No look-ahead violations detected.')

    audit_filing_lag(df)
    audit_overfitting(meta_path)
    audit_multiple_testing()
    audit_fx(df, fix=args.fix, out_path=args.out)

    print('\n── Audit complete ──────────────────────────────────────────────────')
    if args.ci:
        print('  CI mode: look-ahead check PASSED (exit 0)')


def _count_lookahead(df: pd.DataFrame) -> int:
    """Return number of rows where filed_date < period_end (look-ahead)."""
    ann = df[(df['period_type'] == 'annual') & df['filed_date'].notna()].copy()
    ann['filed_date'] = pd.to_datetime(ann['filed_date'], errors='coerce')
    ann['period_end'] = ann.apply(_period_end_date, axis=1)
    ann = ann[ann['period_end'].notna()]
    lag_days = (ann['filed_date'] - ann['period_end']).dt.days
    return int((lag_days < 0).sum())


if __name__ == '__main__':
    main()
