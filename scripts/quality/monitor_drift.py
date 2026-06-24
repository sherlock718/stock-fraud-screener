"""
Model drift monitor — detects feature distribution shift and AUC degradation.

Compares a recent scoring window against the training distribution to flag:
  1. Population Stability Index (PSI) per feature — PSI > 0.2 = significant drift
  2. Val AUC vs current-window AUC (requires forward_return labels)
  3. Score distribution shift (mean predicted probability drift)
  4. Per-alpha IC decay (rolling 3y / 6y / 12y windows) — flags signals with
     3y rolling IC < 0.02 or negative IC in the latest window
  5. Drawdown circuit breaker — warns if realised portfolio drawdown > 20%

Outputs:
  reports/drift_report.json   PSI scores + AUC delta + IC decay per horizon
  reports/drift_report.csv    Per-feature PSI table

Exit code 1 if any PSI > PSI_ALERT or AUC degrades > AUC_DROP_ALERT.
Use in CI/cron to alert on model staleness.

Usage:
    python3 scripts/monitor_drift.py
    python3 scripts/monitor_drift.py --window 2024   # compare vs 2024 data
    python3 scripts/monitor_drift.py --psi-alert 0.2
    python3 scripts/monitor_drift.py --auc-alert 0.05
    python3 scripts/monitor_drift.py --dd-gate 20    # circuit breaker at 20% drawdown
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
warnings.filterwarnings('ignore')

import joblib
import numpy as np
import pandas as pd
from pathlib import Path
from scipy import stats
from scripts._root import ROOT

BASE = ROOT

META_PATH = BASE / 'models' / 'model_meta.json'
DATA_PATH = BASE / 'data' / 'historical_dataset_clean.parquet'
REGISTRY  = BASE / 'data' / 'alpha_registry.json'
BACKTEST  = BASE / 'data' / 'portfolio_backtest.json'
REPORTS   = BASE / 'reports'
REPORTS.mkdir(exist_ok=True)

PSI_ALERT      = 0.20   # PSI threshold — "significant shift"
AUC_DROP_ALERT = 0.05   # flag if AUC drops more than this vs val baseline
IC_DECAY_WARN  = 0.02   # flag if rolling IC falls below this
DD_GATE        = 20.0   # default drawdown circuit-breaker threshold (%)


def _load_data() -> pd.DataFrame:
    df = pd.read_parquet(DATA_PATH)
    df = df[df['period_type'] == 'annual'].copy()
    df = df.sort_values('total_assets', ascending=False, na_position='last')
    df = df.drop_duplicates(subset=['ticker', 'fiscal_year'], keep='first')
    return df.reset_index(drop=True)


def _compute_psi(expected: np.ndarray, actual: np.ndarray, n_bins: int = 10) -> float:
    """Population Stability Index between two 1-D arrays.

    PSI < 0.1  = no significant change
    PSI < 0.2  = minor change (monitor)
    PSI >= 0.2 = significant shift (alert)
    """
    combined = np.concatenate([expected, actual])
    finite   = combined[np.isfinite(combined)]
    if len(finite) < 20:
        return 0.0
    lo, hi = np.nanpercentile(finite, 1), np.nanpercentile(finite, 99)
    if lo == hi:
        return 0.0
    bins = np.linspace(lo, hi, n_bins + 1)
    e_counts, _ = np.histogram(expected[np.isfinite(expected)], bins=bins)
    a_counts, _ = np.histogram(actual[np.isfinite(actual)],   bins=bins)
    e_pct = (e_counts + 0.001) / (e_counts.sum() + 0.001 * n_bins)
    a_pct = (a_counts + 0.001) / (a_counts.sum() + 0.001 * n_bins)
    return float(np.sum((a_pct - e_pct) * np.log(a_pct / e_pct)))


def _score_window(model, df: pd.DataFrame, features: list[str],
                  train_medians: dict) -> np.ndarray | None:
    feats = [f for f in features if f in df.columns]
    if not feats or len(df) < 10:
        return None
    X = df[feats].fillna(pd.Series(train_medians))
    return model.predict_proba(X)[:, 1]


def analyse_horizon(h: str, meta: dict, df_train: pd.DataFrame,
                    df_window: pd.DataFrame,
                    psi_threshold: float = PSI_ALERT,
                    auc_threshold: float = AUC_DROP_ALERT) -> dict:
    m = meta[h]
    features     = m['features']
    beat_col     = m['beat_col']
    train_medians = m['train_medians']

    model_path = BASE / 'models' / f'model_{h}.joblib'
    calibrated_path = BASE / 'models' / f'model_{h}_calibrated.joblib'
    if calibrated_path.exists():
        model = joblib.load(calibrated_path)
    elif model_path.exists():
        model = joblib.load(model_path)
    else:
        return {'error': f'model_{h}.joblib not found — run train_models.py first'}

    # ── Feature PSI ─────────────────────────────────────────────────────────
    psi_rows = []
    for feat in features:
        if feat not in df_train.columns or feat not in df_window.columns:
            continue
        expected = df_train[feat].dropna().values
        actual   = df_window[feat].dropna().values
        if len(expected) < 20 or len(actual) < 10:
            continue
        psi = _compute_psi(expected, actual)
        psi_rows.append({'feature': feat, 'psi': round(psi, 4),
                         'alert': psi >= psi_threshold})

    psi_df = pd.DataFrame(psi_rows).sort_values('psi', ascending=False)
    n_alerts = int((psi_df['psi'] >= psi_threshold).sum())
    mean_psi = float(psi_df['psi'].mean()) if len(psi_df) else 0.0

    # ── Score distribution shift ─────────────────────────────────────────────
    train_scores = _score_window(model, df_train, features, train_medians)
    window_scores = _score_window(model, df_window, features, train_medians)
    score_psi = _compute_psi(train_scores, window_scores) if (
        train_scores is not None and window_scores is not None) else None

    # ── AUC on window (if labels available) ──────────────────────────────────
    window_auc = float('nan')
    val_auc    = m.get('val_auc', float('nan'))
    auc_delta  = float('nan')
    if beat_col in df_window.columns:
        sub = df_window[df_window[beat_col].notna()].copy()
        feats_avail = [f for f in features if f in sub.columns]
        if len(sub) >= 30 and sub[beat_col].nunique() >= 2:
            X = sub[feats_avail].fillna(pd.Series(train_medians))
            y = sub[beat_col].astype(int)
            try:
                window_auc = float(
                    __import__('sklearn.metrics', fromlist=['roc_auc_score'])
                    .roc_auc_score(y, model.predict_proba(X)[:, 1]))
                auc_delta = window_auc - val_auc
            except Exception:
                pass

    return {
        'horizon':           h,
        'n_features':        len(features),
        'n_features_checked':len(psi_df),
        'n_psi_alerts':      n_alerts,
        'mean_psi':          round(mean_psi, 4),
        'score_psi':         round(score_psi, 4) if score_psi is not None else None,
        'val_auc':           val_auc,
        'window_auc':        round(window_auc, 4) if not np.isnan(window_auc) else None,
        'auc_delta':         round(auc_delta, 4) if not np.isnan(auc_delta) else None,
        'auc_alert':         (not np.isnan(auc_delta)) and (auc_delta < -auc_threshold),
        'psi_details':       psi_df.to_dict('records'),
    }


def analyse_ic_decay(df: pd.DataFrame) -> dict:
    """Compute rolling IC for each selected signal over 3y / 6y / 12y windows.

    IC = Spearman rank correlation between signal values and forward_return_1y,
    computed cross-sectionally within each fiscal year, then averaged over the
    most recent N fiscal years.

    Returns a dict with per-signal rolling IC values and decay flags.
    """
    if not REGISTRY.exists():
        return {'error': 'alpha_registry.json not found'}

    with open(REGISTRY) as f:
        reg = json.load(f)

    selected = [s for s in reg['signals'] if s.get('selected', True)]
    signal_ids = [s['signal_id'] for s in selected]
    ret_col = 'forward_return_1y'

    if ret_col not in df.columns:
        return {'error': f'{ret_col} not in dataset'}

    years = sorted(df['fiscal_year'].dropna().unique().astype(int))

    # Compute annual IC for each signal × year
    annual_ic: dict[str, dict[int, float]] = {sid: {} for sid in signal_ids}
    for year in years:
        yr = df[df['fiscal_year'] == year].dropna(subset=[ret_col])
        if len(yr) < 20:
            continue
        for sid in signal_ids:
            if sid not in yr.columns:
                continue
            sub = yr[[sid, ret_col]].dropna()
            if len(sub) < 20:
                continue
            ic_val = float(sub[sid].rank().corr(sub[ret_col].rank()))
            if not np.isnan(ic_val):
                annual_ic[sid][year] = ic_val

    windows = {'3y': 3, '6y': 6, '12y': 12}
    signal_results = []
    decay_alerts = []

    for sid in signal_ids:
        ic_series = annual_ic[sid]
        if not ic_series:
            continue
        sorted_years = sorted(ic_series.keys())
        row: dict = {'signal_id': sid}
        for label, n in windows.items():
            recent = sorted_years[-n:] if len(sorted_years) >= n else sorted_years
            vals = [ic_series[y] for y in recent]
            row[f'ic_{label}'] = round(float(np.mean(vals)), 4) if vals else None

        ic_3y = row.get('ic_3y')
        latest_ic = ic_series.get(sorted_years[-1]) if sorted_years else None
        row['latest_ic'] = round(latest_ic, 4) if latest_ic is not None else None
        row['decay_warn'] = (ic_3y is not None and ic_3y < IC_DECAY_WARN)
        row['decay_alert'] = (latest_ic is not None and latest_ic < 0.0)

        if row['decay_alert']:
            decay_alerts.append(f'{sid}: latest IC={latest_ic:.4f} (NEGATIVE)')
        elif row['decay_warn']:
            decay_alerts.append(f'{sid}: 3y rolling IC={ic_3y:.4f} < {IC_DECAY_WARN}')

        signal_results.append(row)

    return {
        'n_signals': len(signal_results),
        'n_decay_warns': sum(1 for r in signal_results if r['decay_warn']),
        'n_decay_alerts': sum(1 for r in signal_results if r['decay_alert']),
        'decay_messages': decay_alerts,
        'signals': signal_results,
    }


def check_drawdown_circuit_breaker(dd_gate: float) -> dict:
    """Load portfolio_backtest.json and compute current drawdown from peak.

    If the most recent cumulative drawdown exceeds dd_gate (%), emit a warning.
    Returns a dict with cumulative return, peak, current drawdown, and alert flag.
    """
    if not BACKTEST.exists():
        return {'error': 'portfolio_backtest.json not found — run build_portfolio.py first'}

    with open(BACKTEST) as f:
        bt = json.load(f)

    annual = bt.get('annual_returns', [])
    if not annual:
        return {'error': 'no annual_returns in portfolio_backtest.json'}

    returns = np.array([r['return_pct'] / 100.0 for r in annual])
    cum = np.cumprod(1.0 + returns)
    peak = np.maximum.accumulate(cum)
    drawdowns = (cum - peak) / peak
    current_dd_pct = round(float(drawdowns[-1]) * 100, 2)
    max_dd_pct = round(float(drawdowns.min()) * 100, 2)

    alert = current_dd_pct < -dd_gate
    result = {
        'n_years': len(returns),
        'latest_year': annual[-1]['year'],
        'cumulative_return_pct': round(float(cum[-1] - 1) * 100, 2),
        'current_drawdown_pct': current_dd_pct,
        'max_drawdown_pct': max_dd_pct,
        'dd_gate_pct': -dd_gate,
        'circuit_breaker_active': alert,
    }
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--window', type=int, default=None,
                        help='fiscal_year to use as monitoring window (default: latest year in data)')
    parser.add_argument('--psi-alert', type=float, default=PSI_ALERT,
                        help=f'PSI alert threshold (default: {PSI_ALERT})')
    parser.add_argument('--auc-alert', type=float, default=AUC_DROP_ALERT,
                        help=f'AUC drop alert threshold (default: {AUC_DROP_ALERT})')
    parser.add_argument('--dd-gate', type=float, default=DD_GATE, dest='dd_gate',
                        help=f'Drawdown circuit-breaker threshold in %% (default: {DD_GATE}). '
                             'Warns if current portfolio drawdown exceeds this value.')
    parser.add_argument('--skip-ic-decay', action='store_true', dest='skip_ic_decay',
                        help='Skip per-alpha IC decay analysis')
    parser.add_argument('--skip-dd', action='store_true', dest='skip_dd',
                        help='Skip drawdown circuit-breaker check')
    args = parser.parse_args()

    psi_threshold = args.psi_alert
    auc_threshold = args.auc_alert

    if not META_PATH.exists():
        print('ERROR: model_meta.json not found — run train_models.py first', file=sys.stderr)
        sys.exit(1)

    meta = json.loads(META_PATH.read_text())
    df   = _load_data()

    if args.window:
        window_year = args.window
    else:
        window_year = int(df['fiscal_year'].max())

    print(f'Drift monitor — window: fiscal_year={window_year}')
    df_window = df[df['fiscal_year'] == window_year].copy()
    print(f'  Window rows: {len(df_window):,}')
    if len(df_window) < 30:
        print(f'  WARNING: only {len(df_window)} rows in window — results may be unreliable')

    results = {}
    any_alert = False
    all_psi_rows = []

    for h in ['1y', '3y', '5y']:
        if h not in meta:
            continue
        m = meta[h]
        df_train = df[df['fiscal_year'] <= m['train_cutoff']].copy()
        print(f'\n  {h}: ', end='', flush=True)
        r = analyse_horizon(h, meta, df_train, df_window, psi_threshold, auc_threshold)
        results[h] = r

        if 'error' in r:
            print(r['error'])
            continue

        print(f'{r["n_psi_alerts"]} PSI alerts | '
              f'mean PSI={r["mean_psi"]:.3f} | '
              f'score PSI={r["score_psi"] if r["score_psi"] is not None else "N/A":.3f} | '
              f'window AUC={r["window_auc"] if r["window_auc"] is not None else "N/A"} | '
              f'ΔAUC={r["auc_delta"] if r["auc_delta"] is not None else "N/A"}')

        if r['n_psi_alerts'] > 0 or r['auc_alert']:
            any_alert = True
            if r['n_psi_alerts'] > 0:
                top = sorted(r['psi_details'], key=lambda x: x['psi'], reverse=True)[:5]
                for row in top:
                    if row['alert']:
                        print(f'    ! PSI alert: {row["feature"]:<40} PSI={row["psi"]:.4f}')
            if r['auc_alert']:
                print(f'    ! AUC alert: dropped {r["auc_delta"]:+.4f} vs val baseline')

        for row in r['psi_details']:
            all_psi_rows.append({'horizon': h, **row})

    drift_report = {
        'window_year': window_year,
        'psi_alert_threshold':  psi_threshold,
        'auc_alert_threshold':  auc_threshold,
        'any_alert':            any_alert,
        'horizons':             results,
    }

    # ── IC decay analysis ─────────────────────────────────────────────────────
    ic_decay_result: dict = {}
    if not args.skip_ic_decay:
        print('\n  IC decay analysis...', flush=True)
        ic_decay_result = analyse_ic_decay(df)
        if 'error' in ic_decay_result:
            print(f'  WARNING: IC decay skipped — {ic_decay_result["error"]}')
        else:
            n_w = ic_decay_result['n_decay_warns']
            n_a = ic_decay_result['n_decay_alerts']
            print(f'  IC decay: {n_w} warn(s), {n_a} alert(s)')
            for msg in ic_decay_result.get('decay_messages', []):
                print(f'    ! {msg}')
            if n_a > 0:
                any_alert = True
        drift_report['ic_decay'] = ic_decay_result

    # ── Drawdown circuit breaker ──────────────────────────────────────────────
    dd_result: dict = {}
    if not args.skip_dd:
        print('\n  Drawdown circuit-breaker check...', flush=True)
        dd_result = check_drawdown_circuit_breaker(args.dd_gate)
        if 'error' in dd_result:
            print(f'  WARNING: drawdown check skipped — {dd_result["error"]}')
        else:
            print(f'  Current DD: {dd_result["current_drawdown_pct"]:+.1f}%  '
                  f'(max: {dd_result["max_drawdown_pct"]:+.1f}%  '
                  f'gate: {dd_result["dd_gate_pct"]:+.1f}%)')
            if dd_result['circuit_breaker_active']:
                print(f'  !! CIRCUIT BREAKER: drawdown {dd_result["current_drawdown_pct"]:+.1f}%'
                      f' exceeds gate {dd_result["dd_gate_pct"]:+.1f}% — '
                      'HALVE POSITION SIZE, no new positions until DD < 10%')
                any_alert = True
        drift_report['drawdown'] = dd_result

    drift_report['any_alert'] = any_alert
    report_path = REPORTS / 'drift_report.json'
    report_path.write_text(json.dumps(drift_report, indent=2))

    if all_psi_rows:
        pd.DataFrame(all_psi_rows).to_csv(REPORTS / 'drift_report.csv', index=False)

    print(f'\nReport saved → {report_path}')
    if any_alert:
        print('ALERT: drift detected — consider retraining')
        sys.exit(1)
    else:
        print('OK: no significant drift detected')


if __name__ == '__main__':
    main()
