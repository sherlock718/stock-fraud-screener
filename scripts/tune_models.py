"""
Hyperparameter tuning, CatBoost comparison, calibration, and ensemble for ML models.

Workflow:
  1. Optuna search over LightGBM hyperparams (maximise val AUC)
  2. CatBoost training with same feature set (comparison baseline)
  3. Probability calibration (isotonic regression on val split)
  4. Ensemble: average calibrated LGBM + LR + CatBoost probabilities
  5. Save best models and updated model_meta.json

Outputs:
  models/model_{h}_tuned.joblib          best Optuna-tuned LightGBM
  models/model_{h}_catboost.cbm          CatBoost model (native format)
  models/model_{h}_calibrated.joblib     calibrated ensemble Pipeline
  reports/optuna_study_{h}.csv           Optuna trial history

Usage:
    python3 scripts/tune_models.py
    python3 scripts/tune_models.py --horizon 1y  # single horizon
    python3 scripts/tune_models.py --trials 50   # fewer Optuna trials
    python3 scripts/tune_models.py --no-catboost # skip CatBoost
"""
from __future__ import annotations

import argparse
import json
import warnings
warnings.filterwarnings('ignore')

import joblib
import lightgbm as lgb
import numpy as np
import pandas as pd
from pathlib import Path
from sklearn.calibration import CalibratedClassifierCV
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    OPTUNA_AVAILABLE = True
except ImportError:
    OPTUNA_AVAILABLE = False
    print('optuna not installed — skipping hyperparameter search (pip install optuna)')

try:
    import catboost as cb
    CATBOOST_AVAILABLE = True
except ImportError:
    CATBOOST_AVAILABLE = False
    print('catboost not installed — skipping CatBoost comparison (pip install catboost)')

import sys
BASE       = Path(__file__).parent.parent
sys.path.insert(0, str(BASE))
META_PATH  = BASE / 'models' / 'model_meta.json'
MODELS_DIR = BASE / 'models'
REPORTS    = BASE / 'reports'
MODELS_DIR.mkdir(exist_ok=True)
REPORTS.mkdir(exist_ok=True)

HORIZONS = {'6m', '1y', '2y', '3y', '5y'}
N_OPTUNA_TRIALS = 100


def _load_meta() -> dict:
    if not META_PATH.exists():
        raise FileNotFoundError(f'{META_PATH} not found — run train_models.py first')
    return json.loads(META_PATH.read_text())


def _load_data_for_horizon(meta: dict, h: str) -> tuple:
    """Return (df_train, df_val, df_test, features, beat_col, train_medians)."""
    from scripts.train_models import load_data  # reuse loader

    m = meta[h]
    df = load_data()
    train_cutoff = m['train_cutoff']
    val_end      = m['val_end']
    features     = m['features']
    beat_col     = m['beat_col']
    train_medians = m['train_medians']

    filed = pd.to_datetime(df.get('filed_date', pd.NaT), errors='coerce')
    cutoff_date = pd.Timestamp(f'{train_cutoff + 1}-01-01')
    df_train = df[
        (df['fiscal_year'] <= train_cutoff) &
        (filed.isna() | (filed < cutoff_date))
    ].copy()
    df_val   = df[(df['fiscal_year'] > train_cutoff) & (df['fiscal_year'] <= val_end)].copy()
    df_test  = df[df['fiscal_year'] > val_end].copy()
    return df_train, df_val, df_test, features, beat_col, train_medians


def _prep_split(df: pd.DataFrame, features: list[str], beat_col: str,
                train_medians: dict) -> tuple[pd.DataFrame, pd.Series]:
    sub = df[df[beat_col].notna()].copy()
    feats = [f for f in features if f in sub.columns]
    X = sub[feats].fillna(pd.Series(train_medians))
    y = sub[beat_col].astype(int)
    return X, y


def _eval_model(model, X: pd.DataFrame, y: pd.Series) -> float:
    if len(y) < 30 or y.nunique() < 2:
        return float('nan')
    proba = model.predict_proba(X)[:, 1]
    return roc_auc_score(y, proba)


# ── Optuna tuning ────────────────────────────────────────────────────────────

def _lgbm_objective(trial, X_train, y_train, X_val, y_val):
    params = {
        'n_estimators':     trial.suggest_int('n_estimators', 200, 800),
        'max_depth':        trial.suggest_int('max_depth', 3, 7),
        'learning_rate':    trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
        'num_leaves':       trial.suggest_int('num_leaves', 15, 63),
        'subsample':        trial.suggest_float('subsample', 0.5, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
        'min_child_samples':trial.suggest_int('min_child_samples', 10, 60),
        'reg_alpha':        trial.suggest_float('reg_alpha', 1e-4, 10.0, log=True),
        'reg_lambda':       trial.suggest_float('reg_lambda', 1e-4, 10.0, log=True),
        'random_state': 42, 'n_jobs': -1, 'verbose': -1,
    }
    pos = int((y_train == 1).sum())
    neg = int((y_train == 0).sum())
    clf = lgb.LGBMClassifier(scale_pos_weight=neg / max(pos, 1), **params)
    clf.fit(X_train, y_train)
    return _eval_model(clf, X_val, y_val)


def tune_lgbm(X_train, y_train, X_val, y_val, n_trials: int) -> tuple:
    if not OPTUNA_AVAILABLE:
        return None, None
    study = optuna.create_study(direction='maximize',
                                sampler=optuna.samplers.TPESampler(seed=42))
    study.optimize(
        lambda t: _lgbm_objective(t, X_train, y_train, X_val, y_val),
        n_trials=n_trials,
        show_progress_bar=False,
    )
    best = study.best_params
    pos = int((y_train == 1).sum())
    neg = int((y_train == 0).sum())
    clf = lgb.LGBMClassifier(
        scale_pos_weight=neg / max(pos, 1), random_state=42, n_jobs=-1, verbose=-1,
        **best,
    )
    clf.fit(X_train, y_train)
    trials_df = study.trials_dataframe()[['number', 'value', 'params_learning_rate',
                                          'params_n_estimators', 'params_num_leaves']]
    trials_df.columns = ['trial', 'val_auc', 'lr', 'n_est', 'num_leaves']
    return clf, trials_df, best


# ── CatBoost ────────────────────────────────────────────────────────────────

def train_catboost(X_train, y_train, X_val, y_val) -> object | None:
    if not CATBOOST_AVAILABLE:
        return None
    pos = int((y_train == 1).sum())
    neg = int((y_train == 0).sum())
    model = cb.CatBoostClassifier(
        iterations=400,
        depth=5,
        learning_rate=0.04,
        scale_pos_weight=neg / max(pos, 1),
        eval_metric='AUC',
        random_seed=42,
        verbose=False,
    )
    model.fit(X_train, y_train, eval_set=(X_val, y_val), early_stopping_rounds=30)
    return model


# ── Probability calibration + ensemble ──────────────────────────────────────

class _PredictProbaWrapper:
    """Wrap any predict_proba model so CalibratedClassifierCV can use it."""
    def __init__(self, model): self.model = model
    def predict_proba(self, X): return self.model.predict_proba(X)
    def fit(self, X, y): return self
    @property
    def classes_(self): return np.array([0, 1])


class EnsembleClassifier:
    """Average calibrated probabilities from multiple base models."""

    def __init__(self, models: list, weights: list[float] | None = None):
        self.models = models
        self.weights = weights if weights else [1.0 / len(models)] * len(models)

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        probas = np.zeros(len(X))
        for model, w in zip(self.models, self.weights):
            p = model.predict_proba(X)[:, 1]
            probas += w * p
        return np.column_stack([1 - probas, probas])

    def fit(self, X, y): return self

    @property
    def classes_(self): return np.array([0, 1])


class _CalModel:
    """Isotonic-calibrated wrapper. Defined at module level to be picklable."""
    def __init__(self, base, iso): self._base = base; self._iso = iso
    def predict_proba(self, X):
        p = self._base.predict_proba(X)[:, 1]
        cal = self._iso.predict(p)
        return np.column_stack([1 - cal, cal])
    @property
    def classes_(self): return np.array([0, 1])


def calibrate_model(model, X_val: pd.DataFrame, y_val: pd.Series):
    """Isotonic calibration on val split."""
    raw_proba = model.predict_proba(X_val)[:, 1]
    iso = IsotonicRegression(out_of_bounds='clip')
    iso.fit(raw_proba, y_val.values)
    return _CalModel(model, iso)


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--horizon', default=None, choices=['6m', '1y', '2y', '3y', '5y'],
                        help='Single horizon to tune (default: all)')
    parser.add_argument('--trials', type=int, default=N_OPTUNA_TRIALS,
                        help=f'Optuna trials per horizon (default: {N_OPTUNA_TRIALS})')
    parser.add_argument('--no-catboost', action='store_true',
                        help='Skip CatBoost comparison')
    args = parser.parse_args()

    meta = _load_meta()
    horizons = [args.horizon] if args.horizon else sorted(HORIZONS)

    updated_meta = dict(meta)

    for h in horizons:
        if h not in meta:
            print(f'  {h}: no entry in model_meta.json — skipping')
            continue
        print(f'\n── {h} ──────────────────────────────────────────────')
        m = meta[h]
        features, beat_col = m['features'], m['beat_col']
        train_medians = m['train_medians']

        from scripts.train_models import load_data
        df = load_data()
        df_train = df[df['fiscal_year'] <= m['train_cutoff']].copy()
        df_val   = df[(df['fiscal_year'] > m['train_cutoff']) &
                      (df['fiscal_year'] <= m['val_end'])].copy()
        df_test  = df[df['fiscal_year'] > m['val_end']].copy()

        X_train, y_train = _prep_split(df_train, features, beat_col, train_medians)
        X_val,   y_val   = _prep_split(df_val,   features, beat_col, train_medians)
        X_test,  y_test  = _prep_split(df_test,  features, beat_col, train_medians)

        # ── 1. Optuna LGBM tuning ────────────────────────────────────────────
        print(f'  Optuna: {args.trials} trials...', end=' ', flush=True)
        lgbm_tuned, trials_df, best_params = tune_lgbm(X_train, y_train, X_val, y_val, args.trials)
        if lgbm_tuned is not None:
            tuned_val  = _eval_model(lgbm_tuned, X_val, y_val)
            tuned_test = _eval_model(lgbm_tuned, X_test, y_test)
            print(f'val={tuned_val:.4f} test={tuned_test:.4f}')
            joblib.dump(lgbm_tuned, MODELS_DIR / f'model_{h}_tuned.joblib')
            if trials_df is not None:
                trials_df.to_csv(REPORTS / f'optuna_study_{h}.csv', index=False)
        else:
            lgbm_tuned = joblib.load(MODELS_DIR / f'model_{h}.joblib')
            tuned_val = tuned_test = float('nan')
            best_params = None
            print('skipped (optuna not available)')

        # ── 2. CatBoost ──────────────────────────────────────────────────────
        cb_val = cb_test = float('nan')
        cb_model = None
        if not args.no_catboost:
            print(f'  CatBoost...', end=' ', flush=True)
            cb_model = train_catboost(X_train, y_train, X_val, y_val)
            if cb_model is not None:
                cb_val  = _eval_model(cb_model, X_val, y_val)
                cb_test = _eval_model(cb_model, X_test, y_test)
                print(f'val={cb_val:.4f} test={cb_test:.4f}')
                cb_model.save_model(str(MODELS_DIR / f'model_{h}_catboost.cbm'))
            else:
                print('skipped (catboost not available)')

        # ── 3. Load LR baseline ──────────────────────────────────────────────
        lr_path = MODELS_DIR / f'baseline_lr_{h}.joblib'
        lr_model = joblib.load(lr_path) if lr_path.exists() else None

        # ── 4. Calibrate and build ensemble ─────────────────────────────────
        print(f'  Building ensemble...', end=' ', flush=True)
        base_models = [calibrate_model(lgbm_tuned, X_val, y_val)]
        if lr_model is not None:
            base_models.append(calibrate_model(lr_model, X_val, y_val))
        if cb_model is not None:
            base_models.append(calibrate_model(cb_model, X_val, y_val))

        ensemble = EnsembleClassifier(base_models)
        ens_val  = _eval_model(ensemble, X_val, y_val)
        ens_test = _eval_model(ensemble, X_test, y_test)
        print(f'val={ens_val:.4f} test={ens_test:.4f}')
        joblib.dump(ensemble, MODELS_DIR / f'model_{h}_calibrated.joblib')

        updated_meta[h] = {
            **m,
            'tuned_val_auc':    round(tuned_val, 4),
            'tuned_test_auc':   round(tuned_test, 4),
            'cb_val_auc':       round(cb_val, 4),
            'cb_test_auc':      round(cb_test, 4),
            'ensemble_val_auc': round(ens_val, 4),
            'ensemble_test_auc':round(ens_test, 4),
            'n_ensemble_models':len(base_models),
            'best_params':      best_params,
        }

    META_PATH.write_text(json.dumps(updated_meta, indent=2))
    print(f'\nUpdated model_meta.json — models saved to {MODELS_DIR}/')

    print('\n── Summary ─────────────────────────────────────────────────')
    print(f'  {"Horizon":<6}  {"LR val":>8} {"LR test":>9}  '
          f'{"Tuned val":>10} {"Tuned test":>11}  '
          f'{"CB val":>7} {"CB test":>8}  '
          f'{"Ens val":>8} {"Ens test":>9}')
    for h in horizons:
        m = updated_meta.get(h, {})
        print(f'  {h:<6}  '
              f'{m.get("lr_val_auc", float("nan")):>8.4f} {m.get("lr_test_auc", float("nan")):>9.4f}  '
              f'{m.get("tuned_val_auc", float("nan")):>10.4f} {m.get("tuned_test_auc", float("nan")):>11.4f}  '
              f'{m.get("cb_val_auc", float("nan")):>7.4f} {m.get("cb_test_auc", float("nan")):>8.4f}  '
              f'{m.get("ensemble_val_auc", float("nan")):>8.4f} {m.get("ensemble_test_auc", float("nan")):>9.4f}')


if __name__ == '__main__':
    main()
