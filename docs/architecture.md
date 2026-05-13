# System Architecture

A research-grade quantitative alpha generation platform — from raw filings to portfolio construction.

## High-Level Overview

```mermaid
graph TB
    subgraph Sources["Data Sources"]
        A1[SEC EDGAR<br/>US 10-K/10-Q]
        A2[SimFin API<br/>EU/Nordics]
        A3[DART API<br/>Korea]
        A4[TDNET<br/>Japan]
        A5[SEDAR+<br/>Canada]
        A6[B3/CVM<br/>Brazil]
    end

    subgraph Pipeline["Data Pipeline — pipeline/ + scripts/"]
        B1[Step 1<br/>Fetch Tickers]
        B2[Step 2<br/>Build Snapshots]
        B3[Step 3<br/>Enrich Prices<br/>vol_prior 6m/12m/36m/60m]
        B4[Step 4<br/>Enrich Macro]
        B5[Step 5<br/>Compute Features<br/>sector_pct ranked within fiscal_year]
        B6[Step 6<br/>Clean Dataset]
        B7[Quarterly Enrichment<br/>enrich_quarterly_features.py<br/>+5 intra-year columns]
        B8[Survivorship Correction<br/>mark_survivorship.py<br/>impute −50% for delisted]
        B9[Feature Imputation<br/>impute_features.py<br/>+5 quarterly cols + size_category]
        B10[Alpha Scores<br/>compute_alpha.py<br/>5-factor alpha scores]
        B11[ML Scores<br/>score_historical.py<br/>ml_1y/3y/5y → 360 total cols]
        B1 --> B2 --> B3 --> B4 --> B5 --> B6 --> B7 --> B8 --> B9 --> B10 --> B11
    end

    subgraph Factors["5-Factor Layer — alpha/factors/ ✅ Phase B"]
        F1[Value<br/>P/B · EV/EBITDA · FCF yield]
        F2[Quality<br/>ROE · accruals · Piotroski]
        F3[Momentum<br/>12m-1m return · EPS revision]
        F4[Growth<br/>Revenue CAGR · EPS acceleration]
        F5[Fraud Risk<br/>Beneish · AAER · ml_1y/3y/5y]
        FA[Composite Alpha Score<br/>weighted factor blend]
        F1 & F2 & F3 & F4 & F5 --> FA
    end

    subgraph ML["ML System — scripts/train_models.py ✅ Phase C"]
        C0[PSI Filter<br/>drops unstable features<br/>PSI > 0.25 removed]
        C1[ICIR Feature Selection<br/>~45 features/horizon]
        C2[LightGBM<br/>5 horizons: 6m 1y 2y 3y 5y<br/>filed-date PIT-safe]
        C3[Optuna Tuning<br/>100 trials per horizon]
        C4[CatBoost Ensemble]
        C5[Platt Scaling Calibration]
        C6[score_historical.py<br/>writes ml_1y/3y/5y to parquet]
        C6B[generate_oof_scores.py<br/>walk-forward OOF scoring<br/>ml_1y_oof / ml_3y_oof / ml_5y_oof]
        C7[horizon_router.py<br/>maps months → model key<br/>6m / 1y / 2y / 3y / 5y]
        C0 --> C1 --> C2 --> C3 --> C4 --> C5 --> C6
        C5 --> C6B
        C6B --> C7
    end

    subgraph Research["Research — scripts/ + research/"]
        D1[Factor Analysis<br/>IC / ICIR / Decay]
        D2[Walk-Forward Backtester<br/>SPY benchmark · 4 strategies]
        D3[Bias Audit<br/>Look-ahead · Survivorship<br/>Overfitting · Multiple testing]
        D4[Walk-Forward AUC CV<br/>reports/walk_forward_auc_*.csv]
        D5[SPY Returns<br/>fetch_spy_returns.py<br/>data/spy_returns.csv]
        D5 --> D2
    end

    subgraph Storage["Storage"]
        S1[Parquet<br/>data/historical_dataset_clean.parquet<br/>58K rows · 360 columns]
        S2[TimescaleDB<br/>hypertable — infra/db/init.sql<br/>Phase C — deferred]
    end

    subgraph Outputs["Outputs & Serving"]
        E1[Streamlit App<br/>app_v2.py · multi-tab dashboard]
        E2[FastAPI<br/>api/ · screener router<br/>filters + pagination]
        E3[Reports<br/>PDF tearsheet · CSV picks]
        E4[HuggingFace Hub<br/>Dataset + Models]
    end

    Sources --> Pipeline
    Pipeline --> Storage
    Storage --> ML
    Storage --> Factors
    ML --> Factors
    ML --> Research
    Factors --> Outputs
    Research --> Outputs
    Outputs --> E4
```

## Component Map

| Component | Location | Purpose | Status |
|---|---|---|---|
| US pipeline | `scripts/run_pipeline.py` | Fetch + clean US fundamentals | ✅ |
| Multi-market pipeline | `pipeline/step1_*.py` – `step6_*.py` | 14-market unified pipeline | ✅ |
| KR integration | `pipeline/phase_a_integrate_kr.py` | DART KR data integration | ⚠️ running |
| Feature library | `pipeline/feature_library.py` | 326 feature definitions | ✅ |
| Quarterly enrichment | `scripts/enrich_quarterly_features.py` | 5 intra-year dynamics | ✅ |
| Feature imputation | `scripts/impute_features.py` | Quarterly cols + size_category recovery → 341 cols | ✅ |
| Equity + vol patch | `scripts/patch_equity_vol_features.py` | Fix equity coalesce bug + add 5 vol/roa cols → 346 cols | ✅ (one-time; logic now in step3/step5) |
| Beneish/Altman/Piotroski | `pipeline/step5_compute_features.py` | Fixed DEPI (was 1.0), Altman X4 book-equity fallback for non-US, Piotroski F6 Δ(current_ratio); growth features winsorized at p1/p99 | ✅ |
| Montier C-Score + Richardson accruals | `pipeline/step5_compute_features.py` | Montier C-Score (6-binary, Montier 2008) + `sloan_wc_accruals` + `sloan_lt_accruals` (Richardson 2005). C2 uses `ppe_net` — do not change to `property_plant_equipment` (95.7% null) | ✅ |
| Survivorship correction | `scripts/mark_survivorship.py` | Impute −50% return for likely-delisted | ✅ |
| AAER fraud labels | `scripts/fetch_aaer_labels.py` | 492 positive rows / 118 companies | ✅ |
| Train models | `scripts/train_models.py` | LightGBM 5 horizons (6m/1y/2y/3y/5y), filed-date PIT-safe, PSI filter + ICIR selection, n_estimators=600 | ✅ Phase C |
| Tune models | `scripts/tune_models.py` | Optuna 100 trials + CatBoost ensemble + Platt calibration | ✅ |
| OOF scorer | `scripts/generate_oof_scores.py` | Walk-forward OOF → ml_1y_oof / ml_3y_oof / ml_5y_oof (unbiased, NaN for train rows) | ✅ Phase C |
| Historical ML scoring | `scripts/score_historical.py` | Load models → write ml_1y/3y/5y to parquet | ✅ |
| SPY benchmark data | `scripts/fetch_spy_returns.py` | Downloads SPY annual calendar-year returns → data/spy_returns.csv | ✅ Phase C |
| Horizon router | `alpha/horizon_router.py` | Maps investment horizon (months) to nearest discrete model key (6m/1y/2y/3y/5y) | ✅ Phase C |
| **Alpha factor package** | `alpha/factors/` | 5-factor scores: Value · Quality · Momentum · Growth · Fraud Risk | ✅ |
| Backtester | `scripts/backtester.py` | Walk-forward simulation · SPY benchmark · factor attribution (beta/alpha/R²/tracking_error) · 4 strategies | ✅ Phase C |
| Factor research | `scripts/factor_research.py` | IC/ICIR/decay analysis | ✅ |
| Leverage strategy | `scripts/leverage_strategy.py` | Long/short Kelly sizing | ✅ |
| Monitor drift | `scripts/monitor_drift.py` | PSI + AUC monitoring | ✅ |
| Bias audit | `scripts/bias_audit.py` | 4 audits: look-ahead (PIT) · survivorship · overfitting (overfit_gap) · multiple testing (Bonferroni) | ✅ Phase C |
| Generate reports | `scripts/generate_reports.py` | PDF tearsheet + weekly picks | ✅ |
| DB migration | `scripts/migrate_to_db.py` | Load parquet → TimescaleDB hypertable | Phase C — deferred |
| App | `app_v2.py` | Streamlit dashboard (Phase 2: add 5-factor UI) | ✅ |
| FastAPI | `api/` | REST screener with filters + pagination | ✅ |

## Data Flow Detail

```mermaid
flowchart LR
    A[Raw API calls<br/>SimFin / EDGAR / DART] -->|ticker lists| B[Ticker Registry<br/>per market]
    B -->|company metadata| C[Annual + Quarterly Snapshots<br/>fiscal_year × ticker]
    C -->|OHLCV joins| D[Price-Enriched<br/>Snapshots]
    D -->|macro joins<br/>T-bill · inflation| E[Macro-Enriched<br/>Snapshots]
    E -->|321 formulas| F[Feature Matrix<br/>pre-quarterly enrichment<br/>58K rows · 321 cols]
    F -->|+5 quarterly dynamics| Q[Quarterly-Enriched<br/>historical_dataset_clean.parquet<br/>58K rows · 326 cols]
    Q -->|delisted imputation| SB[Survivorship-Corrected<br/>likely_delisted flag]
    SB -->|quarterly imputation<br/>+ size_category| IMP[Imputed Dataset<br/>58K rows · 341 cols]
    IMP -->|Montier C-Score<br/>+ Sloan accruals| FEAT[Feature-Complete Dataset<br/>58K rows · 360 cols]
    FEAT -->|PSI filter<br/>PSI > 0.25 removed| PSI[PSI-Filtered Candidates<br/>~185 features]
    PSI -->|ICIR filter| G[~45 features/horizon]
    G -->|LightGBM fit<br/>5 horizons: 6m 1y 2y 3y 5y| H[Base Models]
    H -->|Optuna search| I[Tuned Models]
    I -->|CatBoost blend| J[Ensemble]
    J -->|Platt scaling| K[Calibrated Proba 0–1]
    K -->|score_historical.py| L[ml_1y / ml_3y / ml_5y<br/>written back to parquet]
    K -->|generate_oof_scores.py<br/>walk-forward OOF| OOF[ml_1y_oof / ml_3y_oof / ml_5y_oof<br/>NaN for training rows]
    OOF -->|horizon_router.py<br/>months → model key| HR[HorizonRouter<br/>6m · 1y · 2y · 3y · 5y]
    L -->|compute_alpha.py| FA[5-Factor<br/>Composite Alpha Score<br/>341 → 360 cols total]
    SB -->|bulk load| DB[TimescaleDB<br/>hypertable — Phase C — deferred]
```

## Deployment Architecture

```mermaid
graph LR
    A[Developer machine] -->|git push| B[GitHub]
    B -->|Sunday 05:00 UTC| C[refresh_data.yml<br/>GitHub Actions]
    B -->|Monday 07:00 UTC| D[monitor_drift.yml<br/>GitHub Actions]
    C -->|test_dataset_quality.py| QA[Dataset Quality<br/>98 checks must pass]
    QA -->|bias_audit.py --ci<br/>hard fail on look-ahead| AUDIT[Bias Audit<br/>PIT · survivorship]
    AUDIT -->|parquet + status| E[HuggingFace Hub<br/>Dataset repo]
    C -->|models| F[HuggingFace Hub<br/>Model repo]
    D -->|drift report| G[Artifacts + warning]
    E -->|download at startup| H[Streamlit Cloud<br/>app_v2.py · dashboard]
    F -->|download at startup| H
    E -->|download at startup| I[FastAPI service<br/>api/main.py · screener router]
    F -->|download at startup| I
    E -->|migrate_to_db.py| J[TimescaleDB<br/>hypertable — Phase C — deferred]
```
