# System Architecture

The screener is a research-grade pipeline from raw filings to portfolio construction.

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

    subgraph Pipeline["Data Pipeline — scripts/run_pipeline.py"]
        B1[Step 1<br/>Fetch Tickers]
        B2[Step 2<br/>Build Snapshots]
        B3[Step 3<br/>Enrich Prices]
        B4[Step 4<br/>Enrich Macro]
        B5[Step 5<br/>Compute Features<br/>314 columns]
        B6[Step 6<br/>Clean Dataset]
        B7[Quarterly Enrichment<br/>enrich_quarterly_features.py<br/>+5 intra-year columns → 319 total]
        B8[Survivorship Correction<br/>mark_survivorship.py<br/>impute −50% for delisted]
        B1 --> B2 --> B3 --> B4 --> B5 --> B6 --> B7 --> B8
    end

    subgraph ML["ML System — scripts/train_models.py"]
        C0[PSI Filter<br/>drops macro-regime features<br/>PSI > 2.0 removed]
        C1[ICIR Feature Selection<br/>~35 features/horizon]
        C2[LightGBM<br/>3 horizons: 1y 3y 5y]
        C3[Optuna Tuning<br/>100 trials per horizon]
        C4[CatBoost Ensemble]
        C5[Platt Scaling Calibration]
        C6[Composite Score<br/>mean 1y+3y+5y]
        C0 --> C1 --> C2 --> C3 --> C4 --> C5 --> C6
    end

    subgraph Research["Research — scripts/ + research/"]
        D1[Factor Analysis<br/>IC / ICIR / Decay]
        D2[Walk-Forward Backtester<br/>4 strategies]
        D3[Bias Audit<br/>Look-ahead / Survivorship]
        D4[Walk-Forward AUC CV<br/>reports/walk_forward_auc_*.csv]
    end

    subgraph Storage["Storage"]
        S1[Parquet<br/>data/historical_dataset_clean.parquet<br/>155K rows · 319 columns]
        S2[TimescaleDB<br/>hypertable — infra/db/init.sql<br/>⚠️ migration pending]
    end

    subgraph Outputs["Outputs & Serving"]
        E1[Streamlit App<br/>app_v2.py · 10 tabs]
        E2[FastAPI<br/>api/ · screener router<br/>filters + pagination]
        E3[Reports<br/>PDF tearsheet · CSV picks]
        E4[HuggingFace Hub<br/>Dataset + Models]
    end

    Sources --> Pipeline
    Pipeline --> Storage
    Storage --> ML
    Storage --> Research
    ML --> Outputs
    Research --> Outputs
    Outputs --> E4
```

## Component Map

| Component | Location | Purpose |
|---|---|---|
| US pipeline | `scripts/run_pipeline.py` | Fetch + clean US fundamentals |
| EU / multi-market | `pipeline/step1_fetch_tickers.py` – `step6_clean_dataset.py` | 14-market unified pipeline |
| KR integration | `pipeline/phase_a_integrate_kr.py` | DART KR data integration |
| Feature library | `pipeline/feature_library.py` | 319 feature definitions |
| Quarterly enrichment | `scripts/enrich_quarterly_features.py` | 5 intra-year dynamics joined to annual rows |
| Survivorship correction | `scripts/mark_survivorship.py` | Impute −50% return for likely-delisted tickers |
| Train models | `scripts/train_models.py` | LightGBM with PSI filter + ICIR selection |
| Tune models | `scripts/tune_models.py` | Optuna + CatBoost ensemble + Platt calibration |
| Backtester | `scripts/backtester.py` | Walk-forward strategy simulation (4 strategies) |
| Factor research | `scripts/factor_research.py` | IC/ICIR/decay library |
| Leverage strategy | `scripts/leverage_strategy.py` | Long/short Kelly sizing |
| Monitor drift | `scripts/monitor_drift.py` | PSI + AUC monitoring |
| Bias audit | `scripts/bias_audit.py` | Temporal leakage + survivorship audit |
| Generate reports | `scripts/generate_reports.py` | PDF tearsheet + weekly picks |
| DB migration | `scripts/migrate_to_db.py` | Load parquet → TimescaleDB hypertable |
| App | `app_v2.py` | 10-tab Streamlit interface |
| FastAPI | `api/` | REST screener with filters + pagination |

## Data Flow Detail

```mermaid
flowchart LR
    A[Raw API calls<br/>SimFin / EDGAR / DART] -->|ticker lists| B[Ticker Registry<br/>per market]
    B -->|company metadata| C[Annual + Quarterly Snapshots<br/>fiscal_year × ticker]
    C -->|OHLCV joins| D[Price-Enriched<br/>Snapshots]
    D -->|macro joins<br/>T-bill · inflation| E[Macro-Enriched<br/>Snapshots]
    E -->|314 formulas| F[Feature Matrix<br/>historical_dataset_clean.parquet<br/>155K rows · 314 cols]
    F -->|+5 quarterly dynamics| Q[Quarterly-Enriched<br/>319 columns]
    Q -->|delisted imputation| SB[Survivorship-Corrected<br/>likely_delisted flag]
    SB -->|PSI filter| PSI[PSI-Filtered Candidates<br/>~185 features]
    PSI -->|ICIR filter| G[~35 features/horizon]
    G -->|LightGBM fit| H[Base Models]
    H -->|Optuna search| I[Tuned Models]
    I -->|CatBoost blend| J[Ensemble]
    J -->|Platt scaling| K[Calibrated Proba 0–1]
    K -->|mean of 1y+3y+5y| L[Composite Score]
    SB -->|bulk load| DB[TimescaleDB<br/>hypertable ⚠️ pending]
```

## Deployment Architecture

```mermaid
graph LR
    A[Developer machine] -->|git push| B[GitHub]
    B -->|Sunday 05:00 UTC| C[refresh_data.yml<br/>GitHub Actions]
    B -->|Monday 07:00 UTC| D[monitor_drift.yml<br/>GitHub Actions]
    C -->|parquet + status| E[HuggingFace Hub<br/>Dataset repo]
    C -->|models| F[HuggingFace Hub<br/>Model repo]
    D -->|drift report| G[Artifacts + warning]
    E -->|download at startup| H[Streamlit Cloud<br/>app_v2.py · 10 tabs]
    F -->|download at startup| H
    E -->|download at startup| I[FastAPI service<br/>api/main.py · screener router]
    F -->|download at startup| I
    E -->|migrate_to_db.py| J[TimescaleDB<br/>hypertable ⚠️ pending]
```
