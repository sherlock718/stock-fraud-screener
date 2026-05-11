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
        B5[Step 5<br/>Compute Features]
        B6[Step 6<br/>Clean Dataset]
        B1 --> B2 --> B3 --> B4 --> B5 --> B6
    end

    subgraph ML["ML System — scripts/train_models.py / tune_models.py"]
        C1[ICIR Feature<br/>Selection ~35 features]
        C2[LightGBM<br/>3 horizons: 1y 3y 5y]
        C3[Optuna Tuning<br/>100 trials per horizon]
        C4[CatBoost<br/>Ensemble]
        C5[Platt Scaling<br/>Calibration]
        C6[Composite Score<br/>mean 1y+3y+5y]
        C1 --> C2 --> C3 --> C4 --> C5 --> C6
    end

    subgraph Research["Research — research/"]
        D1[Factor Analysis<br/>IC / ICIR / Decay]
        D2[Walk-Forward<br/>Backtester]
        D3[Bias Audit<br/>Look-ahead / Survivorship]
    end

    subgraph Outputs["Outputs"]
        E1[Streamlit App<br/>app_v2.py · 10 tabs]
        E2[Reports<br/>PDF tearsheet · CSV picks]
        E3[Portfolio<br/>4 strategies]
        E4[HuggingFace Hub<br/>Dataset + Models]
    end

    Sources --> Pipeline
    Pipeline --> ML
    Pipeline --> Research
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
| Feature library | `pipeline/feature_library.py` | 313 feature definitions |
| Train models | `scripts/train_models.py` | LightGBM with ICIR selection |
| Tune models | `scripts/tune_models.py` | Optuna + calibration |
| Backtester | `scripts/backtester.py` | Walk-forward strategy simulation |
| Factor research | `scripts/factor_research.py` | IC/ICIR/decay library |
| Leverage strategy | `scripts/leverage_strategy.py` | Long/short Kelly sizing |
| Monitor drift | `scripts/monitor_drift.py` | PSI + AUC monitoring |
| Generate reports | `scripts/generate_reports.py` | PDF tearsheet + weekly picks |
| App | `app_v2.py` | 8-tab Streamlit interface |

## Data Flow Detail

```mermaid
flowchart LR
    A[Raw API calls<br/>SimFin / EDGAR / DART] -->|ticker lists| B[Ticker Registry<br/>per market]
    B -->|company metadata| C[Annual Snapshots<br/>fiscal_year × ticker]
    C -->|OHLCV joins| D[Price-Enriched<br/>Snapshots]
    D -->|macro joins<br/>T-bill · inflation| E[Macro-Enriched<br/>Snapshots]
    E -->|313 formulas| F[Feature Matrix<br/>historical_dataset_clean.parquet]
    F -->|ICIR filter| G[~35 features/horizon]
    G -->|LightGBM fit| H[Base Models]
    H -->|Optuna search| I[Tuned Models]
    I -->|CatBoost blend| J[Ensemble]
    J -->|Platt scaling| K[Calibrated Proba 0–1]
    K -->|mean of 1y+3y+5y| L[Composite Score]
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
    E -->|download at startup| H[Streamlit Cloud<br/>app_v2.py]
    F -->|download at startup| H
```
