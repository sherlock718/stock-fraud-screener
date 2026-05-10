# Bias & Validation

The screener includes an explicit audit framework for the three most dangerous model biases in financial ML.

## The Three Biases

### 1. Look-Ahead Bias

**What it is:** Using information that wasn't available at the time a trading decision would be made.

**Risk in this codebase:** Features computed using fiscal year data but applied to a year-end date when the filing wasn't yet published.

**Current mitigation:**
- All features use `fiscal_year` as the cutoff — only data from fiscal year Y is used to score year Y
- Model is trained on data with `fiscal_year ≤ train_cutoff` and tested on `fiscal_year > train_cutoff`

**Known residual risk:**
The pipeline uses `fiscal_year` (integer) not actual `filing_date`. Companies with late 10-K filings (e.g., June 2024 for FY2023) would not have their data available until June. If the backtest assumes the data is available January 2024, there is a 6-month look-ahead error.

**Roadmap fix (Phase 0a):** Store `filing_date`, `effective_date`, `source_timestamp` for every feature snapshot. Use filing date as the knowledge cutoff in all backtests.

### 2. Survivorship Bias

**What it is:** Training only on companies that survived (are still listed). Bankrupt and delisted companies are excluded, making the model overly optimistic.

**Risk in this codebase:** The current universe is built from active listings. Companies that went bankrupt or were delisted in 2010–2020 are not in the training set.

**Current mitigation:**
- The bias audit notebook (`04_bias_audit.ipynb`) quantifies the magnitude
- Known to exist; magnitude estimated at ~2–4 AUC points inflation

**Roadmap fix (Phase 0b):** Collect delisted companies from CRSP delisting codes (US) and SEC EDGAR Form 15 filings. Track delisting reason (bankruptcy, merger, fraud, voluntary). Include dead companies in training set.

### 3. Feature Leakage

**What it is:** A feature that accidentally encodes information about the future (e.g., using fiscal year-end market cap which incorporates price movements that happened after the prediction date).

**Audit process:**
The `scripts/bias_audit.py` script checks:

1. **Temporal leakage test** — fit model on features at year T, test on year T (same year). If AUC > 0.90, there may be leakage.
2. **Shuffle test** — shuffle the target labels and refit. AUC should drop to ~0.50.
3. **Feature correlation with future returns** — compute Pearson correlation between each feature at year T and the price return over year T (same year). Features with |r| > 0.15 are flagged.
4. **Permutation importance stability** — if a feature's importance collapses when tested OOS, it may have been leaking in-sample.

## Running the Bias Audit

```bash
python3 scripts/bias_audit.py

# Output: reports/bias_audit_report.json
# Also prints a summary table to stdout
```

The audit notebook (`research/04_bias_audit.ipynb`) has more visual analysis.

## Walk-Forward as the Primary Defense

The strongest defense against all three biases is the walk-forward validation structure itself:

- The model **never** trains on data from the test period
- Each OOS year is tested on a model that was trained **before** that year existed
- If the OOS AUC is materially lower than the in-sample AUC, it reveals overfitting even if leakage isn't detected directly

**Current OOS gap:**

| Horizon | In-sample AUC | Val AUC | Test AUC | OOS penalty |
|---|---|---|---|---|
| 1-year | ~0.85 | 0.776 | 0.749 | ~10 pts |
| 3-year | ~0.87 | 0.795 | 0.780 | ~8 pts |
| 5-year | ~0.91 | 0.860 | 0.856 | ~5 pts |

The 5-year horizon has the smallest OOS penalty — suggesting the longer-horizon structural signals are more robust and less likely to be overfitted noise.

## Cross-Validation Note

Standard k-fold cross-validation is **not used** in this project. It would introduce temporal leakage (training on future data to predict the past). Only walk-forward (expanding window) splits are used.
