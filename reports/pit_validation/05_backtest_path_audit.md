# Session 5 — Backtest Prediction and Monthly-NAV Path Audit

Date: 2026-07-15

Scope: read-only audit of T11 and T31; no model, data, prediction, or backtest execution

## Executive conclusion

The saved `data/backtest_results.json` cannot be treated as a proven
walk-forward result. It reports one `ml_gates` run for fiscal years 2013–2023
(161 selected holdings), but it persists neither picks nor score values, score
source, model/fold identity, training cutoff, feature lineage, or fallback
choice. The exact 161 row identities and their walk-forward-versus-static score
lineage are therefore unprovable from the saved artifact.

The current code and frozen data are also incompatible at this boundary:

- the frozen clean parquet has 58,190 annual rows but none of the required
  horizon-qualified label date/provenance columns, so
  `training_label_eligible()` rejects every row and the current
  `load_and_score()` would produce zero walk-forward classifier, tree, and
  regression predictions;
- the parquet nevertheless contains non-null final/static `ml_1y`, `ml_3y`,
  `ml_5y`, and `ml_pred_excess_3y` values on all 58,190 rows, without a
  compatible lineage sidecar;
- only `models/model_3y.joblib` is locally present among the classifier files,
  and its metadata says `train_cutoff=2023`; the current scorer would overwrite
  `ml_3y` for every historical year with that final model while retaining the
  parquet's unproven `ml_1y` and `ml_5y` columns;
- `tree_prob` and `reg_3y_wf` would be all missing. Because the tree column
  exists, the `ml_gates` tree filter fills it with zero and rejects every row.

T11 is a latent source-code defect but was not active in the frozen result:
`historical_dataset_clean.parquet` has no `likely_delisted` column, so every
conditional historical gate skipped it. A read-only application of the Step 6
definition to the frozen annual panel would flag 4,535 rows through FY2023,
including 296 rows that survive the common decision filters and Beneish gate.
That counterfactual is not a corporate-action truth and is not used to infer
returns here.

The metric path is not one monthly NAV. When a monthly cache is supplied, only
maximum drawdown and drawdown duration use the reconstructed monthly curve.
CAGR, volatility, Sharpe, Sortino, bootstrap statistics, rolling Sharpe, and the
Calmar numerator remain annual-return calculations. Calmar mixes annual CAGR
and annual volatility with monthly drawdown (unless the 2%/2-sigma proxy takes
over). Monthly returns are neither reconciled nor rescaled to the annual net
return.

All three Session 5 stop conditions are met: saved score lineage is unprovable,
the intended missing-score policy is unresolved, and activating/removing the
future-derived delisting gate cannot be separated from an accepted treatment of
disappearing securities in monthly returns. This report therefore specifies
tests and decisions only; it does not propose or implement a fix.

## Frozen evidence

| Artifact | Evidence |
|---|---|
| `data/historical_dataset_clean.parquet` | SHA-256 `520a9b52e2a63d013a3527abbcde32c484a226c2739450d2a6a48ab175144dae`; 58,190 annual rows, FY2008–FY2027 |
| `data/monthly_prices.parquet` | SHA-256 `9c7ad56e835d50f3cd121d55341d9b6d0ffc07f5aaee2b30708a50031266461e`; 95,863 rows, 448 tickers, 2007-01 through 2026-06 |
| `data/backtest_results.json` | SHA-256 `4806317b6329a32c42b679b7eb87091378aff27d431a207988b0e41145afd683`; generated 2026-06-29; one `ml_gates` result |
| `models/model_3y.joblib` | SHA-256 `f527d7dd616727b9488bb43b8a2c42efafd42b3afca6eeaf2a6f827f9ad02c7c`; final classifier present |
| `models/model_meta.json` | SHA-256 `3f7815a77e4f4da8622c86d938bc884087982ef8273520b94513a1ed10ce31b2`; 3y metadata says `train_cutoff=2023` but does not identify the parquet score columns or saved backtest |

The frozen parquet has static scores on every row. Its OOF columns have 51,475
non-null rows, exactly FY2014–FY2025; FY2008–FY2013 and FY2026–FY2027 are null.
No OOF lineage file exists beside the parquet. The only active
`models/feature_sets_3y.json` is not a row-level prediction manifest.

## Prediction flow

### Current backtest scorer

`backtest/engine.py::load_and_score()` creates four independent walk-forward
families:

1. `ml_{1y,3y,5y}_wf`: one LightGBM classifier per scoring year, trained on
   prior fiscal years with an eligible relative label and fold-selected features;
2. `tree_prob`: one depth-4 decision tree per year using the same eligible
   three-year relative-label population and independently selected fold features;
3. `reg_3y_wf`: one LightGBM regressor per year, using eligible observed
   three-year stock returns, a clean-company training filter, and a fixed target
   clip of `[-1, 5]`;
4. `ml_{1y,3y,5y}`: final/static classifiers, scored over all fiscal years with
   medians calculated from every row satisfying `fiscal_year <= score_year`.

The walk-forward loops require five distinct eligible training years and at
least five selected features; regression additionally requires 100 clean
training rows. None writes a model ID, training maximum date, feature-set hash,
median hash, or score-source column into the scored dataframe.

On the frozen parquet the required `label_end_date_*`,
`stock_label_end_date_*`, and provenance columns are absent. The explicit
fail-closed eligibility helper therefore returns false for every training row.
Without running a model, the code path is provable:

| Output | Frozen-row result under current code |
|---|---:|
| `ml_1y_wf` | 0 / 58,190 non-null |
| `ml_3y_wf` | 0 / 58,190 non-null |
| `ml_5y_wf` | 0 / 58,190 non-null |
| `tree_prob` | 0 / 58,190 non-null |
| `reg_3y_wf` | 0 / 58,190 non-null |
| final/static `ml_3y` | 58,190 / 58,190 after scoring with the 2023-cutoff model |
| retained parquet `ml_1y`, `ml_5y` | 58,190 / 58,190, lineage unproven |

For backtest years through FY2023 this exposes all 44,623 annual rows to final
scores. Of those, 43,384 rows have at least one missing value across the 28
three-year static features and therefore exercise the expanding-median path for
at least one feature (411,381 missing feature cells in total). The median pool
includes all same-fiscal-year rows, including rows filed after the later
portfolio filing cutoff; it is not a decision-time population.

### Score-column chooser and missing-score behavior

`_ml(slice, horizon)` makes a slice-wide choice:

- use `ml_{h}_wf` when more than five rows in the post-gate slice are non-null;
- otherwise use `ml_{h}` when it exists;
- if the static column is absent, return the walk-forward column even if it is
  entirely missing.

This is not row-level fail-closed behavior. It has four distinct outcomes:

- a year/slice with 0–5 walk-forward scores silently activates the static/final
  column for every row;
- a year/slice with more than five walk-forward scores uses that column, but
  rows missing an individual score remain in blended strategies and receive a
  zero contribution while the score weight stays in the denominator;
- `nlargest(top_n, score)` can include missing-score rows at the end when fewer
  than `top_n` non-null scores exist;
- if neither usable walk-forward nor static values exist, blended strategies can
  still select on their non-ML components, whereas `ml_gates` returns empty only
  after its regression and classification fallbacks are exhausted.

The tests intentionally fixture the static fallback for `ml_gates` when fewer
than six rows survive, but no test establishes that this is the intended
historical missing-score policy. The only policy test verifies that explicit
policy-imputed label runs drop legacy static columns; it does not require all
selected rows to have an OOS score.

### Strategy-by-strategy consumption

The five CLI strategies all receive the dataframe after `load_and_score()`:

| Strategy | Model prediction path | `likely_delisted` path |
|---|---|---|
| `composite` | Blends 1y (30%) and 3y (15%) columns chosen by `_ml` with value, quality, and Piotroski ranks | Common hard gate, missing flag fails closed if the column exists |
| `ml_gates` | Tree agreement gate; rank first by `reg_3y_wf` if more than five survive with scores, else by `_ml(..., '3y')` classification | Same common hard gate |
| `qem` | Blends `_ml(..., '1y')` at 25%; can continue without ML | No delisting gate |
| `scdv` | Blends `_ml(..., '3y')` at 25%; can continue without ML | No delisting gate |
| `iarb` | Blends `_ml(..., '3y')` at 25%; can continue without ML | No delisting gate |

The exact `ml_gates` order is: optional market filter; Beneish `< -1.78`;
`likely_delisted == 0`; Piotroski `>= 3`; positive ROA; sector P/S percentile
`<= 0.70`; `tree_prob >= 0.55`; Altman Z `> 1.0`; 12-month momentum `> -0.40`;
then three-year walk-forward regression ranking, falling back to the selected
three-year classifier column. Missing values generally fail their gate, except
P/S and momentum use neutral zero/0.5 fills as coded.

The saved result covers FY2013–FY2023 and reports 161 holdings (15 in every year
except 11 in FY2021), with zero missing annual returns. Those 161 identities are
not serialized. The saved artifact predates the current strict label schema and
does not identify its code revision; consequently none of its eleven years can
be certified as walk-forward or static, and every reported year is affected by
the lineage stop condition.

### Does `alpha_composite` consume model predictions?

Yes, indirectly. `alpha/factors/composite.py` gives `alpha_fraud_risk` a 20%
default weight. `alpha/factors/fraud_risk.py` averages all available fraud,
distress, and `ml_{1y,3y,5y}_oof` event-time ranks. The ML effective weight is
row-dependent because the mean ignores unavailable signals. Thus the factor
called fraud risk uses return-model predictions as a proxy for quality, and the
five-factor composite consumes them.

The engine strategy named `composite` does **not** consume `alpha_composite`; it
uses `value_composite`, `quality_composite`, and direct ML columns. By contrast,
the screener registry consumes `alpha_composite` and often adds OOF columns
again, creating direct plus indirect ML exposure:

| Registry strategy | Direct ML | Indirect through `alpha_composite`/fraud risk | Delisting gate |
|---|---|---|---|
| `COMPOSITE_US` | OOF 1y and 3y | yes | `_quality_gate` |
| `COMPOSITE_INTL` | OOF 3y | yes | `_quality_gate` |
| `COMPOSITE_MICRO` | OOF 3y | no | `_quality_gate` |
| `VALUE_QUALITY` | none | no | `_quality_gate` |
| `MOMENTUM_GROWTH` | OOF 1y | no | none |
| `FRAUD_AVOID` | OOF 1y | yes, through both `alpha_fraud_risk` and `alpha_composite` | none |
| `WIDE_UNIVERSE` | OOF 1y and 3y | yes | explicit gate |

The registry `_ml` chooses OOF whenever the column exists globally, not when it
is populated in a year. On the frozen parquet FY2008–FY2013 therefore omit the
ML component rather than fall back to static, while FY2014–FY2023 use unproven
persisted OOF values. `run_registry()` does not call `load_and_score()`.

`pipeline/build_monthly_price_cache.py` is another historical consumer. Its
ticker-collection dry run applies final/static scores with the same expanding
same-year medians, but only for `composite`, `qem`, and `scdv`; it does not
collect `ml_gates` or `iarb`, does not create walk-forward/tree/regression
scores, and omits the engine's January 1 filing-date gate. The resulting monthly
cache is therefore selected through a different prediction and eligibility
path than the saved official `ml_gates` run.

`portfolio/leverage_strategy.py` also gates on `likely_delisted` and uses final
classification/regression scores, but it is a current-snapshot screener rather
than the audited historical engine. `portfolio/build_portfolio.py` has a
separate annual backtest and no `likely_delisted` filter.

`portfolio/build_alpha_registry.py` additionally backtests five individual
alpha factors and three OOF signals through the engine metrics. Its generic
top-score filter has no delisting gate; the `alpha_fraud_risk` signal is itself
an OOF consumer under current code. The saved `data/alpha_registry.json` is an
older, unversioned artifact: it says fraud risk used static `ml_1y`, `ml_3y`,
and `ml_5y`, while current source uses OOF columns. It selects alpha value,
quality, fraud risk, and all three OOF signals. `portfolio/build_portfolio.py`
then blends those selected signal IDs in its own annual-only backtest, drops
missing realized-return rows before ranking, and calculates annual CAGR,
Sharpe, and drawdown without the engine's monthly path. Neither artifact records
prediction lineage sufficient to resolve that source/artifact mismatch.

## T11: every historical `likely_delisted` use

Step 6 defines the flag from the complete annual panel:

1. take the panel's maximum fiscal year;
2. take each ticker's last fiscal year;
3. mark the ticker when its last year is at most `max_year - 3`;
4. copy that eventual status onto every row for the ticker.

There is no historical decision date in the flag. Future filings, adding a new
maximum year, ticker migration, acquisition, bankruptcy, or a source-coverage
gap can change all prior rows. The policy-label availability dates added in
Session 2B constrain training labels only; no gate checks those dates.

Historical gate/filter call sites are:

- `backtest/engine.py::filter_composite`, covering CLI `composite` and
  `ml_gates`;
- `portfolio/build_screener_registry.py::_quality_gate`, covering
  `COMPOSITE_US`, `COMPOSITE_INTL`, `COMPOSITE_MICRO`, and `VALUE_QUALITY`;
- `portfolio/build_screener_registry.py::filter_wide_universe`;
- the static dry-run `composite` path in
  `pipeline/build_monthly_price_cache.py` through the imported engine filter;
- `portfolio/leverage_strategy.py::quality_gate` for the current screener.

It is not used by engine `qem`, `scdv`, or `iarb`; registry
`MOMENTUM_GROWTH` or `FRAUD_AVOID`; or `portfolio/build_portfolio.py`.

The frozen clean parquet has no `likely_delisted` column, so all conditional
gates above were inactive for `data/backtest_results.json`. To quantify the
latent source behavior without writing data, the Step 6 predicate was applied
in memory to the frozen clean annual panel. Its maximum fiscal year is 2027, so
it labels every ticker whose last filing year is 2024 or earlier. This makes the
result depend even on the panel's FY2026–FY2027 rows.

| FY | Annual rows | Derived flags | After filing/lag/cap filters | Derived flags there | After Beneish | Flags removed by common gate |
|---:|---:|---:|---:|---:|---:|---:|
| 2008 | 1 | 0 | 0 | 0 | 0 | 0 |
| 2009 | 257 | 14 | 10 | 0 | 10 | 0 |
| 2010 | 689 | 30 | 36 | 0 | 36 | 0 |
| 2011 | 1,720 | 144 | 106 | 1 | 103 | 1 |
| 2012 | 1,937 | 169 | 158 | 11 | 144 | 7 |
| 2013 | 1,990 | 166 | 167 | 8 | 144 | 6 |
| 2014 | 2,091 | 186 | 197 | 15 | 178 | 13 |
| 2015 | 2,406 | 207 | 247 | 14 | 227 | 11 |
| 2016 | 2,506 | 205 | 246 | 14 | 222 | 12 |
| 2017 | 2,637 | 237 | 259 | 11 | 239 | 10 |
| 2018 | 3,026 | 281 | 295 | 15 | 277 | 13 |
| 2019 | 3,283 | 314 | 313 | 15 | 267 | 14 |
| 2020 | 3,470 | 344 | 326 | 19 | 288 | 15 |
| 2021 | 5,535 | 712 | 618 | 92 | 572 | 85 |
| 2022 | 6,392 | 760 | 1,433 | 71 | 1,337 | 60 |
| 2023 | 6,683 | 766 | 1,457 | 62 | 1,313 | 49 |
| **Total** | **44,623** | **4,535** | **5,868** | **348** | **5,357** | **296** |

The exact counterfactual row set is the 4,535 distinct `(ticker, fiscal_year)`
keys through FY2023 whose ticker's full-panel maximum year is at most 2024; the
296 gate-affected keys additionally pass the engine's current lag, January 1,
$50M-cap, and Beneish predicates. The report does not call those tickers
delisted or assign them a return.

## Annual return and monthly NAV path

### Annual portfolio return

For each selected fiscal-year slice the engine:

- uses `forward_return_1y` as the holding-period return;
- by default replaces a missing selected return with `-50%` (`impute` mode), or
  drops it under `drop`/`flag_only`;
- subtracts a per-security one-time slippage/transaction-cost estimate;
- computes inverse-volatility weights when possible, then position and sector
  caps; otherwise it equal-weights;
- stores the weighted net annual return as `port_ret`.

A year is omitted when fewer than three valid/imputed picks remain. A literal
`-100%` annual security return is preserved before costs, so subtracting costs
can make the per-security net value less than `-100%`. There is no lower bound
at total loss. Missing-return imputation is a policy sensitivity, not an
observed delisting return.

### Monthly reconstruction

`compute_monthly_nav()` does not reconstruct the same net annual portfolio:

- it uses adjusted-close percentage changes for whatever cached ticker-months
  exist in calendar year `fiscal_year + 1`;
- the first observed month for each ticker is zero after the missing return is
  filled;
- every absent ticker-month is filled with zero;
- a ticker with no cached rows becomes an all-zero return column when at least
  one other pick has data;
- a security that disappears is treated as earning zero after its last cached
  observation, not the annual `-50%` imputation or an observed corporate-action
  return;
- a complete year with no cached subframe falls back to one annual `port_ret`
  step, while a partially covered year does not;
- annual trading costs are not applied to the monthly returns;
- weights are held fixed and renormalized over the original ticker list; there
  is no within-year rebalancing or cash/corporate-action ledger;
- a cached adjusted-close fall to zero would produce a `-100%` monthly security
  return, but the current cache contains no zero or null adjusted closes.

The function does not return the NAV or monthly returns, only maximum drawdown
and duration. It performs no assertion that the product of its monthly returns
equals `1 + port_ret`, and does not rescale the monthly segment to force that
identity. Annual and monthly returns therefore do not reconcile by construction.
The saved result cannot be empirically reconciled because it does not persist
the 161 picks/weights or monthly NAV.

### Metric frequency and NAV consistency

| Metric | Actual input |
|---|---|
| CAGR | Annual `port_ret` wealth |
| Sharpe | Annual CAGR minus 3% divided by sample standard deviation of annual `port_ret` |
| Sortino | Annual CAGR and annual downside deviation |
| Max drawdown/duration | Monthly reconstructed curve when any monthly cache object is supplied; otherwise annual wealth |
| Calmar | Annual CAGR divided by monthly max drawdown when its magnitude is at least 2%; otherwise annual `2 * volatility`/1% proxy |
| Rolling Sharpe, bootstrap Sharpe/CAGR | Annual returns |

Thus CAGR, Sharpe, drawdown, and Calmar do not use the same monthly NAV. Even
within Calmar, numerator and denominator can come from different, unreconciled
return streams.

The saved annual returns do internally reproduce its reported CAGR: their
rounded values compound to 20.4006x over eleven observations, or 31.5402% CAGR,
matching 31.54% after rounding. This proves only annual arithmetic, not monthly
reconciliation or prediction validity.

## Required future-invariance and boundary tests for Session 6

These tests are specifications only.

1. **Historical gate invariance:** build a multi-ticker panel through decision
   date D, record historical eligibility, append later filings (including a new
   panel maximum year and a filing for an old ticker), and assert every
   eligibility decision at or before D is unchanged. Exercise engine common
   gates, every registry gate listed above, monthly-cache collection, missing
   flags, and ticker migrations separately.
2. **Final-model invariance:** score a historical fold with an explicit
   fold/model manifest, then add, replace, or corrupt every final/static model
   and metadata file. Historical OOS scores and picks must be byte-identical.
   Removing the fold model or manifest must fail closed, not fall back.
3. **Same-year later-filing invariance:** score an early filer with missing
   features, append later same-year filers with extreme values, and assert its
   imputed values and prediction do not change. Repeat across market and
   post-gate slices.
4. **Missing walk-forward score:** create more than five valid fold scores plus
   one missing row, and separately create 0–5 valid scores with a populated
   static column. Under the accepted policy, assert the missing row cannot be
   selected and no static fallback activates silently. Cover blended,
   `ml_gates` regression/classification, `qem`, `scdv`, and `iarb`, including
   `top_n` greater than the non-null score count.
5. **Composite lineage:** hold fundamental factors fixed, perturb each OOF
   column, and prove the expected `alpha_fraud_risk` and `alpha_composite`
   dependency. Missing or unproven OOF lineage must follow the accepted
   fail-closed rule. Registry direct and indirect ML contributions must be
   separately observable.
6. **Monthly missingness:** cover no cached year, one missing ticker, first
   missing month, internal missing month, disappearance mid-year, and a
   `-100%` price event. Assert the accepted cash/corporate-action return policy
   explicitly; never silently fill disappearance with zero.
7. **Annual/monthly reconciliation:** persist or return monthly returns and NAV;
   for each year assert monthly compounding after costs equals the authoritative
   annual portfolio return within a declared tolerance. Partial coverage must
   fail closed or follow an explicitly tested fallback.
8. **Single-NAV metrics:** from a synthetic monthly return vector assert exact
   CAGR, annualized Sharpe, drawdown, duration, and Calmar against one canonical
   NAV. A perturbation to any month must flow consistently to every dependent
   metric.
9. **Total-loss bound:** assert portfolio and security wealth cannot pass below
   zero after costs, and test the accepted accounting for transaction cost at a
   total-loss boundary.

## Decisions required before correction

1. **Score lineage and missing scores:** choose the required row-level manifest
   (model/fold ID, training cutoff, label policy, feature/median lineage) and an
   explicit fail-closed selection rule. The saved FY2013–FY2023 result must
   remain unverified unless its 161 rows can be reconstructed from frozen,
   lineage-compatible artifacts; it cannot be relabeled walk-forward from the
   aggregate JSON.
2. **Corporate actions/disappearance:** decide how acquisitions, bankruptcies,
   ticker changes, migrations, and source-coverage losses map to observed
   holding-period returns. Removing or activating the future-derived hard gate
   before this decision would implicitly choose which disappearing securities
   enter the monthly return path.
3. **Canonical return stream:** decide whether authoritative performance is
   monthly total-return NAV or annual labels, how partial cache coverage and
   costs reconcile, and the frequency conventions for CAGR, Sharpe, drawdown,
   and Calmar.

No rebalance calendar, corporate-action outcome, correction, performance
comparison, or Session 6 work is selected here.

## Verification boundary

The audit used bounded source reads, parquet/JSON schema and count summaries,
artifact hashes, and arithmetic over the already-saved annual-return values. It
did not call `load_and_score()`, fit a model, run a strategy, invoke
`run_backtest()`, rebuild a dataset/cache, retrain, or write a generated
artifact. No production test was run because production behavior did not
change.
