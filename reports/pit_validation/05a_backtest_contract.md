# Session 5A — Backtest Lineage, Missingness, and Return Contract

Date: 2026-07-15

Status: **Accepted by user — 2026-07-15**

Scope: policy and interface contract only. No production code, model, data,
prediction, price, calendar, or backtest change is authorized by this document.

## Purpose and acceptance boundary

Session 5 proved that the saved FY2013–FY2023 result has no row-level prediction
lineage, missing-score fallback is ambiguous, the future-derived
`likely_delisted` flag cannot supply a corporate-action outcome, and official
metrics do not share one reconciled monthly NAV.

The user accepted all four contracts in this document on 2026-07-15, including
the amended personal small-cap corporate-action sensitivities and the explicit
point-in-time listing/tradability distinction. Acceptance does not validate or
reproduce `data/backtest_results.json`.
That file remains `LEGACY_SAVED` reference-only evidence. A later clean rerun on
frozen old data is `OLD_RECONSTRUCTED` and is a different analysis.

The recommended contract is deliberately conservative: where lineage or market
evidence is absent, the affected row or portfolio period is unavailable for an
official claim. It is never silently repaired with a static model, zero return,
row deletion, or inferred corporate-action outcome.

## Contract 1 — Row-level historical prediction lineage

### 1.1 Historical score eligibility

A model-derived score may affect a historical gate, rank, factor, weight, or
selection only when all of the following are true:

1. it is produced strictly out of sample for that row's accepted decision
   timestamp;
2. the model's complete training population and every fitted preprocessing or
   feature-selection input are available strictly before that timestamp under
   the accepted label-availability rule;
3. its dataset, label policy, feature selection, preprocessing, model artifact,
   and fold identities are hash-addressed and compatible; and
4. the prediction row carries the complete manifest below.

Final/static models are permitted for a current/live prediction whose decision
timestamp is after the model's proven training cutoff. They are never permitted
as a historical backtest fallback or as a replacement for a missing OOS score.

### 1.2 Canonical prediction-row key

One row is uniquely identified by:

```text
(entity_id, market, fiscal_year, period_type,
 availability_timestamp, decision_timestamp,
 horizon, model_role, label_policy)
```

`entity_id`, not ticker, is the security/issuer identity. Ticker is retained as
a display and market-data mapping field. Duplicate keys fail closed. A security
mapping table may connect issuer/security/ticker histories, but it must be dated
and versioned; an undated ticker substitution is forbidden.

### 1.3 Required prediction manifest fields

Each prediction row must persist, directly or through immutable referenced
manifests:

| Category | Required fields |
|---|---|
| Row identity | entity ID, security ID when available, ticker, market, fiscal year, period type, availability timestamp/provenance |
| Decision | horizon, decision timestamp, scoring calendar/config ID, eligible fiscal cohort |
| Model role | classifier ranker, regression ranker, tree agreement gate, OOF factor input, or explicitly named rule-only role |
| Score | raw prediction, transformed score/rank, score units, non-null status |
| Fold | fold ID, training start, training cutoff, maximum eligible publication timestamp, maximum eligible label date |
| Population | dataset ID/hash, training-population hash/count, label policy, target and horizon |
| Features | selector config/hash, ordered feature-list hash, feature-materialization contract/version |
| Preprocessing | median/scaler/sector/target-bound artifact IDs and hashes |
| Model | model family/config, artifact ID/hash, random seed, code commit and dirty-state hash |
| Outcome | selection eligibility, exclusion code, realized target, target dates/provenance when later attached |

The prediction table must never infer missing lineage from filenames, current
metadata, fiscal year, or a model that merely has compatible feature names.

### 1.4 Allowed historical score sources

Allowed sources are explicitly named `walk_forward_oos` or `oof_oos` and must
meet this contract. `static`, `final`, `legacy_persisted`, `same_year_fallback`,
and `unknown` are ineligible for historical performance.

The following minimum exclusion codes are required:

```text
missing_prediction
missing_prediction_manifest
incompatible_lineage
non_oos_score_source
missing_model_artifact
missing_feature_or_preprocessing_artifact
label_policy_mismatch
decision_timestamp_mismatch
duplicate_prediction_key
unproven_security_identity
```

### 1.5 Legacy result

The aggregate saved result cannot satisfy this manifest because its 161 holding
identities, score sources, folds, and weights were not persisted. It remains
`LEGACY_SAVED`, may be quoted only with that limitation, and cannot be relabeled
walk-forward. Session 7 freezes the evidence that actually exists.

## Contract 2 — Fail-closed missing-score behavior

### 2.1 No silent fallback or weight renormalization

For any strategy that declares an ML prediction role:

- every selected row must have a valid lineage-compatible OOS prediction for
  every required ML gate, ranker, factor input, and weighting input;
- missing OOS scores exclude the row with a recorded reason;
- a static/final/persisted score cannot activate because OOS coverage is low;
- a missing ML factor cannot contribute zero, disappear from a denominator, or
  cause remaining factor weights to be silently renormalized; and
- `nlargest`/sorting must operate only after required-score validation, so a null
  score can never fill the bottom of a requested portfolio.

If an ML input is intended to be optional, that creates a separately named,
versioned rule-only or reduced-signal strategy. It is not a fallback execution
of the original strategy.

### 2.2 Strategy roles

Recommended official roles are:

| Path | Required historical OOS inputs |
|---|---|
| Production `ml_gates` | tree agreement probability and 3y regression ranker |
| Legacy engine `composite` | OOS 1y and 3y classifiers if this strategy is retained as ML composite |
| `qem` | OOS 1y classifier if retained with its current ML weight |
| `scdv` | OOS 3y classifier if retained with its current ML weight |
| `iarb` | OOS 3y classifier if retained with its current ML weight |
| `alpha_fraud_risk` / `alpha_composite` | every OOF family declared by the frozen factor configuration |
| Screener/alpha registries | all direct OOF inputs plus all indirect OOF inputs through selected factors |

The current `ml_gates` classification fallback is not part of the recommended
official production contract. If desired later, a separately named
`ml_gates_classifier_ranked` strategy must be specified and validated rather
than activated when regression coverage fails.

### 2.3 Portfolio coverage

For official comparisons, the recommended primary rule is:

- `target_n` is a frozen strategy parameter;
- all `target_n` holdings must pass every required gate and have every required
  OOS score;
- if fewer than `target_n` eligible scored rows exist, no official portfolio is
  formed for that decision period;
- the missing period is not dropped from a continuous performance series and is
  not replaced with cash or a smaller concentrated portfolio; the official NAV
  for that strategy/horizon is unavailable across that gap.

A smaller-portfolio policy materially changes concentration and must be a
separately named sensitivity with a predeclared `min_n`; it cannot be inferred
from available row count. Under this recommendation the saved FY2021 top-15
period with 11 reported holdings would not qualify for an official reconstructed
top-15 result.

### 2.4 Required coverage reporting

Every decision period reports candidate count, gate-pass count, valid-score
count by required role, selected count, and exclusions by code. Aggregate model
metrics or a non-null count for the year are not substitutes for row-level proof.

### 2.5 Production `ml_gates` preselection ADTV amendment

Accepted correction (2026-07-16): liquidity is a candidate-selection input,
not evidence that may be deferred until after holdings exist. For the production
`ml_gates` contract, freeze AUM at `$200,000`, `target_n=15`, equal-weight
planned position size at `AUM / target_n = $13,333.333333...`, and the maximum
position/ADTV ratio at `0.01`. The exact pass threshold is therefore

```text
median_30_session_dollar_volume >= (200000 / 15) / 0.01
                                >= 1,333,333.333333...
```

The legacy `AUM * 0.01` calculation is not equivalent and is rejected. For
every candidate that has passed the non-liquidity hard gates and has all
required OOS model roles, compute daily dollar volume as unadjusted
regular-session close times regular-session volume for exactly 30 valid
sessions with `market_close < prediction_timestamp`, then take their median.
The evidence timestamp is the prediction timestamp; the accepted Session 8E
entry timestamp is retained separately as the later execution timestamp.

Liquidity must be evaluated candidate-wide before regression ranking and
top-15 selection. Fewer than 30 valid observations, missing/nonpositive close
or volume, ambiguous security identity, missing timestamp, or absent evidence
excludes the candidate. The gate cannot be silently disabled, evaluated only
for a provisional top 15, backfilled from a later window, or deferred to the
post-selection Session 9C evidence pass.

## Contract 3 — Corporate actions and disappearing securities

### 3.1 Historical eligibility

Full-panel or eventual disappearance status is prohibited in historical
selection. A separately named point-in-time listing, filing-staleness, and
tradability gate is permitted when calculated exclusively from information
available at the decision timestamp.

Accordingly, the existing future-derived `likely_delisted` value is prohibited
as a historical gate, feature, rank input, or eligibility filter. Adding future
filings must not change past selection eligibility. The field may remain only as
a clearly labeled research/policy-sensitivity annotation whose value is never
exposed to a past decision.

Permitted as-of fields should be named for the evidence they contain rather than
for an eventual outcome, for example:

```text
asof_listing_eligible
asof_quote_recent
asof_filing_stale
asof_delisting_notice_known
asof_adtv_eligible
```

Each field requires its own decision timestamp, source/provenance, and explicit
missing-value behavior. It may use only listing status, quotes, filings, notices,
or liquidity observations actually available by that timestamp.

Removing that gate does not assign a return. Selected securities remain in the
portfolio until the accepted exit/rebalance rule or a sourced corporate-action
event determines their treatment.

For live personal-investment screening, this does not prohibit a current
tradability/safety check. The live path may require a current quote, recent
filing, active security mapping, and the accepted ADTV/liquidity threshold using
information actually available at today's decision timestamp. That current
check must be separately named and must not be backfilled into historical rows
using the security's eventual outcome.

### 3.2 Evidence hierarchy

The primary observed-only path requires dated evidence from a regulator,
exchange, issuer filing/announcement, or versioned corporate-action/security-
master source. Adjusted-price disappearance, absence of later filings, ticker
absence, or the Step 6 heuristic alone is insufficient evidence.

| Event class | Primary observed-only treatment when evidence is complete |
|---|---|
| Ticker/name/exchange migration | Continue through the dated security mapping and chain the same economic holding |
| Stock merger/reorganization | Apply sourced conversion terms and continue the received security/cash positions |
| Cash acquisition | Recognize sourced cash consideration and distributions on the effective/settlement rule, then hold cash until rebalance |
| Bankruptcy/liquidation | Use observed tradable prices, distributions, recoveries, and dated cancellation terms; zero only when evidence establishes zero recovery |
| Exchange delisting with another trading venue | Continue through the sourced security mapping when price evidence is complete |
| Source-coverage loss or unexplained disappearance | Unresolved; no primary return is assigned |

### 3.3 Unresolved events

An unresolved selected holding invalidates that portfolio period for the primary
observed-only NAV. The holding is not dropped and peers are not reweighted. A
continuous official CAGR/Sharpe/Calmar is unavailable across the gap. The report
must identify the exact security, dates, missing evidence, weight, and affected
strategy/horizon.

This is intentionally stricter than treating absence as zero or `-50%`. It
prevents a favorable survivor-only deletion and an unsupported punitive return.

### 3.4 Policy-imputed sensitivities

For practical personal small-cap analysis, unresolved disappearances are shown
as a range rather than hidden behind one precise result. Two separately named
scenarios are recommended:

- `include_policy_imputed_50`: assign `-50%` to the predefined unresolved-event
  population; and
- `include_policy_imputed_100`: assign `-100%` as the explicit total-loss worst
  case for the same population.

Both scenarios:

- are never used as the observed primary return or as a historical selection gate;
- retain explicit policy provenance and dated training eligibility;
- apply only to the predefined unresolved-event population;
- report affected holdings, weights, periods, and the exact difference from the
  observed-only coverage population; and
- do not describe either value as a delisting, bankruptcy, or corporate-action
  return.

The existing model-training sensitivity remains `-50%` unless a later separately
accepted modeling contract adds a `-100%` training scenario. Session 5A adds the
`-100%` case to portfolio-return analysis only; it does not silently change
training labels or populations.

The primary path may therefore have no headline performance when unresolved
events break continuity, while both portfolio sensitivities remain reportable as
a range. This is intended to support personal investment decisions without
claiming that either assumed loss is the true corporate-action outcome.

## Contract 4 — Canonical return stream and metrics

### 4.1 Authoritative portfolio ledger

The sole authoritative performance source is a security-level portfolio ledger
valued to a continuous month-end net total-return NAV. The accepted Session 8B
calendar will later supply decision, entry, holding, and exit timestamps; this
contract does not choose them.

The ledger must record:

- beginning shares/cash/weights and every trade;
- sourced splits, dividends, distributions, conversions, cash consideration,
  security mappings, and other accepted corporate actions;
- gross security returns and portfolio return;
- commissions, spread/slippage, taxes/fees when configured, turnover, and net
  return;
- missing-price/event status and exclusion code; and
- month-end gross NAV, net NAV, benchmark NAV, and cash.

Adjusted/total-return prices may represent distributions only when their source
and adjustment behavior are frozen and validated. Corporate-action cash or
replacement securities must not be counted twice through both adjusted prices
and the event ledger.

### 4.2 Cash, costs, and total losses

- Uninvested or post-acquisition cash earns 0% in the recommended conservative
  primary configuration unless a time-aligned frozen cash-rate series is
  explicitly configured before the run.
- Costs are charged at actual portfolio trades/rebalances, not subtracted once
  from an unrelated annual label.
- Security economic value is floored at zero. Trading costs are paid from
  portfolio cash and cannot make a security return or portfolio NAV pass below
  total loss.
- Leverage, borrowing, and shorts require separate cash/financing contracts and
  are outside the core long-only validation.

### 4.3 Missing or partial price coverage

Missing first, internal, or final months never default to zero return. No-cache,
partial-cache, and post-disappearance cases invoke Contract 3. A portfolio period
is valid only when all selected holdings and benchmark observations needed by
the accepted calendar are covered or resolved by sourced events.

Session 9C must prove selected-holding coverage from selection-independent
canonical market inputs. The current 448-ticker cache is preserved as legacy
evidence but is not assumed sufficient.

### 4.4 Reconciliation

Annual and horizon returns are reporting aggregations of the canonical net NAV,
never an independent performance input. For every complete calendar year:

```text
annual_net_return = product(1 + monthly_net_return) - 1
```

The stored annual endpoint and monthly product must agree within `1e-10` for
synthetic tests and `1e-8` for persisted numeric artifacts. A mismatch fails the
artifact. Partial first/last years are explicitly labeled and are not silently
annualized as full years.

Realized forward-return labels remain model outcomes for training/evaluation;
they do not substitute for portfolio NAV.

### 4.5 Metric definitions

Every official metric is computed from the same complete net monthly NAV:

| Metric | Accepted definition |
|---|---|
| Monthly return | `NAV_t / NAV_(t-1) - 1` after costs and accepted events |
| CAGR | `(NAV_end / NAV_start) ** (365.2425 / elapsed_days) - 1` |
| Volatility | sample standard deviation of monthly net returns times `sqrt(12)` |
| Sharpe | mean monthly excess return divided by its sample standard deviation times `sqrt(12)`; requires a frozen time-aligned monthly risk-free series |
| Downside deviation | square root of mean squared negative monthly excess returns times `sqrt(12)` |
| Sortino | annualized mean monthly excess return divided by annualized downside deviation |
| Drawdown | `NAV_t / running_max(NAV) - 1` from the same net NAV |
| Drawdown duration | consecutive month-end observations below the prior peak |
| Calmar | CAGR divided by absolute maximum drawdown; no 2-sigma or minimum-drawdown proxy |
| Best/worst and negative periods | net monthly returns from the same series |

If the risk-free series is unavailable or incompatible, Sharpe and Sortino are
reported unavailable; a fixed current annual constant is not substituted.
Benchmarks use the same accepted holding timestamps and month-end valuation
intervals.

## Downstream implementation boundary

After explicit acceptance:

- Session 6A may implement Contracts 1–2 only;
- Session 6B may implement Contracts 3–4 only;
- neither session may regenerate historical scores, prices, data, or official
  performance artifacts;
- Session 7 freezes `LEGACY_SAVED` evidence;
- Session 8B chooses the actual horizon calendars;
- Session 9 generates `OLD_RECONSTRUCTED` and `CORRECTED_PARTIAL` predictions;
- Session 9C proves market-data/event coverage; and
- Session 10 computes controlled performance from the canonical NAV.

## Alternatives deliberately rejected

| Alternative | Reason rejected |
|---|---|
| Use a final model for early/missing folds | Leaks later training information and has no row-level OOS lineage |
| Use a score when at least 5 peers have one | Slice-wide availability does not prove the selected row's score |
| Renormalize a blended score around a missing ML input | Silently changes strategy definition and ranking |
| Form a smaller portfolio whenever fewer rows survive | Silently changes concentration; allowed only as a named sensitivity |
| Keep `likely_delisted` as a safety gate | Reveals future full-panel disappearance status |
| Drop securities with missing realized returns | Creates optimistic survivorship selection |
| Treat missing/disappeared monthly prices as 0% | Confuses missing evidence with cash or unchanged economic value |
| Treat every disappearance as `-50%` or `-100%` | Invents unsupported corporate-action outcomes |
| Splice monthly drawdown into annual CAGR/Sharpe | Produces metrics from inconsistent return streams |
| Force annual reconciliation by rescaling monthly returns | Hides price, cost, event, or interval errors instead of resolving them |

## Acceptance record

On 2026-07-15 the user explicitly accepted:

1. Contract 1: row-level OOS prediction lineage;
2. Contract 2: no fallback, complete required scores, and `target_n` holdings for
   official portfolios; on 2026-07-16 the user further froze the production
   `ml_gates` candidate-wide ADTV amendment in Section 2.5;
3. Contract 3: evidence-backed corporate actions, prohibition of full-panel or
   eventual disappearance status in historical selection, separately named
   point-in-time listing/filing-staleness/tradability gates, unresolved primary
   periods unavailable, and separate `-50%`/`-100%` portfolio sensitivities; and
4. Contract 4: one reconciled monthly net total-return NAV and the metric
   definitions above.

This acceptance authorizes Session 6A and 6B only as later separate bounded
tasks under their roadmap scopes. It does not itself authorize a rebuild,
retraining, artifact generation, calendar choice, production comparison,
commit, or push.

## Verification boundary

This contract was derived from the Session 5 source/artifact audit. No scorer,
model fit, strategy, backtest, data rebuild, price refresh, artifact generation,
calendar selection, commit, or push was performed.
