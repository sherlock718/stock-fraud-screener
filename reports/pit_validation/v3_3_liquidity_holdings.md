# Session V3.3 — Liquidity-Qualified Holdings

## Verdict

Session V3.3 is complete under the frozen `production_v3_ml_gates` contract. The exact accepted V3.1 and V3.2 manifests plus all 156 referenced table, configuration, prediction, model-role, model, and lineage records revalidated before selection. Liquidity was evaluated candidate-wide before ranking; incomplete decision periods formed no portfolio.

## Frozen output

- Source rows: 43,806
- Liquidity-required candidates: 1,428
- Liquidity-pass candidates: 1,174
- Supported decision periods: 6 (2015, 2016, 2017, 2018, 2019, 2020)
- Holdings: 90, exactly 15 per supported period at weight 0.0666666666666667
- Minimum median 30-session dollar volume: $1,333,333.3333333333

Liquidity exclusions:

- `liquidity_median_30_session_dollar_volume_below_threshold`: 221
- `liquidity_session_volume_missing_or_nonpositive`: 33

Daily dollar volume is unadjusted regular-session close multiplied by regular-session volume. Every passing row has exactly 30 valid exchange-calendar sessions whose market close is strictly before its prediction timestamp. Raw responses, request parameters, retrieval timestamps, mappings, session clocks, currency, adjustment policy, and hashes are retained. The 1,428 candidate-scoped Yahoo requests all returned HTTP 200; source defects still fail closed independently of transport success.

## Selection and lineage controls

Ranking is descending OOS LightGBM three-year return prediction with stable row identity as the deterministic tie-breaker, and occurs only after all fixed non-liquidity hard gates, both exact OOS model roles, tree probability at least 0.55, and liquidity pass. Candidate, long-form gate, exclusion, liquidity evidence/coverage, period, holding, weight, source, retrieval, mapping, calendar, raw-response, prediction, model, preprocessing, feature, target, configuration, and accepted-manifest lineage are frozen in the artifact.

## Scope boundary

No threshold or parameter was optimized. Session 9 predictions and substitute models were not used. No performance, NAV, backtest, post-selection-only liquidity collection, or V3.4 market-ledger work was performed.
