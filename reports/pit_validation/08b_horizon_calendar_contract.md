# Session 8B — Horizon Calendar Contract

## Decision

Session 8B accepts calendar contracts for the five existing modeled horizons:
`6m`, `1y`, `2y`, `3y`, and `5y`. It excludes `4y`, `6y`, `7y`, `8y`, `10y`,
and `15y` because the repository has no declared training/OOS prediction path
for them. The frozen machine-readable contract is
`artifacts/pit_validation/calendar_contract/session8b_calendar_contract.json`.

Acceptance freezes policy, not data support. Session 8C must validate the
selection-independent price, benchmark, trading-calendar, adjustment, and
availability evidence before any accepted horizon can enter Session 9. No
calendar choice used model, test, prediction, backtest, Sharpe, CAGR, or
production-comparison results.

## Manifest and evidence gate

The Session 8 manifest parsed successfully and all 31 referenced input, output,
corrected-code-lineage, and dirty-state payloads matched both recorded size and
SHA-256. The raw validation result is in
`/tmp/session8b_manifest_validation.log`.

Evidence reviewed:

- `docs/CODEX_HANDOFF.md` and the Session 8B roadmap contract;
- `reports/pit_validation/08_data_comparison.md` and its eleven-horizon
  structural-eligibility table;
- the validated corrected-partial manifest and preserved Step 2/3/5/6 outputs;
- `pipeline/step3_enrich_prices.py` horizon, entry/exit, benchmark, and label
  date behavior;
- `modeling/label_eligibility.py` strict observed/policy eligibility behavior;
- market Step 2 provenance emitters and `pipeline/event_time_cohorts.py`;
- current model declarations, which support `6m`, `1y`, `2y`, `3y`, and `5y`.

The frozen stale Step 2 snapshots and every corrected-partial downstream
dataset omit `entity_id`, `availability_timestamp`, and
`availability_provenance`. Consequently, zero rows can be certified against the
new filing-availability rule from the current corrected-partial artifacts.
This does not reopen Session 8, but it makes every accepted horizon
`unsupported_pending_session8c` and blocks Session 9.

## Common decision and prediction calendar

For each decision year `Y`:

- information cutoff: June 30 23:59:59 in the source market's local timezone;
- decision timestamp: July 2 00:00:00 UTC;
- prediction timestamp: July 2 00:01:00 UTC, after the eligible universe,
  inputs, preprocessing, and model-training population are frozen;
- eligible fiscal cohort: annual rows with `fiscal_year = Y - 1` only;
- late filing: exclude the issuer for that decision; never carry forward an
  older fiscal cohort and never infer availability from fiscal year;
- training cutoff: a target is permitted only when its applicable
  `label_end_date < decision_timestamp`; equality is excluded.

The one-day buffer between the local June 30 cutoff and the UTC decision avoids
pretending that date-only filings have known intraday timing. A date-only proven
source is conservatively effective at the end of its source-market local day,
then converted to UTC. Missing or unsupported timezone evidence fails closed.

The permitted information set consists only of data with proven effective
availability before the decision timestamp. Annual filing rows additionally
require stable `entity_id`, annual period type, market, fiscal year, publication
timestamp, and permitted source provenance. Derived features must carry lineage
to permitted inputs and training-only fitted transformations. Missing lineage,
release-vintage evidence, or decision-time market data makes the feature or row
ineligible; it is not backfilled.

## Filing-date provenance by market

| Market | Permitted filing evidence | Calendar treatment |
|---|---|---|
| US | SEC primary filing | Permitted; official Session 9 modeling scope |
| JP | EDINET submission | Calendar-valid, but outside current US Session 9 scope |
| KR | DART receipt | Calendar-valid, but outside current US Session 9 scope |
| BR | Reference/statement date without publication proof | Excluded |
| CA | Statement date without publication proof | Excluded |
| EU markets | Statement date without publication proof | Excluded |
| Any | Estimate, missing source/date/entity, unsupported source | Excluded |

Only the earliest primary entity-period version is permitted. Later amendments
are excluded. An unresolved equal-time collision, missing entity identity, or
source ambiguity excludes every colliding row. JP/KR acceptance here does not
authorize their model use; each would require a separate modeled-market scope
and Session 8C market-input support.

## Horizon-specific holding, return, and benchmark calendars

All horizons use the annual decision/prediction schedule above. A vintage's
holding period equals its prediction horizon; horizons longer than one year
therefore create overlapping annual vintages and must be reported by vintage.
No shorter `fiscal_year + 1` holding window may stand in for a longer target.

| Horizon | Status | Entry-to-target interval | Exit rule |
|---|---|---:|---|
| 6m | Accepted | 6 calendar months | First eligible common close on/after target |
| 1y | Accepted | 12 calendar months | Same |
| 2y | Accepted | 24 calendar months | Same |
| 3y | Accepted | 36 calendar months | Same |
| 4y | Excluded: no model/OOS path | 48 calendar months | Frozen if later scoped |
| 5y | Accepted | 60 calendar months | First eligible common close on/after target |
| 6y | Excluded: no model/OOS path | 72 calendar months | Frozen if later scoped |
| 7y | Excluded: no model/OOS path | 84 calendar months | Frozen if later scoped |
| 8y | Excluded: no model/OOS path | 96 calendar months | Frozen if later scoped |
| 10y | Excluded: no model/OOS path | 120 calendar months | Frozen if later scoped |
| 15y | Excluded: no model/OOS path | 180 calendar months | Frozen if later scoped |

Entry is the first common regular-session total-return close for the security
and its frozen benchmark strictly after prediction, within five calendar days.
The target exit date is entry plus the exact calendar-month interval above; exit
is the first common regular-session total-return close on or after that target,
within ten calendar days. Missing common entry/exit evidence excludes the row.

Stock and benchmark cumulative total returns use the identical accepted entry
and exit timestamps. The relative regression label is stock return minus
benchmark return; the classification label is stock return greater than
benchmark return. The relative label becomes available at the later of the
stock and benchmark exit availability timestamps. CAGR is not a training
label. Costs and canonical net NAV remain later backtest inputs, not label
transformations.

For US rows, the benchmark is frozen at decision time from decision-available
market cap: IWC below $300m, IWM from $300m to below $2bn, MDY from $2bn to
below $10bn, and SPY at $10bn or above. Missing size, benchmark mapping, or
validated total-return coverage fails closed. Session 8C must validate these
instruments and adjustment semantics; no proxy substitution is allowed.

## Label availability and population separation

Observed-only is primary. It requires observed stock price provenance for stock
targets and observed stock-plus-benchmark provenance for relative targets, with
the relevant end date strictly before decision. Policy-imputed labels are
allowed only in the separately named `include_policy_imputed` sensitivity after
their explicit policy-availability date is strictly before decision.

The two populations require separate eligibility tables, model fits,
predictions, manifests, and reports. They may not be unioned, used as fallback
for one another, or compared as though they were one sample.

## Resolution of legacy calendar mismatches

The accepted contract does not use `fiscal_year + 1` as a decision proxy, does
not index benchmarks by fiscal year, and does not start target returns at each
row's filing date. Fiscal year selects the cohort; proven publication controls
information eligibility; the common July decision controls prediction; and the
post-prediction common trading closes control both stock and benchmark returns.

Session 8's materialized forward labels start on each filing date and use fixed
day counts (183/365/etc.), with independently found stock and benchmark dates.
They are therefore incompatible with this contract and must not be reused for
Session 9 training or performance claims. Session 8C must first establish
whether frozen market inputs can support contract-aligned labels; it must not
silently reinterpret the existing columns.

## Limitations and stop conditions

- Current corrected-partial artifacts cannot certify filing availability or
  entity identity, so accepted calendars presently have zero certified rows.
- Benchmark total-return semantics, adjustment vintage, common trading dates,
  and coverage remain unverified until Session 8C.
- Macro release vintages and other non-filing feature availability remain
  fail-closed where lineage is absent.
- Overlapping multi-year vintages are accepted for horizon evaluation, but a
  combined capital-allocation rule is not inferred here; official performance
  remains blocked by later canonical-NAV contracts and market-input gates.
- No model, prediction, backtest, threshold, or performance result was created
  or consulted in Session 8B.

Session 8B is complete because every accepted horizon has a complete and
internally consistent calendar contract. This completion does not certify that
the current data can implement it.
