# Session 6B — Canonical Monthly NAV and Historical Gate Correction

## Outcome

Session 6B implements only the accepted Session 5A corporate-action/
disappearance and canonical-return contracts. Historical selection no longer
uses future full-panel disappearance status, and official performance now comes
from one reconciled security-level month-end net total-return NAV.

No dataset, monthly-price cache, model, prediction, registry, saved backtest, or
production comparison artifact was regenerated. Session 6A score consumption,
model labels, target definitions, calendars, and strategy thresholds are
unchanged.

## Historical selection contract

- `likely_delisted` is ignored by the historical engine and removed from the
  historical screener-registry and leverage eligibility paths. Changing or
  appending eventual disappearance annotations cannot change past eligibility.
- Separately named `asof_listing_eligible`, `asof_filing_stale`,
  `asof_delisting_notice_known`, `asof_quote_recent`, and
  `asof_adtv_eligible` fields may gate a historical row only with a non-null
  field-specific timestamp no later than the decision timestamp and a non-empty
  field-specific source. Supplied but incomplete evidence fails closed.
- The existing trailing ADTV calculation remains decision-time bounded and now
  excludes missing ADTV evidence instead of treating it as eligible.
- Removing the future-derived gate assigns no return and does not classify an
  acquisition, bankruptcy, migration, restructuring, or source-coverage loss.

## Canonical ledger and coverage behavior

`backtest/monthly_nav.py` builds the sole authoritative month-end `net_nav`.
For every selected holding it requires a valid entry price and every required
monthly total-return adjusted close, unless one dated corporate-action record
resolves the first missing interval.

The accepted implemented event boundary is deliberately narrow:

- `resolved_cash` requires a source, an event total return no lower than `-100%`,
  and explicit post-event cash treatment; cash then earns 0%;
- `unresolved` requires a source but invalidates `observed_only` performance;
- unsupported or incomplete event terms, missing provenance, missing entry
  prices, internal/final price gaps, duplicate/ambiguous resolutions, and
  non-contiguous holding periods fail closed with ticker, month, weight, year,
  and exclusion code where applicable.

Missing holdings are never dropped and peer weights are never renormalized.
Configured transaction costs are charged at the portfolio trade/rebalance and
reduce investable NAV. Security value and portfolio NAV are floored at zero.

## Explicit return paths

The portfolio-return policies are distinct values and outputs:

1. `observed_only` — no inferred outcome for an unresolved event;
2. `include_policy_imputed_50` — applies `-50%` only to an explicitly recorded
   unresolved event; and
3. `include_policy_imputed_100` — applies `-100%` only to that same explicitly
   recorded unresolved-event population.

The `-100%` scenario is portfolio-analysis-only. Model training policy remains
unchanged. The legacy `fill_missing_return` interface is rejected so a caller
cannot silently select an imputation.

## Reconciliation and metrics

Annual returns are reports derived from the monthly NAV:

```text
annual_net_return = product(1 + monthly_net_return) - 1
```

Synthetic reconciliation tolerance is `1e-10`. No annual forward-return label,
annual cost subtraction, annual drawdown fallback, zero-filled missing month,
or 2-sigma drawdown proxy is an official performance input.

CAGR, volatility, Sharpe, Sortino, drawdown, drawdown duration, Calmar, and
best/worst/negative monthly periods all consume the same `net_nav`. CAGR uses
actual elapsed days. Sharpe and Sortino require a complete frozen time-aligned
monthly risk-free return series and are unavailable otherwise. Calmar is CAGR
divided by the absolute drawdown from that same NAV, with no proxy floor.

The standalone IC-weighted portfolio backtest uses the same long-only NAV
builder. Its long/short mode fails closed because borrowing, financing, and short
ledger terms were not accepted in Session 5A.

## Synthetic verification

Tests cover:

- monotonic positive-price NAV and known drawdown;
- positive annual endpoints that conceal a 50% monthly drawdown;
- no cache, missing entry price, missing first/internal/final month, one missing
  ticker in a multi-holding portfolio, and no silent reweighting;
- mid-year unresolved disappearance in observed-only, `-50%`, and `-100%`
  paths;
- sourced resolved total loss and the zero-value floor;
- transaction costs at portfolio entry;
- exact annual/monthly endpoint reconciliation;
- CAGR, Sharpe, drawdown, and Calmar source consistency;
- missing risk-free coverage making Sharpe unavailable;
- future `likely_delisted` reclassification and appended-future-row invariance;
  and
- fail-closed as-of gate and ADTV provenance/coverage.

Focused verification:

```text
95 passed
```

Full verification:

```text
627 passed, 4 skipped, 78 warnings
```

The warnings are existing pandas fragmentation/date warnings in pipeline tests.
Targeted compilation, `git diff --check`, and the warn-only sync check passed.

## Remaining boundary

This implementation supplies the contract and fail-closed behavior; it does not
certify the current local price cache or provide missing corporate-action facts.
Session 8B still chooses horizon-specific calendars, and Session 9C must prove
selection-independent selected-holding, benchmark, cost, risk-free, and event
coverage before Session 10 can report controlled performance. Unresolved
observed-only periods remain unavailable until then.
