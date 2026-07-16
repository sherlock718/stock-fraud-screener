# Session V3.1 — Production Table and Contract Freeze

Status: **Accepted; V3.2 unblocked**

## Outcome

The immutable observed-only US annual three-year table contains 43,806
stable rows. It retains certified decision and prediction timestamps for every
row and 19,025 certified observed entry
timestamps. No row was deleted for missing target, timestamp, feature, or gate
evidence; every unresolved gate fails that row closed.

The V3.1 builder revalidated exactly the two Session 8F records it consumed and
no unrelated source inventory. It fitted no model, generated no prediction,
selected no holding, sourced no market data, ran no backtest, and did not begin
V3.2.

The accepted strategy is `production_v3_ml_gates`: both model targets are
three-year observed outcomes, fold-local feature selection is capped at 28 from
the frozen candidate pool, and clean training requires certified positive ROA
and Beneish below -1.78. The uncertified `fraud_suspect` heuristic is not
synthesized or silently skipped. The 0.55 tree threshold is a fixed policy
parameter, not a newly optimized result. Legacy performance does not transfer
to this corrected V3 strategy.

## Hard-gate coverage

| Gate | Supported | Passed | Failed closed |
|---|---:|---:|---:|
| market_us | 43,806 | 43,806 | 0 |
| market_cap | 26,232 | 18,690 | 25,116 |
| beneish | 43,806 | 39,312 | 4,494 |
| piotroski | 43,806 | 29,571 | 14,235 |
| roa_positive | 43,806 | 23,990 | 19,816 |
| altman | 43,806 | 33,640 | 10,166 |
| value | 22,605 | 14,338 | 29,468 |
| momentum | 26,232 | 22,129 | 21,677 |

The Beneish, Altman, and sector-relative P/S gates were rematerialized from
their certified 8F components under the SEC-primary availability clock. This
is necessary because 8F's generic UTC-date equality check nulled these derived
columns when New York end-of-day crossed UTC midnight. The original 8F artifact
was not modified.

## Boundary

V3.1 has no remaining contract blocker. V3.2 may consume only this table and
configuration. Candidate-wide volume is not present in the certified Session
8E price records, so V3.3 will require explicit approval for market-data
collection after V3.2 freezes its OOS predictions.
