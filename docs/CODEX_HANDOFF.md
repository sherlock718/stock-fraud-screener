# Codex Handoff

## Current state

Session V3.1 is accepted and V3.2 is unblocked. The manifest-backed artifact is
under `artifacts/pit_validation/session_v3_1_production_contract/`; its manifest
SHA-256 is `2b5249cdb05c7bad1759abbd281ec1c90a8a9ce2fbd72973cd4dc905c8a86e5a`.
The artifact payload is Git-ignored.

The observed-only US annual three-year table contains 43,806 stable rows,
19,025 observed targets and certified entry timestamps, 119 frozen feature
candidates, and row-level values/status/provenance/pass results for all eight
non-model production hard gates. Fold-local feature selection is capped at 28.
Decision and prediction timestamps are present for every row; 4,503 rows pass
all non-model gates before OOS model and liquidity roles.

The accepted corrected strategy is `production_v3_ml_gates`:

- decision tree target: observed three-year local-benchmark outperformance;
- LightGBM target: observed three-year stock return, clipped to `[-1, 5]` for fit;
- clean training: certified positive ROA and Beneish below `-1.78`;
- `fraud_suspect`: excluded because it is not a certified V3 field;
- tree threshold `0.55`: fixed policy parameter, not a newly optimized claim;
- top 15, full target required, equal weights, $200,000 AUM, and 1%-ADTV;
- legacy performance claims do not transfer to this corrected strategy.

V3.1 revalidated only the two exact Session 8F records it consumed and
rematerialized Beneish, Altman, and sector-relative P/S from certified
components under the SEC-primary availability clock. No model, prediction,
holding, external data, or backtest was produced.

## Commit cadence

The roadmap now requires one conventional commit after every verified V3
session. Each checkpoint includes code, tests, docs, changelog, and handoff, but
never generated artifact payloads. Do not combine multiple completed sessions
into one commit and do not create lettered blocker sessions.

## Files changed in V3.1

- `modeling/freeze_session_v3_1.py`
- `tests/modeling/test_freeze_session_v3_1.py`
- `reports/pit_validation/v3_1_production_table_contract.md`
- `docs/CODEX_ROADMAP.md`
- `docs/PRODUCTION_CONFIG.md`
- `CHANGELOG.md`
- `docs/CODEX_HANDOFF.md`

## Verification

- Focused V3.1 contract tests passed.
- Independent validation matched all 14 manifest input, record, code, and report
  hashes; table identity and observed-only assertions passed.
- No external data, model training, holdings selection, optimization, or
  backtest occurred.

## Exact next task

Execute Session V3.2 only, using the accepted V3.1 table and configuration to
generate fold-local OOS decision-tree probabilities and LightGBM three-year
regression predictions with complete lineage. Do not select holdings, collect
market data, or begin V3.3. V3.3 will require explicit approval because the
certified Session 8E normalized price records contain no volume.
