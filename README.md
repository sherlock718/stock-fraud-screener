# Stock Screener — Frozen Research Reference

This repository preserves the completed US free-data V1 research system as a
read-only reference. Product v2 should be implemented in a separate repository
so its simpler design does not inherit this repository's maintenance burden.
Fraud risk remains one input alongside value, quality, momentum, and growth;
it is not the whole product.

## Current status

Session DUR1 completed immutable recovery of all 21 required artifact groups:
56,092 paths and 7,082,517,721 bytes at private revision
`a282a1023f321b9bad84ec6f12e5d846345ff833`. The authoritative route passes,
and the recorded final suite result is 876 passed, 4 skipped, and 78 existing
warnings. No production release was performed.

The shortlist and performance outputs remain research evidence. Coverage is
not certified survivorship-free, US1B still requires human review, and the
provider-certified ledger and immutable `DGS1MO` limitations remain open.

Start with [docs/START_HERE.md](docs/START_HERE.md). The frozen work record is
[docs/CODEX_ROADMAP.md](docs/CODEX_ROADMAP.md).

## Frozen research flow

```text
corrected observed-only US annual data (P2)
    -> fold-local models and OOS predictions (P3/M1)
    -> fixed gates, liquidity, and portfolio rules (P4)
    -> US free-data shortlist and evidence (US1A/US1B)
    -> local consolidation and immutable recovery (US1C/DUR1)
```

Verify the complete frozen route offline with:

```bash
python3 -m workflows.run_us_free_v1
```

Recover the required ignored artifacts into absent destinations with:

```bash
python3 -m data_io.us_free_v1_durability --recover --revision a282a1023f321b9bad84ec6f12e5d846345ff833 --target <absent-path> --evidence-output <absent-json>
```

## Repository map

- `pipeline/` — data preparation and features
- `modeling/` — training and scoring
- `alpha/` — value, quality, momentum, growth, and fraud-risk factors
- `portfolio/` — selection and portfolio construction
- `quality/` — data and temporal-integrity checks
- `backtest/` and `research/` — research support, not the active product surface
- `notebooks/` — preserved experiments; not a production dependency
- `docs/archive/` — retired plans and historical product claims

## Safety boundary

Do not treat archived backtests as verified current performance. Do not refresh
data, retrain models, change policy, promote artifacts, or add Product v2 work
here. Verification and immutable recovery are the only supported operations.
