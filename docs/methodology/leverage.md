# Leverage Strategy

The leverage strategy constructs a long/short portfolio using fraud scores with position sizing governed by the Kelly criterion.

## Strategy Logic

```mermaid
flowchart TD
    A["All scored companies<br/>for latest fiscal year"] --> B{"Quality Gate<br/>Piotroski F ≥ 6?<br/>Beneish M < -1.78?<br/>Altman Z > 1.81?"}
    B -->|All pass| C["Long Candidates<br/>Fraud score < 0.30<br/>Fraud-safe + high quality"]
    B -->|Fails any| D{"Short Gate<br/>Beneish M > -1.78<br/>AND score > 0.70?"}
    D -->|Yes| E["Short Candidates<br/>High fraud signal<br/>+ accounting manipulation flag"]
    D -->|No| F["Excluded<br/>Neither long nor short"]
    C --> G["Kelly Position Sizing<br/>half-Kelly · max 15% per position<br/>max 2x gross leverage"]
    E --> G
    G --> H["Portfolio Output<br/>leverage_picks.csv"]
```

## Quality Gates

Long positions require passing **all three** gates:

| Gate | Threshold | Rationale |
|---|---|---|
| Piotroski F-Score | ≥ 6 | Fundamental quality filter — eliminates deteriorating businesses |
| Beneish M-Score | < −1.78 | Accrual quality — below borderline manipulation zone |
| Altman Z-Score | > 1.81 | Solvency — excludes distress zone companies |

Short candidates require:
- Beneish M-Score > −1.78 (borderline or likely manipulator)
- Composite fraud score > 0.70 (strong model signal)

## Kelly Criterion Sizing

```
kelly_fraction = (p × b - q) / b

where:
  p = probability of gain (estimated from composite score for longs)
  q = 1 - p (probability of loss)
  b = average win/loss ratio (from backtest history)
```

Half-Kelly is used (50% of the full Kelly fraction) to reduce drawdown risk from estimation errors.

```python
HALF_KELLY_FRACTION = 0.5
MAX_POSITION_PCT    = 0.15    # Hard cap: no position > 15%
MAX_LEVERAGE        = 2.0     # Gross leverage cap: 2x
```

Positions are normalized so the sum of absolute weights equals 1.0 (or ≤ 2.0 for levered portfolios).

## Instrument Selection

For long positions:

| Market Cap | Preferred Instrument | Rationale |
|---|---|---|
| < USD 500M | Direct equity | LEAPS options often unavailable or illiquid |
| USD 500M – 2B | LEAPS (1-2 year) or direct equity | Option leverage without margin maintenance |
| > USD 2B | Margin or LEAPS | Deep options markets, reasonable bid-ask spreads |

For short positions:
- Locate borrow first — if borrow unavailable or cost > 5% annualized, exclude from portfolio
- Short via margin account (direct short sale)

## Running the Strategy

```bash
# Default: top 10 long + top 5 short
python3 scripts/leverage_strategy.py

# Custom universe size
python3 scripts/leverage_strategy.py --top-long 15 --top-short 8

# Long-only mode (no shorts)
python3 scripts/leverage_strategy.py --long-only

# Custom thresholds
python3 scripts/leverage_strategy.py --min-piotroski 7 --max-beneish -2.0

# Output
python3 scripts/leverage_strategy.py --output reports/leverage_picks.csv
```

## Output Columns

`reports/leverage_picks.csv`:

| Column | Description |
|---|---|
| `ticker` | Company ticker |
| `side` | `long` or `short` |
| `composite_score` | Fraud probability |
| `piotroski_f_score` | Quality score (longs only) |
| `beneish_m_score` | Manipulation indicator |
| `altman_z_score` | Solvency score |
| `kelly_weight` | Raw Kelly fraction |
| `position_pct` | Normalized position weight (capped at 15%) |
| `instrument` | `equity`, `leaps`, or `margin_short` |
| `market_cap_m` | Market cap in USD millions |

## Risk Management

!!! danger "This strategy involves real financial risk"
    - Short positions have unlimited theoretical loss
    - Small caps can be illiquid — limit orders required, slippage budget is critical
    - Kelly sizing is only as good as the underlying probability estimates
    - The model has a 1-year validation AUC of 0.749 — it is wrong 25% of the time
    - Do not deploy without a stop-loss framework

Recommended position-level stop-loss: 20% adverse move triggers review. 30% adverse move triggers exit regardless of model signal.
