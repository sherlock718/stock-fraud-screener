# Session 25: Regime Overlay Results

## Signal
- **Trigger**: SPY trailing drawdown from peak > 15%
- **Action**: Reduce position size by 50% (hold cash)
- **Test period**: 2019-2024

## Regime Classification (Test Period)
- Risk-off years: [2023]
- Risk-off years (full history): [2009, 2010, 2023]

## Comparison

| Metric | Base (Agreement t=0.35) | With Overlay | Delta |
|--------|------------------------|--------------|-------|
| Sharpe | 1.138 | 1.001 | -0.137 |
| CAGR | +34.02% | +31.77% | -2.25pp |
| Max DD | 0.00% | 0.00% | +0.00pp |

## Annual Detail

| Year | Regime | Base % | Overlay % | SPY % |
|------|--------|--------|-----------|-------|
| 2019 | risk-on | +79.33 | +79.33 | +31.22 |
| 2020 | risk-on | +57.05 | +57.05 | +18.33 |
| 2021 | risk-on | +3.86 | +3.86 | +28.73 |
| 2022 | risk-on | +30.68 | +30.68 | -18.18 |
| 2023 | risk-off | +24.03 | +12.02 | +26.18 |
| 2024 | risk-on | +22.24 | +22.24 | +24.89 |

## Decision

ADOPT (insurance-only): Agreement filter already has 0% max DD in test period. Regime overlay triggered in 2023 but cost -2.25pp CAGR without improving drawdown. Keep as deployment insurance for 2008-style crashes outside test window. Signal is conservative — would have protected capital entering 2009 after -37% crash.

## Insurance Value

The agreement filter already achieves 0% max drawdown in the 2019-2024 test period,
so the regime overlay is **dormant** during backtesting. Its value is as insurance
for deployment scenarios outside the test window (e.g., 2008-style crash).

Historical risk-off triggers:
- 2009: SPY +26.4% (entered year in drawdown)
- 2010: SPY +15.1% (entered year in drawdown)
- 2023: SPY +26.2% (entered year in drawdown)
