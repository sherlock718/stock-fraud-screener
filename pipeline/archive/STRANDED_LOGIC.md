# Stranded Logic — Preserved Before Archive

These features exist ONLY in the archived legacy files. They are NOT in the current
step5 pipeline. If migrated to the parquet pipeline in a future session, reference
the archived source for algorithm logic.

---

## 1. Insider Trading Signals (`enrich_insider_signals.py`)

**Features:**
- `insider_buy_count` — open-market purchase transactions (Form 4)
- `insider_sell_count` — open-market sale transactions
- `net_insider_shares` — total_purchases - total_sales (negative = net selling)
- `insider_selling_flag` — True if net_shares < -10,000 AND sales > buys

**Data source:** SEC EDGAR Form 4 filings (free, public)
**PIT safety:** Partially safe — uses filing dates, but window lookback needs review
**Migration target:** step5 or post-step6 enrichment
**Priority:** P2 (PIPELINE_ATLAS Feature Coverage #8)

---

## 2. Market/Volume Signals (`enrich_market_signals.py`)

**Features:**
- `avg_daily_volume` (ADTV) — 90-day average daily trading volume
- `volume_spike_ratio` — 30-day avg volume / 90-day avg (>3x = spike)
- `illiquidity_flag` — ADTV < 50,000 shares
- `pump_dump_flag` — volume_spike > 3x AND price up > 50% in 30 days

**Data source:** yfinance historical volume (free, public)
**PIT safety:** YES if computed using pre-filing-date window only
**Migration target:** step3 (ADTV) or step5 (derived flags)
**Priority:** P1 (LIQUIDITY-001 in KNOWN_ISSUES.md)

---

## 3. Governance / Going Concern (`enrich_governance.py`)

**Features:**
- `going_concern` — True if 10-K (2021+) has standard going concern language (EDGAR EFTS)
- `auditor_name` — None (not available via free EDGAR API)
- `big4_auditor` — False (requires paid data)
- `small_auditor_flag` — False (requires auditor_name)

**Data source:** SEC EDGAR EFTS full-text search (free, public)
**PIT safety:** YES — uses filing date from EDGAR
**Migration target:** step5 (going_concern flag only; auditor fields not viable without paid data)
**Priority:** P2

---

## 4. Fraud Signal Formulas (`fraud_signals.py`)

**Duplicated in step5:** beneish_m_score, piotroski_f_score, altman_z_score, ohlson_o_score
**Status:** All formulas already implemented in `pipeline/step5_compute_features.py` § C+D.
No stranded logic — safe to archive without loss.

---

## 5. Value Metrics (`value_metrics.py`)

**Duplicated in step5:** PE, PB, PS, EV/EBITDA, FCF yield, dividend yield, book/market
**Status:** All formulas already implemented in `pipeline/step5_compute_features.py` § A+B.
No stranded logic — safe to archive without loss.

---

*Created: Session 8 (2026-06-22). Reference for future feature migration sessions.*
