-- Stock Fraud Screener — TimescaleDB schema
-- Run automatically by docker-compose on first db start

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── Companies ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS companies (
    ticker          TEXT        NOT NULL,
    name            TEXT,
    market          TEXT,
    country         TEXT,
    sic_code        TEXT,
    accounting_std  TEXT,       -- GAAP / IFRS / K-GAAP
    currency        TEXT,
    PRIMARY KEY (ticker, market)
);

-- ── Annual snapshots (time-series core table) ─────────────────────────────
CREATE TABLE IF NOT EXISTS snapshots (
    ticker              TEXT        NOT NULL,
    market              TEXT        NOT NULL,
    fiscal_year         INT         NOT NULL,
    filing_date         DATE,       -- actual SEC/EDGAR filing date (knowledge cutoff)
    as_of_date          DATE,       -- price/macro as-of date used for enrichment
    source_timestamp    TIMESTAMPTZ DEFAULT NOW(),

    -- Fraud scores
    composite_score         FLOAT,
    fraud_score_accounting  FLOAT,
    fraud_score_governance  FLOAT,
    fraud_score_dilution    FLOAT,
    ml_score_1y             FLOAT,
    ml_score_3y             FLOAT,
    ml_score_5y             FLOAT,
    data_confidence         TEXT,

    -- Beneish components
    beneish_m_score     FLOAT,
    dsri                FLOAT,
    gmi                 FLOAT,
    aqi                 FLOAT,
    sgi                 FLOAT,
    depi                FLOAT,
    sgai                FLOAT,
    accruals            FLOAT,
    lvgi                FLOAT,

    -- Altman Z-Score
    altman_z_score      FLOAT,

    -- Piotroski
    piotroski_f_score   INT,

    -- Accruals
    sloan_accruals      FLOAT,

    -- Financials (USD)
    revenue             FLOAT,
    net_income          FLOAT,
    total_assets        FLOAT,
    total_debt          FLOAT,
    cash                FLOAT,
    market_cap_at_filing FLOAT,

    PRIMARY KEY (ticker, market, fiscal_year)
);
SELECT create_hypertable('snapshots', 'fiscal_year',
    chunk_time_interval => 5,
    if_not_exists => TRUE,
    migrate_data => TRUE);

-- ── Fraud event labels ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fraud_labels (
    ticker              TEXT        NOT NULL,
    market              TEXT        NOT NULL,
    fraud_year          INT         NOT NULL,
    label_type          TEXT        NOT NULL,  -- aaer, restatement, 8k, bankruptcy, short_report
    source              TEXT,
    description         TEXT,
    fraud_confirmed     BOOLEAN     DEFAULT FALSE,
    fraud_suspect       BOOLEAN     DEFAULT FALSE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ticker, market, fraud_year, label_type)
);

-- ── Watchlist ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS watchlist (
    id          SERIAL PRIMARY KEY,
    ticker      TEXT        NOT NULL,
    market      TEXT        NOT NULL,
    added_at    TIMESTAMPTZ DEFAULT NOW(),
    notes       TEXT,
    UNIQUE (ticker, market)
);

-- ── Useful indexes ─────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_snapshots_composite ON snapshots (composite_score);
CREATE INDEX IF NOT EXISTS idx_snapshots_market ON snapshots (market, fiscal_year);
CREATE INDEX IF NOT EXISTS idx_fraud_labels_ticker ON fraud_labels (ticker, market);
