-- HIP-3 Arb Research Database Schema
-- Append-only event store + derived state tables

-- ─── Extensions ──────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;  -- optional; falls back to plain PG

-- ─── Market Registry ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS market_registry (
    market_id        TEXT PRIMARY KEY,
    venue_label      TEXT NOT NULL,          -- 'xyz', 'felix', 'hl_native', etc.
    deployer         TEXT,
    symbol           TEXT NOT NULL,
    asset_class      TEXT NOT NULL,          -- 'commodity','index','equity','fx','crypto'
    collateral       TEXT NOT NULL DEFAULT 'USDC',
    oracle_type      TEXT,                   -- 'pyth','chainlink','internal','unknown'
    fee_mode         TEXT,                   -- 'standard','growth','custom'
    funding_formula  TEXT,
    max_leverage     NUMERIC,
    session_notes    TEXT,
    docs_url         TEXT,
    is_active        BOOLEAN NOT NULL DEFAULT TRUE,
    discovered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_registry_venue ON market_registry(venue_label);
CREATE INDEX IF NOT EXISTS idx_registry_asset_class ON market_registry(asset_class);

-- ─── Raw Quotes (top-of-book snapshots) ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_quotes (
    ts              TIMESTAMPTZ NOT NULL,
    market_id       TEXT NOT NULL REFERENCES market_registry(market_id),
    bid_px          NUMERIC NOT NULL,
    bid_sz          NUMERIC NOT NULL,
    ask_px          NUMERIC NOT NULL,
    ask_sz          NUMERIC NOT NULL,
    mid_px          NUMERIC GENERATED ALWAYS AS ((bid_px + ask_px) / 2) STORED,
    spread_bp       NUMERIC GENERATED ALWAYS AS (
                        CASE WHEN bid_px > 0
                        THEN (ask_px - bid_px) / bid_px * 10000
                        ELSE NULL END
                    ) STORED,
    source          TEXT DEFAULT 'ws'
);

SELECT create_hypertable('raw_quotes', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_quotes_market ON raw_quotes(market_id, ts DESC);

-- ─── Raw Trades ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS raw_trades (
    ts              TIMESTAMPTZ NOT NULL,
    market_id       TEXT NOT NULL REFERENCES market_registry(market_id),
    trade_id        TEXT,
    price           NUMERIC NOT NULL,
    size            NUMERIC NOT NULL,
    side            TEXT NOT NULL CHECK (side IN ('buy','sell')),
    is_liquidation  BOOLEAN NOT NULL DEFAULT FALSE
);

SELECT create_hypertable('raw_trades', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_trades_market ON raw_trades(market_id, ts DESC);

-- ─── Market State (mark, OI, funding snapshot) ────────────────────────────────
CREATE TABLE IF NOT EXISTS market_state (
    ts              TIMESTAMPTZ NOT NULL,
    market_id       TEXT NOT NULL REFERENCES market_registry(market_id),
    mark_px         NUMERIC,
    oracle_px       NUMERIC,
    open_interest   NUMERIC,
    day_volume      NUMERIC,
    premium         NUMERIC   -- (mark - oracle) / oracle
);

SELECT create_hypertable('market_state', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_mstate_market ON market_state(market_id, ts DESC);

-- ─── Funding State ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS funding_state (
    ts              TIMESTAMPTZ NOT NULL,
    market_id       TEXT NOT NULL REFERENCES market_registry(market_id),
    funding_rate    NUMERIC NOT NULL,    -- 8h rate
    annualized_rate NUMERIC,
    next_funding_ts TIMESTAMPTZ,
    predicted_rate  NUMERIC
);

SELECT create_hypertable('funding_state', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_funding_market ON funding_state(market_id, ts DESC);

-- ─── Reference State (external price feed) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS reference_state (
    ts              TIMESTAMPTZ NOT NULL,
    symbol          TEXT NOT NULL,      -- underlying reference, e.g. 'XAU', 'SPX'
    price           NUMERIC NOT NULL,
    source          TEXT NOT NULL,      -- 'pyth', 'chainlink', 'yahoo', etc.
    confidence      NUMERIC             -- price feed confidence interval where available
);

SELECT create_hypertable('reference_state', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ref_symbol ON reference_state(symbol, ts DESC);

-- ─── Latency Metrics ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS latency_metrics (
    ts              TIMESTAMPTZ NOT NULL,
    market_id       TEXT,
    event_type      TEXT NOT NULL,   -- 'quote','trade','ack','fill'
    exchange_ts     TIMESTAMPTZ,
    recv_ts         TIMESTAMPTZ NOT NULL,
    process_ts      TIMESTAMPTZ,
    recv_latency_ms NUMERIC,
    proc_latency_ms NUMERIC
);

SELECT create_hypertable('latency_metrics', 'ts', if_not_exists => TRUE);

-- ─── Simulated Orders ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sim_orders (
    order_id        TEXT PRIMARY KEY,
    strategy_id     TEXT NOT NULL,
    market_id       TEXT NOT NULL REFERENCES market_registry(market_id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    side            TEXT NOT NULL CHECK (side IN ('buy','sell')),
    order_type      TEXT NOT NULL CHECK (order_type IN ('limit','market','ioc','post_only')),
    price           NUMERIC,
    size            NUMERIC NOT NULL,
    status          TEXT NOT NULL DEFAULT 'open',
    filled_size     NUMERIC NOT NULL DEFAULT 0,
    avg_fill_px     NUMERIC,
    cancelled_at    TIMESTAMPTZ,
    notes           TEXT
);

-- ─── Simulated Fills ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sim_fills (
    fill_id         TEXT PRIMARY KEY,
    order_id        TEXT NOT NULL REFERENCES sim_orders(order_id),
    market_id       TEXT NOT NULL REFERENCES market_registry(market_id),
    strategy_id     TEXT NOT NULL,
    ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    side            TEXT NOT NULL,
    price           NUMERIC NOT NULL,
    size            NUMERIC NOT NULL,
    fee_bp          NUMERIC NOT NULL,
    fee_usd         NUMERIC NOT NULL,
    is_maker        BOOLEAN NOT NULL DEFAULT FALSE,
    slippage_bp     NUMERIC,
    adverse_sel_bp  NUMERIC
);

SELECT create_hypertable('sim_fills', 'ts', if_not_exists => TRUE);

-- ─── Positions ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS positions (
    ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    strategy_id     TEXT NOT NULL,
    market_id       TEXT NOT NULL REFERENCES market_registry(market_id),
    net_size        NUMERIC NOT NULL DEFAULT 0,
    avg_entry_px    NUMERIC,
    unrealized_pnl  NUMERIC,
    realized_pnl    NUMERIC NOT NULL DEFAULT 0,
    funding_accrued NUMERIC NOT NULL DEFAULT 0,
    PRIMARY KEY (strategy_id, market_id)
);

-- ─── PnL Ledger ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pnl_ledger (
    id              BIGSERIAL PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    strategy_id     TEXT NOT NULL,
    event_type      TEXT NOT NULL,   -- 'fill','funding','fee','mark_to_market'
    market_id       TEXT,
    amount          NUMERIC NOT NULL,
    running_total   NUMERIC,
    notes           TEXT
);

SELECT create_hypertable('pnl_ledger', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_pnl_strategy ON pnl_ledger(strategy_id, ts DESC);

-- ─── Experiment Results ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS experiment_results (
    id              BIGSERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    hypothesis      TEXT NOT NULL,   -- 'A','B','C','D'
    strategy_id     TEXT,
    run_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    params          JSONB,
    metrics         JSONB,
    verdict         TEXT,            -- 'pass','fail','inconclusive'
    notes           TEXT
);

CREATE INDEX IF NOT EXISTS idx_exp_hypothesis ON experiment_results(hypothesis, run_at DESC);

-- ─── Data Gap Log ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS data_gaps (
    id              BIGSERIAL PRIMARY KEY,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    market_id       TEXT,
    data_type       TEXT NOT NULL,   -- 'quotes','trades','funding','reference'
    gap_start       TIMESTAMPTZ NOT NULL,
    gap_end         TIMESTAMPTZ,
    duration_s      NUMERIC,
    resolved        BOOLEAN NOT NULL DEFAULT FALSE
);
