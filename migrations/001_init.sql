-- Dimension: the asset registry, loaded once at service start from the
-- checked-in Trust Wallet snapshot. One row per asset id. Symbols are not
-- unique, which is why price snapshots reference symbols, not asset ids. An
-- asset that leaves the snapshot is stamped removed_at, never deleted, so
-- historical prices still join to it; a re-added asset is revived by the upsert.
CREATE TABLE IF NOT EXISTS assets (
    asset_id          TEXT        PRIMARY KEY,
    coin_id           INTEGER     NOT NULL,
    chain             TEXT        NOT NULL,
    contract_address  TEXT,
    symbol            TEXT        NOT NULL,
    name              TEXT        NOT NULL,
    decimals          INTEGER     NOT NULL,
    token_type        TEXT        NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    removed_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS assets_symbol_upper_idx ON assets (upper(symbol));

-- Raw layer: one row per HTTP request, success or failure. kind says which of
-- the source's two requests a row is: 'pairs' (exchangeInfo, which registry
-- symbols trade against the quote asset) or 'tickers' (the 24-hour ticker for
-- those pairs); snapshots descend from 'tickers' rows only. The body is JSONB:
-- the full content as parsed JSON, not the wire bytes (key order and
-- whitespace are normalised). A failed request keeps a body only when the
-- source sent a JSON error, has http_status 0 when no response arrived, and
-- carries the error text. This is the audit log and the replay point; nothing
-- is ever updated or deleted here.
CREATE TABLE IF NOT EXISTS raw_fetches (
    id               BIGSERIAL   PRIMARY KEY,
    fetched_at       TIMESTAMPTZ NOT NULL,
    source           TEXT        NOT NULL,
    kind             TEXT        NOT NULL CHECK (kind IN ('pairs', 'tickers')),
    endpoint         TEXT        NOT NULL,
    http_status      INTEGER     NOT NULL,
    request          JSONB       NOT NULL,
    response         JSONB,
    duration_ms      INTEGER     NOT NULL,
    attempts         INTEGER     NOT NULL,
    requested_count  INTEGER     NOT NULL,
    returned_count   INTEGER     NOT NULL,
    outcome          TEXT        NOT NULL CHECK (outcome IN ('success', 'failure')),
    error            TEXT,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS raw_fetches_fetched_at_idx ON raw_fetches (fetched_at);

-- Processed layer. Grain: one row per symbol per poll; (raw_fetch_id, symbol)
-- is the stable key. asset_id is the registry asset the symbol was verified
-- against in reference/symbol_map.csv, so every row carries its identity and
-- consumers join on it, never on the symbol. source_time is when Binance last
-- updated the pair, fetched_at when this pipeline received it; last_trade_id
-- tells a new trade from a repeated observation. The price is the source
-- value as sent. The surrogate id is monotonic and drives the checkpointed
-- export; a replay rewrite issues new ones, so file readers dedupe on the
-- stable key with the latest loaded_at.
CREATE TABLE IF NOT EXISTS price_snapshots (
    id               BIGSERIAL   NOT NULL UNIQUE,
    raw_fetch_id     BIGINT      NOT NULL REFERENCES raw_fetches (id),
    asset_id         TEXT        NOT NULL REFERENCES assets (asset_id),
    symbol           TEXT        NOT NULL,
    quote_currency   TEXT        NOT NULL,
    price            NUMERIC     NOT NULL CHECK (price > 0),
    source_time      TIMESTAMPTZ NOT NULL,
    last_trade_id    BIGINT      NOT NULL,
    fetched_at       TIMESTAMPTZ NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (raw_fetch_id, symbol)
);

CREATE INDEX IF NOT EXISTS price_snapshots_symbol_fetched_at_idx ON price_snapshots (symbol, fetched_at);
CREATE INDEX IF NOT EXISTS price_snapshots_asset_fetched_at_idx ON price_snapshots (asset_id, fetched_at);

-- Export progress per dataset. Order of operations is: write file, then advance
-- the checkpoint. A crash in between replays at most one chunk into the file.
CREATE TABLE IF NOT EXISTS export_checkpoints (
    name        TEXT        PRIMARY KEY,
    last_id     BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
