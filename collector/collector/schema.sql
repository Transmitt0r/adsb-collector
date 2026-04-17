-- FlightTracker database schema
-- Requires: PostgreSQL 14+ with TimescaleDB extension

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Aircraft registry: one row per unique ICAO hex address
CREATE TABLE IF NOT EXISTS aircraft (
    hex             TEXT PRIMARY KEY,
    first_seen      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT now(),
    callsigns       TEXT[] NOT NULL DEFAULT '{}',
    -- Enrichment fields (populated by bot enrichment job)
    registration    TEXT,
    type            TEXT,
    operator        TEXT,
    flag            TEXT,
    fetched_at      TIMESTAMPTZ,
    story_score     INT,
    story_tags      TEXT[],
    lm_annotation   TEXT,
    enriched_at     TIMESTAMPTZ
);

-- Callsign route cache: origin/destination per flight callsign
CREATE TABLE IF NOT EXISTS callsign_routes (
    callsign        TEXT PRIMARY KEY,
    origin_iata     TEXT,
    origin_icao     TEXT,
    origin_city     TEXT,
    origin_country  TEXT,
    dest_iata       TEXT,
    dest_icao       TEXT,
    dest_city       TEXT,
    dest_country    TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Sightings: one row per continuous observation session of an aircraft
CREATE TABLE IF NOT EXISTS sightings (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    hex             TEXT NOT NULL REFERENCES aircraft(hex),
    callsign        TEXT,
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ,
    min_altitude    INT,
    max_altitude    INT,
    min_distance    DOUBLE PRECISION,
    max_distance    DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_sightings_hex ON sightings(hex);
CREATE INDEX IF NOT EXISTS idx_sightings_started_at ON sightings(started_at DESC);

-- Position updates: high-frequency position samples (TimescaleDB hypertable)
CREATE TABLE IF NOT EXISTS position_updates (
    time        TIMESTAMPTZ NOT NULL,
    hex         TEXT NOT NULL,
    lat         DOUBLE PRECISION,
    lon         DOUBLE PRECISION,
    alt_baro    INT,
    gs          DOUBLE PRECISION,
    track       DOUBLE PRECISION,
    squawk      TEXT,
    rssi        DOUBLE PRECISION
);

SELECT create_hypertable(
    'position_updates', 'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_position_updates_hex_time
    ON position_updates(hex, time DESC);

-- Compression policy: compress chunks older than 7 days
ALTER TABLE position_updates
    SET (timescaledb.compress,
         timescaledb.compress_segmentby = 'hex',
         timescaledb.compress_orderby = 'time DESC');

SELECT add_compression_policy('position_updates', INTERVAL '7 days',
                              if_not_exists => TRUE);
