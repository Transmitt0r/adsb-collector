-- migrate:up

-- Bulk aircraft database downloaded daily from ADSBx
CREATE TABLE bulk_aircraft (
    hex         TEXT        NOT NULL PRIMARY KEY,
    registration TEXT,
    icao_type   TEXT,
    model       TEXT,
    operator    TEXT,
    short_type  TEXT,   -- ADSBx category: L2J, H1T, etc. First char: L=large, H=heli, G=glider
    mil         BOOLEAN NOT NULL DEFAULT false,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Track which callsign was active when enrichment happened.
-- NULL = aircraft was not broadcasting a callsign at enrichment time.
-- Used to re-trigger enrichment when a callsign becomes available.
ALTER TABLE enriched_aircraft ADD COLUMN callsign TEXT;

-- migrate:down
DROP TABLE IF EXISTS bulk_aircraft;
ALTER TABLE enriched_aircraft DROP COLUMN IF EXISTS callsign;
