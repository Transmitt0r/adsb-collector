-- migrate:up
ALTER TABLE callsign_routes
    ADD COLUMN origin_lat DOUBLE PRECISION,
    ADD COLUMN origin_lon DOUBLE PRECISION,
    ADD COLUMN dest_lat DOUBLE PRECISION,
    ADD COLUMN dest_lon DOUBLE PRECISION;

-- migrate:down
ALTER TABLE callsign_routes
    DROP COLUMN origin_lat,
    DROP COLUMN origin_lon,
    DROP COLUMN dest_lat,
    DROP COLUMN dest_lon;
