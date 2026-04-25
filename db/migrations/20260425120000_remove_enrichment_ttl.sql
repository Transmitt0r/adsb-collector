-- migrate:up
ALTER TABLE enriched_aircraft DROP COLUMN expires_at;

-- migrate:down
ALTER TABLE enriched_aircraft ADD COLUMN expires_at TIMESTAMPTZ NOT NULL DEFAULT '2099-12-31';
