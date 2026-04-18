-- Cutover migration: v1 schema (collector + bot) → squawk
-- Run this MANUALLY on the production DB before deploying squawk.
-- Do NOT run dbmate — the prod DB has no schema_migrations table.
--
-- After running this, stamp dbmate so the entrypoint's `dbmate up` is a no-op.

BEGIN;

-- 1. Create enriched_aircraft (new table, doesn't exist in v1)
CREATE TABLE enriched_aircraft (
    hex           TEXT        PRIMARY KEY REFERENCES aircraft(hex),
    registration  TEXT,
    type          TEXT,
    operator      TEXT,
    flag          TEXT,
    story_score   INT,
    story_tags    TEXT[]      NOT NULL DEFAULT '{}',
    annotation    TEXT,
    enriched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL
);

-- 2. Add last_seen to sightings with backfill
ALTER TABLE sightings ADD COLUMN last_seen TIMESTAMPTZ;
UPDATE sightings SET last_seen = COALESCE(ended_at, started_at) WHERE last_seen IS NULL;
ALTER TABLE sightings ALTER COLUMN last_seen SET NOT NULL;

-- 3. Drop enrichment columns from aircraft (moved to enriched_aircraft)
ALTER TABLE aircraft
    DROP COLUMN registration,
    DROP COLUMN type,
    DROP COLUMN operator,
    DROP COLUMN flag,
    DROP COLUMN fetched_at,
    DROP COLUMN story_score,
    DROP COLUMN story_tags,
    DROP COLUMN lm_annotation,
    DROP COLUMN enriched_at;

-- 4. Recreate digests with new schema (old: period_start/period_end; new: reference_date/n_days)
DROP TABLE digests;
CREATE TABLE digests (
    id              BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    reference_date  DATE        NOT NULL,
    n_days          INT         NOT NULL,
    content         TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (reference_date, n_days)
);

-- 5. Add retention policy (not in old collector)
SELECT add_retention_policy('position_updates', INTERVAL '90 days');

COMMIT;

-- 6. Stamp dbmate (outside transaction — DDL)
CREATE TABLE schema_migrations (version TEXT NOT NULL PRIMARY KEY);
INSERT INTO schema_migrations VALUES ('20260417192449'), ('20260417192450');
