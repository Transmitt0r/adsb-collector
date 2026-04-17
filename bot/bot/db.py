"""Bot database: user registration, digest cache, and enrichment helpers."""

from __future__ import annotations

import logging
from datetime import datetime

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

_SQUAWK_MEANINGS = {
    "7700": "General emergency",
    "7600": "Radio communication failure",
    "7500": "Hijack declared",
}


def get_conn(database_url: str):
    return psycopg2.connect(database_url)


def init_schema(database_url: str) -> None:
    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id   BIGINT PRIMARY KEY,
                username  TEXT,
                registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                active    BOOLEAN NOT NULL DEFAULT true
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS digests (
                id           SERIAL PRIMARY KEY,
                period_start TIMESTAMPTZ NOT NULL,
                period_end   TIMESTAMPTZ NOT NULL,
                content      TEXT NOT NULL,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)
        # Enrichment columns on aircraft (idempotent for existing installs)
        cur.execute("""
            ALTER TABLE aircraft
                ADD COLUMN IF NOT EXISTS registration  TEXT,
                ADD COLUMN IF NOT EXISTS type          TEXT,
                ADD COLUMN IF NOT EXISTS operator      TEXT,
                ADD COLUMN IF NOT EXISTS flag          TEXT,
                ADD COLUMN IF NOT EXISTS fetched_at    TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS story_score   INT,
                ADD COLUMN IF NOT EXISTS story_tags    TEXT[],
                ADD COLUMN IF NOT EXISTS lm_annotation TEXT,
                ADD COLUMN IF NOT EXISTS enriched_at   TIMESTAMPTZ
        """)
        cur.execute("""
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
            )
        """)
    logger.info("Bot database schema initialized")


def register_user(database_url: str, chat_id: int, username: str | None) -> bool:
    """Register a user. Returns True if newly registered."""
    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO users (chat_id, username, active)
            VALUES (%s, %s, true)
            ON CONFLICT (chat_id) DO UPDATE
                SET active = true, username = EXCLUDED.username
            RETURNING (xmax = 0) AS inserted
        """,
            (chat_id, username),
        )
        row = cur.fetchone()
        return bool(row and row[0])


def unregister_user(database_url: str, chat_id: int) -> bool:
    """Unregister a user. Returns True if was active."""
    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE users SET active = false
            WHERE chat_id = %s AND active = true
        """,
            (chat_id,),
        )
        return cur.rowcount > 0


def get_active_users(database_url: str) -> list[int]:
    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT chat_id FROM users WHERE active = true")
        return [row[0] for row in cur.fetchall()]


def get_cached_digest(database_url: str, period_start: datetime, period_end: datetime):
    """Return cached DigestOutput for this period if it exists."""
    from .agent import DigestOutput  # local import to avoid circular dependency

    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT content FROM digests
            WHERE period_start = %s AND period_end = %s
            ORDER BY created_at DESC LIMIT 1
        """,
            (period_start, period_end),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return DigestOutput.model_validate_json(row[0])


def cache_digest(
    database_url: str, period_start: datetime, period_end: datetime, digest
) -> None:
    """Store a generated digest."""
    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO digests (period_start, period_end, content)
            VALUES (%s, %s, %s)
        """,
            (period_start, period_end, digest.model_dump_json()),
        )


def get_unenriched_aircraft(
    database_url: str, limit: int = 50
) -> list[tuple[str, str | None]]:
    """Return (hex, most_recent_callsign) for aircraft needing enrichment.

    Includes rows where enriched_at IS NULL or older than 90 days.
    Ordered by last_seen DESC so recently-active aircraft are prioritised.
    """
    with get_conn(database_url) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT a.hex, s.callsign
            FROM aircraft a
            LEFT JOIN LATERAL (
                SELECT callsign FROM sightings
                WHERE hex = a.hex AND callsign IS NOT NULL
                ORDER BY started_at DESC LIMIT 1
            ) s ON true
            WHERE a.enriched_at IS NULL
               OR a.enriched_at < now() - interval '90 days'
            ORDER BY a.last_seen DESC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
        return [(row[0], row[1]) for row in cur.fetchall()]


def store_enrichment(
    database_url: str,
    hex_: str,
    callsign: str | None,
    aircraft_dict: dict,
    route_dict: dict | None,
    score_result: dict,
) -> None:
    """Persist enrichment data for one aircraft."""
    with get_conn(database_url) as conn, conn.cursor() as cur:
        # Update aircraft row (skip registration fields if lookup returned error)
        if "error" not in aircraft_dict:
            cur.execute(
                """
                UPDATE aircraft SET
                    registration  = %(registration)s,
                    type          = %(type)s,
                    operator      = %(operator)s,
                    flag          = %(flag)s,
                    fetched_at    = now(),
                    story_score   = %(score)s,
                    story_tags    = %(tags)s,
                    lm_annotation = %(annotation)s,
                    enriched_at   = now()
                WHERE hex = %(hex)s
                """,
                {
                    "hex": hex_,
                    "registration": aircraft_dict.get("registration"),
                    "type": aircraft_dict.get("type"),
                    "operator": aircraft_dict.get("operator"),
                    "flag": aircraft_dict.get("flag"),
                    "score": score_result.get("score"),
                    "tags": score_result.get("tags") or [],
                    "annotation": score_result.get("annotation") or "",
                },
            )
        else:
            # No registration data — still store score and mark enriched
            cur.execute(
                """
                UPDATE aircraft SET
                    story_score   = %(score)s,
                    story_tags    = %(tags)s,
                    lm_annotation = %(annotation)s,
                    enriched_at   = now()
                WHERE hex = %(hex)s
                """,
                {
                    "hex": hex_,
                    "score": score_result.get("score"),
                    "tags": score_result.get("tags") or [],
                    "annotation": score_result.get("annotation") or "",
                },
            )

        # Upsert callsign_routes if we have valid route data
        if callsign and route_dict and "error" not in route_dict:
            origin = route_dict.get("origin", {}) or {}
            dest = route_dict.get("destination", {}) or {}
            cur.execute(
                """
                INSERT INTO callsign_routes (
                    callsign, origin_iata, origin_icao, origin_city, origin_country,
                    dest_iata, dest_icao, dest_city, dest_country, fetched_at
                ) VALUES (
                    %(callsign)s, %(origin_iata)s, %(origin_icao)s,
                    %(origin_city)s, %(origin_country)s,
                    %(dest_iata)s, %(dest_icao)s, %(dest_city)s, %(dest_country)s,
                    now()
                )
                ON CONFLICT (callsign) DO UPDATE SET
                    origin_iata    = EXCLUDED.origin_iata,
                    origin_icao    = EXCLUDED.origin_icao,
                    origin_city    = EXCLUDED.origin_city,
                    origin_country = EXCLUDED.origin_country,
                    dest_iata      = EXCLUDED.dest_iata,
                    dest_icao      = EXCLUDED.dest_icao,
                    dest_city      = EXCLUDED.dest_city,
                    dest_country   = EXCLUDED.dest_country,
                    fetched_at     = now()
                """,
                {
                    "callsign": callsign.upper().strip(),
                    "origin_iata": origin.get("iata"),
                    "origin_icao": origin.get("icao"),
                    "origin_city": origin.get("city"),
                    "origin_country": origin.get("country"),
                    "dest_iata": dest.get("iata"),
                    "dest_icao": dest.get("icao"),
                    "dest_city": dest.get("city"),
                    "dest_country": dest.get("country"),
                },
            )


def get_digest_candidates(database_url: str, days: int = 7) -> list[dict]:
    """Return enriched aircraft seen in the last N days, sorted by story_score DESC.

    Joins sightings with aircraft enrichment and callsign_routes.
    Returns up to 20 candidates.
    """
    with get_conn(database_url) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                WITH recent AS (
                    SELECT
                        s.hex,
                        COUNT(*)                                    AS visit_count,
                        MIN(s.min_distance)                         AS closest_nm,
                        MAX(s.max_altitude)                         AS max_alt_ft,
                        MIN(s.started_at)                           AS first_seen,
                        MODE() WITHIN GROUP (ORDER BY s.callsign)   AS most_used_callsign
                    FROM sightings s
                    WHERE s.started_at > now() - (%(days)s || ' days')::interval
                    GROUP BY s.hex
                )
                SELECT
                    r.hex,
                    r.visit_count,
                    r.closest_nm,
                    r.max_alt_ft,
                    TO_CHAR(r.first_seen AT TIME ZONE 'Europe/Berlin', 'Dy HH24:MI')
                        AS first_seen_local,
                    r.most_used_callsign                AS callsign,
                    a.registration,
                    a.type,
                    a.operator,
                    a.flag,
                    a.story_score,
                    a.story_tags,
                    a.lm_annotation,
                    cr.origin_iata,
                    cr.origin_city,
                    cr.origin_country,
                    cr.dest_iata,
                    cr.dest_city,
                    cr.dest_country
                FROM recent r
                JOIN aircraft a ON a.hex = r.hex
                LEFT JOIN callsign_routes cr ON cr.callsign = r.most_used_callsign
                ORDER BY a.story_score DESC NULLS LAST, r.visit_count DESC
                LIMIT 20
                """,
                {"days": days},
            )
            return [dict(row) for row in cur.fetchall()]


def get_digest_stats(database_url: str, days: int = 7) -> dict:
    """Return aggregate stats for the digest: counts, peak hour, squawk alerts."""
    with get_conn(database_url) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COUNT(*)            AS total_sightings,
                    COUNT(DISTINCT hex) AS unique_aircraft
                FROM sightings
                WHERE started_at > now() - (%(days)s || ' days')::interval
                """,
                {"days": days},
            )
            counts = dict(cur.fetchone())

            cur.execute(
                """
                SELECT COUNT(*) AS new_aircraft
                FROM aircraft
                WHERE first_seen > now() - (%(days)s || ' days')::interval
                """,
                {"days": days},
            )
            new_aircraft = int(cur.fetchone()["new_aircraft"])

            cur.execute(
                """
                SELECT EXTRACT(HOUR FROM started_at AT TIME ZONE 'Europe/Berlin')::int AS hr,
                       COUNT(*) AS cnt
                FROM sightings
                WHERE started_at > now() - (%(days)s || ' days')::interval
                GROUP BY hr
                ORDER BY cnt DESC
                LIMIT 1
                """,
                {"days": days},
            )
            peak_row = cur.fetchone()
            peak_hour = int(peak_row["hr"]) if peak_row else None
            peak_count = int(peak_row["cnt"]) if peak_row else None

            cur.execute(
                """
                SELECT DISTINCT ON (hex, squawk)
                    time AT TIME ZONE 'Europe/Berlin' AS time_local,
                    hex,
                    squawk
                FROM position_updates
                WHERE time > now() - (%(days)s || ' days')::interval
                  AND squawk = ANY(%(codes)s)
                ORDER BY hex, squawk, time DESC
                """,
                {"days": days, "codes": list(_SQUAWK_MEANINGS.keys())},
            )
            squawk_alerts = []
            for row in cur.fetchall():
                squawk_alerts.append(
                    {
                        "time": row["time_local"].strftime("%a %H:%M")
                        if row["time_local"]
                        else "",
                        "hex": row["hex"],
                        "squawk": row["squawk"],
                        "meaning": _SQUAWK_MEANINGS.get(row["squawk"], "Unknown"),
                    }
                )

    return {
        "total_sightings": int(counts["total_sightings"]),
        "unique_aircraft": int(counts["unique_aircraft"]),
        "new_aircraft": new_aircraft,
        "peak_hour": peak_hour,
        "peak_count": peak_count,
        "squawk_alerts": squawk_alerts,
    }
