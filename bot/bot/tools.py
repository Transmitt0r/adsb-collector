"""ADK agent tools: flight data queries and aircraft lookup."""

from __future__ import annotations

import json
import logging
from typing import Literal

import psycopg2
import psycopg2.extras
import requests

logger = logging.getLogger(__name__)

_SQUAWK_MEANINGS = {
    "7700": "General emergency",
    "7600": "Radio communication failure",
    "7500": "Hijack declared",
}

_SORT_COLUMNS = {
    "closest": ("s.min_distance", "ASC NULLS LAST"),
    "highest": ("s.max_altitude", "DESC NULLS LAST"),
    "lowest": ("s.min_altitude", "ASC NULLS LAST"),
    "longest": ("duration_minutes", "DESC NULLS LAST"),
    "recent": ("s.started_at", "DESC"),
}

# Known cargo airline ICAO prefixes
_CARGO_PREFIXES = {
    "UPS",
    "FDX",
    "GTI",
    "CLX",
    "TAY",
    "BCS",
    "DHK",
    "DHL",
    "ABX",
    "ATN",
    "GEC",
    "MPH",
    "PAC",
    "SWN",
    "TTF",
}

# Known military callsign prefixes
_MILITARY_CALLSIGNS = {
    "RCH",
    "RRR",
    "DUKE",
    "REACH",
    "MAGMA",
    "NATO",
    "CASA",
    "GAF",
    "BAF",
    "FAF",
    "GLEX",
    "DRGN",
    "TALO",
}

# ICAO hex ranges for military aircraft (lower-cased)
_MILITARY_HEX_RANGES = [
    ("ae0000", "afffff"),  # US military
    ("43c000", "43cfff"),  # UK military
    ("3c4000", "3c7fff"),  # German military (Luftwaffe)
    ("3a0000", "3a7fff"),  # French military
    ("480000", "4803ff"),  # Italian military
]


def lookup_aircraft(icao_hex: str) -> str:
    """Look up registration, aircraft type, and operator for an ICAO hex code.

    Uses the public adsbdb.com API. Returns registration, type, icao_type,
    operator, country, or an error message.

    Args:
        icao_hex: The 6-character ICAO 24-bit hex address (e.g. "3c6444").
    """
    key = icao_hex.lower()
    try:
        url = f"https://api.adsbdb.com/v0/aircraft/{key}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 404:
            return json.dumps({"error": "aircraft not found in database"})
        resp.raise_for_status()
        data = resp.json()
        aircraft = data.get("response", {}).get("aircraft", {})
        return json.dumps(
            {
                "registration": aircraft.get("registration"),
                "type": aircraft.get("type"),
                "icao_type": aircraft.get("icao_type"),
                "operator": aircraft.get("registered_owner"),
                "country": aircraft.get("registered_owner_country_name"),
                "flag": aircraft.get("registered_owner_country_iso_name"),
            }
        )
    except Exception as exc:
        logger.exception("lookup_aircraft failed for %s", icao_hex)
        return json.dumps({"error": str(exc)})


def lookup_route(callsign: str) -> str:
    """Look up the origin and destination airports for a flight callsign.

    Uses the public adsbdb.com API. Returns origin and destination airport
    details (IATA/ICAO codes, city, country), or an error if unknown.

    Args:
        callsign: The flight callsign (e.g. "DLH123", "EZY4241").
    """
    key = callsign.upper().strip()
    try:
        url = f"https://api.adsbdb.com/v0/callsign/{key}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 404:
            return json.dumps({"error": "route not found in database"})
        resp.raise_for_status()
        data = resp.json()
        route = data.get("response", {}).get("flightroute", {})
        if not route:
            return json.dumps({"error": "no route data available"})

        def _airport(ap: dict) -> dict:
            return {
                "iata": ap.get("iata_code"),
                "icao": ap.get("icao_code"),
                "name": ap.get("name"),
                "city": ap.get("municipality"),
                "country": ap.get("country_name"),
            }

        return json.dumps(
            {
                "callsign": route.get("callsign"),
                "origin": _airport(route.get("origin", {})),
                "destination": _airport(route.get("destination", {})),
            }
        )
    except Exception as exc:
        logger.exception("lookup_route failed for %s", callsign)
        return json.dumps({"error": str(exc)})


def lookup_photo(icao_hex: str) -> str:
    """Look up a photo of an aircraft by its ICAO hex code.

    Uses the planespotters.net public API. Returns the direct image URL
    and photographer credit if a photo is available.

    Args:
        icao_hex: The 6-character ICAO 24-bit hex address (e.g. "3c6444").
    """
    key = icao_hex.lower()
    try:
        url = f"https://api.planespotters.net/pub/photos/hex/{key}"
        resp = requests.get(url, timeout=10, headers={"User-Agent": "squawk-bot/1.0"})
        if resp.status_code == 404:
            return json.dumps({"error": "no photo found"})
        resp.raise_for_status()
        data = resp.json()
        photos = data.get("photos", [])
        if not photos:
            return json.dumps({"error": "no photo found"})
        photo = photos[0]
        return json.dumps(
            {
                "photo_url": photo.get("thumbnail_large", {}).get("src"),
                "link": photo.get("link"),
                "photographer": photo.get("photographer"),
                "registration": photo.get("aircraft", {}).get("reg"),
            }
        )
    except Exception as exc:
        logger.exception("lookup_photo failed for %s", icao_hex)
        return json.dumps({"error": str(exc)})


def make_tools(collector_database_url: str) -> list:
    """Create agent tools closed over the collector DB URL."""

    def get_stats(days: int = 7) -> str:
        """Get aggregate statistics for the digest Fakten section.

        Returns counts only — no row lists — so it is very compact:
        - total_sightings, unique_aircraft, new_aircraft_count
        - top_operators: list of {prefix, count} sorted by frequency
        - squawk_alert_count: number of emergency squawk events

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT
                            COUNT(*)                          AS total_sightings,
                            COUNT(DISTINCT hex)               AS unique_aircraft,
                            COUNT(DISTINCT callsign)
                                FILTER (WHERE callsign IS NOT NULL) AS unique_callsigns
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                    """,
                        {"days": days},
                    )
                    counts = dict(cur.fetchone())

                    cur.execute(
                        """
                        SELECT LEFT(callsign, 3) AS prefix, COUNT(*) AS cnt
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                          AND callsign IS NOT NULL
                          AND LENGTH(callsign) >= 3
                          AND LEFT(callsign, 3) ~ '^[A-Z]{3}$'
                        GROUP BY prefix
                        ORDER BY cnt DESC
                        LIMIT 5
                    """,
                        {"days": days},
                    )
                    top_operators = [
                        {"prefix": r["prefix"], "count": int(r["cnt"])}
                        for r in cur.fetchall()
                    ]

                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM aircraft
                        WHERE first_seen > now() - (%(days)s || ' days')::interval
                    """,
                        {"days": days},
                    )
                    new_aircraft_count = int(cur.fetchone()["cnt"])

                    cur.execute(
                        """
                        SELECT COUNT(*) AS cnt
                        FROM position_updates
                        WHERE time > now() - (%(days)s || ' days')::interval
                          AND squawk = ANY(%(codes)s)
                    """,
                        {"days": days, "codes": list(_SQUAWK_MEANINGS.keys())},
                    )
                    squawk_alert_count = int(cur.fetchone()["cnt"])

            return json.dumps(
                {
                    **{k: int(v) for k, v in counts.items()},
                    "new_aircraft_count": new_aircraft_count,
                    "top_operators": top_operators,
                    "squawk_alert_count": squawk_alert_count,
                }
            )
        except Exception as exc:
            logger.exception("get_stats failed")
            return json.dumps({"error": str(exc)})

    def get_top_sightings(
        days: int = 7,
        sort_by: Literal["closest", "highest", "longest", "recent"] = "closest",
        limit: int = 10,
    ) -> str:
        """Get a ranked list of sightings from the past N days.

        Use this to find interesting flights to highlight. Call it multiple times
        with different sort_by values if you need different angles.

        sort_by options:
        - "closest":  nearest to the receiver (most likely overhead)
        - "highest":  highest altitude seen
        - "longest":  longest continuous observation session
        - "recent":   most recently seen

        Returns hex, callsign, started_at, duration_minutes, max_altitude (feet),
        min_distance (nautical miles) for each sighting.

        Args:
            days: How many days back to look (default 7).
            sort_by: Ranking criterion (default "closest").
            limit: Max rows to return, 1–20 (default 10).
        """
        if sort_by not in _SORT_COLUMNS:
            return json.dumps(
                {"error": f"sort_by must be one of {list(_SORT_COLUMNS)}"}
            )
        limit = max(1, min(limit, 20))
        col, direction = _SORT_COLUMNS[sort_by]

        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        f"""
                        SELECT
                            s.hex,
                            s.callsign,
                            s.started_at,
                            EXTRACT(EPOCH FROM (COALESCE(s.ended_at, now()) - s.started_at)) / 60
                                AS duration_minutes,
                            s.max_altitude,
                            s.min_distance
                        FROM sightings s
                        WHERE s.started_at > now() - (%(days)s || ' days')::interval
                        ORDER BY {col} {direction}
                        LIMIT %(limit)s
                    """,
                        {"days": days, "limit": limit},
                    )
                    rows = cur.fetchall()

            lines = ["hex,callsign,started_at_utc,duration_min,max_alt_ft,min_dist_nm"]
            for r in rows:
                lines.append(
                    f"{r['hex']},"
                    f"{r['callsign'] or ''},"
                    f"{r['started_at'].strftime('%Y-%m-%dT%H:%M') if r['started_at'] else ''},"
                    f"{round(float(r['duration_minutes']), 1) if r['duration_minutes'] else ''},"
                    f"{int(r['max_altitude']) if r['max_altitude'] else ''},"
                    f"{round(float(r['min_distance']), 1) if r['min_distance'] else ''}"
                )
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_top_sightings failed")
            return json.dumps({"error": str(exc)})

    def get_record(
        days: int = 7,
        record_type: Literal[
            "furthest", "highest", "fastest", "longest", "return_visitors"
        ] = "furthest",
    ) -> str:
        """Get a single record extreme from the past N days.

        Call once per record type you want to highlight. Each call returns
        a small, focused result.

        record_type options:
        - "furthest":        aircraft seen at greatest distance (nautical miles)
        - "highest":         aircraft seen at greatest altitude (feet)
        - "fastest":         aircraft with highest ground speed (knots)
        - "longest":         aircraft observed for longest continuous session (minutes)
        - "return_visitors": aircraft seen multiple times (top 5, sorted by visit count)

        Args:
            days: How many days back to look (default 7).
            record_type: Which record to fetch (default "furthest").
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    if record_type == "furthest":
                        cur.execute(
                            """
                            SELECT hex, callsign, max_distance AS value
                            FROM sightings
                            WHERE started_at > now() - (%(days)s || ' days')::interval
                              AND max_distance IS NOT NULL
                            ORDER BY max_distance DESC LIMIT 1
                        """,
                            {"days": days},
                        )
                        row = cur.fetchone()
                        return json.dumps(
                            {
                                "record_type": "furthest_nm",
                                **(dict(row) if row else {}),
                            },
                            default=str,
                        )

                    elif record_type == "highest":
                        cur.execute(
                            """
                            SELECT hex, callsign, max_altitude AS value
                            FROM sightings
                            WHERE started_at > now() - (%(days)s || ' days')::interval
                              AND max_altitude IS NOT NULL
                            ORDER BY max_altitude DESC LIMIT 1
                        """,
                            {"days": days},
                        )
                        row = cur.fetchone()
                        return json.dumps(
                            {"record_type": "highest_ft", **(dict(row) if row else {})},
                            default=str,
                        )

                    elif record_type == "fastest":
                        cur.execute(
                            """
                            WITH top AS (
                                SELECT hex, MAX(gs) AS value
                                FROM position_updates
                                WHERE time > now() - (%(days)s || ' days')::interval
                                  AND gs IS NOT NULL
                                GROUP BY hex ORDER BY value DESC LIMIT 1
                            )
                            SELECT t.hex, t.value, s.callsign
                            FROM top t
                            LEFT JOIN sightings s ON s.hex = t.hex
                                AND s.started_at > now() - (%(days)s || ' days')::interval
                            ORDER BY s.started_at DESC LIMIT 1
                        """,
                            {"days": days},
                        )
                        row = cur.fetchone()
                        return json.dumps(
                            {"record_type": "fastest_kt", **(dict(row) if row else {})},
                            default=str,
                        )

                    elif record_type == "longest":
                        cur.execute(
                            """
                            SELECT hex, callsign,
                                   EXTRACT(EPOCH FROM (COALESCE(ended_at, now()) - started_at)) / 60 AS value
                            FROM sightings
                            WHERE started_at > now() - (%(days)s || ' days')::interval
                            ORDER BY value DESC LIMIT 1
                        """,
                            {"days": days},
                        )
                        row = cur.fetchone()
                        return json.dumps(
                            {
                                "record_type": "longest_min",
                                **(dict(row) if row else {}),
                            },
                            default=str,
                        )

                    elif record_type == "return_visitors":
                        cur.execute(
                            """
                            SELECT hex, COUNT(*) AS visit_count,
                                   MODE() WITHIN GROUP (ORDER BY callsign) AS callsign
                            FROM sightings
                            WHERE started_at > now() - (%(days)s || ' days')::interval
                            GROUP BY hex HAVING COUNT(*) > 1
                            ORDER BY visit_count DESC LIMIT 5
                        """,
                            {"days": days},
                        )
                        rows = cur.fetchall()
                        return json.dumps(
                            {
                                "record_type": "return_visitors",
                                "visitors": [dict(r) for r in rows],
                            },
                            default=str,
                        )

                    return json.dumps({"error": f"unknown record_type: {record_type}"})
        except Exception as exc:
            logger.exception("get_record failed")
            return json.dumps({"error": str(exc)})

    def get_new_aircraft(days: int = 7) -> str:
        """Get aircraft seen by our receiver for the very first time during the past N days.

        Returns a compact CSV list: hex, callsign, first_seen_utc.
        Total count is also returned. Use lookup_aircraft(hex) to get full
        details (type, operator, registration) for any that look interesting.

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT a.hex, s.callsign, a.first_seen
                        FROM aircraft a
                        LEFT JOIN LATERAL (
                            SELECT callsign FROM sightings
                            WHERE hex = a.hex
                              AND callsign IS NOT NULL
                            ORDER BY started_at DESC LIMIT 1
                        ) s ON true
                        WHERE a.first_seen > now() - (%(days)s || ' days')::interval
                        ORDER BY a.first_seen DESC
                        LIMIT 20
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()

                    cur.execute(
                        """
                        SELECT COUNT(*) FROM aircraft
                        WHERE first_seen > now() - (%(days)s || ' days')::interval
                    """,
                        {"days": days},
                    )
                    total = cur.fetchone()[0]

            lines = ["hex,callsign,first_seen_utc"]
            for hex_, callsign, first_seen in rows:
                lines.append(
                    f"{hex_},{callsign or ''},"
                    f"{first_seen.strftime('%Y-%m-%dT%H:%M') if first_seen else ''}"
                )

            return f"total_new:{total}\n" + "\n".join(lines)
        except Exception as exc:
            logger.exception("get_new_aircraft failed")
            return json.dumps({"error": str(exc)})

    def get_squawk_alerts(days: int = 7) -> str:
        """Check if any aircraft broadcast emergency squawk codes while over our area.

        Emergency squawk codes: 7700 (general emergency), 7600 (radio failure), 7500 (hijack).
        If alerts exist, treat them as the lead story of the digest.

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT DISTINCT ON (hex, squawk) time, hex, squawk
                        FROM position_updates
                        WHERE time > now() - (%(days)s || ' days')::interval
                          AND squawk = ANY(%(codes)s)
                        ORDER BY hex, squawk, time DESC
                    """,
                        {"days": days, "codes": list(_SQUAWK_MEANINGS.keys())},
                    )
                    rows = cur.fetchall()

            alerts = []
            for r in rows:
                d = dict(r)
                d["meaning"] = _SQUAWK_MEANINGS.get(d["squawk"], "Unknown")
                if d.get("time"):
                    d["time"] = d["time"].isoformat()
                alerts.append(d)

            return json.dumps(
                {"alert_count": len(alerts), "alerts": alerts}, default=str
            )
        except Exception as exc:
            logger.exception("get_squawk_alerts failed")
            return json.dumps({"error": str(exc)})

    def get_night_flights(days: int = 7) -> str:
        """Get flights observed between 22:00 and 06:00 local time (Europe/Berlin).

        Night flights are often cargo jets, red-eye passenger routes, or unusual
        ferry/positioning flights — great story material.

        Returns CSV: hex, callsign, started_at_local, max_altitude_ft, min_distance_nm

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            hex,
                            callsign,
                            TO_CHAR(started_at AT TIME ZONE 'Europe/Berlin',
                                    'Dy HH24:MI') AS started_local,
                            max_altitude,
                            min_distance
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                          AND EXTRACT(HOUR FROM started_at AT TIME ZONE 'Europe/Berlin')
                              NOT BETWEEN 6 AND 21
                        ORDER BY started_at DESC
                        LIMIT 20
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()
            lines = ["hex,callsign,started_local,max_alt_ft,min_dist_nm"]
            for r in rows:
                lines.append(
                    f"{r[0]},{r[1] or ''},{r[2]},"
                    f"{r[3] or ''},{round(r[4], 1) if r[4] else ''}"
                )
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_night_flights failed")
            return json.dumps({"error": str(exc)})

    def get_silent_aircraft(days: int = 7) -> str:
        """Get aircraft observed without a callsign — potentially military, private,
        or ferry flights.

        Returns CSV: hex, first_seen_local, max_altitude_ft, min_distance_nm.
        Use lookup_aircraft to investigate interesting ones.

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            hex,
                            TO_CHAR(MIN(started_at) AT TIME ZONE 'Europe/Berlin',
                                    'Dy HH24:MI') AS first_seen_local,
                            MAX(max_altitude)  AS max_alt,
                            MIN(min_distance)  AS min_dist
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                          AND (callsign IS NULL OR callsign = '')
                        GROUP BY hex
                        ORDER BY min_dist ASC NULLS LAST
                        LIMIT 15
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()
            lines = ["hex,first_seen_local,max_alt_ft,min_dist_nm"]
            for r in rows:
                lines.append(
                    f"{r[0]},{r[1]},{r[2] or ''},{round(r[3], 1) if r[3] else ''}"
                )
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_silent_aircraft failed")
            return json.dumps({"error": str(exc)})

    def get_altitude_bands(days: int = 7) -> str:
        """Get distribution of flights by altitude bracket.

        Brackets (in feet):
          ground:     alt_baro = "ground" or < 1,000 ft
          low:        1,000 – 10,000 ft   (~300 – 3,000 m)
          medium:    10,000 – 25,000 ft   (~3,000 – 7,600 m)
          high:      25,000 – 40,000 ft   (~7,600 – 12,200 m)
          very_high: > 40,000 ft          (> 12,200 m)

        Useful for spotting unusual low-altitude traffic or record-high cruising.

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            CASE
                                WHEN max_altitude IS NULL        THEN 'unknown'
                                WHEN max_altitude < 1000         THEN 'ground_or_low'
                                WHEN max_altitude < 10000        THEN 'low'
                                WHEN max_altitude < 25000        THEN 'medium'
                                WHEN max_altitude < 40000        THEN 'high'
                                ELSE 'very_high'
                            END AS band,
                            COUNT(*) AS flights
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                        GROUP BY band
                        ORDER BY MIN(COALESCE(max_altitude, 0))
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()
            lines = ["band,flights"]
            for r in rows:
                lines.append(f"{r[0]},{r[1]}")
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_altitude_bands failed")
            return json.dumps({"error": str(exc)})

    def get_speed_outliers(days: int = 7) -> str:
        """Get unusually fast or slow aircraft from position data.

        Fast (> 550 kt): likely military jets or unusual high-speed aircraft.
        Slow (< 120 kt): prop planes, helicopters, or very slow movers.

        Returns CSV: hex, callsign, max_gs_kt or min_gs_kt, type.

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH fast AS (
                            SELECT p.hex, s.callsign, MAX(p.gs) AS gs, 'fast' AS type
                            FROM position_updates p
                            LEFT JOIN LATERAL (
                                SELECT callsign FROM sightings
                                WHERE hex = p.hex
                                  AND started_at > now() - (%(days)s || ' days')::interval
                                ORDER BY started_at DESC LIMIT 1
                            ) s ON true
                            WHERE p.time > now() - (%(days)s || ' days')::interval
                              AND p.gs > 550
                            GROUP BY p.hex, s.callsign
                            ORDER BY gs DESC LIMIT 5
                        ),
                        slow AS (
                            SELECT p.hex, s.callsign, MIN(p.gs) AS gs, 'slow' AS type
                            FROM position_updates p
                            LEFT JOIN LATERAL (
                                SELECT callsign FROM sightings
                                WHERE hex = p.hex
                                  AND started_at > now() - (%(days)s || ' days')::interval
                                ORDER BY started_at DESC LIMIT 1
                            ) s ON true
                            WHERE p.time > now() - (%(days)s || ' days')::interval
                              AND p.gs > 0 AND p.gs < 120
                            GROUP BY p.hex, s.callsign
                            ORDER BY gs ASC LIMIT 5
                        )
                        SELECT * FROM fast UNION ALL SELECT * FROM slow
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()
            lines = ["hex,callsign,gs_kt,type"]
            for r in rows:
                lines.append(f"{r[0]},{r[1] or ''},{round(r[2], 0)},{r[3]}")
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_speed_outliers failed")
            return json.dumps({"error": str(exc)})

    def get_busy_slots(days: int = 7) -> str:
        """Get the peak 1-hour traffic slot for each day in the past N days.

        Useful for sentences like "Mittwoch war um 18:00 Uhr am vollsten mit 12 Flügen."

        Returns CSV: date, peak_hour_local, flights_in_that_hour

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH hourly AS (
                            SELECT
                                DATE(started_at AT TIME ZONE 'Europe/Berlin') AS day,
                                EXTRACT(HOUR FROM started_at AT TIME ZONE 'Europe/Berlin')::int AS hr,
                                COUNT(*) AS cnt
                            FROM sightings
                            WHERE started_at > now() - (%(days)s || ' days')::interval
                            GROUP BY day, hr
                        )
                        SELECT DISTINCT ON (day)
                            TO_CHAR(day, 'Dy DD.MM.') AS day_label,
                            hr,
                            cnt
                        FROM hourly
                        ORDER BY day, cnt DESC
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()
            lines = ["day,peak_hour,flights"]
            for r in rows:
                lines.append(f"{r[0]},{r[1]:02d}:00,{r[2]}")
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_busy_slots failed")
            return json.dumps({"error": str(exc)})

    def get_sightings_by_category(
        days: int = 7,
        category: Literal[
            "airline", "cargo", "military", "private", "general_aviation"
        ] = "airline",
    ) -> str:
        """Get sightings filtered by aircraft category.

        Categories:
          airline:          scheduled passenger carriers (3-letter ICAO + flight number callsign)
          cargo:            known cargo operators (UPS, FedEx, DHL, Atlas Air, etc.)
          military:         known military hex ranges or callsign patterns
          private:          no callsign, or registration-style callsign (business jets, etc.)
          general_aviation: slow, low-altitude flights not matching other categories

        Returns CSV: hex, callsign, started_at_local, max_alt_ft, min_dist_nm

        Args:
            days: How many days back to look (default 7).
            category: Aircraft category to filter by (default "airline").
        """
        try:
            cargo_list = list(_CARGO_PREFIXES)
            mil_callsigns = list(_MILITARY_CALLSIGNS)
            mil_ranges_sql = " OR ".join(
                f"(hex >= '{lo}' AND hex <= '{hi}')" for lo, hi in _MILITARY_HEX_RANGES
            )

            where_clauses = {
                "airline": (
                    "callsign ~ '^[A-Z]{3}[0-9]' "
                    "AND LEFT(callsign,3) != ALL(%(cargo)s) "
                    "AND LEFT(callsign,3) != ALL(%(mil_cs)s)"
                ),
                "cargo": "LEFT(callsign,3) = ANY(%(cargo)s)",
                "military": (
                    f"({mil_ranges_sql}) OR LEFT(callsign,3) = ANY(%(mil_cs)s)"
                ),
                "private": (
                    "(callsign IS NULL OR callsign = '' "
                    " OR callsign !~ '^[A-Z]{3}[0-9]')"
                    " AND NOT (LEFT(callsign,3) = ANY(%(cargo)s))"
                    f" AND NOT ({mil_ranges_sql})"
                ),
                "general_aviation": (
                    "max_altitude < 15000 "
                    "AND (callsign IS NULL OR callsign !~ '^[A-Z]{3}[0-9]')"
                ),
            }

            clause = where_clauses.get(category)
            if not clause:
                return json.dumps({"error": f"unknown category: {category}"})

            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            hex,
                            callsign,
                            TO_CHAR(started_at AT TIME ZONE 'Europe/Berlin',
                                    'Dy HH24:MI') AS started_local,
                            max_altitude,
                            min_distance
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                          AND {clause}
                        ORDER BY min_distance ASC NULLS LAST
                        LIMIT 20
                    """,
                        {"days": days, "cargo": cargo_list, "mil_cs": mil_callsigns},
                    )
                    rows = cur.fetchall()

            lines = ["hex,callsign,started_local,max_alt_ft,min_dist_nm"]
            for r in rows:
                lines.append(
                    f"{r[0]},{r[1] or ''},{r[2]},"
                    f"{r[3] or ''},{round(r[4], 1) if r[4] else ''}"
                )
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_sightings_by_category failed")
            return json.dumps({"error": str(exc)})

    def get_operator_breakdown(days: int = 7) -> str:
        """Per-airline statistics: flight count, avg/min altitude, closest approach.

        Great for sentences like "Ryanair war 10× da, immer auf 9.000–11.000 m,
        nächster Überflug nur 4 km entfernt."

        Returns CSV: operator_prefix, flights, avg_alt_ft, min_alt_ft, max_alt_ft,
                     closest_nm, avg_dist_nm

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            LEFT(callsign, 3)            AS prefix,
                            COUNT(*)                     AS flights,
                            ROUND(AVG(max_altitude))     AS avg_alt,
                            MIN(max_altitude)            AS min_alt,
                            MAX(max_altitude)            AS max_alt,
                            ROUND(MIN(min_distance)::numeric, 2) AS closest,
                            ROUND(AVG(min_distance)::numeric, 1) AS avg_dist
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                          AND callsign IS NOT NULL
                          AND callsign ~ '^[A-Z]{3}[0-9]'
                        GROUP BY prefix
                        ORDER BY flights DESC
                        LIMIT 15
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()
            lines = [
                "prefix,flights,avg_alt_ft,min_alt_ft,max_alt_ft,closest_nm,avg_dist_nm"
            ]
            for r in rows:
                lines.append(",".join(str(v or "") for v in r))
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_operator_breakdown failed")
            return json.dumps({"error": str(exc)})

    def get_low_passes(days: int = 7, max_alt_ft: int = 5000) -> str:
        """Get flights that came in low — under max_alt_ft feet.

        Catches helicopters, military low-level, VFR traffic, and approach traffic.
        Default threshold is 5,000 ft (~1,500 m). These make great story material.

        Returns CSV: hex, callsign, started_local, max_alt_ft, min_dist_nm

        Args:
            days: How many days back to look (default 7).
            max_alt_ft: Altitude ceiling in feet (default 5000).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            hex, callsign,
                            TO_CHAR(started_at AT TIME ZONE 'Europe/Berlin',
                                    'Dy HH24:MI') AS started_local,
                            max_altitude, min_distance
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                          AND max_altitude IS NOT NULL
                          AND max_altitude < %(max_alt)s
                          AND max_altitude > 100
                        ORDER BY min_distance ASC NULLS LAST
                        LIMIT 15
                    """,
                        {"days": days, "max_alt": max_alt_ft},
                    )
                    rows = cur.fetchall()
            lines = ["hex,callsign,started_local,max_alt_ft,min_dist_nm"]
            for r in rows:
                lines.append(
                    f"{r[0]},{r[1] or ''},{r[2]},"
                    f"{r[3] or ''},{round(r[4], 2) if r[4] else ''}"
                )
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_low_passes failed")
            return json.dumps({"error": str(exc)})

    def get_return_visitors_detail(days: int = 7) -> str:
        """Get aircraft seen multiple times, with detail on each visit.

        Great for stories like "N373GG war diese Woche 3× da — Mo, Mi, Fr."

        Returns JSON: list of aircraft with hex, callsign, visit_count, visits list
        (each visit: started_local, max_alt_ft, min_dist_nm).

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT hex, COUNT(*) AS visit_count
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                        GROUP BY hex HAVING COUNT(*) > 1
                        ORDER BY visit_count DESC
                        LIMIT 10
                    """,
                        {"days": days},
                    )
                    repeats = cur.fetchall()

                    result = []
                    for row in repeats:
                        cur.execute(
                            """
                            SELECT
                                callsign,
                                TO_CHAR(started_at AT TIME ZONE 'Europe/Berlin',
                                        'Dy HH24:MI') AS started_local,
                                max_altitude, min_distance
                            FROM sightings
                            WHERE hex = %(hex)s
                              AND started_at > now() - (%(days)s || ' days')::interval
                            ORDER BY started_at
                        """,
                            {"hex": row["hex"], "days": days},
                        )
                        visits = cur.fetchall()
                        callsign = next(
                            (v["callsign"] for v in visits if v["callsign"]), None
                        )
                        result.append(
                            {
                                "hex": row["hex"],
                                "callsign": callsign,
                                "visit_count": row["visit_count"],
                                "visits": [
                                    {
                                        "when": v["started_local"],
                                        "max_alt_ft": v["max_altitude"],
                                        "min_dist_nm": round(v["min_distance"], 2)
                                        if v["min_distance"]
                                        else None,
                                    }
                                    for v in visits
                                ],
                            }
                        )
            return json.dumps(result, default=str)
        except Exception as exc:
            logger.exception("get_return_visitors_detail failed")
            return json.dumps({"error": str(exc)})

    def get_track_distribution(days: int = 7) -> str:
        """Get dominant flight directions (compass bearings) observed by the receiver.

        Reveals the main traffic corridors overhead — e.g. "most flights come from
        the NW, i.e. the Frankfurt–Stuttgart corridor."

        Returns CSV: direction (N/NE/E/SE/S/SW/W/NW), flight_count

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            CASE
                                WHEN track >= 337.5 OR track < 22.5   THEN 'N'
                                WHEN track >= 22.5  AND track < 67.5  THEN 'NE'
                                WHEN track >= 67.5  AND track < 112.5 THEN 'E'
                                WHEN track >= 112.5 AND track < 157.5 THEN 'SE'
                                WHEN track >= 157.5 AND track < 202.5 THEN 'S'
                                WHEN track >= 202.5 AND track < 247.5 THEN 'SW'
                                WHEN track >= 247.5 AND track < 292.5 THEN 'W'
                                WHEN track >= 292.5 AND track < 337.5 THEN 'NW'
                            END AS direction,
                            COUNT(DISTINCT hex) AS aircraft
                        FROM position_updates
                        WHERE time > now() - (%(days)s || ' days')::interval
                          AND track IS NOT NULL
                        GROUP BY direction
                        ORDER BY aircraft DESC
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()
            lines = ["direction,aircraft"]
            for r in rows:
                lines.append(f"{r[0]},{r[1]}")
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_track_distribution failed")
            return json.dumps({"error": str(exc)})

    def lookup_route_batch(callsigns: str) -> str:
        """Look up origin and destination for multiple callsigns at once.

        Pass a comma-separated list of callsigns. Returns one JSON object per
        callsign (same format as lookup_route). Use this instead of calling
        lookup_route N times.

        Args:
            callsigns: Comma-separated callsigns, e.g. "DLH123,RYR4AB,EZY99"
        """
        results = {}
        for cs in [c.strip().upper() for c in callsigns.split(",") if c.strip()]:
            results[cs] = json.loads(lookup_route(cs))
        return json.dumps(results)

    def get_rare_visitors(days: int = 7) -> str:
        """Get aircraft seen only once or twice in the entire database history.

        True one-time visitors — the most unusual guests. Returns hex, callsign,
        total lifetime sightings, and when they were last seen.

        Returns CSV: hex, callsign, lifetime_sightings, last_seen_local

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH recent AS (
                            SELECT DISTINCT hex FROM sightings
                            WHERE started_at > now() - (%(days)s || ' days')::interval
                        ),
                        lifetime AS (
                            SELECT hex, COUNT(*) AS total,
                                   MAX(started_at) AS last_seen
                            FROM sightings GROUP BY hex
                        )
                        SELECT
                            r.hex,
                            s.callsign,
                            l.total,
                            TO_CHAR(l.last_seen AT TIME ZONE 'Europe/Berlin',
                                    'Dy DD.MM. HH24:MI') AS last_seen_local
                        FROM recent r
                        JOIN lifetime l ON l.hex = r.hex
                        LEFT JOIN LATERAL (
                            SELECT callsign FROM sightings
                            WHERE hex = r.hex AND callsign IS NOT NULL
                            ORDER BY started_at DESC LIMIT 1
                        ) s ON true
                        WHERE l.total <= 2
                        ORDER BY l.total ASC, l.last_seen DESC
                        LIMIT 10
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()
            lines = ["hex,callsign,lifetime_sightings,last_seen_local"]
            for r in rows:
                lines.append(f"{r[0]},{r[1] or ''},{r[2]},{r[3]}")
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_rare_visitors failed")
            return json.dumps({"error": str(exc)})

    def get_approach_hints(days: int = 7) -> str:
        """Get aircraft observed descending below 10,000 ft — hints at nearby airport approaches.

        Aircraft descending through low altitudes near Stuttgart may be approaching
        STR, FKB (Karlsruhe), FRA (Frankfurt) or other nearby fields.

        Returns CSV: hex, callsign, min_alt_ft, min_dist_nm, started_local

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            s.hex, s.callsign,
                            s.min_altitude,
                            s.min_distance,
                            TO_CHAR(s.started_at AT TIME ZONE 'Europe/Berlin',
                                    'Dy HH24:MI') AS started_local
                        FROM sightings s
                        WHERE s.started_at > now() - (%(days)s || ' days')::interval
                          AND s.min_altitude IS NOT NULL
                          AND s.min_altitude < 10000
                          AND s.min_altitude > 500
                        ORDER BY s.min_altitude ASC
                        LIMIT 15
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()
            lines = ["hex,callsign,min_alt_ft,min_dist_nm,started_local"]
            for r in rows:
                lines.append(
                    f"{r[0]},{r[1] or ''},{r[2] or ''},"
                    f"{round(r[3], 2) if r[3] else ''},{r[4]}"
                )
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_approach_hints failed")
            return json.dumps({"error": str(exc)})

    def get_signal_records(days: int = 7) -> str:
        """Get RSSI signal strength extremes — closest overhead and farthest detected.

        RSSI is in dBFS (negative; closer to 0 = stronger signal).
        Strongest signal = aircraft directly overhead.
        Weakest-but-still-tracked = receiver range test.

        Returns JSON with strongest and weakest signal sightings.

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT p.hex, s.callsign,
                               MAX(p.rssi) AS best_rssi,
                               MIN(s.min_distance) AS min_dist
                        FROM position_updates p
                        LEFT JOIN LATERAL (
                            SELECT callsign, min_distance FROM sightings
                            WHERE hex = p.hex
                              AND started_at > now() - (%(days)s || ' days')::interval
                            ORDER BY started_at DESC LIMIT 1
                        ) s ON true
                        WHERE p.time > now() - (%(days)s || ' days')::interval
                          AND p.rssi IS NOT NULL
                        GROUP BY p.hex, s.callsign, s.min_distance
                        ORDER BY best_rssi DESC
                        LIMIT 1
                    """,
                        {"days": days},
                    )
                    strongest = dict(cur.fetchone() or {})

                    cur.execute(
                        """
                        SELECT p.hex, s.callsign,
                               MIN(p.rssi) AS worst_rssi,
                               MAX(s.max_distance) AS max_dist
                        FROM position_updates p
                        LEFT JOIN LATERAL (
                            SELECT callsign, max_distance FROM sightings
                            WHERE hex = p.hex
                              AND started_at > now() - (%(days)s || ' days')::interval
                            ORDER BY started_at DESC LIMIT 1
                        ) s ON true
                        WHERE p.time > now() - (%(days)s || ' days')::interval
                          AND p.rssi IS NOT NULL
                        GROUP BY p.hex, s.callsign, s.max_distance
                        ORDER BY worst_rssi ASC
                        LIMIT 1
                    """,
                        {"days": days},
                    )
                    weakest = dict(cur.fetchone() or {})

            for d in (strongest, weakest):
                for k, v in d.items():
                    if hasattr(v, "isoformat"):
                        d[k] = v.isoformat()
                    elif v is not None:
                        try:
                            d[k] = float(v)
                        except (TypeError, ValueError):
                            pass
            return json.dumps(
                {"strongest_signal": strongest, "weakest_signal": weakest}
            )
        except Exception as exc:
            logger.exception("get_signal_records failed")
            return json.dumps({"error": str(exc)})

    def get_squawk_distribution(days: int = 7) -> str:
        """Get breakdown of ATC transponder codes (squawks) observed.

        Beyond emergency codes, squawks reveal ATC assignments and VFR traffic.
        Common codes: 1000=IFR in Europe, 2000=VFR entering controlled airspace,
        7000=VFR general.

        Returns CSV: squawk, count, meaning

        Args:
            days: How many days back to look (default 7).
        """
        _SQUAWK_NOTES = {
            "1000": "IFR standard (Europa)",
            "2000": "VFR eintretend in kontrollierten Luftraum",
            "7000": "VFR allgemein",
            "7700": "Notfall",
            "7600": "Funkausfall",
            "7500": "Entführung",
        }
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT squawk, COUNT(DISTINCT hex) AS aircraft
                        FROM position_updates
                        WHERE time > now() - (%(days)s || ' days')::interval
                          AND squawk IS NOT NULL AND squawk != '0000'
                        GROUP BY squawk
                        ORDER BY aircraft DESC
                        LIMIT 15
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()
            lines = ["squawk,aircraft,meaning"]
            for squawk, count in rows:
                meaning = _SQUAWK_NOTES.get(squawk, "")
                lines.append(f"{squawk},{count},{meaning}")
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_squawk_distribution failed")
            return json.dumps({"error": str(exc)})

    def get_formation_windows(days: int = 7) -> str:
        """Find time windows where multiple aircraft with the same operator prefix
        were airborne simultaneously or within 30 minutes of each other.

        Hints at charter groups, military formations, or airline bank operations.

        Returns CSV: prefix, window_start_local, aircraft_count, callsigns

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH tagged AS (
                            SELECT
                                LEFT(callsign, 3) AS prefix,
                                callsign,
                                DATE_TRUNC('hour', started_at AT TIME ZONE 'Europe/Berlin')
                                    + INTERVAL '30 min' *
                                      FLOOR(EXTRACT(MINUTE FROM started_at AT TIME ZONE 'Europe/Berlin') / 30)
                                    AS window_start,
                                hex
                            FROM sightings
                            WHERE started_at > now() - (%(days)s || ' days')::interval
                              AND callsign ~ '^[A-Z]{3}[0-9]'
                        )
                        SELECT
                            prefix,
                            TO_CHAR(window_start, 'Dy DD.MM. HH24:MI') AS window_local,
                            COUNT(DISTINCT hex) AS aircraft_count,
                            STRING_AGG(DISTINCT callsign, ', ' ORDER BY callsign) AS callsigns
                        FROM tagged
                        GROUP BY prefix, window_start
                        HAVING COUNT(DISTINCT hex) >= 3
                        ORDER BY aircraft_count DESC, window_start DESC
                        LIMIT 10
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()
            lines = ["prefix,window_local,aircraft_count,callsigns"]
            for r in rows:
                lines.append(f"{r[0]},{r[1]},{r[2]},{r[3]}")
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_formation_windows failed")
            return json.dumps({"error": str(exc)})

    def get_callsign_history(icao_hex: str) -> str:
        """Get all callsigns ever used by a specific aircraft in our database.

        Useful for spotting fleet reassignments or aircraft flying under multiple
        identities. Also returns total lifetime sighting count and date range.

        Args:
            icao_hex: 6-character ICAO hex address.
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            callsign,
                            COUNT(*) AS times_used,
                            MIN(started_at) AT TIME ZONE 'Europe/Berlin' AS first_use,
                            MAX(started_at) AT TIME ZONE 'Europe/Berlin' AS last_use
                        FROM sightings
                        WHERE hex = %(hex)s AND callsign IS NOT NULL
                        GROUP BY callsign
                        ORDER BY times_used DESC
                    """,
                        {"hex": icao_hex.lower()},
                    )
                    rows = cur.fetchall()

                    cur.execute(
                        """
                        SELECT COUNT(*), MIN(started_at), MAX(started_at)
                        FROM sightings WHERE hex = %(hex)s
                    """,
                        {"hex": icao_hex.lower()},
                    )
                    total, first, last = cur.fetchone()

            lines = [
                f"hex:{icao_hex},total_sightings:{total},"
                f"first_seen:{first.strftime('%d.%m.%Y') if first else ''},"
                f"last_seen:{last.strftime('%d.%m.%Y') if last else ''}"
            ]
            lines.append("callsign,times_used,first_use,last_use")
            for r in rows:
                lines.append(
                    f"{r[0]},{r[1]},"
                    f"{r[2].strftime('%d.%m.%Y') if r[2] else ''},"
                    f"{r[3].strftime('%d.%m.%Y') if r[3] else ''}"
                )
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_callsign_history failed for %s", icao_hex)
            return json.dumps({"error": str(exc)})

    def get_weekly_rhythm(weeks: int = 4) -> str:
        """Get average flights per weekday across the last N weeks.

        Shows the weekly pattern: are Sundays busiest? Does Monday have less traffic?
        Returns avg flights per day-of-week (Mo–So).

        Args:
            weeks: How many weeks of history to average (default 4).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            TO_CHAR(DATE(started_at AT TIME ZONE 'Europe/Berlin'),
                                    'ID') AS dow_num,
                            TO_CHAR(DATE(started_at AT TIME ZONE 'Europe/Berlin'),
                                    'Dy') AS dow_name,
                            COUNT(*) AS total_flights,
                            COUNT(DISTINCT DATE(started_at AT TIME ZONE 'Europe/Berlin'))
                                AS days_seen,
                            ROUND(COUNT(*)::numeric /
                                NULLIF(COUNT(DISTINCT DATE(
                                    started_at AT TIME ZONE 'Europe/Berlin')), 0), 1)
                                AS avg_flights_per_day
                        FROM sightings
                        WHERE started_at > now() - (%(weeks)s || ' weeks')::interval
                        GROUP BY dow_num, dow_name
                        ORDER BY dow_num
                    """,
                        {"weeks": weeks},
                    )
                    rows = cur.fetchall()
            lines = ["dow,avg_flights_per_day"]
            for r in rows:
                lines.append(f"{r[1]},{r[4]}")
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_weekly_rhythm failed")
            return json.dumps({"error": str(exc)})

    def get_vertical_speed_outliers(days: int = 7) -> str:
        """Get aircraft with extreme rates of climb or descent.

        Computed from consecutive position_updates. Fast climbers/divers are
        often military, unusual ops, or emergency situations.

        Returns CSV: hex, callsign, max_climb_fpm, max_descent_fpm

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        WITH diffs AS (
                            SELECT
                                hex,
                                (alt_baro - LAG(alt_baro) OVER (PARTITION BY hex ORDER BY time))
                                  / NULLIF(EXTRACT(EPOCH FROM
                                      time - LAG(time) OVER (PARTITION BY hex ORDER BY time)
                                  ) / 60, 0) AS fpm
                            FROM position_updates
                            WHERE time > now() - (%(days)s || ' days')::interval
                              AND alt_baro IS NOT NULL AND alt_baro > 0
                        )
                        SELECT
                            d.hex,
                            s.callsign,
                            ROUND(MAX(fpm))  AS max_climb_fpm,
                            ROUND(MIN(fpm))  AS max_descent_fpm
                        FROM diffs d
                        LEFT JOIN LATERAL (
                            SELECT callsign FROM sightings
                            WHERE hex = d.hex
                              AND started_at > now() - (%(days)s || ' days')::interval
                            ORDER BY started_at DESC LIMIT 1
                        ) s ON true
                        WHERE fpm IS NOT NULL AND ABS(fpm) > 3000
                        GROUP BY d.hex, s.callsign
                        ORDER BY GREATEST(ABS(MAX(fpm)), ABS(MIN(fpm))) DESC
                        LIMIT 10
                    """,
                        {"days": days},
                    )
                    rows = cur.fetchall()
            lines = ["hex,callsign,max_climb_fpm,max_descent_fpm"]
            for r in rows:
                lines.append(f"{r[0]},{r[1] or ''},{r[2] or ''},{r[3] or ''}")
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("get_vertical_speed_outliers failed")
            return json.dumps({"error": str(exc)})

    def get_distance_percentiles(days: int = 7) -> str:
        """Get detection distance percentiles — characterises how far the receiver reaches.

        Returns p50 (typical), p90 (good conditions), p99 (best catch) distances in nm,
        plus the farthest single detection. Useful for "unser Empfänger reichte diese
        Woche bis zu X km."

        Args:
            days: How many days back to look (default 7).
        """
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP
                                (ORDER BY max_distance)::numeric, 1) AS p50,
                            ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP
                                (ORDER BY max_distance)::numeric, 1) AS p90,
                            ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP
                                (ORDER BY max_distance)::numeric, 1) AS p99,
                            ROUND(MAX(max_distance)::numeric, 1)     AS max_dist,
                            hex AS farthest_hex,
                            callsign AS farthest_callsign
                        FROM sightings
                        WHERE started_at > now() - (%(days)s || ' days')::interval
                          AND max_distance IS NOT NULL
                        GROUP BY hex, callsign
                        ORDER BY MAX(max_distance) DESC
                        LIMIT 1
                    """,
                        {"days": days},
                    )
                    row = cur.fetchone()
            if not row:
                return json.dumps({"error": "no data"})
            p50, p90, p99, max_d, hex_, cs = row
            return (
                f"p50_nm:{p50},p90_nm:{p90},p99_nm:{p99},"
                f"max_nm:{max_d},farthest_hex:{hex_},farthest_callsign:{cs or ''}"
            )
        except Exception as exc:
            logger.exception("get_distance_percentiles failed")
            return json.dumps({"error": str(exc)})

    def compare_periods(
        unit: Literal["day", "week", "month"] = "week",
        n: int = 4,
    ) -> str:
        """Compare flight traffic across the last N days, weeks, or months.

        Use this to write trend sentences like "diese Woche 20% mehr als letzte Woche"
        or "April war der bisher ruhigste Monat".

        Returns a CSV with one row per period:
          period, total_sightings, unique_aircraft, new_aircraft, top_operator

        unit options:
          "day"   — last N calendar days (max 14)
          "week"  — last N ISO weeks (max 12)
          "month" — last N calendar months (max 12)

        Args:
            unit: Time unit to group by (default "week").
            n: Number of periods to return (default 4).
        """
        n = max(1, min(n, 14 if unit == "day" else 12))
        try:
            with psycopg2.connect(collector_database_url) as conn:
                with conn.cursor() as cur:
                    if unit == "day":
                        trunc = "day"
                        label_sql = "TO_CHAR(p, 'Dy DD.MM.')"
                    elif unit == "week":
                        trunc = "week"
                        label_sql = "'KW' || TO_CHAR(p, 'IW')"
                    else:
                        trunc = "month"
                        label_sql = "TO_CHAR(p, 'Mon YYYY')"

                    cur.execute(
                        f"""
                        WITH periods AS (
                            SELECT generate_series(
                                DATE_TRUNC('{trunc}', now() AT TIME ZONE 'Europe/Berlin')
                                    - (%(n)s - 1) * INTERVAL '1 {trunc}',
                                DATE_TRUNC('{trunc}', now() AT TIME ZONE 'Europe/Berlin'),
                                INTERVAL '1 {trunc}'
                            )::date AS p
                        ),
                        sightings_agg AS (
                            SELECT
                                DATE_TRUNC('{trunc}',
                                    started_at AT TIME ZONE 'Europe/Berlin')::date AS p,
                                COUNT(*)                    AS total_sightings,
                                COUNT(DISTINCT hex)         AS unique_aircraft,
                                LEFT(MODE() WITHIN GROUP (ORDER BY callsign), 3) AS top_op
                            FROM sightings
                            WHERE callsign IS NOT NULL
                            GROUP BY 1
                        ),
                        new_agg AS (
                            SELECT
                                DATE_TRUNC('{trunc}',
                                    first_seen AT TIME ZONE 'Europe/Berlin')::date AS p,
                                COUNT(*) AS new_aircraft
                            FROM aircraft
                            GROUP BY 1
                        )
                        SELECT
                            {label_sql}             AS period,
                            COALESCE(s.total_sightings, 0) AS total_sightings,
                            COALESCE(s.unique_aircraft, 0) AS unique_aircraft,
                            COALESCE(na.new_aircraft, 0)   AS new_aircraft,
                            COALESCE(s.top_op, '')         AS top_operator
                        FROM periods pr
                        LEFT JOIN sightings_agg s  ON s.p  = pr.p
                        LEFT JOIN new_agg       na ON na.p = pr.p
                        ORDER BY pr.p
                    """,
                        {"n": n},
                    )
                    rows = cur.fetchall()

            lines = ["period,total_sightings,unique_aircraft,new_aircraft,top_operator"]
            for row in rows:
                lines.append(",".join(str(v) for v in row))
            return "\n".join(lines)
        except Exception as exc:
            logger.exception("compare_periods failed")
            return json.dumps({"error": str(exc)})

    return [
        get_stats,
        get_top_sightings,
        get_record,
        get_new_aircraft,
        get_squawk_alerts,
        get_night_flights,
        get_silent_aircraft,
        get_altitude_bands,
        get_speed_outliers,
        get_busy_slots,
        get_sightings_by_category,
        get_operator_breakdown,
        get_low_passes,
        get_return_visitors_detail,
        get_track_distribution,
        lookup_route_batch,
        get_rare_visitors,
        get_approach_hints,
        get_signal_records,
        get_squawk_distribution,
        get_formation_windows,
        get_callsign_history,
        get_weekly_rhythm,
        get_vertical_speed_outliers,
        get_distance_percentiles,
        compare_periods,
        lookup_aircraft,
        lookup_route,
        lookup_photo,
    ]
