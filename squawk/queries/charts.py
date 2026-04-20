"""ChartQuery — read-only queries for traffic chart generation."""

from __future__ import annotations

from dataclasses import dataclass

import asyncpg


@dataclass(frozen=True)
class HourlyCount:
    hour: int
    flights: int


class ChartQuery:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get_hourly(self, days: int) -> list[HourlyCount]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    EXTRACT(
                        HOUR FROM started_at AT TIME ZONE 'Europe/Berlin'
                    )::int AS hour,
                    COUNT(*) AS flights
                FROM sightings
                WHERE started_at > now() - ($1 || ' days')::interval
                GROUP BY 1
                ORDER BY 1
                """,
                str(days),
            )
        return [HourlyCount(hour=r["hour"], flights=r["flights"]) for r in rows]
