"""BulkAircraftRepository — owns the bulk_aircraft table.

Written exclusively by the mictronics downloader (daily refresh).
Read by enrich_batch() as a fast local lookup.

Public API:
    BulkAircraftLookup  — Protocol for read-only lookup (used by pipeline/enrichment)
    BulkAircraftRepository — concrete implementation (also handles writes)
"""

from __future__ import annotations

from typing import Protocol

import asyncpg

from squawk.clients.adsbdb import AircraftInfo


class BulkAircraftLookup(Protocol):
    """Read-only protocol for bulk aircraft DB lookup.

    Satisfied by BulkAircraftRepository and test doubles alike.
    """

    async def lookup(self, hex: str) -> AircraftInfo | None: ...


class BulkAircraftRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def lookup(self, hex: str) -> AircraftInfo | None:
        """Return aircraft info for a given ICAO hex, or None if not found."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT registration, icao_type, model
                FROM bulk_aircraft WHERE hex = $1
                """,
                hex.lower(),
            )
        if row is None:
            return None
        reg = row["registration"]
        icao_type = row["icao_type"]
        model = row["model"]
        if not any([reg, icao_type, model]):
            return None
        return AircraftInfo(
            registration=reg,
            type=model or icao_type,  # prefer human-readable desc, fall back to code
            operator=None,  # mictronics doesn't provide operator
            flag=None,
            icao_type=icao_type,
        )

    async def truncate(self) -> None:
        """Truncate bulk_aircraft before a fresh ingest."""
        async with self._pool.acquire() as conn:
            await conn.execute("TRUNCATE TABLE bulk_aircraft")

    async def insert_batch(
        self,
        records: list[tuple[str, str | None, str | None, str | None]],
    ) -> None:
        """Bulk-insert (hex, registration, icao_type, model) tuples."""
        async with self._pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO bulk_aircraft (hex, registration, icao_type, model)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (hex) DO NOTHING
                """,
                records,
            )
