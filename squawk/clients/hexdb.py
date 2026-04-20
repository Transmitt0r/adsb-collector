"""hexdb.io aircraft registry client.

Public API:
    HexdbClient — concrete AircraftLookupClient implementation
"""

from __future__ import annotations

import asyncio
import logging
from typing import Protocol

import aiohttp

from squawk.clients.adsbdb import AircraftInfo

logger = logging.getLogger(__name__)

_TIMEOUT = aiohttp.ClientTimeout(total=10)


class AircraftLookupClient(Protocol):
    async def lookup(self, hex: str) -> AircraftInfo | None: ...


class HexdbClient:
    """Async hexdb.io aircraft lookup.

    hexdb.io returns clean data sourced from government registries.
    More accurate than adsbdb when it has data, but has more 404s.

    Retry policy:
        404  → return None
        429  → exponential backoff, up to max_retries
        5xx  → exponential backoff, up to max_retries
        other → raise immediately
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        base_url: str = "https://hexdb.io/api/v1",
        max_retries: int = 3,
    ) -> None:
        self._session = session
        self._base_url = base_url.rstrip("/")
        self._max_retries = max_retries

    async def lookup(self, hex: str) -> AircraftInfo | None:
        url = f"{self._base_url}/aircraft/{hex.lower()}"
        for attempt in range(self._max_retries + 1):
            async with self._session.get(url, timeout=_TIMEOUT) as resp:
                if resp.status == 404:
                    return None
                if resp.status == 429 or resp.status >= 500:
                    if attempt < self._max_retries:
                        await asyncio.sleep(2**attempt)
                        continue
                    resp.raise_for_status()
                resp.raise_for_status()
                data = await resp.json()
                # hexdb returns {"status": "404", "error": "..."} as 200 in some cases
                if data.get("status") == "404" or data.get("error"):
                    return None
                return AircraftInfo(
                    registration=data.get("Registration") or None,
                    type=data.get("Type") or None,
                    operator=data.get("RegisteredOwners") or None,
                    flag=data.get("OperatorFlagCode") or None,
                    icao_type=data.get("ICAOTypeCode") or None,
                )
        return None
