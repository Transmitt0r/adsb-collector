"""Data models for aircraft observations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class AircraftState:
    """A single aircraft observation from tar1090 JSON."""

    hex: str
    timestamp: datetime
    flight: str | None = None
    alt_baro: int | None = None
    gs: float | None = None
    track: float | None = None
    lat: float | None = None
    lon: float | None = None
    squawk: str | None = None
    category: str | None = None
    r_dst: float | None = None
    rssi: float | None = None
    messages: int | None = None
    seen: float | None = None

    @classmethod
    def from_json(cls, data: dict[str, Any], now: float) -> AircraftState:
        """Parse an aircraft entry from tar1090 aircraft.json.

        Args:
            data: Single aircraft dict from the ``aircraft`` array.
            now: The ``now`` timestamp from the top-level JSON response.
        """
        seen = data.get("seen", 0.0)
        ts = datetime.fromtimestamp(now - seen, tz=timezone.utc)

        return cls(
            hex=data["hex"],
            timestamp=ts,
            flight=data.get("flight", "").strip() or None,
            alt_baro=data.get("alt_baro")
            if isinstance(data.get("alt_baro"), int)
            else None,
            gs=data.get("gs"),
            track=data.get("track"),
            lat=data.get("lat"),
            lon=data.get("lon"),
            squawk=data.get("squawk"),
            category=data.get("category"),
            r_dst=data.get("r_dst"),
            rssi=data.get("rssi"),
            messages=data.get("messages"),
            seen=seen,
        )
