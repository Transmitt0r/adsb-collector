"""Tests for collector.poller."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from collector.config import Config
from collector.poller import poll_aircraft

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_RESPONSE: dict[str, Any] = {
    "now": 1700000000.0,
    "aircraft": [
        {
            "hex": "3c6752",
            "flight": "DLH1A  ",
            "alt_baro": 36000,
            "gs": 450.0,
            "track": 180.0,
            "lat": 48.76,
            "lon": 9.15,
            "squawk": "1000",
            "category": "A3",
            "r_dst": 5.2,
            "rssi": -20.0,
            "messages": 150,
            "seen": 1.5,
        },
        {
            "hex": "aaaaaa",
            "seen": 0.0,
        },
    ],
}


def _make_config(**overrides: Any) -> Config:
    defaults = {
        "aircraft_url": "http://localhost/test/aircraft.json",
        "poll_interval": 5.0,
        "session_timeout": 300.0,
        "database_url": "postgresql://localhost/test",
    }
    defaults.update(overrides)
    return Config(**defaults)


def _mock_session(payload: dict[str, Any] | Exception) -> MagicMock:
    """Create a mock aiohttp.ClientSession that returns *payload* from GET."""
    resp = AsyncMock()
    resp.raise_for_status = MagicMock()
    resp.json = AsyncMock(return_value=payload)

    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=resp)
    ctx.__aexit__ = AsyncMock(return_value=False)

    session = MagicMock()

    if isinstance(payload, Exception):
        session.get = MagicMock(side_effect=payload)
    else:
        session.get = MagicMock(return_value=ctx)

    return session


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_poll_parses_aircraft() -> None:
    session = _mock_session(SAMPLE_RESPONSE)
    config = _make_config()

    states = await poll_aircraft(session, config)

    assert len(states) == 2
    assert states[0].hex == "3c6752"
    assert states[0].flight == "DLH1A"  # stripped
    assert states[0].alt_baro == 36000
    assert states[1].hex == "aaaaaa"
    assert states[1].flight is None


@pytest.mark.asyncio
async def test_poll_skips_entries_without_hex() -> None:
    payload = {
        "now": 1700000000.0,
        "aircraft": [{"flight": "NOHEX"}],
    }
    session = _mock_session(payload)
    states = await poll_aircraft(session, _make_config())

    assert states == []


@pytest.mark.asyncio
async def test_poll_returns_empty_on_timeout() -> None:
    session = _mock_session(TimeoutError("timed out"))
    states = await poll_aircraft(session, _make_config())

    assert states == []


@pytest.mark.asyncio
async def test_poll_returns_empty_on_connection_error() -> None:
    from aiohttp import ClientConnectionError

    session = _mock_session(ClientConnectionError("refused"))
    states = await poll_aircraft(session, _make_config())

    assert states == []


@pytest.mark.asyncio
async def test_poll_returns_empty_when_now_missing() -> None:
    payload: dict[str, Any] = {"aircraft": [{"hex": "abc123"}]}
    session = _mock_session(payload)
    states = await poll_aircraft(session, _make_config())

    assert states == []
