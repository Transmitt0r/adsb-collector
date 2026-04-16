"""Tests for collector.tracker — session tracking state machine."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from collector.config import Config
from collector.models import AircraftState
from collector.tracker import SessionTracker, _max_opt, _min_opt


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(**overrides) -> Config:
    defaults = {
        "aircraft_url": "http://localhost/test/aircraft.json",
        "poll_interval": 5.0,
        "session_timeout": 300.0,
        "database_url": "postgresql://localhost/test",
    }
    defaults.update(overrides)
    return Config(**defaults)


def _ts(offset: float = 0.0) -> datetime:
    """Return a UTC datetime at a fixed base + offset seconds."""
    return datetime.fromtimestamp(1700000000.0 + offset, tz=timezone.utc)


def _state(hex: str = "3c6752", offset: float = 0.0, **kwargs) -> AircraftState:
    defaults = {
        "hex": hex,
        "timestamp": _ts(offset),
        "flight": "DLH1A",
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
        "seen": 1.0,
    }
    defaults.update(kwargs)
    return AircraftState(**defaults)


def _mock_pool():
    """Create a mock asyncpg pool with transaction support."""
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value="UPDATE 0")
    conn.executemany = AsyncMock()
    conn.fetchval = AsyncMock(return_value=1)

    # transaction context manager
    tx = AsyncMock()
    tx.__aenter__ = AsyncMock(return_value=None)
    tx.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=tx)

    # acquire context manager
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)

    pool = MagicMock()
    pool.acquire = MagicMock(return_value=ctx)

    return pool, conn


# ---------------------------------------------------------------------------
# Unit tests: helpers
# ---------------------------------------------------------------------------


class TestMinMaxOpt:
    def test_min_both_none(self):
        assert _min_opt(None, None) is None

    def test_min_one_none(self):
        assert _min_opt(None, 5) == 5
        assert _min_opt(3, None) == 3

    def test_min_both_values(self):
        assert _min_opt(3, 5) == 3

    def test_max_both_none(self):
        assert _max_opt(None, None) is None

    def test_max_one_none(self):
        assert _max_opt(None, 5) == 5
        assert _max_opt(3, None) == 3

    def test_max_both_values(self):
        assert _max_opt(3, 5) == 5


# ---------------------------------------------------------------------------
# SessionTracker tests
# ---------------------------------------------------------------------------


class TestSessionTracker:
    @pytest.mark.asyncio
    async def test_recover_closes_orphaned_sightings(self):
        pool, conn = _mock_pool()
        tracker = SessionTracker(pool, _make_config())

        await tracker.recover()

        conn.execute.assert_awaited_once()
        sql = conn.execute.call_args[0][0]
        assert "UPDATE sightings SET ended_at" in sql
        assert "WHERE ended_at IS NULL" in sql

    @pytest.mark.asyncio
    async def test_process_poll_opens_new_sighting(self):
        pool, conn = _mock_pool()
        conn.fetchval = AsyncMock(return_value=42)
        tracker = SessionTracker(pool, _make_config())

        states = [_state(hex="aabbcc", offset=0)]
        await tracker.process_poll(states)

        assert tracker.active_count == 1
        assert "aabbcc" in tracker._active
        assert tracker._active["aabbcc"].sighting_id == 42

        # Should have called executemany for aircraft upsert
        calls = conn.executemany.call_args_list
        aircraft_call = calls[0]
        assert "INSERT INTO aircraft" in aircraft_call[0][0]

        # Should have called fetchval for sighting INSERT RETURNING
        conn.fetchval.assert_awaited()
        assert "INSERT INTO sightings" in conn.fetchval.call_args[0][0]

    @pytest.mark.asyncio
    async def test_process_poll_updates_existing_sighting(self):
        pool, conn = _mock_pool()
        conn.fetchval = AsyncMock(return_value=10)
        tracker = SessionTracker(pool, _make_config())

        # First poll — open sighting
        await tracker.process_poll(
            [_state(hex="aabbcc", offset=0, alt_baro=30000, r_dst=10.0)]
        )
        assert tracker._active["aabbcc"].min_altitude == 30000

        # Second poll — update aggregates
        await tracker.process_poll(
            [_state(hex="aabbcc", offset=5, alt_baro=28000, r_dst=8.0)]
        )
        active = tracker._active["aabbcc"]
        assert active.min_altitude == 28000
        assert active.max_altitude == 30000
        assert active.min_distance == 8.0
        assert active.max_distance == 10.0

    @pytest.mark.asyncio
    async def test_process_poll_expires_missing_aircraft(self):
        pool, conn = _mock_pool()
        conn.fetchval = AsyncMock(return_value=10)
        tracker = SessionTracker(pool, _make_config())

        # First poll — open sighting for aabbcc
        await tracker.process_poll([_state(hex="aabbcc", offset=0)])
        assert tracker.active_count == 1

        # Second poll — aabbcc gone, new aircraft dddddd
        conn.fetchval = AsyncMock(return_value=20)
        await tracker.process_poll([_state(hex="dddddd", offset=5)])

        assert tracker.active_count == 1
        assert "dddddd" in tracker._active
        assert "aabbcc" not in tracker._active

    @pytest.mark.asyncio
    async def test_process_poll_reopens_after_timeout(self):
        pool, conn = _mock_pool()
        conn.fetchval = AsyncMock(return_value=10)
        config = _make_config(session_timeout=60.0)
        tracker = SessionTracker(pool, config)

        # First poll
        await tracker.process_poll([_state(hex="aabbcc", offset=0)])
        assert tracker._active["aabbcc"].sighting_id == 10

        # Second poll after timeout gap (>60s)
        conn.fetchval = AsyncMock(return_value=20)
        await tracker.process_poll([_state(hex="aabbcc", offset=120)])

        # Should have expired old and opened new
        assert tracker._active["aabbcc"].sighting_id == 20

    @pytest.mark.asyncio
    async def test_process_poll_skips_stale_observations(self):
        pool, conn = _mock_pool()
        tracker = SessionTracker(pool, _make_config())

        # Aircraft with seen > 60s should be filtered out
        states = [_state(hex="aabbcc", seen=90.0)]
        await tracker.process_poll(states)

        assert tracker.active_count == 0

    @pytest.mark.asyncio
    async def test_process_poll_empty_input(self):
        pool, conn = _mock_pool()
        tracker = SessionTracker(pool, _make_config())

        await tracker.process_poll([])
        assert tracker.active_count == 0
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_shutdown_closes_all(self):
        pool, conn = _mock_pool()
        conn.fetchval = AsyncMock(return_value=10)
        tracker = SessionTracker(pool, _make_config())

        await tracker.process_poll([_state(hex="aabbcc"), _state(hex="dddddd")])
        assert tracker.active_count == 2

        await tracker.shutdown()

        assert tracker.active_count == 0
        # shutdown uses its own acquire call
        shutdown_calls = [
            c for c in conn.executemany.call_args_list if "ended_at" in str(c)
        ]
        assert len(shutdown_calls) > 0

    @pytest.mark.asyncio
    async def test_shutdown_noop_when_empty(self):
        pool, conn = _mock_pool()
        tracker = SessionTracker(pool, _make_config())

        await tracker.shutdown()
        # Should not acquire a connection at all for empty shutdown
        assert tracker.active_count == 0

    @pytest.mark.asyncio
    async def test_inserts_position_updates(self):
        pool, conn = _mock_pool()
        conn.fetchval = AsyncMock(return_value=1)
        tracker = SessionTracker(pool, _make_config())

        states = [_state(hex="aabbcc", lat=48.76, lon=9.15)]
        await tracker.process_poll(states)

        # Find the position INSERT call
        position_calls = [
            c for c in conn.executemany.call_args_list if "position_updates" in str(c)
        ]
        assert len(position_calls) == 1
        rows = position_calls[0][0][1]
        assert len(rows) == 1
        assert rows[0][1] == "aabbcc"  # hex
