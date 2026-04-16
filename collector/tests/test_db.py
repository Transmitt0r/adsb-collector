"""Tests for collector.db module."""

from __future__ import annotations

import importlib.resources
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from collector.config import Config
from collector.db import create_pool, init_schema


def _make_config(**overrides) -> Config:
    defaults = {
        "aircraft_url": "http://localhost/test/aircraft.json",
        "poll_interval": 5.0,
        "session_timeout": 300.0,
        "database_url": "postgresql://user:pass@localhost/flighttracker",
    }
    defaults.update(overrides)
    return Config(**defaults)


@pytest.mark.asyncio
@patch("collector.db.asyncpg")
async def test_create_pool(mock_asyncpg: MagicMock) -> None:
    mock_pool = AsyncMock()
    mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)
    config = _make_config()

    pool = await create_pool(config)

    mock_asyncpg.create_pool.assert_awaited_once_with(
        config.database_url, min_size=1, max_size=5
    )
    assert pool is mock_pool


@pytest.mark.asyncio
async def test_init_schema_executes_sql() -> None:
    mock_conn = AsyncMock()

    # pool.acquire() returns an async context manager (not a coroutine)
    ctx = AsyncMock()
    ctx.__aenter__.return_value = mock_conn
    ctx.__aexit__.return_value = False

    mock_pool = MagicMock()
    mock_pool.acquire.return_value = ctx

    await init_schema(mock_pool)

    mock_conn.execute.assert_awaited_once()
    executed_sql = mock_conn.execute.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS aircraft" in executed_sql
    assert "CREATE TABLE IF NOT EXISTS sightings" in executed_sql
    assert "CREATE TABLE IF NOT EXISTS position_updates" in executed_sql


@pytest.mark.asyncio
async def test_schema_sql_is_readable() -> None:
    """Verify schema.sql is packaged and readable."""
    sql = importlib.resources.files("collector").joinpath("schema.sql").read_text()
    assert "CREATE TABLE" in sql
    assert "timescaledb" in sql.lower()
