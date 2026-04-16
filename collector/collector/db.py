"""Database pool management and schema initialization."""

from __future__ import annotations

import importlib.resources
import logging

import asyncpg

from collector.config import Config

logger = logging.getLogger(__name__)


async def create_pool(config: Config) -> asyncpg.Pool:
    """Create and return an asyncpg connection pool."""
    pool = await asyncpg.create_pool(config.database_url, min_size=1, max_size=5)
    logger.info("Database pool created (%s)", config.database_url.split("@")[-1])
    return pool


async def init_schema(pool: asyncpg.Pool) -> None:
    """Apply schema.sql to the database (idempotent)."""
    schema_sql = (
        importlib.resources.files("collector").joinpath("schema.sql").read_text()
    )
    async with pool.acquire() as conn:
        await conn.execute(schema_sql)
    logger.info("Database schema initialized")
