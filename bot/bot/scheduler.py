"""Weekly digest scheduler and 15-minute enrichment job."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from .agent import generate_digest
from .bot import broadcast
from .config import Config
from .db import cache_digest, get_cached_digest
from .enrichment import run_enrichment

logger = logging.getLogger(__name__)


def _week_bounds() -> tuple[datetime, datetime]:
    """Return (start, end) for the past 7 days as UTC datetimes."""
    now = datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
    return now - timedelta(days=7), now


async def run_weekly_digest(config: Config) -> None:
    logger.info("Weekly digest job started")
    period_start, period_end = _week_bounds()

    digest = get_cached_digest(config.database_url, period_start, period_end)
    if digest:
        logger.info("Using cached digest")
    else:
        logger.info("Generating new digest")
        digest = await asyncio.to_thread(generate_digest, config, 7)
        cache_digest(config.database_url, period_start, period_end, digest)

    await broadcast(config, digest)
    logger.info("Weekly digest sent")


def create_scheduler(config: Config) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler()

    scheduler.add_job(
        run_enrichment,
        CronTrigger(minute="*/15"),
        kwargs={"config": config},
        name="enrichment",
    )

    parts = config.digest_schedule.split()
    digest_trigger = CronTrigger(
        minute=parts[0],
        hour=parts[1],
        day=parts[2],
        month=parts[3],
        day_of_week=parts[4],
        timezone="Europe/Berlin",
    )
    scheduler.add_job(
        run_weekly_digest,
        trigger=digest_trigger,
        kwargs={"config": config},
        name="weekly_digest",
    )

    logger.info("Digest scheduled: %s", config.digest_schedule)
    return scheduler
