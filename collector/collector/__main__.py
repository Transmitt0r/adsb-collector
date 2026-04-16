"""Entry point for the flight tracker collector: python -m collector."""

from __future__ import annotations

import asyncio
import logging
import signal

import aiohttp

from collector.config import Config
from collector.db import create_pool, init_schema
from collector.poller import poll_aircraft
from collector.tracker import SessionTracker

logger = logging.getLogger("collector")


async def run() -> None:
    """Main async loop: init → recover → poll → shutdown."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    config = Config.from_env()
    logger.info(
        "Starting collector (poll=%ss, timeout=%ss, url=%s)",
        config.poll_interval,
        config.session_timeout,
        config.aircraft_url,
    )

    pool = await create_pool(config)
    await init_schema(pool)

    tracker = SessionTracker(pool, config)
    await tracker.recover()

    # Graceful shutdown via signal
    stop = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("Shutdown signal received")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _signal_handler)

    logger.info("Collector running — press Ctrl+C to stop")

    async with aiohttp.ClientSession() as session:
        while not stop.is_set():
            try:
                states = await poll_aircraft(session, config)
                await tracker.process_poll(states)
            except Exception:
                logger.exception("Unhandled error in poll cycle")

            try:
                await asyncio.wait_for(stop.wait(), timeout=config.poll_interval)
            except TimeoutError:
                pass  # Normal: poll interval elapsed, loop again

    # Clean shutdown
    await tracker.shutdown()
    await pool.close()
    logger.info("Collector stopped")


def main() -> None:
    """Sync entry point."""
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
