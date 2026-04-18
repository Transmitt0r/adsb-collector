"""Pipeline — continuous polling and enrichment loop."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

import tar1090
from squawk.clients.adsbdb import AircraftLookupClient
from squawk.clients.routes import RouteClient
from squawk.enrichment import ScoringClient, enrich_batch
from squawk.repositories.enrichment import EnrichmentRepository
from squawk.repositories.sightings import SightingRepository

logger = logging.getLogger(__name__)


async def run_pipeline(
    poll_url: str,
    poll_interval: float,
    session_timeout: float,
    sightings: SightingRepository,
    enrichment_repo: EnrichmentRepository,
    aircraft_client: AircraftLookupClient,
    route_client: RouteClient,
    scoring_client: ScoringClient,
    enrichment_ttl: timedelta,
    batch_size: int,
    flush_interval: float,
) -> None:
    """Poll tar1090 in a loop, record sightings, enrich new aircraft.

    In-memory state: a `pending` list of (hex, callsign) pairs waiting for
    enrichment. Lost on restart — at most one batch worth of aircraft will
    wait for the next TTL expiry cycle to be re-enriched. Acceptable.

    Crash recovery: close_open_sightings() runs on startup and shutdown. It
    sets ended_at on any sighting with ended_at IS NULL. Safe to call twice
    (no-op when nothing is open).

    Loop body:
    1. Poll tar1090 → list[AircraftState].
    2. Call sightings.record_poll(states, session_timeout):
       - Updates open sightings (last_seen, altitude/distance aggregates).
       - Opens new sightings for unseen aircraft.
       - Closes stale sightings (last_seen older than session_timeout).
       - Returns list[NewSighting] for aircraft new to the aircraft table.
    3. Add new sightings to pending enrichment list.
    4. Check enrichment TTL expiry → add expired (hex, callsign) to pending.
    5. If pending >= batch_size or flush_interval elapsed:
       call enrich_batch(), clear pending.
    6. Sleep poll_interval, repeat.

    Error handling: tar1090 poll failure or record_poll failure → log,
    skip cycle, continue. enrich_batch failure → log, continue (pending
    is cleared regardless — failed aircraft will be retried via TTL expiry).
    """
    await sightings.close_open_sightings()
    logger.info("pipeline started, polling %s every %.1fs", poll_url, poll_interval)

    pending: list[tuple[str, str | None]] = []
    loop = asyncio.get_running_loop()
    last_flush = loop.time()

    try:
        while True:
            # 1. Poll tar1090.
            try:
                states = await tar1090.poll(poll_url, timeout=poll_interval)
            except Exception:
                logger.exception("pipeline: tar1090 poll failed, skipping cycle")
                await asyncio.sleep(poll_interval)
                continue

            # 2. Record poll.
            try:
                new_sightings = await sightings.record_poll(states, session_timeout)
            except Exception:
                logger.exception("pipeline: record_poll failed, skipping cycle")
                await asyncio.sleep(poll_interval)
                continue

            # 3. Add new aircraft to pending.
            for ns in new_sightings:
                pending.append((ns.hex, ns.callsign))

            # 4. Check enrichment TTL expiry.
            current_hexes = [s.hex for s in states]
            if current_hexes:
                try:
                    expired = await enrichment_repo.get_expired(
                        current_hexes, enrichment_ttl
                    )
                    pending.extend(expired)
                except Exception:
                    logger.exception(
                        "pipeline: get_expired failed, skipping expiry check"
                    )

            # 5. Flush if batch is full or flush_interval has elapsed.
            now = loop.time()
            if pending and (
                len(pending) >= batch_size or (now - last_flush) >= flush_interval
            ):
                items = list(pending)
                pending.clear()
                last_flush = now
                try:
                    await enrich_batch(
                        items=items,
                        aircraft_client=aircraft_client,
                        route_client=route_client,
                        scoring_client=scoring_client,
                        enrichment_repo=enrichment_repo,
                        enrichment_ttl=enrichment_ttl,
                    )
                except Exception:
                    logger.exception(
                        "pipeline: enrich_batch failed for %d items", len(items)
                    )

            # 6. Sleep until next poll.
            await asyncio.sleep(poll_interval)
    finally:
        await sightings.close_open_sightings()
