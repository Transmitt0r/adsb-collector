"""Pipeline — continuous polling and enrichment loop."""

from __future__ import annotations

import asyncio
import logging

import tar1090
from squawk.clients.adsbdb import AircraftLookupClient
from squawk.clients.routes import RouteClient
from squawk.enrichment import EnrichItem, ScoringClient, enrich_batch
from squawk.repositories.bulk_aircraft import BulkAircraftLookup
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
    hexdb_client: AircraftLookupClient,
    bulk_repo: BulkAircraftLookup,
    route_client: RouteClient,
    scoring_client: ScoringClient,
    batch_size: int,
    flush_interval: float,
) -> None:
    """Poll tar1090 in a loop, record sightings, enrich new aircraft.

    In-memory state: a `pending` list of EnrichItems waiting for enrichment.
    Lost on restart — at most one batch worth of aircraft will not be
    enriched. Acceptable.

    Loop body:
    1. Poll tar1090 → list[AircraftState].
    2. Build hex → state lookup for telemetry (alt, speed, squawk).
    3. Call sightings.record_poll(states, session_timeout):
       - Opens new sightings, updates existing, closes stale.
       - Returns list[NewSighting] for aircraft new to the aircraft table.
    4. Add new sightings to pending (with live telemetry).
    5. When aircraft with null callsign now broadcast one: fetch route only
       (no re-scoring).
    6. If pending >= batch_size or flush_interval elapsed: enrich_batch().
    7. Sleep poll_interval, repeat.
    """
    await sightings.close_open_sightings()
    logger.info("pipeline started, polling %s every %.1fs", poll_url, poll_interval)

    pending: list[EnrichItem] = []
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

            # 2. Build hex → state lookup for telemetry.
            state_by_hex = {s.hex: s for s in states}

            # 3. Record poll.
            try:
                new_sightings = await sightings.record_poll(states, session_timeout)
            except Exception:
                logger.exception("pipeline: record_poll failed, skipping cycle")
                await asyncio.sleep(poll_interval)
                continue

            # 4. Add new aircraft to pending with live telemetry.
            for ns in new_sightings:
                state = state_by_hex.get(ns.hex)
                pending.append(
                    EnrichItem(
                        hex=ns.hex,
                        callsign=ns.callsign,
                        alt_baro=state.alt_baro if state else None,
                        gs=state.gs if state else None,
                        squawk=state.squawk if state else None,
                    )
                )

            # 5. Fetch route when a callsign appears for a previously-anonymous hex.
            callsign_hexes = [s.hex for s in states if s.flight]
            if callsign_hexes:
                try:
                    null_callsign = await enrichment_repo.get_null_callsign_cached(
                        callsign_hexes
                    )
                    for hex_ in null_callsign:
                        state = state_by_hex.get(hex_)
                        if state and state.flight:
                            try:
                                route = await route_client.lookup(state.flight)
                                if route is not None:
                                    await enrichment_repo.update_route_only(
                                        hex_, state.flight, route
                                    )
                            except Exception:
                                logger.exception(
                                    "pipeline: route update failed for hex=%s",
                                    hex_,
                                )
                except Exception:
                    logger.exception(
                        "pipeline: get_null_callsign_cached failed, skipping"
                    )

            # 6. Flush if batch is full or flush_interval has elapsed.
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
                        hexdb_client=hexdb_client,
                        bulk_repo=bulk_repo,
                        route_client=route_client,
                        scoring_client=scoring_client,
                        enrichment_repo=enrichment_repo,
                    )
                except Exception:
                    logger.exception(
                        "pipeline: enrich_batch failed for %d items", len(items)
                    )

            # 7. Sleep until next poll.
            await asyncio.sleep(poll_interval)
    finally:
        await sightings.close_open_sightings()
