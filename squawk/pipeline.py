"""Pipeline — continuous polling and enrichment loop."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

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
    enrichment_ttl: timedelta,
    batch_size: int,
    flush_interval: float,
) -> None:
    """Poll tar1090 in a loop, record sightings, enrich new aircraft.

    In-memory state: a `pending` list of EnrichItems waiting for enrichment.
    Lost on restart — at most one batch worth of aircraft will wait for the
    next TTL expiry cycle to be re-enriched. Acceptable.

    Loop body:
    1. Poll tar1090 → list[AircraftState].
    2. Build hex → state lookup for telemetry (alt, speed, squawk).
    3. Call sightings.record_poll(states, session_timeout):
       - Opens new sightings, updates existing, closes stale.
       - Returns list[NewSighting] for aircraft new to the aircraft table.
    4. Add new sightings to pending (with live telemetry).
    5. Check enrichment TTL expiry → add expired to pending (with telemetry).
    6. Re-enrich any cached aircraft that now have a callsign but were
       previously enriched without one.
    7. If pending >= batch_size or flush_interval elapsed: enrich_batch().
    8. Sleep poll_interval, repeat.
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

            # 5. Check enrichment TTL expiry.
            current_hexes = [s.hex for s in states]
            if current_hexes:
                try:
                    expired = await enrichment_repo.get_expired(
                        current_hexes, enrichment_ttl
                    )
                    for hex_, callsign in expired:
                        state = state_by_hex.get(hex_)
                        pending.append(
                            EnrichItem(
                                hex=hex_,
                                callsign=callsign,
                                alt_baro=state.alt_baro if state else None,
                                gs=state.gs if state else None,
                                squawk=state.squawk if state else None,
                            )
                        )
                except Exception:
                    logger.exception(
                        "pipeline: get_expired failed, skipping expiry check"
                    )

            # 6. Re-enrich when a callsign appears for a previously-anonymous hex.
            callsign_hexes = [s.hex for s in states if s.flight]
            if callsign_hexes:
                try:
                    null_callsign = await enrichment_repo.get_null_callsign_cached(
                        callsign_hexes
                    )
                    for hex_ in null_callsign:
                        state = state_by_hex.get(hex_)
                        if state:
                            pending.append(
                                EnrichItem(
                                    hex=hex_,
                                    callsign=state.flight,
                                    alt_baro=state.alt_baro,
                                    gs=state.gs,
                                    squawk=state.squawk,
                                )
                            )
                except Exception:
                    logger.exception(
                        "pipeline: get_null_callsign_cached failed, skipping"
                    )

            # 7. Flush if batch is full or flush_interval has elapsed.
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
                        enrichment_ttl=enrichment_ttl,
                    )
                except Exception:
                    logger.exception(
                        "pipeline: enrich_batch failed for %d items", len(items)
                    )

            # 8. Sleep until next poll.
            await asyncio.sleep(poll_interval)
    finally:
        await sightings.close_open_sightings()
