"""Enrichment — fetch external data, score via AI, store results."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol

from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.genai import types as genai_types
from pydantic import BaseModel

from squawk.clients.adsbdb import AircraftInfo, AircraftLookupClient
from squawk.clients.routes import RouteClient, RouteInfo
from squawk.repositories.bulk_aircraft import BulkAircraftLookup
from squawk.repositories.enrichment import EnrichmentRepository

logger = logging.getLogger(__name__)

_APP_NAME = "adsb_enrichment"

_SCORING_SYSTEM_PROMPT = """
You rate aircraft for a weekly ADS-B digest near Stuttgart, Germany.
For each aircraft in the list, return a score (1–10):

Score guidelines:
- 1–3: Routine commercial traffic (Ryanair, Eurowings, short domestic routes)
- 4–6: Interesting but normal (long-haul, cargo, unfamiliar operator)
- 7–8: Unusual (military, private jet, exotic destination, rare type)
- 9–10: Very rare or extraordinary (historic aircraft, emergency squawk,
        medical evacuation, VIP transport)

IMPORTANT rules:
1. Squawk 7500 (hijack), 7600 (radio failure), 7700 (emergency) → score ≥ 9.
2. Plausibility check: if the observed altitude (alt_baro_ft) or speed (gs_knots)
   is physically inconsistent with the claimed aircraft type, the database data is
   wrong — score based on actual observed behavior, not the claimed type. Examples:
   helicopters do not exceed ~20,000 ft; piston/turboprop GA aircraft rarely exceed
   25,000 ft; WWII-era types (DC-3, Catalina) cannot cruise above 24,000 ft at
   jet speeds.
3. No hallucination: if type, registration, and operator are all null/unknown, do
   NOT guess or infer the aircraft type from the hex code or any other source.
   Score ≤ 4 unless the callsign or squawk provides clear evidence of something
   unusual.

Input fields:
- hex, callsign, registration, operator, flag: identity data
- type: human-readable aircraft type from database (may be wrong — apply rule 2)
- icao_type: ICAO type designator (e.g. B38M, A21N, H145) — more reliable than type
- alt_baro_ft: observed barometric altitude in feet (ground truth)
- gs_knots: observed ground speed in knots (ground truth)
- squawk: transponder squawk code
- origin/dest fields: route information

tags: short English keywords (e.g. "military", "cargo", "bizjet",
      "emergency", "long-haul", "low-altitude", "unusual-operator")

annotation: a single English sentence explaining why the aircraft is interesting.
Leave empty ("") if unremarkable (score ≤ 3).

The output is a JSON object with a field "results" containing an array with exactly
as many entries as the input list — in the same order.
""".strip()


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EnrichItem:
    """One aircraft to enrich. Telemetry fields are from the live sighting."""

    hex: str
    callsign: str | None
    alt_baro: int | None  # feet
    gs: float | None  # knots
    squawk: str | None


@dataclass(frozen=True)
class ScoreResult:
    score: int  # 1–10
    tags: list[str]
    annotation: str  # one English sentence; empty string if unremarkable


class ScoringClient(Protocol):
    async def score_batch(
        self,
        aircraft: list[tuple[EnrichItem, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]: ...


# ---------------------------------------------------------------------------
# Private Pydantic models for ADK structured output
# ---------------------------------------------------------------------------


class _ScoreResultModel(BaseModel):
    score: int
    tags: list[str]
    annotation: str


class _ScoreBatchModel(BaseModel):
    results: list[_ScoreResultModel]


# ---------------------------------------------------------------------------
# Private ADK-backed ScoringClient implementation
# ---------------------------------------------------------------------------

_FALLBACK_SCORE = ScoreResult(score=1, tags=[], annotation="")


def _aircraft_to_dict(
    item: EnrichItem,
    info: AircraftInfo | None,
    route: RouteInfo | None,
) -> dict:
    return {
        "hex": item.hex,
        "callsign": item.callsign,
        "alt_baro_ft": item.alt_baro,
        "gs_knots": item.gs,
        "squawk": item.squawk,
        "registration": info.registration if info else None,
        "type": info.type if info else None,
        "icao_type": info.icao_type if info else None,
        "operator": info.operator if info else None,
        "flag": info.flag if info else None,
        "origin_iata": route.origin_iata if route else None,
        "origin_icao": route.origin_icao if route else None,
        "origin_city": route.origin_city if route else None,
        "origin_country": route.origin_country if route else None,
        "dest_iata": route.dest_iata if route else None,
        "dest_icao": route.dest_icao if route else None,
        "dest_city": route.dest_city if route else None,
        "dest_country": route.dest_country if route else None,
    }


class _GeminiScoringClient:
    """ADK-backed ScoringClient using a single LlmAgent per batch call."""

    def __init__(self, model: str = "gemini-3-flash-preview") -> None:
        self._model = model

    async def score_batch(
        self,
        aircraft: list[tuple[EnrichItem, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]:
        if not aircraft:
            return []

        # Deduplicate by hex
        seen: dict[str, int] = {}
        deduped: list[tuple[EnrichItem, AircraftInfo | None, RouteInfo | None]] = []
        index_map: list[int] = []
        for item, info, route in aircraft:
            if item.hex not in seen:
                seen[item.hex] = len(deduped)
                deduped.append((item, info, route))
            index_map.append(seen[item.hex])

        scores = await self._score_deduped(deduped)
        return [scores[i] for i in index_map]

    async def _score_deduped(
        self,
        aircraft: list[tuple[EnrichItem, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]:
        input_dicts = [
            _aircraft_to_dict(item, info, route) for item, info, route in aircraft
        ]
        user_text = "Aircraft:\n" + json.dumps(
            input_dicts, ensure_ascii=False, indent=2
        )

        agent = LlmAgent(
            model=self._model,
            name="scoring_agent",
            description="Scores aircraft for ADS-B digest.",
            instruction=_SCORING_SYSTEM_PROMPT,
            output_schema=_ScoreBatchModel,
        )
        session_service = InMemorySessionService()
        runner = Runner(
            agent=agent, app_name=_APP_NAME, session_service=session_service
        )
        session_id = str(uuid.uuid4())
        await session_service.create_session(
            app_name=_APP_NAME, user_id="enrichment", session_id=session_id
        )
        message = genai_types.Content(
            role="user", parts=[genai_types.Part(text=user_text)]
        )

        try:
            async for event in runner.run_async(
                user_id="enrichment", session_id=session_id, new_message=message
            ):
                if event.is_final_response() and event.content and event.content.parts:
                    text = event.content.parts[0].text
                    if text is None:
                        continue
                    batch = _ScoreBatchModel.model_validate_json(text)
                    if len(batch.results) != len(aircraft):
                        logger.warning(
                            "score_batch: length mismatch — expected %d got %d; "
                            "falling back to per-aircraft calls",
                            len(aircraft),
                            len(batch.results),
                        )
                        return await self._fallback(aircraft)
                    return [
                        ScoreResult(
                            score=r.score,
                            tags=list(r.tags),
                            annotation=r.annotation,
                        )
                        for r in batch.results
                    ]
        except Exception:
            logger.exception(
                "score_batch: Gemini call failed for %d aircraft; using fallback",
                len(aircraft),
            )
            return await self._fallback(aircraft)

        logger.warning("score_batch: agent produced no output; using fallback")
        return await self._fallback(aircraft)

    async def _fallback(
        self,
        aircraft: list[tuple[EnrichItem, AircraftInfo | None, RouteInfo | None]],
    ) -> list[ScoreResult]:
        results: list[ScoreResult] = []
        for item, info, route in aircraft:
            input_dicts = [_aircraft_to_dict(item, info, route)]
            user_text = "Aircraft:\n" + json.dumps(input_dicts, ensure_ascii=False)

            agent = LlmAgent(
                model=self._model,
                name="scoring_agent_single",
                description="Scores one aircraft for ADS-B digest.",
                instruction=_SCORING_SYSTEM_PROMPT,
                output_schema=_ScoreBatchModel,
            )
            session_service = InMemorySessionService()
            runner = Runner(
                agent=agent, app_name=_APP_NAME, session_service=session_service
            )
            session_id = str(uuid.uuid4())
            await session_service.create_session(
                app_name=_APP_NAME, user_id="enrichment", session_id=session_id
            )
            message = genai_types.Content(
                role="user", parts=[genai_types.Part(text=user_text)]
            )

            score: ScoreResult | None = None
            try:
                async for event in runner.run_async(
                    user_id="enrichment",
                    session_id=session_id,
                    new_message=message,
                ):
                    if (
                        event.is_final_response()
                        and event.content
                        and event.content.parts
                    ):
                        text = event.content.parts[0].text
                        if text is None:
                            continue
                        batch = _ScoreBatchModel.model_validate_json(text)
                        if batch.results:
                            r = batch.results[0]
                            score = ScoreResult(
                                score=r.score,
                                tags=list(r.tags),
                                annotation=r.annotation,
                            )
            except Exception:
                logger.exception("fallback: Gemini call failed for hex=%s", item.hex)

            if score is None:
                logger.warning(
                    "fallback: no result for hex=%s; using default", item.hex
                )
                results.append(_FALLBACK_SCORE)
            else:
                results.append(score)

        return results


# ---------------------------------------------------------------------------
# Source merging
# ---------------------------------------------------------------------------


def _merge_aircraft_info(
    bulk: AircraftInfo | None,
    hexdb: AircraftInfo | None,
    adsbdb: AircraftInfo | None,
) -> AircraftInfo | None:
    """Merge aircraft info from three sources.

    Priority per field:
    - registration: bulk (mictronics) > hexdb > adsbdb
    - icao_type:    bulk > hexdb
    - type (human): adsbdb > hexdb > bulk  (adsbdb has best human-readable names)
    - operator:     hexdb > adsbdb          (mictronics has no operator)
    - flag:         adsbdb > hexdb          (adsbdb has ISO country name)

    Returns None only if all three sources returned None.
    """
    if bulk is None and hexdb is None and adsbdb is None:
        return None

    def first(*values: str | None) -> str | None:
        return next((v for v in values if v), None)

    return AircraftInfo(
        registration=first(
            bulk.registration if bulk else None,
            hexdb.registration if hexdb else None,
            adsbdb.registration if adsbdb else None,
        ),
        type=first(
            adsbdb.type if adsbdb else None,
            hexdb.type if hexdb else None,
            bulk.type if bulk else None,
        ),
        operator=first(
            hexdb.operator if hexdb else None,
            adsbdb.operator if adsbdb else None,
        ),
        flag=first(
            adsbdb.flag if adsbdb else None,
            hexdb.flag if hexdb else None,
        ),
        icao_type=first(
            bulk.icao_type if bulk else None,
            hexdb.icao_type if hexdb else None,
        ),
    )


# ---------------------------------------------------------------------------
# enrich_batch — public function
# ---------------------------------------------------------------------------


async def enrich_batch(
    items: list[EnrichItem],
    aircraft_client: AircraftLookupClient,
    hexdb_client: AircraftLookupClient,
    bulk_repo: BulkAircraftLookup,
    route_client: RouteClient,
    scoring_client: ScoringClient,
    enrichment_repo: EnrichmentRepository,
    enrichment_ttl: timedelta,
) -> None:
    """Fetch external data from three sources, score via AI, store results.

    1. Fetch AircraftInfo from adsbdb, hexdb, and bulk_aircraft in parallel.
    2. Merge the three sources into one AircraftInfo per aircraft.
    3. Fetch RouteInfo in parallel.
    4. Call scoring_client.score_batch() — one Gemini call for the whole batch.
    5. Store each result via enrichment_repo.store() (upsert, idempotent).
    """
    if not items:
        return

    logger.info("enrich_batch: scoring %d aircraft", len(items))

    # Fetch all three aircraft info sources in parallel
    adsbdb_infos, hexdb_infos, bulk_infos, route_infos = await asyncio.gather(
        asyncio.gather(*[_fetch_aircraft(aircraft_client, i.hex) for i in items]),
        asyncio.gather(*[_fetch_aircraft(hexdb_client, i.hex) for i in items]),
        asyncio.gather(*[_fetch_bulk(bulk_repo, i.hex) for i in items]),
        asyncio.gather(*[_fetch_route(route_client, i.callsign) for i in items]),
    )

    merged_infos = [
        _merge_aircraft_info(bulk, hexdb, adsbdb)
        for bulk, hexdb, adsbdb in zip(bulk_infos, hexdb_infos, adsbdb_infos)
    ]

    scoring_input = [
        (item, info, route)
        for item, info, route in zip(items, merged_infos, route_infos)
    ]

    try:
        scores = await scoring_client.score_batch(scoring_input)
    except Exception:
        logger.exception(
            "enrich_batch: score_batch failed for batch of %d; skipping", len(items)
        )
        return

    if len(scores) != len(items):
        logger.error(
            "enrich_batch: score_batch returned %d results for %d inputs; skipping",
            len(scores),
            len(items),
        )
        return

    for i, item in enumerate(items):
        score = scores[i]
        try:
            await enrichment_repo.store(
                hex=item.hex,
                score=score.score,
                tags=score.tags,
                annotation=score.annotation,
                aircraft_info=merged_infos[i],
                route_info=route_infos[i],
                callsign=item.callsign,
                enrichment_ttl=enrichment_ttl,
            )
        except Exception:
            logger.exception(
                "enrich_batch: store failed for hex=%s; skipping", item.hex
            )


async def _fetch_aircraft(
    client: AircraftLookupClient, hex_: str
) -> AircraftInfo | None:
    try:
        return await client.lookup(hex_)
    except Exception:
        logger.warning("enrich_batch: aircraft lookup failed for hex=%s", hex_)
        return None


async def _fetch_bulk(repo: BulkAircraftLookup, hex_: str) -> AircraftInfo | None:
    try:
        return await repo.lookup(hex_)
    except Exception:
        logger.warning("enrich_batch: bulk lookup failed for hex=%s", hex_)
        return None


async def _fetch_route(client: RouteClient, callsign: str | None) -> RouteInfo | None:
    if callsign is None:
        return None
    try:
        return await client.lookup(callsign)
    except Exception:
        logger.warning("enrich_batch: route lookup failed for callsign=%s", callsign)
        return None
