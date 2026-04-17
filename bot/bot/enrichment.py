"""Background enrichment job: score and annotate aircraft every 15 minutes."""

from __future__ import annotations

import json
import logging
import re

import anthropic

from .config import Config
from .db import get_unenriched_aircraft, store_enrichment
from .tools import lookup_aircraft, lookup_route

logger = logging.getLogger(__name__)

_SCORE_PROMPT = """\
Score this aircraft for story interest in a weekly aviation digest (1-10, 10 = most interesting).

hex: {hex_}
callsign: {callsign}
aircraft data: {aircraft_json}
route data: {route_json}

Scoring guide:
- Military aircraft (any country, e.g. GAF, RCH, REACH, NATO): 9-10
- Private/executive jet (Gulfstream, Bombardier Global, Dassault Falcon, etc.): 7-8
- Long-haul exotic operator or unusual destination (intercontinental, Middle East, Asia): 6-8
- Cargo aircraft or unusual type (A380, 747, C-130, etc.): 5-7
- Unknown aircraft with no registration data: 5-6
- Regular short-haul (Ryanair, Wizz, EasyJet, Eurowings, TUI, Condor): 1-3
- Standard charter or regional flight: 3-5

Also provide:
- tags: 1-3 short English tags (e.g. ["military", "luftwaffe"] or ["private_jet", "us_registered"])
- annotation: one German sentence about why this aircraft is interesting (empty string if not interesting)

Output ONLY valid JSON (no explanation):
{{"score": <int 1-10>, "tags": [<strings>], "annotation": "<string>"}}\
"""


def _score_and_annotate(
    client: anthropic.Anthropic,
    hex_: str,
    callsign: str | None,
    aircraft_json: str,
    route_json: str | None,
) -> dict:
    """Call Haiku to score and annotate one aircraft. Never raises."""
    try:
        prompt = _SCORE_PROMPT.format(
            hex_=hex_,
            callsign=callsign or "unknown",
            aircraft_json=aircraft_json,
            route_json=route_json or "unknown",
        )
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=150,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text
        match = re.search(r"\{[^{}]+\}", text, re.DOTALL)
        if match:
            result = json.loads(match.group())
            return {
                "score": int(result.get("score", 3)),
                "tags": list(result.get("tags", [])),
                "annotation": str(result.get("annotation", "")),
            }
        logger.warning("No JSON in score response for %s: %s", hex_, text[:200])
    except Exception:
        logger.exception("_score_and_annotate failed for %s", hex_)
    return {"score": 3, "tags": [], "annotation": ""}


def run_enrichment(config: Config) -> None:
    """Enrich up to 50 unenriched aircraft. Called every 15 minutes by the scheduler."""
    try:
        rows = get_unenriched_aircraft(config.database_url, limit=50)
    except Exception:
        logger.exception("get_unenriched_aircraft failed")
        return

    if not rows:
        logger.debug("No aircraft to enrich")
        return

    logger.info("Enriching %d aircraft", len(rows))
    client = anthropic.Anthropic(api_key=config.anthropic_api_key)

    for hex_, callsign in rows:
        try:
            aircraft_json = lookup_aircraft(hex_)
            route_json = lookup_route(callsign) if callsign else None
            aircraft_dict = json.loads(aircraft_json)
            route_dict = json.loads(route_json) if route_json else None

            score_result = _score_and_annotate(
                client, hex_, callsign, aircraft_json, route_json
            )
            store_enrichment(
                config.database_url,
                hex_,
                callsign,
                aircraft_dict,
                route_dict,
                score_result,
            )
            logger.debug(
                "Enriched %s (callsign=%s score=%s tags=%s)",
                hex_,
                callsign,
                score_result["score"],
                score_result["tags"],
            )
        except Exception:
            logger.exception("Failed to enrich %s", hex_)
