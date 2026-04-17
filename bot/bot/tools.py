"""External API lookups: aircraft registry, routes, and photos."""

from __future__ import annotations

import json
import logging

import requests

logger = logging.getLogger(__name__)


def lookup_aircraft(icao_hex: str) -> str:
    """Look up registration, aircraft type, and operator for an ICAO hex code.

    Uses the public adsbdb.com API. Returns registration, type, icao_type,
    operator, country, or an error message.

    Args:
        icao_hex: The 6-character ICAO 24-bit hex address (e.g. "3c6444").
    """
    key = icao_hex.lower()
    try:
        url = f"https://api.adsbdb.com/v0/aircraft/{key}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 404:
            return json.dumps({"error": "aircraft not found in database"})
        resp.raise_for_status()
        data = resp.json()
        aircraft = data.get("response", {}).get("aircraft", {})
        return json.dumps(
            {
                "registration": aircraft.get("registration"),
                "type": aircraft.get("type"),
                "icao_type": aircraft.get("icao_type"),
                "operator": aircraft.get("registered_owner"),
                "country": aircraft.get("registered_owner_country_name"),
                "flag": aircraft.get("registered_owner_country_iso_name"),
            }
        )
    except Exception as exc:
        logger.exception("lookup_aircraft failed for %s", icao_hex)
        return json.dumps({"error": str(exc)})


def lookup_route(callsign: str) -> str:
    """Look up the origin and destination airports for a flight callsign.

    Uses the public adsbdb.com API. Returns origin and destination airport
    details (IATA/ICAO codes, city, country), or an error if unknown.

    Args:
        callsign: The flight callsign (e.g. "DLH123", "EZY4241").
    """
    key = callsign.upper().strip()
    try:
        url = f"https://api.adsbdb.com/v0/callsign/{key}"
        resp = requests.get(url, timeout=10)
        if resp.status_code == 404:
            return json.dumps({"error": "route not found in database"})
        resp.raise_for_status()
        data = resp.json()
        route = data.get("response", {}).get("flightroute", {})
        if not route:
            return json.dumps({"error": "no route data available"})

        def _airport(ap: dict) -> dict:
            return {
                "iata": ap.get("iata_code"),
                "icao": ap.get("icao_code"),
                "name": ap.get("name"),
                "city": ap.get("municipality"),
                "country": ap.get("country_name"),
            }

        return json.dumps(
            {
                "callsign": route.get("callsign"),
                "origin": _airport(route.get("origin", {})),
                "destination": _airport(route.get("destination", {})),
            }
        )
    except Exception as exc:
        logger.exception("lookup_route failed for %s", callsign)
        return json.dumps({"error": str(exc)})


def lookup_photo(icao_hex: str) -> str:
    """Look up a photo of an aircraft by its ICAO hex code.

    Uses the planespotters.net public API. Returns the direct image URL
    and photographer credit if a photo is available.

    Args:
        icao_hex: The 6-character ICAO 24-bit hex address (e.g. "3c6444").
    """
    key = icao_hex.lower()
    try:
        url = f"https://api.planespotters.net/pub/photos/hex/{key}"
        resp = requests.get(url, timeout=10, headers={"User-Agent": "squawk-bot/1.0"})
        if resp.status_code == 404:
            return json.dumps({"error": "no photo found"})
        resp.raise_for_status()
        data = resp.json()
        photos = data.get("photos", [])
        if not photos:
            return json.dumps({"error": "no photo found"})
        photo = photos[0]
        return json.dumps(
            {
                "photo_url": photo.get("thumbnail_large", {}).get("src"),
                "link": photo.get("link"),
                "photographer": photo.get("photographer"),
                "registration": photo.get("aircraft", {}).get("reg"),
            }
        )
    except Exception as exc:
        logger.exception("lookup_photo failed for %s", icao_hex)
        return json.dumps({"error": str(exc)})
