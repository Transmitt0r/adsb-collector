"""Tests for bot.tools — external API lookups with mocked requests."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from bot.tools import lookup_aircraft, lookup_photo, lookup_route


def _mock_response(status_code: int = 200, json_data: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# lookup_aircraft
# ---------------------------------------------------------------------------


def test_lookup_aircraft_happy_path() -> None:
    payload = {
        "response": {
            "aircraft": {
                "registration": "D-ABCD",
                "type": "Airbus A320",
                "icao_type": "A320",
                "registered_owner": "Lufthansa",
                "registered_owner_country_name": "Germany",
                "registered_owner_country_iso_name": "DE",
            }
        }
    }
    with patch("bot.tools.requests.get", return_value=_mock_response(200, payload)):
        result = lookup_aircraft("3c6444")

    data = json.loads(result)
    assert data["registration"] == "D-ABCD"
    assert data["operator"] == "Lufthansa"
    assert data["icao_type"] == "A320"


def test_lookup_aircraft_404_returns_error() -> None:
    with patch("bot.tools.requests.get", return_value=_mock_response(404)):
        result = lookup_aircraft("000000")

    data = json.loads(result)
    assert "error" in data
    assert "not found" in data["error"]


def test_lookup_aircraft_network_error_returns_error() -> None:
    with patch("bot.tools.requests.get", side_effect=Exception("timeout")):
        result = lookup_aircraft("3c6444")

    data = json.loads(result)
    assert "error" in data


# ---------------------------------------------------------------------------
# lookup_route
# ---------------------------------------------------------------------------


def test_lookup_route_happy_path() -> None:
    payload = {
        "response": {
            "flightroute": {
                "callsign": "DLH123",
                "origin": {
                    "iata_code": "FRA",
                    "icao_code": "EDDF",
                    "name": "Frankfurt Airport",
                    "municipality": "Frankfurt",
                    "country_name": "Germany",
                },
                "destination": {
                    "iata_code": "JFK",
                    "icao_code": "KJFK",
                    "name": "John F. Kennedy Airport",
                    "municipality": "New York",
                    "country_name": "United States",
                },
            }
        }
    }
    with patch("bot.tools.requests.get", return_value=_mock_response(200, payload)):
        result = lookup_route("DLH123")

    data = json.loads(result)
    assert data["callsign"] == "DLH123"
    assert data["origin"]["iata"] == "FRA"
    assert data["destination"]["iata"] == "JFK"


def test_lookup_route_404_returns_error() -> None:
    with patch("bot.tools.requests.get", return_value=_mock_response(404)):
        result = lookup_route("XXXXXX")

    data = json.loads(result)
    assert "error" in data


def test_lookup_route_empty_flightroute_returns_error() -> None:
    with patch(
        "bot.tools.requests.get",
        return_value=_mock_response(200, {"response": {"flightroute": {}}}),
    ):
        result = lookup_route("DLH123")

    data = json.loads(result)
    assert "error" in data


# ---------------------------------------------------------------------------
# lookup_photo
# ---------------------------------------------------------------------------


def test_lookup_photo_happy_path() -> None:
    payload = {
        "photos": [
            {
                "thumbnail_large": {"src": "https://example.com/photo.jpg"},
                "link": "https://example.com",
                "photographer": "Hans Mueller",
                "aircraft": {"reg": "D-ABCD"},
            }
        ]
    }
    with patch("bot.tools.requests.get", return_value=_mock_response(200, payload)):
        result = lookup_photo("3c6444")

    data = json.loads(result)
    assert data["photo_url"] == "https://example.com/photo.jpg"
    assert data["photographer"] == "Hans Mueller"
    assert data["registration"] == "D-ABCD"


def test_lookup_photo_no_photos_returns_error() -> None:
    with patch(
        "bot.tools.requests.get", return_value=_mock_response(200, {"photos": []})
    ):
        result = lookup_photo("3c6444")

    data = json.loads(result)
    assert "error" in data
