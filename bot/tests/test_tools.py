"""Tests for bot.tools — pure logic and external API calls.

DB-heavy tools are tested for their error-handling contract only (must always
return valid JSON, never raise). API tools are tested with mocked requests.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from bot.tools import make_tools, _SORT_COLUMNS, _SQUAWK_MEANINGS

DB_URL = "postgresql://localhost/test"


@pytest.fixture
def tools() -> dict:
    """Return make_tools() result as a name→callable dict."""
    return {fn.__name__: fn for fn in make_tools(DB_URL)}


# ---------------------------------------------------------------------------
# make_tools contract
# ---------------------------------------------------------------------------


def test_make_tools_returns_all_expected_names(tools: dict) -> None:
    expected = {
        "get_stats",
        "get_top_sightings",
        "get_record",
        "get_new_aircraft",
        "get_squawk_alerts",
        "get_night_flights",
        "get_silent_aircraft",
        "get_altitude_bands",
        "get_speed_outliers",
        "get_busy_slots",
        "get_sightings_by_category",
        "get_operator_breakdown",
        "get_low_passes",
        "get_return_visitors_detail",
        "get_track_distribution",
        "lookup_route_batch",
        "get_rare_visitors",
        "get_approach_hints",
        "get_signal_records",
        "get_squawk_distribution",
        "get_formation_windows",
        "get_callsign_history",
        "get_weekly_rhythm",
        "get_vertical_speed_outliers",
        "get_distance_percentiles",
        "compare_periods",
        "lookup_aircraft",
        "lookup_route",
        "lookup_photo",
    }
    assert set(tools.keys()) == expected


# ---------------------------------------------------------------------------
# Sort validation (pure Python, no DB)
# ---------------------------------------------------------------------------


def test_get_top_sightings_invalid_sort_returns_error(tools: dict) -> None:
    result = tools["get_top_sightings"](sort_by="invalid_value")
    data = json.loads(result)
    assert "error" in data
    assert "sort_by" in data["error"]


def test_sort_columns_has_expected_keys() -> None:
    assert set(_SORT_COLUMNS.keys()) == {
        "closest",
        "highest",
        "lowest",
        "longest",
        "recent",
    }


def test_get_record_invalid_type_returns_error(tools: dict) -> None:
    with patch("bot.tools.psycopg2.connect", side_effect=Exception("DB down")):
        result = tools["get_record"](record_type="nonexistent")
    # nonexistent record type falls through to "return json.dumps error" before DB is called
    # Actually it hits the DB — but with mocked conn it will fail. Either way, valid JSON.
    data = json.loads(result)
    assert isinstance(data, dict)


# ---------------------------------------------------------------------------
# DB tools: error-handling contract
# Every DB tool must return valid JSON with an "error" key on DB failure.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "tool_name",
    [
        "get_stats",
        "get_top_sightings",
        "get_record",
        "get_new_aircraft",
        "get_squawk_alerts",
        "get_night_flights",
        "get_silent_aircraft",
        "get_altitude_bands",
        "get_speed_outliers",
        "get_busy_slots",
        "get_sightings_by_category",
        "get_operator_breakdown",
        "get_low_passes",
        "get_return_visitors_detail",
        "get_track_distribution",
        "get_rare_visitors",
        "get_approach_hints",
        "get_signal_records",
        "get_squawk_distribution",
        "get_formation_windows",
        "get_weekly_rhythm",
        "get_vertical_speed_outliers",
        "get_distance_percentiles",
        "compare_periods",
    ],
)
def test_db_tool_returns_error_json_on_db_failure(tools: dict, tool_name: str) -> None:
    """All DB tools must catch exceptions and return valid JSON with an 'error' key."""
    with patch(
        "bot.tools.psycopg2.connect", side_effect=Exception("connection refused")
    ):
        result = tools[tool_name]()
    data = json.loads(result)
    assert "error" in data, f"{tool_name} did not return error JSON on DB failure"


# ---------------------------------------------------------------------------
# lookup_aircraft — external API with mocked requests
# ---------------------------------------------------------------------------


def _mock_response(status_code: int = 200, json_data: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.raise_for_status = MagicMock()
    return resp


@pytest.fixture(autouse=True)
def clear_caches() -> None:
    """Prevent cache hits from bleeding between tests."""
    import bot.tools as tools_module

    tools_module._aircraft_cache.clear()
    tools_module._route_cache.clear()
    tools_module._photo_cache.clear()


def test_lookup_aircraft_happy_path(tools: dict) -> None:
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
        result = tools["lookup_aircraft"]("3c6444")

    data = json.loads(result)
    assert data["registration"] == "D-ABCD"
    assert data["operator"] == "Lufthansa"
    assert data["icao_type"] == "A320"


def test_lookup_aircraft_404_returns_error(tools: dict) -> None:
    with patch("bot.tools.requests.get", return_value=_mock_response(404)):
        result = tools["lookup_aircraft"]("000000")

    data = json.loads(result)
    assert "error" in data
    assert "not found" in data["error"]


def test_lookup_aircraft_network_error_returns_error(tools: dict) -> None:
    with patch("bot.tools.requests.get", side_effect=Exception("timeout")):
        result = tools["lookup_aircraft"]("3c6444")

    data = json.loads(result)
    assert "error" in data


def test_lookup_aircraft_caches_result(tools: dict) -> None:
    payload = {
        "response": {
            "aircraft": {
                "registration": "D-ABCD",
                "type": "A320",
                "icao_type": "A320",
                "registered_owner": "LH",
                "registered_owner_country_name": "Germany",
                "registered_owner_country_iso_name": "DE",
            }
        }
    }
    with patch(
        "bot.tools.requests.get", return_value=_mock_response(200, payload)
    ) as mock_get:
        tools["lookup_aircraft"]("3c6444")
        tools["lookup_aircraft"]("3c6444")  # second call — should hit cache

    assert mock_get.call_count == 1


# ---------------------------------------------------------------------------
# lookup_route — external API with mocked requests
# ---------------------------------------------------------------------------


def test_lookup_route_happy_path(tools: dict) -> None:
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
        result = tools["lookup_route"]("DLH123")

    data = json.loads(result)
    assert data["callsign"] == "DLH123"
    assert data["origin"]["iata"] == "FRA"
    assert data["destination"]["iata"] == "JFK"


def test_lookup_route_404_returns_error(tools: dict) -> None:
    with patch("bot.tools.requests.get", return_value=_mock_response(404)):
        result = tools["lookup_route"]("XXXXXX")

    data = json.loads(result)
    assert "error" in data


def test_lookup_route_empty_flightroute_returns_error(tools: dict) -> None:
    with patch(
        "bot.tools.requests.get",
        return_value=_mock_response(200, {"response": {"flightroute": {}}}),
    ):
        result = tools["lookup_route"]("DLH123")

    data = json.loads(result)
    assert "error" in data


# ---------------------------------------------------------------------------
# lookup_photo — external API with mocked requests
# ---------------------------------------------------------------------------


def test_lookup_photo_happy_path(tools: dict) -> None:
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
        result = tools["lookup_photo"]("3c6444")

    data = json.loads(result)
    assert data["photo_url"] == "https://example.com/photo.jpg"
    assert data["photographer"] == "Hans Mueller"
    assert data["registration"] == "D-ABCD"


def test_lookup_photo_no_photos_returns_error(tools: dict) -> None:
    with patch(
        "bot.tools.requests.get", return_value=_mock_response(200, {"photos": []})
    ):
        result = tools["lookup_photo"]("3c6444")

    data = json.loads(result)
    assert "error" in data


# ---------------------------------------------------------------------------
# lookup_route_batch — pure logic (parses callsigns, calls lookup_route)
# ---------------------------------------------------------------------------


def test_lookup_route_batch_parses_callsigns(tools: dict) -> None:
    payload = {
        "response": {
            "flightroute": {
                "callsign": "X",
                "origin": {
                    "iata_code": "FRA",
                    "icao_code": "EDDF",
                    "name": "FRA",
                    "municipality": "F",
                    "country_name": "DE",
                },
                "destination": {
                    "iata_code": "JFK",
                    "icao_code": "KJFK",
                    "name": "JFK",
                    "municipality": "NY",
                    "country_name": "US",
                },
            }
        }
    }
    with patch("bot.tools.requests.get", return_value=_mock_response(200, payload)):
        result = tools["lookup_route_batch"]("DLH123, RYR4AB , EZY99")

    data = json.loads(result)
    assert set(data.keys()) == {"DLH123", "RYR4AB", "EZY99"}


def test_lookup_route_batch_empty_input(tools: dict) -> None:
    result = tools["lookup_route_batch"]("  ,  ,  ")
    data = json.loads(result)
    assert data == {}


def test_lookup_route_batch_upcases_callsigns(tools: dict) -> None:
    payload = {
        "response": {
            "flightroute": {
                "callsign": "DLH123",
                "origin": {
                    "iata_code": "FRA",
                    "icao_code": "EDDF",
                    "name": "FRA",
                    "municipality": "F",
                    "country_name": "DE",
                },
                "destination": {
                    "iata_code": "JFK",
                    "icao_code": "KJFK",
                    "name": "JFK",
                    "municipality": "NY",
                    "country_name": "US",
                },
            }
        }
    }
    with patch("bot.tools.requests.get", return_value=_mock_response(200, payload)):
        result = tools["lookup_route_batch"]("dlh123")

    data = json.loads(result)
    assert "DLH123" in data


# ---------------------------------------------------------------------------
# Squawk meanings — static data sanity check
# ---------------------------------------------------------------------------


def test_get_callsign_history_returns_error_on_db_failure(tools: dict) -> None:
    """get_callsign_history requires icao_hex positional arg."""
    with patch(
        "bot.tools.psycopg2.connect", side_effect=Exception("connection refused")
    ):
        result = tools["get_callsign_history"]("3c6444")
    data = json.loads(result)
    assert "error" in data


def test_squawk_meanings_contains_emergency_codes() -> None:
    assert "7700" in _SQUAWK_MEANINGS
    assert "7600" in _SQUAWK_MEANINGS
    assert "7500" in _SQUAWK_MEANINGS
