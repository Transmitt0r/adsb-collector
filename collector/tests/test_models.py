"""Tests for collector.models.AircraftState.from_json."""

from __future__ import annotations

from datetime import datetime, timezone

from collector.models import AircraftState

NOW = 1700000000.0


def test_alt_baro_ground_string_returns_none() -> None:
    """alt_baro='ground' must become None — key fact in CLAUDE.md."""
    state = AircraftState.from_json({"hex": "abc123", "alt_baro": "ground"}, NOW)
    assert state.alt_baro is None


def test_alt_baro_int_is_kept() -> None:
    state = AircraftState.from_json({"hex": "abc123", "alt_baro": 35000}, NOW)
    assert state.alt_baro == 35000


def test_alt_baro_float_returns_none() -> None:
    """Non-int alt_baro is discarded (isinstance guard)."""
    state = AircraftState.from_json({"hex": "abc123", "alt_baro": 35000.5}, NOW)
    assert state.alt_baro is None


def test_flight_stripped() -> None:
    """Callsigns from tar1090 have trailing spaces — must be stripped."""
    state = AircraftState.from_json({"hex": "abc123", "flight": "DLH1A  "}, NOW)
    assert state.flight == "DLH1A"


def test_empty_flight_is_none() -> None:
    state = AircraftState.from_json({"hex": "abc123", "flight": "   "}, NOW)
    assert state.flight is None


def test_missing_flight_is_none() -> None:
    state = AircraftState.from_json({"hex": "abc123"}, NOW)
    assert state.flight is None


def test_missing_optional_fields_are_none() -> None:
    state = AircraftState.from_json({"hex": "abc123"}, NOW)
    assert state.alt_baro is None
    assert state.gs is None
    assert state.lat is None
    assert state.lon is None
    assert state.squawk is None
    assert state.r_dst is None
    assert state.rssi is None


def test_timestamp_computed_from_seen() -> None:
    """timestamp = now - seen."""
    state = AircraftState.from_json({"hex": "abc123", "seen": 5.0}, NOW)
    expected = datetime.fromtimestamp(NOW - 5.0, tz=timezone.utc)
    assert state.timestamp == expected


def test_seen_defaults_to_zero() -> None:
    state = AircraftState.from_json({"hex": "abc123"}, NOW)
    assert state.seen == 0.0
    expected = datetime.fromtimestamp(NOW, tz=timezone.utc)
    assert state.timestamp == expected


def test_all_fields_populated() -> None:
    data = {
        "hex": "3c6752",
        "flight": "DLH123",
        "alt_baro": 36000,
        "gs": 450.0,
        "track": 180.0,
        "lat": 48.76,
        "lon": 9.15,
        "squawk": "1000",
        "category": "A3",
        "r_dst": 5.2,
        "rssi": -20.0,
        "messages": 150,
        "seen": 1.5,
    }
    state = AircraftState.from_json(data, NOW)
    assert state.hex == "3c6752"
    assert state.flight == "DLH123"
    assert state.alt_baro == 36000
    assert state.gs == 450.0
    assert state.track == 180.0
    assert state.lat == 48.76
    assert state.lon == 9.15
    assert state.squawk == "1000"
    assert state.category == "A3"
    assert state.r_dst == 5.2
    assert state.rssi == -20.0
    assert state.messages == 150
    assert state.seen == 1.5
