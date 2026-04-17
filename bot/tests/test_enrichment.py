"""Tests for bot.enrichment._score_and_annotate."""

from __future__ import annotations

from unittest.mock import MagicMock

from bot.enrichment import _score_and_annotate


def _make_client(response_text: str) -> MagicMock:
    """Build a mock Anthropic client whose messages.create returns response_text."""
    content_block = MagicMock()
    content_block.text = response_text
    message = MagicMock()
    message.content = [content_block]
    client = MagicMock()
    client.messages.create.return_value = message
    return client


def test_score_and_annotate_happy_path() -> None:
    client = _make_client(
        '{"score": 9, "tags": ["military", "luftwaffe"], "annotation": "Bundeswehr-Transportflugzeug."}'
    )
    result = _score_and_annotate(client, "3c4001", "GAF123", '{"type": "A400M"}', None)

    assert result["score"] == 9
    assert "military" in result["tags"]
    assert result["annotation"] == "Bundeswehr-Transportflugzeug."


def test_score_and_annotate_returns_default_on_api_exception() -> None:
    client = MagicMock()
    client.messages.create.side_effect = Exception("API timeout")

    result = _score_and_annotate(client, "3c4001", "GAF123", "{}", None)

    assert result == {"score": 3, "tags": [], "annotation": ""}


def test_score_and_annotate_returns_default_on_malformed_json() -> None:
    client = _make_client("Sorry, I cannot score this aircraft.")

    result = _score_and_annotate(client, "abcdef", None, '{"error": "not found"}', None)

    assert result == {"score": 3, "tags": [], "annotation": ""}


def test_score_and_annotate_clamps_score_to_int() -> None:
    client = _make_client('{"score": "8", "tags": ["private_jet"], "annotation": ""}')
    result = _score_and_annotate(
        client, "a1b2c3", "N123GG", '{"type": "Gulfstream G650"}', None
    )

    assert isinstance(result["score"], int)
    assert result["score"] == 8


def test_score_and_annotate_passes_route_context() -> None:
    client = _make_client('{"score": 5, "tags": [], "annotation": ""}')
    aircraft_json = '{"type": "Boeing 737", "operator": "Ryanair"}'
    route_json = '{"origin": {"city": "London"}, "destination": {"city": "Stuttgart"}}'

    _score_and_annotate(client, "4ca1b2", "RYR123", aircraft_json, route_json)

    call_args = client.messages.create.call_args
    prompt = call_args.kwargs["messages"][0]["content"]
    assert route_json in prompt
