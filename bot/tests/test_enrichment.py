"""Tests for bot.enrichment._score_and_annotate."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bot.enrichment import _score_and_annotate


def _fake_final_event(json_text: str) -> MagicMock:
    """Build a mock ADK event that looks like a final response."""
    part = MagicMock()
    part.text = json_text
    event = MagicMock()
    event.is_final_response.return_value = True
    event.content.parts = [part]
    return event


async def _aiter(items):
    for item in items:
        yield item


@pytest.fixture()
def mock_runner():
    """Patch Runner so run_async yields a controllable event."""
    with patch("bot.enrichment.Runner") as mock_cls:
        instance = MagicMock()
        mock_cls.return_value = instance
        yield instance


@pytest.fixture()
def mock_session():
    with patch("bot.enrichment.InMemorySessionService") as mock_cls:
        instance = MagicMock()
        instance.create_session = AsyncMock()
        mock_cls.return_value = instance
        yield instance


async def test_score_and_annotate_happy_path(mock_runner, mock_session) -> None:
    event = _fake_final_event(
        '{"score": 9, "tags": ["military", "luftwaffe"], "annotation": "Bundeswehr-Transportflugzeug."}'
    )
    mock_runner.run_async.return_value = _aiter([event])

    result = await _score_and_annotate("3c4001", "GAF123", '{"type": "A400M"}', None)

    assert result["score"] == 9
    assert "military" in result["tags"]
    assert result["annotation"] == "Bundeswehr-Transportflugzeug."


async def test_score_and_annotate_returns_default_on_runner_exception(
    mock_runner, mock_session
) -> None:
    mock_runner.run_async.side_effect = Exception("LLM timeout")

    result = await _score_and_annotate("3c4001", "GAF123", "{}", None)

    assert result == {"score": 3, "tags": [], "annotation": ""}


async def test_score_and_annotate_returns_default_when_no_final_event(
    mock_runner, mock_session
) -> None:
    non_final = MagicMock()
    non_final.is_final_response.return_value = False
    mock_runner.run_async.return_value = _aiter([non_final])

    result = await _score_and_annotate("abcdef", None, '{"error": "not found"}', None)

    assert result == {"score": 3, "tags": [], "annotation": ""}


async def test_score_and_annotate_returns_default_on_invalid_json(
    mock_runner, mock_session
) -> None:
    event = _fake_final_event("not valid json at all")
    mock_runner.run_async.return_value = _aiter([event])

    result = await _score_and_annotate("abcdef", None, "{}", None)

    assert result == {"score": 3, "tags": [], "annotation": ""}


async def test_score_and_annotate_passes_data_to_runner(
    mock_runner, mock_session
) -> None:
    event = _fake_final_event('{"score": 5, "tags": [], "annotation": ""}')
    mock_runner.run_async.return_value = _aiter([event])

    aircraft_json = '{"type": "Boeing 737", "operator": "Ryanair"}'
    route_json = '{"origin": {"city": "London"}, "destination": {"city": "Stuttgart"}}'
    await _score_and_annotate("4ca1b2", "RYR123", aircraft_json, route_json)

    call_kwargs = mock_runner.run_async.call_args.kwargs
    prompt_text = call_kwargs["new_message"].parts[0].text
    assert "RYR123" in prompt_text
    assert route_json in prompt_text
