"""Unit tests for HexdbClient using mocked HTTP responses."""

from __future__ import annotations

from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from squawk.clients.hexdb import HexdbClient


def _mock_response(status: int, json_data: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status = status
    resp.raise_for_status = MagicMock()
    if json_data is not None:
        resp.json = AsyncMock(return_value=json_data)
    resp.__aenter__ = AsyncMock(return_value=resp)
    resp.__aexit__ = AsyncMock(return_value=False)
    return resp


def _make_client(
    max_retries: int = 3,
) -> tuple[HexdbClient, MagicMock]:
    """Return (client, mock_session). Configure mock_session.get before use."""
    mock_session = MagicMock()
    client = HexdbClient(
        session=cast(aiohttp.ClientSession, mock_session),
        base_url="https://hexdb.io/api/v1",
        max_retries=max_retries,
    )
    return client, mock_session


async def test_lookup_success() -> None:
    client, mock_session = _make_client()
    data = {
        "Registration": "G-EZWD",
        "Type": "Airbus A320",
        "RegisteredOwners": "easyJet",
        "OperatorFlagCode": "EZY",
        "ICAOTypeCode": "A320",
    }
    mock_session.get = MagicMock(return_value=_mock_response(200, data))

    result = await client.lookup("406a72")

    assert result is not None
    assert result.registration == "G-EZWD"
    assert result.type == "Airbus A320"
    assert result.operator == "easyJet"
    assert result.flag == "EZY"
    assert result.icao_type == "A320"


async def test_lookup_404_returns_none() -> None:
    client, mock_session = _make_client()
    mock_session.get = MagicMock(return_value=_mock_response(404))

    result = await client.lookup("000000")
    assert result is None


async def test_lookup_200_with_error_field_returns_none() -> None:
    # hexdb sometimes returns 200 with {"status": "404", "error": "..."}
    client, mock_session = _make_client()
    data = {"status": "404", "error": "Aircraft not found"}
    mock_session.get = MagicMock(return_value=_mock_response(200, data))

    result = await client.lookup("000000")
    assert result is None


async def test_lookup_retries_on_429() -> None:
    client, mock_session = _make_client()
    success_data = {
        "Registration": "D-AIWE",
        "Type": "Airbus A320neo",
        "RegisteredOwners": "Lufthansa",
        "OperatorFlagCode": "DLH",
        "ICAOTypeCode": "A20N",
    }
    responses = [_mock_response(429), _mock_response(200, success_data)]
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        result = responses[min(call_count, len(responses) - 1)]
        call_count += 1
        return result

    mock_session.get = MagicMock(side_effect=side_effect)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        result = await client.lookup("3c6585")

    assert result is not None
    assert result.registration == "D-AIWE"
    assert call_count == 2


async def test_lookup_retries_on_500() -> None:
    client, mock_session = _make_client()
    success_data = {
        "Registration": "PH-AXD",
        "Type": "Airbus A321neo",
        "RegisteredOwners": "KLM",
        "OperatorFlagCode": "KLM",
        "ICAOTypeCode": "A21N",
    }
    responses = [_mock_response(500), _mock_response(200, success_data)]
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        result = responses[min(call_count, len(responses) - 1)]
        call_count += 1
        return result

    mock_session.get = MagicMock(side_effect=side_effect)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        result = await client.lookup("486742")

    assert result is not None
    assert result.registration == "PH-AXD"
    assert call_count == 2


async def test_lookup_raises_after_max_retries() -> None:
    client, mock_session = _make_client(max_retries=1)
    resp = _mock_response(429)
    resp.raise_for_status = MagicMock(side_effect=Exception("rate limited"))
    mock_session.get = MagicMock(return_value=resp)

    with patch("asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(Exception, match="rate limited"):
            await client.lookup("000000")


async def test_lookup_url_uses_lowercase_hex() -> None:
    client, mock_session = _make_client()
    mock_session.get = MagicMock(return_value=_mock_response(404))

    await client.lookup("4D216E")

    call_args = mock_session.get.call_args
    url = call_args[0][0]
    assert "4d216e" in url
    assert "4D216E" not in url


async def test_lookup_empty_strings_become_none() -> None:
    client, mock_session = _make_client()
    data = {
        "Registration": "",
        "Type": "Airbus A320",
        "RegisteredOwners": "",
        "OperatorFlagCode": "",
        "ICAOTypeCode": "A320",
    }
    mock_session.get = MagicMock(return_value=_mock_response(200, data))

    result = await client.lookup("406a72")

    assert result is not None
    assert result.registration is None
    assert result.operator is None
    assert result.flag is None
    assert result.type == "Airbus A320"
