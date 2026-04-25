from __future__ import annotations

from datetime import date
from typing import Any

from life.connectors.notion import NotionConnector
from life.connectors.oura import OuraConnector
from life.enums import OuraDailyEndpoint


class _FakeResponse:
    def __init__(self, body: dict[str, Any]) -> None:
        self._body = body

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._body


def test_notion_connector_query_pagination_shape() -> None:
    connector = NotionConnector("token", "db-id")

    responses = [
        {
            "results": [{"id": "page-1", "properties": {}}],
            "has_more": True,
            "next_cursor": "cursor-1",
        },
        {
            "results": [{"id": "page-2", "properties": {}}],
            "has_more": False,
            "next_cursor": None,
        },
    ]
    calls: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], timeout: int) -> _FakeResponse:
        calls.append({"url": url, "json": dict(json), "timeout": timeout})
        body = responses[len(calls) - 1]
        return _FakeResponse(body)

    connector.session.post = fake_post  # type: ignore[assignment]

    records = connector.fetch(date_start=date(2026, 1, 1), date_end=date(2026, 1, 31))

    assert len(records) == 2
    assert calls[0]["json"]["filter"]["and"][0]["property"] == "Date"
    assert calls[1]["json"]["start_cursor"] == "cursor-1"


def test_oura_connector_v2_pagination_shape() -> None:
    connector = OuraConnector("token", "https://api.ouraring.com/v2/usercollection")

    responses = [
        {
            "data": [{"id": "oura-1", "day": "2026-01-01", "score": 80}],
            "next_token": "token-1",
        },
        {
            "data": [{"id": "oura-2", "day": "2026-01-02", "score": 82}],
            "next_token": None,
        },
    ]
    calls: list[dict[str, Any]] = []

    def fake_get(url: str, params: dict[str, Any], timeout: int) -> _FakeResponse:
        calls.append({"url": url, "params": dict(params), "timeout": timeout})
        body = responses[len(calls) - 1]
        return _FakeResponse(body)

    connector.session.get = fake_get  # type: ignore[assignment]

    rows = connector.fetch(
        date_start=date(2026, 1, 1),
        date_end=date(2026, 1, 2),
        endpoint=OuraDailyEndpoint.DAILY_SLEEP,
    )

    assert len(rows) == 2
    assert calls[0]["url"].endswith("/daily_sleep")
    assert calls[1]["params"]["next_token"] == "token-1"
