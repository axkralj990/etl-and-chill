from __future__ import annotations

from datetime import date

import requests

from life.enums import OuraDailyEndpoint
from life.pipeline.shared import run_oura_sync
from life.storage.duckdb import DuckDBStorage


class _FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class _FakeOuraConnector:
    def fetch(self, *, date_start, date_end, endpoint):
        if endpoint == OuraDailyEndpoint.DAILY_RESILIENCE:
            raise requests.HTTPError(
                "401 unauthorized",
                response=_FakeResponse(401),
            )
        return [{"id": f"id-{endpoint.value}", "day": "2026-04-20", "score": 80}]


def test_run_oura_sync_skips_unauthorized_endpoint(tmp_path) -> None:
    storage = DuckDBStorage(tmp_path / "test.duckdb")
    connector = _FakeOuraConnector()

    count = run_oura_sync(
        storage,
        connector,  # type: ignore[arg-type]
        date(2026, 4, 1),
        date(2026, 4, 24),
        [
            OuraDailyEndpoint.DAILY_ACTIVITY,
            OuraDailyEndpoint.DAILY_RESILIENCE,
            OuraDailyEndpoint.DAILY_SLEEP,
        ],
    )

    assert count >= 1
    rows = storage.conn.execute("select count(*) from canonical_oura_daily").fetchone()[0]
    assert rows >= 1
