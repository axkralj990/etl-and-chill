from __future__ import annotations

from datetime import date

from life.enums import OuraDailyEndpoint
from life.pipeline.shared import run_notion_sync, run_oura_sync
from life.storage.duckdb import DuckDBStorage


class _FailNotionConnector:
    def fetch(self, *, date_start, date_end):
        raise AssertionError("Notion connector should not be called for empty window")


class _FailOuraConnector:
    def fetch(self, *, date_start, date_end, endpoint):
        raise AssertionError("Oura connector should not be called for empty window")


def test_run_notion_sync_skips_when_start_after_end(tmp_path) -> None:
    storage = DuckDBStorage(tmp_path / "test.duckdb")
    count = run_notion_sync(
        storage,
        _FailNotionConnector(),  # type: ignore[arg-type]
        date(2026, 4, 25),
        date(2026, 4, 24),
    )
    assert count == 0


def test_run_oura_sync_skips_when_start_after_end(tmp_path) -> None:
    storage = DuckDBStorage(tmp_path / "test.duckdb")
    count = run_oura_sync(
        storage,
        _FailOuraConnector(),  # type: ignore[arg-type]
        date(2026, 4, 25),
        date(2026, 4, 24),
        [OuraDailyEndpoint.DAILY_SLEEP],
    )
    assert count == 0
