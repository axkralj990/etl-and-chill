from __future__ import annotations

from datetime import date, timedelta

from life.config import Settings
from life.connectors.notion import NotionConnector
from life.connectors.oura import OuraConnector
from life.enums import PipelineMode, SourceName
from life.pipeline.runtime_config import load_runtime_config
from life.pipeline.shared import (
    finalize_features,
    run_notion_sync,
    run_oura_sync,
    run_wrapper,
)
from life.storage.duckdb import DuckDBStorage


def run(storage: DuckDBStorage, settings: Settings) -> None:
    missing_oura = not settings.oura_access_token
    missing_notion = not settings.notion_token
    missing_database = not settings.notion_database_id
    if missing_oura or missing_notion or missing_database:
        raise ValueError(
            "Incremental mode requires OURA_ACCESS_TOKEN, "
            "NOTION_TOKEN, and NOTION_DATABASE_ID"
        )

    notion_connector = NotionConnector(settings.notion_token, settings.notion_database_id)
    oura_connector = OuraConnector(settings.oura_access_token, settings.oura_base_url)
    runtime = load_runtime_config(settings.pipeline_config_path)

    def _run() -> dict:
        today = date.today()
        lookback_days = runtime.incremental.lookback_days
        fallback_days = runtime.incremental.fallback_days

        notion_max = storage.get_sync_state(SourceName.NOTION.value, "daily_logs")
        notion_start = (
            notion_max - timedelta(days=lookback_days)
            if notion_max
            else (today - timedelta(days=fallback_days))
        )

        oura_max_candidates = []
        for endpoint in runtime.oura.endpoints:
            current = storage.get_sync_state(SourceName.OURA.value, endpoint.value)
            if current:
                oura_max_candidates.append(current)
        max_oura = max(oura_max_candidates) if oura_max_candidates else None
        oura_start = (
            max_oura - timedelta(days=lookback_days)
            if max_oura
            else (today - timedelta(days=fallback_days))
        )

        notion_count = run_notion_sync(storage, notion_connector, notion_start, today)
        oura_count = run_oura_sync(
            storage,
            oura_connector,
            oura_start,
            today,
            runtime.oura.endpoints,
        )
        finalize_features(storage)
        return {
            "mode": PipelineMode.INCREMENTAL_SYNC.value,
            "notion_count": notion_count,
            "oura_count": oura_count,
            "date_start_notion": str(notion_start),
            "date_start_oura": str(oura_start),
            "date_end": str(today),
        }

    run_wrapper(storage, PipelineMode.INCREMENTAL_SYNC, _run)
