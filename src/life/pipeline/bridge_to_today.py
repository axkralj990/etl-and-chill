from __future__ import annotations

from datetime import date

from life.config import Settings
from life.connectors.notion import NotionConnector
from life.connectors.oura import OuraConnector
from life.enums import PipelineMode, SourceName
from life.pipeline.runtime_config import load_runtime_config
from life.pipeline.shared import (
    default_start_from,
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
            "Bridge mode requires OURA_ACCESS_TOKEN, NOTION_TOKEN, and NOTION_DATABASE_ID"
        )

    notion_connector = NotionConnector(settings.notion_token, settings.notion_database_id)
    oura_connector = OuraConnector(settings.oura_access_token, settings.oura_base_url)
    runtime = load_runtime_config(settings.pipeline_config_path)

    def _run() -> dict:
        notion_max = storage.get_sync_state(SourceName.NOTION.value, "daily_logs")
        notion_start = default_start_from(notion_max, runtime.bridge.fallback_days)

        oura_max_candidates: list[date] = []
        for endpoint in runtime.oura.endpoints:
            current = storage.get_sync_state(SourceName.OURA.value, endpoint.value)
            if current:
                oura_max_candidates.append(current)
        oura_start = default_start_from(
            max(oura_max_candidates) if oura_max_candidates else None,
            runtime.bridge.fallback_days,
        )

        today = date.today()
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
            "mode": PipelineMode.BRIDGE_TO_TODAY.value,
            "notion_count": notion_count,
            "oura_count": oura_count,
            "date_end": str(today),
        }

    run_wrapper(storage, PipelineMode.BRIDGE_TO_TODAY, _run)
