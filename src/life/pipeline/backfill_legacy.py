from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from life.config import Settings
from life.enums import OuraDailyEndpoint, PipelineMode, SourceName
from life.logging import get_logger
from life.normalizers.notion_daily import NotionDailyNormalizer
from life.normalizers.oura_daily import OuraDailyNormalizer, merge_oura_daily_rows
from life.pipeline.runtime_config import load_runtime_config
from life.pipeline.shared import (
    finalize_features,
    parse_legacy_notion_csv,
    parse_legacy_oura_csv,
    run_wrapper,
)
from life.storage.duckdb import DuckDBStorage

LOGGER = get_logger(__name__)

LEGACY_OURA_MAP = {
    "dailyactivity.csv": OuraDailyEndpoint.DAILY_ACTIVITY,
    "dailysleep.csv": OuraDailyEndpoint.DAILY_SLEEP,
    "sleepmodel.csv": OuraDailyEndpoint.SLEEP,
    "dailyreadiness.csv": OuraDailyEndpoint.DAILY_READINESS,
    "dailyspo2.csv": OuraDailyEndpoint.DAILY_SPO2,
    "dailystress.csv": OuraDailyEndpoint.DAILY_STRESS,
    "dailyresilience.csv": OuraDailyEndpoint.DAILY_RESILIENCE,
    "dailycardiovascularage.csv": OuraDailyEndpoint.DAILY_CARDIOVASCULAR_AGE,
}


def run(storage: DuckDBStorage, settings: Settings) -> None:
    runtime = load_runtime_config(settings.pipeline_config_path)

    def _run() -> dict:
        notion_rows = _load_legacy_notion(storage, settings.legacy_path / "notion")
        oura_rows = _load_legacy_oura(
            storage,
            settings.legacy_path / "oura",
            runtime.oura.endpoints,
        )
        finalize_features(storage)
        return {
            "notion_rows": notion_rows,
            "oura_rows": oura_rows,
            "mode": PipelineMode.BACKFILL_LEGACY.value,
        }

    run_wrapper(storage, PipelineMode.BACKFILL_LEGACY, _run)


def _load_legacy_notion(storage: DuckDBStorage, notion_dir: Path) -> int:
    all_records: list[dict] = []
    for path in sorted(notion_dir.glob("*.csv")):
        all_records.extend(parse_legacy_notion_csv(path))

    raw_rows = [
        {
            "source": SourceName.LEGACY.value,
            "endpoint": "notion_csv",
            "source_id": r["id"],
            "day": None,
            "payload": r,
            "ingested_at": datetime.utcnow(),
        }
        for r in all_records
    ]
    storage.insert_raw_records(raw_rows)

    notion_norm = NotionDailyNormalizer().normalize(all_records)
    storage.upsert_notion_daily(notion_norm)

    max_date = max((r["date_local"] for r in notion_norm), default=None)
    if max_date:
        storage.set_sync_state(SourceName.NOTION.value, "daily_logs", max_date)

    LOGGER.info(
        "legacy notion loaded",
        source=SourceName.LEGACY.value,
        record_count=len(notion_norm),
    )
    return len(notion_norm)


def _load_legacy_oura(
    storage: DuckDBStorage,
    oura_dir: Path,
    enabled_endpoints: list[OuraDailyEndpoint],
) -> int:
    normalizer = OuraDailyNormalizer()
    normalized_all: list[dict] = []
    raw_rows: list[dict] = []

    for filename, endpoint in LEGACY_OURA_MAP.items():
        if endpoint not in enabled_endpoints:
            continue
        path = oura_dir / filename
        if not path.exists():
            continue

        parsed = parse_legacy_oura_csv(path)
        for row in parsed:
            raw_rows.append(
                {
                    "source": SourceName.LEGACY.value,
                    "endpoint": endpoint.value,
                    "source_id": row.get("id", f"legacy-{endpoint.value}"),
                    "day": date.fromisoformat(row["day"]) if row.get("day") else None,
                    "payload": row,
                    "ingested_at": datetime.utcnow(),
                }
            )
        normalized_all.extend(normalizer.normalize(parsed, endpoint=endpoint))

        endpoint_max = max(
            (date.fromisoformat(r["day"]) for r in parsed if isinstance(r.get("day"), str)),
            default=None,
        )
        if endpoint_max:
            storage.set_sync_state(SourceName.OURA.value, endpoint.value, endpoint_max)

    storage.insert_raw_records(raw_rows)
    merged = merge_oura_daily_rows(normalized_all)
    storage.upsert_oura_daily(merged)

    LOGGER.info("legacy oura loaded", source=SourceName.LEGACY.value, record_count=len(merged))
    return len(merged)
