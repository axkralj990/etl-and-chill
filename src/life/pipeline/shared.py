from __future__ import annotations

import csv
import json
import uuid
from contextlib import suppress
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import requests

from life.connectors.notion import NotionConnector
from life.connectors.oura import OuraConnector
from life.enums import OuraDailyEndpoint, PipelineMode, RunStatus, SourceName
from life.logging import get_logger
from life.normalizers.notion_daily import NotionDailyNormalizer
from life.normalizers.oura_daily import OuraDailyNormalizer, merge_oura_daily_rows
from life.storage.duckdb import DuckDBStorage

LOGGER = get_logger(__name__)


def _raw_row(
    source: SourceName,
    endpoint: str,
    source_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    day = payload.get("day")
    return {
        "source": source.value,
        "endpoint": endpoint,
        "source_id": source_id,
        "day": date.fromisoformat(day) if isinstance(day, str) else None,
        "payload": payload,
        "ingested_at": datetime.now(UTC),
    }


def run_oura_sync(
    storage: DuckDBStorage,
    connector: OuraConnector,
    date_start: date,
    date_end: date,
    endpoints: list[OuraDailyEndpoint],
) -> int:
    if date_start > date_end:
        LOGGER.info(
            "oura fetch skipped due to empty date window",
            source=SourceName.OURA.value,
            date_start=str(date_start),
            date_end=str(date_end),
        )
        return 0

    normalizer = OuraDailyNormalizer()
    all_norm: list[dict[str, Any]] = []
    raw_rows: list[dict[str, Any]] = []

    for endpoint in endpoints:
        try:
            fetched = connector.fetch(date_start=date_start, date_end=date_end, endpoint=endpoint)
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            LOGGER.warning(
                "oura endpoint fetch failed; skipping endpoint",
                source=SourceName.OURA.value,
                endpoint=endpoint.value,
                date_start=str(date_start),
                date_end=str(date_end),
                status_code=status_code,
                error=str(exc),
            )
            continue
        LOGGER.info(
            "oura fetch complete",
            source=SourceName.OURA.value,
            endpoint=endpoint.value,
            date_start=str(date_start),
            date_end=str(date_end),
            record_count=len(fetched),
        )

        raw_rows.extend(
            _raw_row(SourceName.OURA, endpoint.value, row.get("id", "unknown"), row)
            for row in fetched
        )

        normalized = normalizer.normalize(fetched, endpoint=endpoint)
        all_norm.extend(normalized)

        endpoint_max = max(
            (date.fromisoformat(r["day"]) for r in fetched if isinstance(r.get("day"), str)),
            default=None,
        )
        if endpoint_max:
            storage.set_sync_state(SourceName.OURA.value, endpoint.value, endpoint_max)

    storage.insert_raw_records(raw_rows)
    merged = merge_oura_daily_rows(all_norm)
    storage.upsert_oura_daily(merged)
    return len(merged)


def run_notion_sync(
    storage: DuckDBStorage,
    connector: NotionConnector,
    date_start: date,
    date_end: date,
) -> int:
    if date_start > date_end:
        LOGGER.info(
            "notion fetch skipped due to empty date window",
            source=SourceName.NOTION.value,
            date_start=str(date_start),
            date_end=str(date_end),
        )
        return 0

    fetched = connector.fetch(date_start=date_start, date_end=date_end)
    LOGGER.info(
        "notion fetch complete",
        source=SourceName.NOTION.value,
        endpoint="daily_logs",
        date_start=str(date_start),
        date_end=str(date_end),
        record_count=len(fetched),
    )

    raw_rows = [
        _raw_row(SourceName.NOTION, "database_query", row.get("id", "unknown"), row)
        for row in fetched
    ]
    storage.insert_raw_records(raw_rows)

    normalized = NotionDailyNormalizer().normalize(fetched)
    storage.upsert_notion_daily(normalized)

    max_date = max((row["date_local"] for row in normalized), default=None)
    if max_date:
        storage.set_sync_state(SourceName.NOTION.value, "daily_logs", max_date)
    return len(normalized)


def parse_legacy_oura_csv(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter=";")
        for row in reader:
            payload: dict[str, Any] = dict(row)
            for key, value in list(payload.items()):
                if not isinstance(value, str):
                    continue
                stripped = value.strip()
                if not stripped:
                    continue
                if not (
                    (stripped.startswith("{") and stripped.endswith("}"))
                    or (stripped.startswith("[") and stripped.endswith("]"))
                ):
                    continue
                with suppress(json.JSONDecodeError):
                    payload[key] = json.loads(stripped)
            rows.append(payload)
    return rows


def _parse_any_date(value: str) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        pass
    for fmt in ("%B %d, %Y", "%A, %B %d, %Y", "%A, %B, %d, %Y", "%A %B %d, %Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def parse_legacy_notion_csv(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            date_value = _parse_any_date((row.get("Date") or "").strip())
            if not date_value:
                continue

            properties: dict[str, Any] = {
                "Date": {"date": {"start": date_value.isoformat()}},
                "Name": {"title": [{"plain_text": row.get("Name", "")}]},
                "Anxiety Status": {"select": {"name": (row.get("Anxiety Status") or "").strip()}},
                "Physical Status": {"select": {"name": (row.get("Physical Status") or "").strip()}},
                "Productivity": {"select": {"name": (row.get("Productivity") or "").strip()}},
                "Weight (kg)": {"number": _try_float(row.get("Weight (kg)"))},
                "Alcohol (unt)": {"number": _try_float(row.get("Alcohol (unt)"))},
                "Mindful (min)": {"number": _try_float(row.get("Mindful (min)"))},
                "Points": {"number": _try_float(row.get("Points"))},
                "Coffee (#)": {"number": _try_float(row.get("Coffee (#)"))},
                "Fasting": {"number": _try_float(row.get("Fasting"))},
                "Sleep (hrs)": {"number": _try_float(row.get("Sleep (hrs)"))},
                "Cold (min)": {"number": _try_float(row.get("Cold (min)"))},
                "Substances": {"rich_text": [{"plain_text": row.get("Substances", "")}]},
                "General Notes": {"rich_text": [{"plain_text": row.get("General Notes", "")}]},
                "Supplements": {"rich_text": [{"plain_text": row.get("Supplements", "")}]},
                "Weather": {"rich_text": [{"plain_text": row.get("Weather", "")}]},
                "Learned": {"rich_text": [{"plain_text": row.get("Learned", "")}]},
                "Workout": {"rich_text": [{"plain_text": row.get("Workout", "")}]},
            }
            rows.append(
                {
                    "id": f"legacy-{date_value.isoformat()}-{uuid.uuid4().hex[:8]}",
                    "properties": properties,
                }
            )
    return rows


def _try_float(value: str | None) -> float | None:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def finalize_features(storage: DuckDBStorage) -> None:
    storage.build_daily_features()


def run_wrapper(storage: DuckDBStorage, mode: PipelineMode, fn) -> None:
    run_id = f"{mode.value}-{uuid.uuid4()}"
    storage.insert_run_start(run_id, mode.value)
    try:
        details = fn()
        storage.finalize_run(run_id, RunStatus.SUCCESS, json.dumps(details, ensure_ascii=True))
    except Exception as exc:
        storage.finalize_run(
            run_id,
            RunStatus.FAILED,
            json.dumps({"error": str(exc)}, ensure_ascii=True),
        )
        raise


def default_start_from(max_date: date | None, fallback_days: int) -> date:
    if max_date:
        return max_date + timedelta(days=1)
    return date.today() - timedelta(days=fallback_days)
