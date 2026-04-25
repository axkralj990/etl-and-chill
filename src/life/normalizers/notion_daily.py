from __future__ import annotations

import json
import re
from datetime import date
from typing import Any

from dateutil import parser

from life.enums import WarningCode
from life.logging import get_logger
from life.normalizers.base import BaseNormalizer

LOGGER = get_logger(__name__)

ANXIETY_MAP = {"1": 1, "2": 2, "3": 3, "4": 4, "5": 5}
PRODUCTIVITY_MAP = {"low": 1, "medium": 2, "high": 3}
PHYSICAL_MAP = {"bad": 1, "not so good": 2, "ok": 3, "good": 4}


class NotionDailyNormalizer(BaseNormalizer):
    @staticmethod
    def _extract_text(prop: dict[str, Any], key: str = "rich_text") -> str | None:
        chunks = prop.get(key, [])
        if not chunks:
            return None
        text = "".join(chunk.get("plain_text", "") for chunk in chunks)
        return text.strip() or None

    @staticmethod
    def _extract_select(prop: dict[str, Any]) -> str | None:
        select_obj = prop.get("select")
        if not select_obj:
            return None
        return select_obj.get("name")

    @staticmethod
    def _extract_number(prop: dict[str, Any]) -> float | None:
        return prop.get("number")

    @classmethod
    def _extract_number_or_zero(cls, prop: dict[str, Any]) -> float:
        value = cls._extract_number(prop)
        if value is None:
            return 0.0
        return float(value)

    @classmethod
    def _extract_indicator(cls, prop: dict[str, Any]) -> bool:
        value = cls._extract_number(prop)
        if value is None:
            return False
        return float(value) > 0

    @staticmethod
    def _extract_date(prop: dict[str, Any]) -> date | None:
        date_obj = prop.get("date")
        if not date_obj:
            return None
        value = date_obj.get("start")
        if not value:
            return None
        return parser.isoparse(value).date()

    @staticmethod
    def _extract_title(prop: dict[str, Any]) -> str | None:
        title_chunks = prop.get("title", [])
        if not title_chunks:
            return None
        text = "".join(chunk.get("plain_text", "") for chunk in title_chunks).strip()
        return text or None

    @staticmethod
    def _week_start_monday(value: date) -> date:
        return value.fromordinal(value.toordinal() - value.weekday())

    @staticmethod
    def _parse_title_date(title: str | None) -> date | None:
        if not title:
            return None
        cleaned = re.sub(r"\s+", " ", title.replace(",", " ")).strip()
        try:
            return parser.parse(cleaned, fuzzy=True).date()
        except (ValueError, TypeError, OverflowError):
            return None

    @staticmethod
    def _parse_cigarettes(substances: str | None) -> int | None:
        if not substances:
            return None

        lower = substances.lower()
        explicit = re.findall(r"(\d+)\s*cigs?", lower)
        if explicit:
            return sum(int(x) for x in explicit)

        token_c = re.findall(r"\bC\b", substances)
        if token_c:
            return len(token_c)

        compact = re.search(r"\b(\d+)\s*c\b", lower)
        if compact:
            return int(compact.group(1))

        return 0 if "cig" in lower else None

    @staticmethod
    def _clean_workout_name(raw: str) -> str:
        no_url = re.sub(r"\s*\(https?://[^)]+\)\s*", "", raw).strip()
        return re.sub(r"\s+", " ", no_url)

    @classmethod
    def _split_workouts(cls, workout_raw: str | None) -> list[str]:
        if not workout_raw:
            return []
        parts = [cls._clean_workout_name(part) for part in workout_raw.split(",")]
        return [part for part in parts if part]

    @staticmethod
    def _workout_type(name: str) -> str:
        lower = name.lower().strip()
        if lower.startswith("running") or lower.startswith("run"):
            return "running"
        if lower.startswith("strength"):
            return "strength"
        if lower.startswith("bjj"):
            return "bjj"
        if lower.startswith("climbing"):
            return "climbing"
        if lower.startswith("hiking"):
            return "hiking"
        if lower.startswith("yoga"):
            return "yoga"
        if lower.startswith("walk"):
            return "walk"
        if lower.startswith("cycling") or lower.startswith("bike"):
            return "cycling"
        if lower.startswith("swim"):
            return "swimming"
        return "other"

    @classmethod
    def _workout_elements(cls, name: str, workout_type: str) -> dict[str, Any]:
        elements: dict[str, Any] = {}

        if workout_type in {"running", "cycling", "hiking"}:
            km_match = re.search(r"(\d+(?:\.\d+)?)\s*k\b", name, flags=re.IGNORECASE)
            if km_match:
                elements["elements"] = float(km_match.group(1))

        if workout_type == "strength":
            reps_match = re.search(r"-\s*(\d+)\b", name)
            if reps_match is None:
                reps_match = re.search(r"\b(\d+)\b", name)
            if reps_match:
                elements["elements"] = int(reps_match.group(1))

        return elements

    @classmethod
    def _parse_workout_fields(cls, workout_raw: str | None) -> tuple[str | None, int, str | None]:
        entries = cls._split_workouts(workout_raw)
        if not entries:
            return None, 0, None

        parsed_entries: list[dict[str, Any]] = []
        workout_types: list[str] = []
        for entry in entries:
            w_type = cls._workout_type(entry)
            workout_types.append(w_type)
            parsed_entries.append(
                {
                    "name": entry,
                    "type": w_type,
                    "elements": cls._workout_elements(entry, w_type),
                }
            )

        unique_types = sorted(set(workout_types))
        workout_type = unique_types[0] if len(unique_types) == 1 else "mixed"
        elements_json = json.dumps(parsed_entries, ensure_ascii=True)
        return workout_type, len(entries), elements_json

    def normalize(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for record in records:
            properties = record.get("properties", {})
            date_local = self._extract_date(properties.get("Date", {}))
            if date_local is None:
                continue

            name = self._extract_title(properties.get("Name", {}))
            name_date = self._parse_title_date(name)
            name_date_match: bool | None
            if name_date is None and name:
                LOGGER.warning(
                    "notion title date unparsable",
                    warning_code=WarningCode.NOTION_TITLE_DATE_UNPARSEABLE.value,
                    source_id=record.get("id"),
                    notion_name=name,
                    notion_date=str(date_local),
                )
                name_date_match = None
            else:
                name_date_match = (name_date == date_local) if name_date else None
                if name_date and name_date != date_local:
                    LOGGER.warning(
                        "notion title/date mismatch",
                        warning_code=WarningCode.NOTION_TITLE_DATE_MISMATCH.value,
                        source_id=record.get("id"),
                        notion_name=name,
                        parsed_name_date=str(name_date),
                        notion_date=str(date_local),
                    )

            anxiety_label = self._extract_select(properties.get("Anxiety Status", {}))
            productivity_label = self._extract_select(properties.get("Productivity", {}))
            physical_label = self._extract_select(properties.get("Physical Status", {}))

            substances = self._extract_text(properties.get("Substances", {}))
            workout_raw = self._extract_text(properties.get("Workout Resolved", {}))
            if not workout_raw:
                workout_raw = self._extract_text(properties.get("Workout", {}))
            workout_type, workout_count, workout_elements_json = self._parse_workout_fields(
                workout_raw
            )

            normalized = {
                "source_id": record.get("id"),
                "date_local": date_local,
                "week_start_monday": self._week_start_monday(date_local),
                "iso_week": date_local.isocalendar().week,
                "year": date_local.year,
                "month": date_local.month,
                "name": name,
                "name_date_match": name_date_match,
                "anxiety_status_label": anxiety_label,
                "anxiety_status_score": ANXIETY_MAP.get(anxiety_label or ""),
                "physical_status_label": physical_label,
                "physical_status_score": PHYSICAL_MAP.get((physical_label or "").lower()),
                "productivity_label": productivity_label,
                "productivity_score": PRODUCTIVITY_MAP.get((productivity_label or "").lower()),
                "weight_kg": self._extract_number(properties.get("Weight (kg)", {})),
                "alcohol_units": self._extract_indicator(properties.get("Alcohol (unt)", {})),
                "mindful_min": self._extract_indicator(properties.get("Mindful (min)", {})),
                "points": self._extract_number(properties.get("Points", {})),
                "coffee_count": self._extract_number(properties.get("Coffee (#)", {})),
                "fasting_hours": self._extract_number(properties.get("Fasting", {})),
                "sleep_hours_self_reported": self._extract_number(
                    properties.get("Sleep (hrs)", {})
                ),
                "cold_min": self._extract_indicator(properties.get("Cold (min)", {})),
                "cigarettes_count": self._parse_cigarettes(substances),
                "substances_raw": substances,
                "workout_raw": workout_raw,
                "workout_type": workout_type,
                "workout_count": workout_count,
                "workout_elements_json": workout_elements_json,
                "general_notes": self._extract_text(properties.get("General Notes", {})),
                "supplements": self._extract_text(properties.get("Supplements", {})),
                "weather": self._extract_text(properties.get("Weather", {})),
                "learned": self._extract_text(properties.get("Learned", {})),
            }
            out.append(normalized)
        return out
