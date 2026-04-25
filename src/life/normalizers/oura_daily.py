from __future__ import annotations

from datetime import date
from json import JSONDecodeError, loads
from typing import Any

from life.enums import OuraDailyEndpoint
from life.normalizers.base import BaseNormalizer


class OuraDailyNormalizer(BaseNormalizer):
    @staticmethod
    def _as_int(value: Any) -> int | None:
        if value is None or value == "":
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _sample_average(value: Any) -> float | None:
        sample = OuraDailyNormalizer._as_dict(value)
        items = sample.get("items")
        if not isinstance(items, list):
            return None
        numeric = [float(v) for v in items if isinstance(v, (int, float))]
        if not numeric:
            return None
        return sum(numeric) / len(numeric)

    @staticmethod
    def _as_dict(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("{") and stripped.endswith("}"):
                try:
                    parsed = loads(stripped)
                    if isinstance(parsed, dict):
                        return parsed
                except JSONDecodeError:
                    return {}
        return {}

    @staticmethod
    def _as_date(value: str | None) -> date | None:
        if not value:
            return None
        return date.fromisoformat(value)

    def normalize(
        self,
        records: list[dict[str, Any]],
        *,
        endpoint: OuraDailyEndpoint,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for record in records:
            day = self._as_date(record.get("day"))
            if day is None:
                continue

            normalized: dict[str, Any] = {
                "date_local": day,
                "source_payload": record,
                "endpoint": endpoint.value,
            }

            if endpoint == OuraDailyEndpoint.DAILY_ACTIVITY:
                normalized["activity_score"] = self._as_int(record.get("score"))
                normalized["steps"] = self._as_int(record.get("steps"))
                normalized["active_calories"] = self._as_int(record.get("active_calories"))
                normalized["total_calories"] = self._as_int(record.get("total_calories"))
                normalized["target_calories"] = self._as_int(record.get("target_calories"))
                normalized["inactivity_alerts"] = self._as_int(record.get("inactivity_alerts"))

            elif endpoint == OuraDailyEndpoint.DAILY_SLEEP:
                normalized["sleep_score"] = self._as_int(record.get("score"))

            elif endpoint == OuraDailyEndpoint.SLEEP:
                sleep_type = record.get("type")
                if sleep_type and sleep_type != "long_sleep":
                    out.append(normalized)
                    continue
                normalized["sleep_time_in_bed"] = self._as_int(record.get("time_in_bed"))
                normalized["sleep_total_duration"] = self._as_int(
                    record.get("total_sleep_duration")
                )
                normalized["sleep_deep_duration"] = self._as_int(
                    record.get("deep_sleep_duration")
                )
                normalized["sleep_rem_duration"] = self._as_int(
                    record.get("rem_sleep_duration")
                )
                normalized["sleep_light_duration"] = self._as_int(
                    record.get("light_sleep_duration")
                )
                normalized["sleep_lowest_hr"] = self._as_int(record.get("lowest_heart_rate"))
                normalized["sleep_avg_hr"] = self._as_float(record.get("average_heart_rate"))
                normalized["sleep_avg_hrv"] = self._as_float(record.get("average_hrv"))

            elif endpoint == OuraDailyEndpoint.DAILY_READINESS:
                normalized["readiness_score"] = self._as_int(record.get("score"))
                normalized["sleep_temperature_deviation"] = self._as_float(
                    record.get("temperature_deviation")
                )
                normalized["sleep_temperature_trend_deviation"] = self._as_float(
                    record.get("temperature_trend_deviation")
                )

            elif endpoint == OuraDailyEndpoint.DAILY_SPO2:
                spo2_obj = self._as_dict(record.get("spo2_percentage"))
                normalized["spo2_average"] = self._as_float(
                    spo2_obj.get("average")
                )

            elif endpoint == OuraDailyEndpoint.DAILY_STRESS:
                avg_candidate = record.get("daytime_stress_avg")
                if avg_candidate is None:
                    summary = record.get("day_summary")
                    summary_map = {
                        "restored": 25.0,
                        "normal": 50.0,
                        "stressful": 75.0,
                    }
                    avg_candidate = summary_map.get(summary)
                normalized["daytime_stress_avg"] = self._as_float(avg_candidate)
                normalized["stress_summary"] = record.get("day_summary")
                normalized["stress_high"] = self._as_int(record.get("stress_high"))
                normalized["stress_recovery"] = self._as_int(record.get("recovery_high"))

            elif endpoint == OuraDailyEndpoint.DAILY_RESILIENCE:
                normalized["resilience_level"] = record.get("level")

            elif endpoint == OuraDailyEndpoint.DAILY_CARDIOVASCULAR_AGE:
                normalized["cardiovascular_age"] = self._as_int(record.get("vascular_age"))

            out.append(normalized)
        return out


def merge_oura_daily_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[date, dict[str, Any]] = {}
    for row in rows:
        day = row["date_local"]
        current = grouped.setdefault(
            day,
            {
                "date_local": day,
                "activity_score": None,
                "steps": None,
                "active_calories": None,
                "total_calories": None,
                "target_calories": None,
                "inactivity_alerts": None,
                "sleep_score": None,
                "sleep_time_in_bed": None,
                "sleep_total_duration": None,
                "sleep_deep_duration": None,
                "sleep_rem_duration": None,
                "sleep_light_duration": None,
                "sleep_lowest_hr": None,
                "sleep_avg_hr": None,
                "sleep_avg_hrv": None,
                "readiness_score": None,
                "sleep_temperature_deviation": None,
                "sleep_temperature_trend_deviation": None,
                "spo2_average": None,
                "daytime_stress_avg": None,
                "stress_summary": None,
                "stress_high": None,
                "stress_recovery": None,
                "resilience_level": None,
                "cardiovascular_age": None,
                "source_payloads": {},
            },
        )

        endpoint = row.get("endpoint")
        payload = row.get("source_payload")
        if endpoint and payload:
            current["source_payloads"][endpoint] = payload

        for key in (
            "activity_score",
            "steps",
            "active_calories",
            "total_calories",
            "target_calories",
            "inactivity_alerts",
            "sleep_score",
            "sleep_time_in_bed",
            "sleep_total_duration",
            "sleep_deep_duration",
            "sleep_rem_duration",
            "sleep_light_duration",
            "sleep_lowest_hr",
            "sleep_avg_hr",
            "sleep_avg_hrv",
            "readiness_score",
            "sleep_temperature_deviation",
            "sleep_temperature_trend_deviation",
            "spo2_average",
            "daytime_stress_avg",
            "stress_summary",
            "stress_high",
            "stress_recovery",
            "resilience_level",
            "cardiovascular_age",
        ):
            if row.get(key) is not None:
                current[key] = row[key]
    return list(grouped.values())
