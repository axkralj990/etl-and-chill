from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from life.enums import OuraDailyEndpoint, SourceName


class RawRecord(BaseModel):
    model_config = ConfigDict(extra="allow")

    source: SourceName
    endpoint: str
    source_id: str
    day: date | None = None
    payload: dict[str, Any]
    ingested_at: datetime


class CanonicalNotionDaily(BaseModel):
    source_id: str
    date_local: date
    week_start_monday: date
    iso_week: int
    year: int
    month: int

    name: str | None = None
    name_date_match: bool | None = None

    anxiety_status_label: str | None = None
    anxiety_status_score: int | None = None
    physical_status_label: str | None = None
    physical_status_score: int | None = None
    productivity_label: str | None = None
    productivity_score: int | None = None

    weight_kg: float | None = None
    alcohol_units: bool | None = None
    mindful_min: bool | None = None
    points: float | None = None
    coffee_count: float | None = None
    fasting_hours: float | None = None
    sleep_hours_self_reported: float | None = None
    cold_min: bool | None = None

    cigarettes_count: int | None = None
    substances_raw: str | None = None

    general_notes: str | None = None
    supplements: str | None = None
    weather: str | None = None
    learned: str | None = None


class CanonicalOuraDaily(BaseModel):
    date_local: date

    activity_score: int | None = None
    steps: int | None = None
    active_calories: int | None = None
    total_calories: int | None = None
    target_calories: int | None = None
    inactivity_alerts: int | None = None

    sleep_score: int | None = None
    sleep_time_in_bed: int | None = None
    sleep_total_duration: int | None = None
    sleep_deep_duration: int | None = None
    sleep_rem_duration: int | None = None
    sleep_light_duration: int | None = None
    sleep_lowest_hr: int | None = None
    sleep_avg_hr: float | None = None
    sleep_avg_hrv: float | None = None

    readiness_score: int | None = None
    sleep_temperature_deviation: float | None = None
    sleep_temperature_trend_deviation: float | None = None

    spo2_average: float | None = None
    daytime_stress_avg: float | None = None
    stress_summary: str | None = None
    stress_high: int | None = None
    stress_recovery: int | None = None
    resilience_level: str | None = None
    cardiovascular_age: int | None = None

    source_payloads: dict[str, dict[str, Any]] = Field(default_factory=dict)


class SyncState(BaseModel):
    source: SourceName
    endpoint: str
    max_date: date | None = None


class OuraDailyConfig(BaseModel):
    endpoint: OuraDailyEndpoint
    field_prefix: str
