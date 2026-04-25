from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

SUM_METRICS = {
    "steps",
    "active_calories",
    "total_calories",
    "target_calories",
    "inactivity_alerts",
    "mindful_min",
    "points",
    "cigarettes_count",
    "alcohol_units",
    "sleep_time_in_bed",
    "sleep_total_duration",
    "sleep_deep_duration",
    "sleep_rem_duration",
    "sleep_light_duration",
    "sleep_time_in_bed_hours",
    "sleep_total_hours",
    "sleep_deep_hours",
    "sleep_rem_hours",
    "sleep_light_hours",
}

MEAN_METRICS = {
    "anxiety_status_score",
    "physical_status_score",
    "productivity_score",
    "activity_score",
    "sleep_score",
    "readiness_score",
    "sleep_avg_hr",
    "sleep_lowest_hr",
    "sleep_avg_hrv",
    "sleep_temperature_deviation",
    "sleep_temperature_trend_deviation",
    "spo2_average",
    "daytime_stress_avg",
    "sleep_efficiency_pct",
    "sleep_deep_share_pct",
    "sleep_rem_share_pct",
    "sleep_light_share_pct",
}

MODE_METRICS = {
    "stress_summary",
    "resilience_level",
}


def _conn(db_path: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path), read_only=True)


def load_daily_frame(db_path: Path) -> pd.DataFrame:
    conn = _conn(db_path)
    try:
        df = conn.execute(
            """
            select
                f.*,
                o.stress_summary,
                o.resilience_level
            from daily_features f
            left join canonical_oura_daily o
              on o.date_local = f.date_local
            order by f.date_local
            """
        ).df()
    finally:
        conn.close()

    if not df.empty:
        df["date_local"] = pd.to_datetime(df["date_local"])
    return df


def metric_columns(df: pd.DataFrame) -> list[str]:
    blocked = {"date_local", "week_start_monday", "iso_week", "year", "month", "weekday"}
    numeric = [
        c
        for c in df.columns
        if c not in blocked
        and pd.api.types.is_numeric_dtype(df[c])
        and not pd.api.types.is_bool_dtype(df[c])
    ]
    return sorted(numeric)


def aggregate_period(df: pd.DataFrame, period: str) -> pd.DataFrame:
    frame = df.copy()
    if period == "week":
        frame["period_start"] = pd.to_datetime(frame["week_start_monday"])
    elif period == "month":
        frame["period_start"] = frame["date_local"].dt.to_period("M").dt.to_timestamp()
    else:
        frame["period_start"] = frame["date_local"].dt.to_period("Y").dt.to_timestamp()

    agg_map: dict[str, str] = {}
    for col in frame.columns:
        if col in SUM_METRICS:
            agg_map[col] = "sum"
        elif col in MEAN_METRICS:
            agg_map[col] = "mean"
        elif col in MODE_METRICS:
            agg_map[col] = "first"
        elif pd.api.types.is_numeric_dtype(frame[col]) and not pd.api.types.is_bool_dtype(
            frame[col]
        ):
            agg_map[col] = "mean"

    grouped = frame.groupby("period_start", dropna=False)
    result = grouped.agg(agg_map)
    result["days_present"] = grouped["date_local"].count()
    days_divisor = {"week": 7.0, "month": 30.0, "year": 365.0}[period]
    result["coverage_pct"] = 100.0 * result["days_present"] / days_divisor

    for col in MODE_METRICS:
        if col in frame.columns:
            mode_series = grouped[col].agg(
                lambda s: s.mode().iloc[0] if not s.mode().empty else None
            )
            result[col] = mode_series

    return result.reset_index().sort_values("period_start")
