from __future__ import annotations

import json
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

GOAL_METRIC_SPECS = [
    {
        "metric": "steps",
        "metric_label": "Steps",
        "goal_key": "steps_per_day",
        "direction": "higher",
    },
    {
        "metric": "sleep_total_hours",
        "metric_label": "Sleep total",
        "goal_key": "sleep_hours_per_day",
        "direction": "higher",
    },
]

METRIC_UNITS = {
    "sleep_lowest_hr": "bpm",
    "sleep_avg_hr": "bpm",
    "sleep_avg_hrv": "ms",
    "steps": "steps",
    "active_calories": "kcal",
    "sleep_total_hours": "h",
    "sleep_efficiency_pct": "%",
    "readiness_score": "pts",
    "activity_score": "pts",
    "sleep_score": "pts",
    "daytime_stress_avg": "score",
    "strength_elements": "elements",
    "mindful_min": "min",
}

ANXIETY_STATUS_COLORS = {
    1: "#0f379c",
    2: "#5397cc",
    3: "#c2c800",
    4: "#f2935c",
    5: "#d7263d",
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
                o.resilience_level,
                n.general_notes,
                n.workout_raw,
                n.workout_type,
                n.workout_count,
                n.workout_elements_json
            from daily_features f
            left join canonical_oura_daily o
              on o.date_local = f.date_local
            left join canonical_notion_daily n
              on n.date_local = f.date_local
            order by f.date_local
            """
        ).df()
    finally:
        conn.close()

    if not df.empty:
        df["date_local"] = pd.to_datetime(df["date_local"])
        deprecated = {
            "points",
            "coffee_count",
            "cigarettes_count",
            "sleep_hours_self_reported",
            "productivity_score",
            "weight_kg",
        }
        drop_cols = [col for col in deprecated if col in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols)
    return df


def metric_columns(df: pd.DataFrame) -> list[str]:
    blocked = {
        "date_local",
        "week_start_monday",
        "iso_week",
        "year",
        "month",
        "weekday",
        "points",
        "coffee_count",
        "cigarettes_count",
        "sleep_hours_self_reported",
        "productivity_score",
        "weight_kg",
    }
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


def _pick_record_row(df: pd.DataFrame, metric: str, direction: str) -> pd.Series | None:
    if metric not in df.columns:
        return None

    metric_values = pd.to_numeric(df[metric], errors="coerce")
    base = pd.DataFrame(
        {
            "date_local": pd.to_datetime(df["date_local"]),
            metric: metric_values,
            "general_notes": df.get("general_notes"),
        }
    ).dropna(subset=[metric, "date_local"])

    if base.empty:
        return None

    target = float(base[metric].min()) if direction == "min" else float(base[metric].max())

    winners = base[base[metric] == target].sort_values("date_local")
    return winners.iloc[-1]


def format_metric_value(value: float, metric: str) -> str:
    rounded = f"{value:,.2f}"
    if metric in {
        "steps",
        "active_calories",
        "readiness_score",
        "activity_score",
        "sleep_score",
        "strength_elements",
    }:
        rounded = f"{value:,.0f}"
    unit = METRIC_UNITS.get(metric)
    return f"{rounded} {unit}" if unit else rounded


def build_leaderboard(df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "metric",
        "metric_label",
        "record_type",
        "record_label",
        "value",
        "unit",
        "value_display",
        "date_local",
        "general_notes",
    ]
    if df.empty or "date_local" not in df.columns:
        return pd.DataFrame(columns=columns)

    rules = [
        {
            "metric": "sleep_lowest_hr",
            "metric_label": "Sleep lowest HR",
            "records": [
                ("best", "min", "Lowest"),
                ("worst", "max", "Highest"),
            ],
        },
        {
            "metric": "sleep_avg_hrv",
            "metric_label": "Sleep average HRV",
            "records": [
                ("best", "max", "Highest"),
                ("worst", "min", "Lowest"),
            ],
        },
        {
            "metric": "sleep_avg_hr",
            "metric_label": "Sleep average HR",
            "records": [
                ("best", "min", "Lowest"),
                ("worst", "max", "Highest"),
            ],
        },
        {
            "metric": "daytime_stress_avg",
            "metric_label": "Daytime stress",
            "records": [
                ("best", "min", "Lowest"),
                ("worst", "max", "Highest"),
            ],
        },
        {
            "metric": "steps",
            "metric_label": "Steps",
            "records": [("max", "max", "Most")],
        },
        {
            "metric": "active_calories",
            "metric_label": "Active calories",
            "records": [("max", "max", "Most")],
        },
        {
            "metric": "sleep_total_hours",
            "metric_label": "Sleep total",
            "records": [("max", "max", "Longest")],
        },
        {
            "metric": "sleep_efficiency_pct",
            "metric_label": "Sleep efficiency",
            "records": [("max", "max", "Highest")],
        },
        {
            "metric": "readiness_score",
            "metric_label": "Readiness score",
            "records": [("max", "max", "Highest")],
        },
        {
            "metric": "activity_score",
            "metric_label": "Activity score",
            "records": [("max", "max", "Highest")],
        },
        {
            "metric": "sleep_score",
            "metric_label": "Sleep score",
            "records": [("max", "max", "Highest")],
        },
    ]

    rows: list[dict[str, object]] = []
    for rule in rules:
        metric = rule["metric"]
        for record_type, direction, record_label in rule["records"]:
            winner = _pick_record_row(df, metric, direction)
            if winner is None:
                continue
            rows.append(
                {
                    "metric": metric,
                    "metric_label": rule["metric_label"],
                    "record_type": record_type,
                    "record_label": record_label,
                    "value": float(winner[metric]),
                    "unit": METRIC_UNITS.get(metric),
                    "value_display": format_metric_value(float(winner[metric]), metric),
                    "date_local": pd.to_datetime(winner["date_local"]),
                    "general_notes": winner.get("general_notes"),
                }
            )

    if not rows:
        return pd.DataFrame(columns=columns)

    out = pd.DataFrame(rows)
    return out.sort_values(["metric_label", "record_type"]).reset_index(drop=True)


def build_goals_progress(df: pd.DataFrame, period: str, goals: dict[str, float]) -> pd.DataFrame:
    columns = [
        "period_start",
        "period_end",
        "period_label",
        "metric",
        "metric_label",
        "direction",
        "goal",
        "avg_value",
        "delta",
        "delta_pct",
        "on_track",
        "days_with_data",
        "unit",
    ]
    if period not in {"week", "month"} or df.empty or "date_local" not in df.columns:
        return pd.DataFrame(columns=columns)

    today = pd.Timestamp.now().normalize()
    if period == "week":
        period_start = today - pd.Timedelta(days=today.weekday())
        period_end = period_start + pd.Timedelta(days=6)
        period_label = f"Week of {period_start.date()}"
    else:
        period_start = today.to_period("M").start_time
        period_end = today.to_period("M").end_time.normalize()
        period_label = period_start.strftime("%B %Y")

    frame = df.copy()
    frame["date_local"] = pd.to_datetime(frame["date_local"])
    frame = frame[(frame["date_local"] >= period_start) & (frame["date_local"] <= period_end)]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    rows: list[dict[str, object]] = []

    for spec in GOAL_METRIC_SPECS:
        metric = spec["metric"]
        goal_key = spec["goal_key"]
        if metric not in frame.columns or goal_key not in goals:
            continue

        goal = float(goals[goal_key])
        if goal <= 0:
            continue

        series = pd.to_numeric(frame[metric], errors="coerce").dropna()
        if series.empty:
            continue

        avg_value_float = float(series.mean())
        delta = avg_value_float - goal
        delta_pct = 100.0 * delta / goal

        rows.append(
            {
                "period_start": period_start,
                "period_end": period_end,
                "period_label": period_label,
                "metric": metric,
                "metric_label": spec["metric_label"],
                "direction": spec["direction"],
                "goal": goal,
                "avg_value": avg_value_float,
                "delta": delta,
                "delta_pct": delta_pct,
                "on_track": delta >= 0,
                "days_with_data": int(series.shape[0]),
                "unit": METRIC_UNITS.get(metric),
            }
        )

    period_goal_key = (
        "strength_elements_per_week" if period == "week" else "strength_elements_per_month"
    )
    goal_value = goals.get(period_goal_key)
    if isinstance(goal_value, int | float) and float(goal_value) > 0:
        strength_values: list[float] = []
        if "workout_elements_json" in frame.columns:
            for raw in frame["workout_elements_json"].dropna():
                if not isinstance(raw, str) or not raw.strip():
                    continue
                try:
                    parsed = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if not isinstance(parsed, list):
                    continue
                for item in parsed:
                    if not isinstance(item, dict):
                        continue
                    if item.get("type") != "strength":
                        continue
                    elements = item.get("elements")
                    if not isinstance(elements, dict):
                        continue
                    value = elements.get("elements")
                    if isinstance(value, int | float):
                        strength_values.append(float(value))

        total_strength = float(sum(strength_values))
        goal = float(goal_value)
        delta = total_strength - goal
        rows.append(
            {
                "period_start": period_start,
                "period_end": period_end,
                "period_label": period_label,
                "metric": "strength_elements",
                "metric_label": "Strength elements",
                "direction": "higher",
                "goal": goal,
                "avg_value": total_strength,
                "delta": delta,
                "delta_pct": 100.0 * delta / goal,
                "on_track": delta >= 0,
                "days_with_data": int(frame["date_local"].nunique()),
                "unit": "elements",
            }
        )

    mindful_goal_key = (
        "mindful_minutes_per_week" if period == "week" else "mindful_minutes_per_month"
    )
    mindful_goal_value = goals.get(mindful_goal_key)
    if (
        isinstance(mindful_goal_value, int | float)
        and float(mindful_goal_value) > 0
        and "mindful_min" in frame.columns
    ):
        mindful_series = pd.to_numeric(frame["mindful_min"], errors="coerce").dropna()
        total_mindful = float(mindful_series.sum()) if not mindful_series.empty else 0.0
        mindful_goal = float(mindful_goal_value)
        mindful_delta = total_mindful - mindful_goal
        rows.append(
            {
                "period_start": period_start,
                "period_end": period_end,
                "period_label": period_label,
                "metric": "mindful_min",
                "metric_label": "Mindful minutes",
                "direction": "higher",
                "goal": mindful_goal,
                "avg_value": total_mindful,
                "delta": mindful_delta,
                "delta_pct": 100.0 * mindful_delta / mindful_goal,
                "on_track": mindful_delta >= 0,
                "days_with_data": int(mindful_series.shape[0]),
                "unit": "min",
            }
        )

    if not rows:
        return pd.DataFrame(columns=columns)

    return pd.DataFrame(rows).sort_values(["metric_label"]).reset_index(drop=True)


def build_anxiety_status_mix(df: pd.DataFrame, period: str) -> pd.DataFrame:
    columns = ["period_start", "period_label", "status", "count", "pct"]
    if period not in {"week", "month"}:
        return pd.DataFrame(columns=columns)
    if df.empty or "anxiety_status_score" not in df.columns or "date_local" not in df.columns:
        return pd.DataFrame(columns=columns)

    today = pd.Timestamp.now().normalize()
    if period == "week":
        period_start = today - pd.Timedelta(days=today.weekday())
        period_end = period_start + pd.Timedelta(days=6)
        period_label = f"Week of {period_start.date()}"
    else:
        period_start = today.to_period("M").start_time
        period_end = today.to_period("M").end_time.normalize()
        period_label = period_start.strftime("%B %Y")

    frame = pd.DataFrame(
        {
            "date_local": pd.to_datetime(df["date_local"]),
            "status": pd.to_numeric(df["anxiety_status_score"], errors="coerce").round(),
        }
    )
    frame = frame[(frame["date_local"] >= period_start) & (frame["date_local"] <= period_end)]
    frame = frame.dropna(subset=["status"])
    if frame.empty:
        return pd.DataFrame(columns=columns)

    frame["status"] = frame["status"].astype(int)
    frame = frame[frame["status"].between(1, 5)]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    counts = frame["status"].value_counts().reindex([1, 2, 3, 4, 5], fill_value=0).sort_index()
    total = int(counts.sum())
    if total == 0:
        return pd.DataFrame(columns=columns)

    rows = [
        {
            "period_start": period_start,
            "period_label": period_label,
            "status": int(status),
            "count": int(count),
            "pct": 100.0 * float(count) / float(total),
        }
        for status, count in counts.items()
    ]
    return pd.DataFrame(rows)


def build_anxiety_status_counts(df: pd.DataFrame, period: str) -> pd.DataFrame:
    columns = ["period_start", "status", "count"]
    if period not in {"week", "month"}:
        return pd.DataFrame(columns=columns)
    if df.empty or "anxiety_status_score" not in df.columns or "date_local" not in df.columns:
        return pd.DataFrame(columns=columns)

    frame = pd.DataFrame(
        {
            "date_local": pd.to_datetime(df["date_local"]),
            "status": pd.to_numeric(df["anxiety_status_score"], errors="coerce").round(),
        }
    ).dropna(subset=["date_local", "status"])
    if frame.empty:
        return pd.DataFrame(columns=columns)

    frame["status"] = frame["status"].astype(int)
    frame = frame[frame["status"].between(1, 5)]
    if frame.empty:
        return pd.DataFrame(columns=columns)

    if period == "week":
        frame["period_start"] = frame["date_local"].dt.to_period("W").dt.start_time
    else:
        frame["period_start"] = frame["date_local"].dt.to_period("M").dt.start_time

    counts = (
        frame.groupby(["period_start", "status"], dropna=False)
        .size()
        .rename("count")
        .reset_index()
        .sort_values(["period_start", "status"])
    )
    return counts
