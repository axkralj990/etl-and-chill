from __future__ import annotations

import pandas as pd

from life.dashboard.data import build_leaderboard


def _record(df: pd.DataFrame, metric: str, record_type: str) -> pd.Series:
    out = df[(df["metric"] == metric) & (df["record_type"] == record_type)]
    assert len(out) == 1
    return out.iloc[0]


def test_leaderboard_builds_best_worst_and_max_records() -> None:
    frame = pd.DataFrame(
        {
            "date_local": pd.to_datetime(["2026-04-01", "2026-04-02", "2026-04-03"]),
            "general_notes": ["low hr day", "", "high hrv day"],
            "sleep_lowest_hr": [44, 50, 60],
            "sleep_avg_hrv": [52.0, 70.0, 88.0],
            "sleep_avg_hr": [58.0, 54.0, 61.0],
            "daytime_stress_avg": [55.0, 25.0, 70.0],
            "steps": [11000, 12500, 9800],
            "active_calories": [500, 610, 580],
            "sleep_total_hours": [6.2, 8.4, 7.9],
            "sleep_efficiency_pct": [81.0, 88.0, 86.0],
            "readiness_score": [72, 90, 84],
            "activity_score": [75, 83, 79],
            "sleep_score": [70, 92, 88],
        }
    )

    leaderboard = build_leaderboard(frame)

    best_low_hr = _record(leaderboard, "sleep_lowest_hr", "best")
    assert best_low_hr["value"] == 44
    assert best_low_hr["unit"] == "bpm"
    assert best_low_hr["value_display"] == "44.00 bpm"
    assert str(best_low_hr["date_local"].date()) == "2026-04-01"
    assert best_low_hr["general_notes"] == "low hr day"

    worst_low_hr = _record(leaderboard, "sleep_lowest_hr", "worst")
    assert worst_low_hr["value"] == 60
    assert str(worst_low_hr["date_local"].date()) == "2026-04-03"

    best_hrv = _record(leaderboard, "sleep_avg_hrv", "best")
    assert best_hrv["value"] == 88.0

    max_steps = _record(leaderboard, "steps", "max")
    assert max_steps["value"] == 12500
    assert max_steps["value_display"] == "12,500 steps"
    assert str(max_steps["date_local"].date()) == "2026-04-02"

    longest_sleep = _record(leaderboard, "sleep_total_hours", "max")
    assert longest_sleep["record_label"] == "Longest"
    assert longest_sleep["value"] == 8.4

    max_readiness = _record(leaderboard, "readiness_score", "max")
    assert max_readiness["value"] == 90

    max_activity = _record(leaderboard, "activity_score", "max")
    assert max_activity["value"] == 83

    max_sleep_score = _record(leaderboard, "sleep_score", "max")
    assert max_sleep_score["value"] == 92


def test_leaderboard_tie_breaks_to_most_recent_date() -> None:
    frame = pd.DataFrame(
        {
            "date_local": pd.to_datetime(["2026-04-01", "2026-04-02"]),
            "general_notes": ["older", "newer"],
            "steps": [12000, 12000],
            "sleep_total_hours": [8.0, 8.0],
            "readiness_score": [88, 88],
            "activity_score": [82, 82],
            "sleep_score": [90, 90],
            "sleep_lowest_hr": [45, 45],
            "sleep_avg_hrv": [75.0, 75.0],
            "sleep_avg_hr": [56.0, 56.0],
            "daytime_stress_avg": [40.0, 40.0],
            "active_calories": [600, 600],
            "sleep_efficiency_pct": [87.0, 87.0],
        }
    )

    leaderboard = build_leaderboard(frame)
    steps_record = _record(leaderboard, "steps", "max")
    assert str(steps_record["date_local"].date()) == "2026-04-02"
    assert steps_record["general_notes"] == "newer"


def test_leaderboard_handles_missing_metrics() -> None:
    frame = pd.DataFrame(
        {
            "date_local": pd.to_datetime(["2026-04-01", "2026-04-02"]),
            "general_notes": [None, ""],
            "steps": [1000, 2000],
        }
    )

    leaderboard = build_leaderboard(frame)
    assert len(leaderboard) == 1
    row = leaderboard.iloc[0]
    assert row["metric"] == "steps"
    assert row["record_type"] == "max"
    assert str(row["date_local"].date()) == "2026-04-02"
