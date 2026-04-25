from __future__ import annotations

import pandas as pd

from life.dashboard.data import (
    ANXIETY_STATUS_COLORS,
    build_anxiety_status_mix,
    build_goals_progress,
)


def test_build_goals_progress_weekly_delta_signs() -> None:
    today = pd.Timestamp.now().normalize()
    week_start = today - pd.Timedelta(days=today.weekday())
    frame = pd.DataFrame(
        {
            "date_local": pd.to_datetime(
                [
                    week_start,
                    week_start + pd.Timedelta(days=1),
                    week_start + pd.Timedelta(days=3),
                    week_start + pd.Timedelta(days=4),
                ]
            ),
            "steps": [8000, 7500, 9000, 8500],
            "sleep_total_hours": [6.5, 7.1, None, 7.0],
            "workout_elements_json": [
                '[{"name":"Strength - 120","type":"strength","elements":{"elements":120}}]',
                '[{"name":"Strength - 90","type":"strength","elements":{"elements":90}}]',
                None,
                '[{"name":"Running - 10k","type":"running","elements":{"elements":10}}]',
            ],
        }
    )
    goals = {
        "steps_per_day": 7500.0,
        "sleep_hours_per_day": 7.0,
        "strength_elements_per_week": 300.0,
        "strength_elements_per_month": 1000.0,
    }

    out = build_goals_progress(frame, period="week", goals=goals)
    assert not out.empty
    assert set(out["metric"]) == {"steps", "sleep_total_hours", "strength_elements"}

    steps_row = out[out["metric"] == "steps"].iloc[0]
    assert steps_row["delta"] > 0
    assert steps_row["delta_pct"] > 0
    assert bool(steps_row["on_track"])

    sleep_row = out[out["metric"] == "sleep_total_hours"].iloc[0]
    assert sleep_row["days_with_data"] == 3
    assert sleep_row["delta"] < 0
    assert not bool(sleep_row["on_track"])

    strength_row = out[out["metric"] == "strength_elements"].iloc[0]
    assert strength_row["avg_value"] == 210.0
    assert strength_row["goal"] == 300.0
    assert strength_row["delta"] == -90.0


def test_build_anxiety_status_mix_current_month() -> None:
    today = pd.Timestamp.now().normalize()
    month_start = today.to_period("M").start_time
    prev_month_day = month_start - pd.Timedelta(days=1)
    frame = pd.DataFrame(
        {
            "date_local": pd.to_datetime(
                [
                    month_start + pd.Timedelta(days=1),
                    month_start + pd.Timedelta(days=5),
                    month_start + pd.Timedelta(days=10),
                    prev_month_day,
                ]
            ),
            "anxiety_status_score": [1, 2, 2, 5],
        }
    )

    mix = build_anxiety_status_mix(frame, period="month")
    assert len(mix) == 5
    status_2 = mix[mix["status"] == 2].iloc[0]
    assert status_2["count"] == 2
    assert round(float(mix["pct"].sum()), 6) == 100.0
    assert set(ANXIETY_STATUS_COLORS) == {1, 2, 3, 4, 5}
