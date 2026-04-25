from __future__ import annotations

from datetime import datetime

import pandas as pd

from life.dashboard.data import aggregate_period


def test_weekly_aggregation_uses_sum_and_mean_rules() -> None:
    df = pd.DataFrame(
        {
            "date_local": pd.to_datetime(["2026-01-05", "2026-01-06", "2026-01-07"]),
            "week_start_monday": pd.to_datetime(["2026-01-05", "2026-01-05", "2026-01-05"]),
            "steps": [1000, 2000, 3000],
            "anxiety_status_score": [2.0, 4.0, 3.0],
            "stress_summary": ["normal", "stressful", "normal"],
        }
    )

    out = aggregate_period(df, "week")
    row = out.iloc[0]
    assert row["period_start"] == datetime(2026, 1, 5)
    assert row["steps"] == 6000
    assert row["anxiety_status_score"] == 3.0
    assert row["stress_summary"] == "normal"
    assert row["days_present"] == 3
