from __future__ import annotations

from datetime import date

import pytest

from life.enums import OuraDailyEndpoint
from life.normalizers.notion_daily import NotionDailyNormalizer
from life.normalizers.oura_daily import OuraDailyNormalizer, merge_oura_daily_rows
from life.storage.duckdb import DuckDBStorage


def _notion_page() -> dict:
    return {
        "id": "notion-1",
        "properties": {
            "Name": {"title": [{"plain_text": "Monday, January 6, 2026"}]},
            "Date": {"date": {"start": "2026-01-06"}},
            "Anxiety Status": {"select": {"name": "3"}},
            "Physical Status": {"select": {"name": "good"}},
            "Productivity": {"select": {"name": "high"}},
            "Sleep (hrs)": {"number": 7.1},
            "Alcohol (unt)": {"number": 0.0},
            "Coffee (#)": {"number": 2.0},
            "Mindful (min)": {"number": 20.0},
            "Points": {"number": 8.0},
            "Substances": {"rich_text": [{"plain_text": "C"}]},
            "Workout": {
                "rich_text": [{"plain_text": "Strength - 150 (https://www.notion.so/strength-150)"}]
            },
            "General Notes": {"rich_text": [{"plain_text": "Solid day."}]},
            "Supplements": {"rich_text": [{"plain_text": "omega3"}]},
            "Weather": {"rich_text": [{"plain_text": "cold"}]},
            "Learned": {"rich_text": [{"plain_text": "focused work"}]},
        },
    }


def test_mocked_pipeline_to_features_table(tmp_path) -> None:
    storage = DuckDBStorage(tmp_path / "test.duckdb")

    notion_rows = NotionDailyNormalizer().normalize([_notion_page()])
    storage.upsert_notion_daily(notion_rows)

    oura_normalizer = OuraDailyNormalizer()
    oura_rows = []
    oura_rows.extend(
        oura_normalizer.normalize(
            [{"id": "a", "day": "2026-01-06", "score": 77}],
            endpoint=OuraDailyEndpoint.DAILY_SLEEP,
        )
    )
    oura_rows.extend(
        oura_normalizer.normalize(
            [{"id": "b", "day": "2026-01-06", "score": 81}],
            endpoint=OuraDailyEndpoint.DAILY_READINESS,
        )
    )
    oura_rows.extend(
        oura_normalizer.normalize(
            [{"id": "c", "day": "2026-01-06", "score": 74}],
            endpoint=OuraDailyEndpoint.DAILY_ACTIVITY,
        )
    )
    oura_rows.extend(
        oura_normalizer.normalize(
            [
                {
                    "id": "e",
                    "day": "2026-01-06",
                    "steps": 10234,
                    "active_calories": 610,
                    "total_calories": 2500,
                    "target_calories": 700,
                    "inactivity_alerts": 2,
                    "score": 74,
                }
            ],
            endpoint=OuraDailyEndpoint.DAILY_ACTIVITY,
        )
    )
    oura_rows.extend(
        oura_normalizer.normalize(
            [
                {
                    "id": "f",
                    "day": "2026-01-06",
                    "time_in_bed": 28000,
                    "total_sleep_duration": 25000,
                    "deep_sleep_duration": 6000,
                    "rem_sleep_duration": 5000,
                    "light_sleep_duration": 14000,
                    "lowest_heart_rate": 42,
                    "average_heart_rate": 51.2,
                    "average_hrv": 88,
                }
            ],
            endpoint=OuraDailyEndpoint.SLEEP,
        )
    )
    oura_rows.extend(
        oura_normalizer.normalize(
            [
                {
                    "id": "d",
                    "day": "2026-01-06",
                    "day_summary": "normal",
                    "stress_high": 1200,
                    "recovery_high": 300,
                }
            ],
            endpoint=OuraDailyEndpoint.DAILY_STRESS,
        )
    )

    storage.upsert_oura_daily(merge_oura_daily_rows(oura_rows))
    storage.build_daily_features()

    row = storage.conn.execute(
        """
        select
            date_local,
            sleep_score,
            readiness_score,
            activity_score,
            steps,
            sleep_lowest_hr,
            sleep_avg_hrv,
            sleep_total_hours,
            sleep_efficiency_pct,
            sleep_deep_share_pct,
            hrv_7d_avg,
            anxiety_status_score,
            cigarettes_count
        from daily_features
        where date_local = ?
        """,
        [date(2026, 1, 6)],
    ).fetchone()

    assert row is not None
    assert row[1] == 77
    assert row[2] == 81
    assert row[3] == 74
    assert row[4] == 10234
    assert row[5] == 42
    assert row[6] == 88
    assert row[7] == pytest.approx(25000 / 3600)
    assert row[8] == pytest.approx(25000 / 28000 * 100)
    assert row[9] == pytest.approx(6000 / 25000 * 100)
    assert row[10] == 88
    assert row[11] == 3
    assert row[12] == 1

    notion_row = storage.conn.execute(
        """
        select workout_raw, workout_type, workout_count, workout_elements_json
        from canonical_notion_daily
        where date_local = ?
        """,
        [date(2026, 1, 6)],
    ).fetchone()
    assert notion_row is not None
    assert notion_row[0] == "Strength - 150 (https://www.notion.so/strength-150)"
    assert notion_row[1] == "strength"
    assert notion_row[2] == 1
    assert '"points": 150' not in (notion_row[3] or "")
