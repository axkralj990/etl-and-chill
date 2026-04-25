from __future__ import annotations

from life.normalizers.notion_daily import NotionDailyNormalizer


def _record(name: str, date_str: str, substances: str) -> dict:
    return {
        "id": "abc",
        "properties": {
            "Name": {"title": [{"plain_text": name}]},
            "Date": {"date": {"start": date_str}},
            "Substances": {"rich_text": [{"plain_text": substances}]},
            "Anxiety Status": {"select": {"name": "3"}},
            "Physical Status": {"select": {"name": "good"}},
            "Productivity": {"select": {"name": "high"}},
        },
    }


def test_cigarette_parsing_for_c_token() -> None:
    row = _record("Tuesday, January 2, 2024", "2024-01-02", "C C")
    out = NotionDailyNormalizer().normalize([row])
    assert out[0]["cigarettes_count"] == 2


def test_name_date_match_true() -> None:
    row = _record("Tuesday, January 2, 2024", "2024-01-02", "")
    out = NotionDailyNormalizer().normalize([row])
    assert out[0]["name_date_match"] is True


def test_missing_alcohol_mindful_cold_defaults_to_zero() -> None:
    row = _record("Tuesday, January 2, 2024", "2024-01-02", "")
    out = NotionDailyNormalizer().normalize([row])
    assert out[0]["alcohol_units"] == 0.0
    assert out[0]["mindful_min"] == 0.0
    assert out[0]["cold_min"] == 0.0


def test_alcohol_mindful_cold_are_indicators() -> None:
    row = {
        "id": "abc",
        "properties": {
            "Name": {"title": [{"plain_text": "Tuesday, January 2, 2024"}]},
            "Date": {"date": {"start": "2024-01-02"}},
            "Substances": {"rich_text": [{"plain_text": ""}]},
            "Anxiety Status": {"select": {"name": "3"}},
            "Physical Status": {"select": {"name": "good"}},
            "Productivity": {"select": {"name": "high"}},
            "Alcohol (unt)": {"number": 2},
            "Mindful (min)": {"number": 15},
            "Cold (min)": {"number": 0},
        },
    }

    out = NotionDailyNormalizer().normalize([row])
    assert out[0]["alcohol_units"] == 1.0
    assert out[0]["mindful_min"] == 1.0
    assert out[0]["cold_min"] == 0.0


def test_workout_parsing_from_legacy_text() -> None:
    row = {
        "id": "abc",
        "properties": {
            "Name": {"title": [{"plain_text": "Sunday, May 18, 2025"}]},
            "Date": {"date": {"start": "2025-05-18"}},
            "Anxiety Status": {"select": {"name": "2"}},
            "Physical Status": {"select": {"name": "good"}},
            "Productivity": {"select": {"name": "high"}},
            "Workout": {
                "rich_text": [
                    {
                        "plain_text": (
                            "Running - Roznik 20k (https://www.notion.so/a), "
                            "Strength - 150 (https://www.notion.so/b)"
                        )
                    }
                ]
            },
        },
    }

    out = NotionDailyNormalizer().normalize([row])
    assert out[0]["workout_raw"] is not None
    assert out[0]["workout_count"] == 2
    assert out[0]["workout_type"] == "mixed"
    assert '"elements": 20.0' in (out[0]["workout_elements_json"] or "")
    assert '"elements": 150' in (out[0]["workout_elements_json"] or "")


def test_workout_parsing_from_resolved_relation_names() -> None:
    row = {
        "id": "abc",
        "properties": {
            "Name": {"title": [{"plain_text": "Tuesday, July 29, 2025"}]},
            "Date": {"date": {"start": "2025-07-29"}},
            "Anxiety Status": {"select": {"name": "3"}},
            "Physical Status": {"select": {"name": "good"}},
            "Productivity": {"select": {"name": "high"}},
            "Workout Resolved": {
                "rich_text": [{"plain_text": "Running - Mandrija 7k, Running - Easy 6k"}]
            },
        },
    }

    out = NotionDailyNormalizer().normalize([row])
    assert out[0]["workout_count"] == 2
    assert out[0]["workout_type"] == "running"
    assert '"elements": 7.0' in (out[0]["workout_elements_json"] or "")
    assert '"elements": 6.0' in (out[0]["workout_elements_json"] or "")


def test_other_sports_keep_name_and_ignore_elements() -> None:
    row = {
        "id": "abc",
        "properties": {
            "Name": {"title": [{"plain_text": "Friday, August 30, 2024"}]},
            "Date": {"date": {"start": "2024-08-30"}},
            "Anxiety Status": {"select": {"name": "2"}},
            "Physical Status": {"select": {"name": "good"}},
            "Productivity": {"select": {"name": "high"}},
            "Workout": {"rich_text": [{"plain_text": "BJJ - #134 GB1, Climbing - Balvanija"}]},
        },
    }

    out = NotionDailyNormalizer().normalize([row])
    assert out[0]["workout_type"] == "mixed"
    assert '"elements": {}' in (out[0]["workout_elements_json"] or "")
