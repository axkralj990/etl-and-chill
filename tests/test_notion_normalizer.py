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
