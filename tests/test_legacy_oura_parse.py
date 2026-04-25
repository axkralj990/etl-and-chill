from __future__ import annotations

from pathlib import Path

from life.pipeline.shared import parse_legacy_oura_csv


def test_parse_legacy_oura_csv_parses_json_columns(tmp_path: Path) -> None:
    path = tmp_path / "dailyspo2.csv"
    path.write_text(
        'id;day;spo2_percentage\n1;2022-08-02;{"average": 98.343}\n',
        encoding="utf-8",
    )

    rows = parse_legacy_oura_csv(path)
    assert isinstance(rows[0]["spo2_percentage"], dict)
    assert rows[0]["spo2_percentage"]["average"] == 98.343
