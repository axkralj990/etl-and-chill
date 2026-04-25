from __future__ import annotations

from pathlib import Path

from life.pipeline.shared import parse_legacy_notion_csv


def test_parse_legacy_notion_csv_includes_workout_property(tmp_path: Path) -> None:
    path = tmp_path / "notion.csv"
    path.write_text(
        "\n".join(
            [
                "Name,Date,Workout",
                (
                    '"Sunday, May 18, 2025","May 18, 2025",'
                    '"Running - Roznik (+50) (https://www.notion.so/x)"'
                ),
            ]
        ),
        encoding="utf-8",
    )

    rows = parse_legacy_notion_csv(path)
    assert len(rows) == 1
    props = rows[0]["properties"]
    workout_text = props["Workout"]["rich_text"][0]["plain_text"]
    assert workout_text.startswith("Running - Roznik")
