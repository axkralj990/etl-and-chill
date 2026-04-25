from __future__ import annotations

from datetime import date

from life.pipeline.shared import default_start_from


def test_default_start_from_uses_max_date_plus_one() -> None:
    assert default_start_from(date(2026, 4, 20), 365) == date(2026, 4, 21)


def test_default_start_from_uses_fallback_when_missing() -> None:
    fallback = default_start_from(None, 10)
    delta = date.today() - fallback
    assert delta.days == 10
