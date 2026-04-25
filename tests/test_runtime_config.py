from __future__ import annotations

from pathlib import Path

from life.pipeline.runtime_config import load_runtime_config


def test_runtime_config_defaults_when_file_missing(tmp_path: Path) -> None:
    cfg = load_runtime_config(tmp_path / "missing.toml")
    assert cfg.incremental.lookback_days == 7
    assert cfg.bridge.fallback_days == 365
    assert cfg.goals.steps_per_day == 7500
    assert cfg.goals.sleep_hours_per_day == 7.0
    assert cfg.goals.strength_elements_per_week == 300
    assert cfg.goals.strength_elements_per_month == 1000
    assert cfg.goals.mindful_minutes_per_week == 50
    assert cfg.goals.mindful_minutes_per_month == 200
    assert "daily_sleep" in [endpoint.value for endpoint in cfg.oura.endpoints]


def test_runtime_config_from_toml(tmp_path: Path) -> None:
    path = tmp_path / "pipeline.toml"
    path.write_text(
        """
[oura]
endpoints = ["daily_sleep", "daily_readiness"]

[incremental]
lookback_days = 14
fallback_days = 45

[bridge]
fallback_days = 180

[goals]
steps_per_day = 8500
sleep_hours_per_day = 7.5
strength_elements_per_week = 350
strength_elements_per_month = 1200
mindful_minutes_per_week = 60
mindful_minutes_per_month = 240
""".strip(),
        encoding="utf-8",
    )
    cfg = load_runtime_config(path)

    endpoint_values = [endpoint.value for endpoint in cfg.oura.endpoints]
    assert endpoint_values == ["daily_sleep", "daily_readiness"]
    assert cfg.incremental.lookback_days == 14
    assert cfg.incremental.fallback_days == 45
    assert cfg.bridge.fallback_days == 180
    assert cfg.goals.steps_per_day == 8500
    assert cfg.goals.sleep_hours_per_day == 7.5
    assert cfg.goals.strength_elements_per_week == 350
    assert cfg.goals.strength_elements_per_month == 1200
    assert cfg.goals.mindful_minutes_per_week == 60
    assert cfg.goals.mindful_minutes_per_month == 240
