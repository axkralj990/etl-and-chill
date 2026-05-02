from __future__ import annotations

from pathlib import Path

OPENAPI_PATH = Path(__file__).parent / "fixtures" / "oura_openapi_contract_snapshot.json"


def test_oura_openapi_contract_contains_daily_endpoints() -> None:
    assert OPENAPI_PATH.exists()
    text = OPENAPI_PATH.read_text(encoding="utf-8")

    expected_paths = [
        '"/v2/usercollection/daily_activity"',
        '"/v2/usercollection/daily_sleep"',
        '"/v2/usercollection/sleep"',
        '"/v2/usercollection/daily_spo2"',
        '"/v2/usercollection/daily_readiness"',
        '"/v2/usercollection/daily_stress"',
        '"/v2/usercollection/daily_resilience"',
        '"/v2/usercollection/daily_cardiovascular_age"',
    ]
    for path in expected_paths:
        assert path in text


def test_oura_openapi_contract_contains_expected_stress_and_spo2_fields() -> None:
    text = OPENAPI_PATH.read_text(encoding="utf-8")
    assert '"PublicDailyStress"' in text
    assert '"day_summary"' in text
    assert '"stress_high"' in text
    assert '"recovery_high"' in text

    assert '"PublicDailySpO2"' in text
    assert '"spo2_percentage"' in text
    assert '"PublicSpo2AggregatedValues"' in text
    assert '"average"' in text


def test_oura_openapi_contract_contains_resilience_and_cardio_fields() -> None:
    text = OPENAPI_PATH.read_text(encoding="utf-8")
    assert '"DailyResilienceModel"' in text
    assert '"contributors"' in text
    assert '"level"' in text

    assert '"PublicDailyCardiovascularAge"' in text
    assert '"vascular_age"' in text
