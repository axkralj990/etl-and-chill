from __future__ import annotations

from life.enums import OuraDailyEndpoint
from life.normalizers.oura_daily import OuraDailyNormalizer, merge_oura_daily_rows


def test_oura_merge_daily_rows() -> None:
    normalizer = OuraDailyNormalizer()
    activity = normalizer.normalize(
        [{"id": "1", "day": "2024-01-02", "score": "80"}], endpoint=OuraDailyEndpoint.DAILY_ACTIVITY
    )
    sleep = normalizer.normalize(
        [{"id": "2", "day": "2024-01-02", "score": "77"}], endpoint=OuraDailyEndpoint.DAILY_SLEEP
    )
    merged = merge_oura_daily_rows(activity + sleep)
    assert len(merged) == 1
    assert merged[0]["activity_score"] == 80
    assert merged[0]["sleep_score"] == 77


def test_daily_stress_uses_day_summary_proxy_when_avg_missing() -> None:
    normalizer = OuraDailyNormalizer()
    stress = normalizer.normalize(
        [
            {
                "id": "stress-1",
                "day": "2024-01-02",
                "day_summary": "stressful",
                "stress_high": 1800,
                "recovery_high": 600,
            }
        ],
        endpoint=OuraDailyEndpoint.DAILY_STRESS,
    )
    assert stress[0]["stress_summary"] == "stressful"
    assert stress[0]["daytime_stress_avg"] == 75.0


def test_sleep_endpoint_maps_hr_hrv_and_durations() -> None:
    normalizer = OuraDailyNormalizer()
    sleep = normalizer.normalize(
        [
            {
                "id": "sleep-1",
                "day": "2024-01-02",
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

    assert sleep[0]["sleep_time_in_bed"] == 28000
    assert sleep[0]["sleep_total_duration"] == 25000
    assert sleep[0]["sleep_deep_duration"] == 6000
    assert sleep[0]["sleep_rem_duration"] == 5000
    assert sleep[0]["sleep_light_duration"] == 14000
    assert sleep[0]["sleep_lowest_hr"] == 42
    assert sleep[0]["sleep_avg_hr"] == 51.2
    assert sleep[0]["sleep_avg_hrv"] == 88.0


def test_sleep_endpoint_skips_nap_sleep_type() -> None:
    normalizer = OuraDailyNormalizer()
    sleep = normalizer.normalize(
        [
            {
                "id": "sleep-nap",
                "day": "2024-01-02",
                "type": "sleep",
                "total_sleep_duration": 1200,
                "time_in_bed": 1800,
            }
        ],
        endpoint=OuraDailyEndpoint.SLEEP,
    )

    assert len(sleep) == 1
    assert sleep[0]["endpoint"] == "sleep"
    assert "sleep_total_duration" not in sleep[0]
    assert "sleep_time_in_bed" not in sleep[0]
