from enum import StrEnum


class SourceName(StrEnum):
    NOTION = "notion"
    OURA = "oura"
    LEGACY = "legacy"


class OuraDailyEndpoint(StrEnum):
    DAILY_ACTIVITY = "daily_activity"
    DAILY_SLEEP = "daily_sleep"
    SLEEP = "sleep"
    DAILY_READINESS = "daily_readiness"
    DAILY_SPO2 = "daily_spo2"
    DAILY_STRESS = "daily_stress"
    DAILY_RESILIENCE = "daily_resilience"
    DAILY_CARDIOVASCULAR_AGE = "daily_cardiovascular_age"


class PipelineMode(StrEnum):
    BACKFILL_LEGACY = "backfill_legacy"
    BRIDGE_TO_TODAY = "bridge_to_today"
    INCREMENTAL_SYNC = "incremental_sync"


class WarningCode(StrEnum):
    NOTION_TITLE_DATE_MISMATCH = "notion_title_date_mismatch"
    NOTION_TITLE_DATE_UNPARSEABLE = "notion_title_date_unparseable"
    SUBSTANCES_PARSE_FALLBACK = "substances_parse_fallback"


class RunStatus(StrEnum):
    STARTED = "started"
    SUCCESS = "success"
    FAILED = "failed"
