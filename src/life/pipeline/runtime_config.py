from __future__ import annotations

from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[import-not-found,no-redef]

from pydantic import BaseModel, Field

from life.enums import OuraDailyEndpoint


class OuraRuntimeConfig(BaseModel):
    endpoints: list[OuraDailyEndpoint] = Field(
        default_factory=lambda: [
            OuraDailyEndpoint.DAILY_ACTIVITY,
            OuraDailyEndpoint.DAILY_SLEEP,
            OuraDailyEndpoint.SLEEP,
            OuraDailyEndpoint.DAILY_READINESS,
            OuraDailyEndpoint.DAILY_SPO2,
            OuraDailyEndpoint.DAILY_STRESS,
            OuraDailyEndpoint.DAILY_RESILIENCE,
            OuraDailyEndpoint.DAILY_CARDIOVASCULAR_AGE,
        ]
    )


class IncrementalRuntimeConfig(BaseModel):
    lookback_days: int = 7
    fallback_days: int = 30


class BridgeRuntimeConfig(BaseModel):
    fallback_days: int = 365


class RuntimeConfig(BaseModel):
    oura: OuraRuntimeConfig = Field(default_factory=OuraRuntimeConfig)
    incremental: IncrementalRuntimeConfig = Field(default_factory=IncrementalRuntimeConfig)
    bridge: BridgeRuntimeConfig = Field(default_factory=BridgeRuntimeConfig)


def load_runtime_config(path: Path) -> RuntimeConfig:
    if not path.exists():
        return RuntimeConfig()
    with path.open("rb") as handle:
        payload = tomllib.load(handle)
    return RuntimeConfig.model_validate(payload)
