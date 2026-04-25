from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    timezone: str = Field(default="Europe/Ljubljana", alias="TIMEZONE")
    week_start: int = Field(default=0, alias="WEEK_START")

    duckdb_path: Path = Field(default=Path("data/life.duckdb"), alias="DUCKDB_PATH")
    data_dir: Path = Field(default=Path("data"), alias="DATA_DIR")
    legacy_path: Path = Field(default=Path("../life_legacy/data/raw"), alias="LEGACY_PATH")
    pipeline_config_path: Path = Field(
        default=Path("config/pipeline.toml"), alias="PIPELINE_CONFIG_PATH"
    )

    notion_token: str | None = Field(default=None, alias="NOTION_TOKEN")
    notion_database_id: str | None = Field(default=None, alias="NOTION_DATABASE_ID")

    oura_access_token: str | None = Field(default=None, alias="OURA_ACCESS_TOKEN")
    oura_refresh_token: str | None = Field(default=None, alias="OURA_REFRESH_TOKEN")
    oura_auto_refresh: bool = Field(default=True, alias="OURA_AUTO_REFRESH")
    oura_token_store_path: Path = Field(
        default=Path("data/oura_tokens.json"),
        alias="OURA_TOKEN_STORE_PATH",
    )
    oura_base_url: str = Field(
        default="https://api.ouraring.com/v2/usercollection",
        alias="OURA_BASE_URL",
    )
    oura_oauth_authorize_url: str = Field(
        default="https://cloud.ouraring.com/oauth/authorize",
        alias="OURA_OAUTH_AUTHORIZE_URL",
    )
    oura_oauth_token_url: str = Field(
        default="https://api.ouraring.com/oauth/token",
        alias="OURA_OAUTH_TOKEN_URL",
    )
    oura_oauth_revoke_url: str = Field(
        default="https://api.ouraring.com/oauth/revoke",
        alias="OURA_OAUTH_REVOKE_URL",
    )
    oura_client_id: str | None = Field(default=None, alias="OURA_CLIENT_ID")
    oura_client_secret: str | None = Field(default=None, alias="OURA_CLIENT_SECRET")
    oura_redirect_uri: str | None = Field(default=None, alias="OURA_REDIRECT_URI")
    oura_scopes: str = Field(
        default="daily spo2 tag session workout heartrate personal email",
        alias="OURA_SCOPES",
    )

    openai_api_key: str | None = Field(default=None, alias="OPENAI_API_KEY")
    openai_model: str | None = Field(default=None, alias="OPENAI_MODEL")
    enable_openai_tags: bool = Field(default=False, alias="ENABLE_OPENAI_TAGS")


def load_settings() -> Settings:
    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    settings.oura_token_store_path.parent.mkdir(parents=True, exist_ok=True)
    return settings
