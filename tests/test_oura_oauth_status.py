from __future__ import annotations

from life.cli import _resolve_oura_access_token_source


def test_status_source_prefers_env() -> None:
    source = _resolve_oura_access_token_source(
        env_access_token="env-token",
        stored_access_token="stored-token",
        auto_refresh=True,
        refresh_token="refresh-token",
        oauth_configured=True,
    )
    assert source == "env"


def test_status_source_uses_store_when_no_env() -> None:
    source = _resolve_oura_access_token_source(
        env_access_token=None,
        stored_access_token="stored-token",
        auto_refresh=True,
        refresh_token="refresh-token",
        oauth_configured=True,
    )
    assert source == "token_store"


def test_status_source_can_refresh() -> None:
    source = _resolve_oura_access_token_source(
        env_access_token=None,
        stored_access_token=None,
        auto_refresh=True,
        refresh_token="refresh-token",
        oauth_configured=True,
    )
    assert source == "refresh"


def test_status_source_none_when_unavailable() -> None:
    source = _resolve_oura_access_token_source(
        env_access_token=None,
        stored_access_token=None,
        auto_refresh=False,
        refresh_token=None,
        oauth_configured=False,
    )
    assert source == "none"
