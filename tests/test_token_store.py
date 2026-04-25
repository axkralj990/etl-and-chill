from __future__ import annotations

from life.oauth.token_store import OuraTokenStore


def test_token_store_roundtrip(tmp_path) -> None:
    store = OuraTokenStore(tmp_path / "tokens.json")
    store.set_tokens(
        access_token="acc",
        refresh_token="ref",
        expires_in=3600,
        token_type="bearer",
        scope="daily",
    )
    assert store.get_access_token() == "acc"
    assert store.get_refresh_token() == "ref"
