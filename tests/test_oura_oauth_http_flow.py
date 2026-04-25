from __future__ import annotations

from typing import Any

import pytest
import requests

from life.oauth.oura import OuraOAuthClient


class _Resp:
    def __init__(
        self,
        status_code: int,
        body: dict[str, Any] | None = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._body = body or {}
        self.text = text

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> dict[str, Any]:
        return self._body


def _client() -> OuraOAuthClient:
    return OuraOAuthClient(
        authorize_url="https://cloud.ouraring.com/oauth/authorize",
        token_url="https://api.ouraring.com/oauth/token",
        revoke_url="https://api.ouraring.com/oauth/revoke",
        client_id="cid",
        client_secret="secret",
        redirect_uri="http://localhost:8080",
        scope="daily spo2",
    )


def test_token_post_uses_basic_auth_fallback(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_post(url: str, data: dict[str, str], headers: dict[str, str], timeout: int, auth=None):
        calls.append({"url": url, "data": dict(data), "auth": auth})
        if len(calls) == 1:
            return _Resp(400, text="invalid_client")
        return _Resp(200, body={"access_token": "ok"})

    monkeypatch.setattr(requests, "post", fake_post)
    out = _client().exchange_code("abc")

    assert out["access_token"] == "ok"
    assert calls[0]["data"]["client_id"] == "cid"
    assert calls[0]["data"]["client_secret"] == "secret"
    assert calls[1]["auth"] == ("cid", "secret")


def test_token_post_raises_with_details(monkeypatch) -> None:
    def fake_post(url: str, data: dict[str, str], headers: dict[str, str], timeout: int, auth=None):
        return _Resp(400, text="bad_request_detail")

    monkeypatch.setattr(requests, "post", fake_post)
    with pytest.raises(requests.HTTPError) as exc:
        _client().refresh_token("r1")
    assert "bad_request_detail" in str(exc.value)
