from __future__ import annotations

from life.oauth.oura import OuraOAuthClient


def test_build_authorize_url_contains_expected_params() -> None:
    client = OuraOAuthClient(
        authorize_url="https://cloud.ouraring.com/oauth/authorize",
        token_url="https://api.ouraring.com/oauth/token",
        revoke_url="https://api.ouraring.com/oauth/revoke",
        client_id="cid",
        client_secret="secret",
        redirect_uri="https://example.com/callback",
        scope="daily spo2",
    )
    url = client.build_authorize_url(state="abc", response_type="code")
    assert "response_type=code" in url
    assert "client_id=cid" in url
    assert "redirect_uri=https%3A%2F%2Fexample.com%2Fcallback" in url
    assert "scope=daily+spo2" in url
    assert "state=abc" in url
