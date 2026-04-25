from __future__ import annotations

from life.cli import _extract_code_from_redirect_url


def test_extract_code_from_escaped_redirect_url() -> None:
    url = (
        "http://localhost:8080/\\?iss\\=https%3A%2F%2Fmoi.ouraring.com%2Foauth"
        "%2Fv2%2Fext%2Foauth-anonymous\\&code\\=abc123\\&state\\=life-init"
    )
    assert _extract_code_from_redirect_url(url) == "abc123"


def test_extract_code_from_fragment_redirect_url() -> None:
    url = "http://localhost:8080/#token_type=bearer&code=fragcode&state=x"
    assert _extract_code_from_redirect_url(url) == "fragcode"
