from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import requests


@dataclass
class OuraOAuthClient:
    authorize_url: str
    token_url: str
    revoke_url: str
    client_id: str
    client_secret: str
    redirect_uri: str
    scope: str

    def build_authorize_url(self, state: str, response_type: str = "code") -> str:
        query = {
            "response_type": response_type,
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": self.scope,
            "state": state,
        }
        return f"{self.authorize_url}?{urlencode(query)}"

    def exchange_code(self, code: str) -> dict[str, Any]:
        payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.redirect_uri,
        }
        return self._post_token(payload)

    def refresh_token(self, refresh_token: str) -> dict[str, Any]:
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        return self._post_token(payload)

    def revoke_access_token(self, access_token: str) -> None:
        params = {"access_token": access_token}
        response = requests.get(self.revoke_url, params=params, timeout=30)
        response.raise_for_status()

    def _post_token(self, payload: dict[str, str]) -> dict[str, Any]:
        headers = {"Accept": "application/json"}

        first_payload = {
            **payload,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        response = requests.post(self.token_url, data=first_payload, headers=headers, timeout=30)
        if response.ok:
            return response.json()

        fallback_response = requests.post(
            self.token_url,
            data=payload,
            headers=headers,
            auth=(self.client_id, self.client_secret),
            timeout=30,
        )
        if fallback_response.ok:
            return fallback_response.json()

        detail = fallback_response.text or response.text
        raise requests.HTTPError(
            f"Token exchange failed ({fallback_response.status_code}): {detail}",
            response=fallback_response,
        )
