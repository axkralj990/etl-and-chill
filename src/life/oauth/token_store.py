from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class OuraTokenStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        with self.path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def save(self, data: dict[str, Any]) -> None:
        with self.path.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, ensure_ascii=True)

    def set_tokens(
        self,
        *,
        access_token: str,
        refresh_token: str | None,
        expires_in: int | None,
        token_type: str | None,
        scope: str | None,
    ) -> None:
        payload = self.load()
        payload["access_token"] = access_token
        if refresh_token:
            payload["refresh_token"] = refresh_token
        payload["expires_in"] = expires_in
        payload["token_type"] = token_type
        payload["scope"] = scope
        self.save(payload)

    def get_access_token(self) -> str | None:
        return self.load().get("access_token")

    def get_refresh_token(self) -> str | None:
        return self.load().get("refresh_token")
