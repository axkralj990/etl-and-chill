from __future__ import annotations

from datetime import date
from typing import Any

import requests

from life.connectors.base import BaseConnector
from life.enums import OuraDailyEndpoint


class OuraConnector(BaseConnector):
    def __init__(self, access_token: str, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {access_token}"})

    def fetch(
        self,
        *,
        date_start: date | None = None,
        date_end: date | None = None,
        endpoint: OuraDailyEndpoint | None = None,
    ) -> list[dict[str, Any]]:
        if endpoint is None:
            raise ValueError("Oura endpoint is required")

        params: dict[str, Any] = {}
        if date_start:
            params["start_date"] = date_start.isoformat()
        if date_end:
            params["end_date"] = date_end.isoformat()

        out: list[dict[str, Any]] = []
        next_token: str | None = None
        while True:
            req_params = dict(params)
            if next_token:
                req_params["next_token"] = next_token

            url = f"{self.base_url}/{endpoint.value}"
            response = self.session.get(url, params=req_params, timeout=30)
            response.raise_for_status()
            body = response.json()

            items = body.get("data", [])
            out.extend(items)

            next_token = body.get("next_token")
            if not next_token:
                break
        return out
