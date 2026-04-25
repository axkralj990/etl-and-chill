from __future__ import annotations

from datetime import date
from typing import Any

import requests

from life.connectors.base import BaseConnector


class NotionConnector(BaseConnector):
    def __init__(self, token: str, database_id: str) -> None:
        self.token = token
        self.database_id = database_id
        self.base_url = f"https://api.notion.com/v1/databases/{database_id}/query"
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json",
            }
        )

    def fetch(
        self,
        *,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {"page_size": 100}
        if date_start and date_end:
            payload["filter"] = {
                "and": [
                    {"property": "Date", "date": {"on_or_after": date_start.isoformat()}},
                    {"property": "Date", "date": {"on_or_before": date_end.isoformat()}},
                ]
            }

        results: list[dict[str, Any]] = []
        next_cursor: str | None = None
        while True:
            req_payload = dict(payload)
            if next_cursor:
                req_payload["start_cursor"] = next_cursor
            response = self.session.post(self.base_url, json=req_payload, timeout=30)
            response.raise_for_status()
            body = response.json()
            results.extend(body.get("results", []))
            if not body.get("has_more"):
                break
            next_cursor = body.get("next_cursor")
        return results
