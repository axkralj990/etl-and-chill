from __future__ import annotations

from datetime import date
from typing import Any

import requests

from life.connectors.base import BaseConnector


class NotionConnector(BaseConnector):
    def __init__(self, token: str, database_id: str) -> None:
        self.token = token
        self.database_id = database_id
        self.api_base = "https://api.notion.com/v1"
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

        self._attach_workout_names(results)
        return results

    @staticmethod
    def _page_title(page: dict[str, Any]) -> str | None:
        props = page.get("properties", {})
        for prop in props.values():
            if not isinstance(prop, dict):
                continue
            if prop.get("type") != "title":
                continue
            chunks = prop.get("title", [])
            if not chunks:
                continue
            text = "".join(chunk.get("plain_text", "") for chunk in chunks).strip()
            if text:
                return text
        return None

    def _attach_workout_names(self, pages: list[dict[str, Any]]) -> None:
        workout_ids: set[str] = set()
        for page in pages:
            relation = page.get("properties", {}).get("Workout", {}).get("relation", [])
            workout_ids.update(item.get("id") for item in relation if item.get("id"))

        if not workout_ids:
            return

        resolved: dict[str, str] = {}
        for workout_id in sorted(workout_ids):
            response = self.session.get(f"{self.api_base}/pages/{workout_id}", timeout=30)
            response.raise_for_status()
            page = response.json()
            title = self._page_title(page)
            if title:
                resolved[workout_id] = title

        for page in pages:
            relation = page.get("properties", {}).get("Workout", {}).get("relation", [])
            names = [
                resolved[item.get("id", "")] for item in relation if item.get("id") in resolved
            ]
            if not names:
                continue
            page.setdefault("properties", {})["Workout Resolved"] = {
                "rich_text": [{"plain_text": ", ".join(names)}]
            }
