from __future__ import annotations

from dataclasses import dataclass


@dataclass
class OpenAITaggingConfig:
    enabled: bool
    model: str | None = None
    api_key: str | None = None


class OpenAITagEnricher:
    """
    Extension point for enriching Notion free text with predefined tags.

    The initial pipeline keeps this disabled by default. A later implementation can
    wire an LLM call here and persist outputs to dedicated columns/tables.
    """

    def __init__(self, config: OpenAITaggingConfig) -> None:
        self.config = config

    def enrich(self, rows: list[dict]) -> list[dict]:
        return rows
