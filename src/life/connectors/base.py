from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any


class BaseConnector(ABC):
    @abstractmethod
    def fetch(
        self,
        *,
        date_start: date | None = None,
        date_end: date | None = None,
    ) -> list[dict[str, Any]]:
        raise NotImplementedError
