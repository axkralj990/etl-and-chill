from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseNormalizer(ABC):
    @abstractmethod
    def normalize(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        raise NotImplementedError
