"""Collector interfaces for candidate acquisition."""

from __future__ import annotations

from abc import ABC, abstractmethod

from xueqiu_daily.config import XueqiuDailyConfig
from xueqiu_daily.models import PostCandidate


class BaseCollector(ABC):
    """Abstract collector for daily candidate posts."""

    @abstractmethod
    def collect(self, config: XueqiuDailyConfig) -> list[PostCandidate]:
        """Return candidate posts for the target date."""

