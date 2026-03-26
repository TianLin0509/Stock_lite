"""Configuration for Xueqiu daily Top50 pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


@dataclass(slots=True)
class SourceWeights:
    """Priority weights for different candidate sources."""

    priority_author: float = 1.0
    longform_column: float = 0.9
    hot_discussion: float = 0.7


@dataclass(slots=True)
class ScoreWeights:
    """Score weights for Top50 ranking."""

    source: float = 0.2
    author: float = 0.2
    engagement: float = 0.2
    content: float = 0.35
    penalty: float = 0.05


@dataclass(slots=True)
class XueqiuDailyConfig:
    """Runtime config for Xueqiu daily ranking."""

    run_date: date
    top_n: int = 50
    min_like_count: int = 50
    priority_author_followers: int = 60_000
    max_posts_per_author: int = 3
    max_posts_per_topic: int = 5
    source_weights: SourceWeights = field(default_factory=SourceWeights)
    score_weights: ScoreWeights = field(default_factory=ScoreWeights)
    base_dir: Path = field(
        default_factory=lambda: Path(__file__).resolve().parent.parent
    )

    @property
    def storage_dir(self) -> Path:
        return self.base_dir / "storage"

    @property
    def daily_storage_dir(self) -> Path:
        return self.storage_dir / "xueqiu_daily"

    @property
    def reports_dir(self) -> Path:
        return self.daily_storage_dir / "reports"

    @property
    def cache_dir(self) -> Path:
        return self.daily_storage_dir / "cache"

    @property
    def db_path(self) -> Path:
        return self.daily_storage_dir / "xueqiu_daily.db"
