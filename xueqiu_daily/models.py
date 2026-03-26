"""Data models for Xueqiu daily pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class AuthorProfile:
    author_id: str
    author_name: str
    profile_url: str
    follower_count: int = 0
    historical_quality_ratio: float = 0.0
    avg_like_count: float = 0.0
    is_priority_author: bool = False


@dataclass(slots=True)
class PostCandidate:
    post_id: str
    title: str
    url: str
    author_id: str
    author_name: str
    author_url: str
    publish_time: datetime
    source_type: str
    like_count: int = 0
    comment_count: int = 0
    repost_count: int = 0
    follower_count: int = 0
    content_text: str = ""
    topic_tags: list[str] = field(default_factory=list)
    extracted_symbols: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    is_priority_author: bool = False
    is_longform: bool = False
    content_length: int = 0
    score_breakdown: dict[str, float] = field(default_factory=dict)
    quality_score: float = 0.0
    summary: str = ""
    labels: list[str] = field(default_factory=list)

    def ensure_derived_fields(self) -> None:
        if not self.content_length:
            self.content_length = len((self.content_text or "").strip())
        if not self.is_longform:
            self.is_longform = self.content_length >= 800 or self.source_type == "longform_column"


@dataclass(slots=True)
class DailyRunResult:
    run_id: str
    run_date: str
    candidate_count: int
    selected_count: int
    markdown_path: str
    top_posts: list[PostCandidate]
