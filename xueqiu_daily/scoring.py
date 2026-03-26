"""Top50 ranking logic for Xueqiu daily posts."""

from __future__ import annotations

import math
from collections import Counter

from xueqiu_daily.config import XueqiuDailyConfig
from xueqiu_daily.models import PostCandidate


FINANCE_KEYWORDS = (
    "营收",
    "净利润",
    "现金流",
    "分红",
    "估值",
    "pe",
    "pb",
    "roe",
    "毛利率",
    "风险",
    "竞争格局",
    "产能",
)

EMOTION_KEYWORDS = ("暴涨", "起飞", "梭哈", "无脑", "满仓", "翻倍", "冲冲冲")


def _normalized_log(value: int, cap: float) -> float:
    if value <= 0:
        return 0.0
    return min(math.log1p(value) / cap, 1.0)


def _source_score(post: PostCandidate, config: XueqiuDailyConfig) -> float:
    mapping = {
        "priority_author": config.source_weights.priority_author,
        "longform_column": config.source_weights.longform_column,
        "hot_discussion": config.source_weights.hot_discussion,
    }
    return mapping.get(post.source_type, 0.5)


def _author_score(post: PostCandidate, config: XueqiuDailyConfig) -> float:
    follower_score = min(post.follower_count / max(config.priority_author_followers, 1), 1.0)
    priority_bonus = 0.2 if post.is_priority_author else 0.0
    return min(follower_score + priority_bonus, 1.0)


def _engagement_score(post: PostCandidate) -> float:
    like_score = _normalized_log(post.like_count, math.log1p(500))
    comment_score = _normalized_log(post.comment_count, math.log1p(200))
    repost_score = _normalized_log(post.repost_count, math.log1p(100))
    return round(like_score * 0.5 + comment_score * 0.3 + repost_score * 0.2, 4)


def _content_score(post: PostCandidate) -> float:
    post.ensure_derived_fields()
    text = (post.content_text or "").lower()
    length_score = min(post.content_length / 2000, 1.0)
    keyword_hits = sum(1 for keyword in FINANCE_KEYWORDS if keyword in text)
    keyword_score = min(keyword_hits / 6, 1.0)
    longform_bonus = 0.2 if post.is_longform else 0.0
    return min(length_score * 0.5 + keyword_score * 0.3 + longform_bonus, 1.0)


def _penalty_score(post: PostCandidate) -> float:
    text = (post.content_text or "").lower()
    short_penalty = 0.15 if post.content_length and post.content_length < 300 else 0.0
    emotion_hits = sum(1 for keyword in EMOTION_KEYWORDS if keyword in text)
    emotion_penalty = min(emotion_hits * 0.05, 0.2)
    return short_penalty + emotion_penalty


def score_post(post: PostCandidate, config: XueqiuDailyConfig) -> PostCandidate:
    post.ensure_derived_fields()
    post.is_priority_author = post.follower_count >= config.priority_author_followers

    source_score = _source_score(post, config)
    author_score = _author_score(post, config)
    engagement_score = _engagement_score(post)
    content_score = _content_score(post)
    penalty = _penalty_score(post)

    final_score = (
        source_score * config.score_weights.source
        + author_score * config.score_weights.author
        + engagement_score * config.score_weights.engagement
        + content_score * config.score_weights.content
        - penalty * config.score_weights.penalty
    )
    post.score_breakdown = {
        "source": round(source_score, 4),
        "author": round(author_score, 4),
        "engagement": round(engagement_score, 4),
        "content": round(content_score, 4),
        "penalty": round(penalty, 4),
    }
    post.quality_score = round(final_score * 100, 2)
    return post


def select_top_posts(
    posts: list[PostCandidate],
    config: XueqiuDailyConfig,
) -> list[PostCandidate]:
    scored = [score_post(post, config) for post in posts]
    scored.sort(key=lambda item: item.quality_score, reverse=True)

    selected: list[PostCandidate] = []
    author_counter: Counter[str] = Counter()
    topic_counter: Counter[str] = Counter()

    for post in scored:
        if post.like_count <= config.min_like_count:
            continue

        if author_counter[post.author_id] >= config.max_posts_per_author:
            continue

        dominant_topic = post.topic_tags[0] if post.topic_tags else ""
        if dominant_topic and topic_counter[dominant_topic] >= config.max_posts_per_topic:
            continue

        selected.append(post)
        author_counter[post.author_id] += 1
        if dominant_topic:
            topic_counter[dominant_topic] += 1

        if len(selected) >= config.top_n:
            break

    return selected
