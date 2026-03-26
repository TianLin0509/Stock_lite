"""Main runner for Xueqiu daily Top50."""

from __future__ import annotations

from typing import Callable

from xueqiu_daily.collector.base import BaseCollector
from xueqiu_daily.collector.mock import MockCollector
from xueqiu_daily.config import XueqiuDailyConfig
from xueqiu_daily.models import DailyRunResult, PostCandidate
from xueqiu_daily.reporting import render_daily_report
from xueqiu_daily.scoring import select_top_posts
from xueqiu_daily.storage import save_run


SummaryGenerator = Callable[[list[PostCandidate]], list[PostCandidate]]


def _default_summarizer(posts: list[PostCandidate]) -> list[PostCandidate]:
    for post in posts:
        if not post.summary:
            parts = []
            if post.topic_tags:
                parts.append(f"主题：{' / '.join(post.topic_tags[:3])}")
            if post.extracted_symbols:
                parts.append(f"标的：{'、'.join(post.extracted_symbols[:3])}")
            parts.append(
                f"互动数据为点赞 {post.like_count}、评论 {post.comment_count}、转发 {post.repost_count}"
            )
            post.summary = "；".join(parts) + "。"
    return posts


def run_daily_top50(
    config: XueqiuDailyConfig,
    collector: BaseCollector | None = None,
    summarizer: SummaryGenerator | None = None,
) -> DailyRunResult:
    active_collector = collector or MockCollector()
    active_summarizer = summarizer or _default_summarizer

    candidates = active_collector.collect(config)
    selected_posts = select_top_posts(candidates, config)
    selected_posts = active_summarizer(selected_posts)
    markdown_text = render_daily_report(selected_posts, config, candidate_count=len(candidates))
    return save_run(
        config=config,
        candidate_count=len(candidates),
        selected_posts=selected_posts,
        markdown_text=markdown_text,
    )
