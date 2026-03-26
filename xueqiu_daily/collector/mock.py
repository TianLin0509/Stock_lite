"""Mock collector to keep the pipeline executable before real crawling is wired in."""

from __future__ import annotations

from datetime import datetime, time

from xueqiu_daily.collector.base import BaseCollector
from xueqiu_daily.config import XueqiuDailyConfig
from xueqiu_daily.models import PostCandidate


class MockCollector(BaseCollector):
    """Simple in-memory collector used for development and tests."""

    def collect(self, config: XueqiuDailyConfig) -> list[PostCandidate]:
        run_dt = datetime.combine(config.run_date, time(hour=10, minute=30))
        samples = [
            PostCandidate(
                post_id="mock-author-1",
                title="白酒龙头的现金流与提价逻辑",
                url="https://xueqiu.example.com/mock-author-1",
                author_id="author-1",
                author_name="深度价值派",
                author_url="https://xueqiu.example.com/u/author-1",
                publish_time=run_dt,
                source_type="priority_author",
                like_count=188,
                comment_count=32,
                repost_count=19,
                follower_count=125000,
                content_text=(
                    "公司核心逻辑在于品牌溢价、渠道库存稳定、现金流质量高。"
                    "正文加入未来两年的盈利预测、分红能力、估值区间以及风险点。"
                )
                * 20,
                topic_tags=["消费", "白酒", "估值"],
                extracted_symbols=["600519.SH"],
            ),
            PostCandidate(
                post_id="mock-column-1",
                title="从订单和产能看 AI 硬件链景气度",
                url="https://xueqiu.example.com/mock-column-1",
                author_id="author-2",
                author_name="产业链研究员",
                author_url="https://xueqiu.example.com/u/author-2",
                publish_time=run_dt,
                source_type="longform_column",
                like_count=98,
                comment_count=15,
                repost_count=13,
                follower_count=42000,
                content_text=(
                    "文章从订单增速、ASP、产能利用率、资本开支和行业格局展开，"
                    "并对 2026 年盈利预测和估值重估空间做了说明，同时提示库存和竞争风险。"
                )
                * 16,
                topic_tags=["AI", "算力", "景气度"],
                extracted_symbols=["300308.SZ", "300502.SZ"],
            ),
            PostCandidate(
                post_id="mock-hot-1",
                title="热门讨论里的高赞跟踪帖",
                url="https://xueqiu.example.com/mock-hot-1",
                author_id="author-3",
                author_name="热点跟踪者",
                author_url="https://xueqiu.example.com/u/author-3",
                publish_time=run_dt,
                source_type="hot_discussion",
                like_count=65,
                comment_count=40,
                repost_count=8,
                follower_count=18000,
                content_text=(
                    "帖子围绕政策催化和板块轮动展开，但论证偏短，主要优点是时效性高。"
                    "附带少量数据与情绪观察。"
                )
                * 8,
                topic_tags=["热点", "政策"],
            ),
        ]
        for sample in samples:
            sample.ensure_derived_fields()
        return samples
