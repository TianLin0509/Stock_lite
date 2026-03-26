"""Markdown report rendering for Xueqiu daily Top50."""

from __future__ import annotations

from collections import Counter

from xueqiu_daily.config import XueqiuDailyConfig
from xueqiu_daily.models import PostCandidate


def _source_name(source_type: str) -> str:
    mapping = {
        "priority_author": "优质作者",
        "longform_column": "长文专栏",
        "hot_discussion": "热门讨论",
    }
    return mapping.get(source_type, source_type)


def build_post_summary(post: PostCandidate) -> str:
    if post.summary:
        return post.summary

    base = f"来源于{_source_name(post.source_type)}，作者粉丝 {post.follower_count}。"
    if post.content_length >= 800:
        base += " 正文较长，具备深度阅读价值。"
    if post.topic_tags:
        base += f" 主题集中在：{' / '.join(post.topic_tags[:3])}。"
    if post.extracted_symbols:
        base += f" 提及标的：{'、'.join(post.extracted_symbols[:3])}。"
    return base


def render_daily_report(
    posts: list[PostCandidate],
    config: XueqiuDailyConfig,
    candidate_count: int,
) -> str:
    topic_counter = Counter(
        topic
        for post in posts
        for topic in post.topic_tags[:3]
    )
    author_counter = Counter(post.author_name for post in posts)

    hot_topics = "、".join(topic for topic, _ in topic_counter.most_common(8)) or "暂无"
    key_authors = "、".join(name for name, _ in author_counter.most_common(8)) or "暂无"

    lines = [
        f"# 雪球高质量观点 Top{len(posts)} 日报",
        "",
        f"- 运行日期：{config.run_date.isoformat()}",
        f"- 候选帖子数：{candidate_count}",
        f"- 入选帖子数：{len(posts)}",
        f"- 入池门槛：当天发布，点赞 > {config.min_like_count}",
        f"- 重点作者阈值：粉丝数 > {config.priority_author_followers}",
        "",
        "## 今日概览",
        "",
        f"- 高频主题：{hot_topics}",
        f"- 重点作者：{key_authors}",
        "",
        "## Top10 精选",
        "",
    ]

    for index, post in enumerate(posts[:10], start=1):
        lines.extend(
            [
                f"### {index}. {post.title}",
                f"- 作者：{post.author_name}",
                f"- 来源：{_source_name(post.source_type)}",
                f"- 热度：点赞 {post.like_count} / 评论 {post.comment_count} / 转发 {post.repost_count}",
                f"- 评分：{post.quality_score}",
                f"- 链接：{post.url}",
                f"- 摘要：{build_post_summary(post)}",
                "",
            ]
        )

    lines.extend(["## Top50 全列表", ""])
    for index, post in enumerate(posts, start=1):
        tags = " / ".join(post.topic_tags[:4]) or "未标注"
        lines.append(
            f"{index}. [{post.title}]({post.url}) | {post.author_name} | "
            f"{_source_name(post.source_type)} | 赞 {post.like_count} | 评分 {post.quality_score} | {tags}"
        )

    return "\n".join(lines) + "\n"
