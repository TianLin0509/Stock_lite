"""SQLite persistence for Xueqiu daily pipeline."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime

from xueqiu_daily.config import XueqiuDailyConfig
from xueqiu_daily.models import DailyRunResult, PostCandidate


def ensure_storage(config: XueqiuDailyConfig) -> None:
    config.daily_storage_dir.mkdir(parents=True, exist_ok=True)
    config.reports_dir.mkdir(parents=True, exist_ok=True)
    config.cache_dir.mkdir(parents=True, exist_ok=True)


def init_db(config: XueqiuDailyConfig) -> None:
    ensure_storage(config)
    with sqlite3.connect(config.db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS xq_authors (
                author_id TEXT PRIMARY KEY,
                author_name TEXT NOT NULL,
                author_url TEXT NOT NULL,
                follower_count INTEGER NOT NULL DEFAULT 0,
                is_priority_author INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS xq_posts (
                post_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                url TEXT NOT NULL,
                author_id TEXT NOT NULL,
                publish_time TEXT NOT NULL,
                source_type TEXT NOT NULL,
                content_text TEXT NOT NULL,
                content_length INTEGER NOT NULL DEFAULT 0,
                topic_tags TEXT NOT NULL DEFAULT '[]',
                extracted_symbols TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS xq_post_metrics_daily (
                id TEXT PRIMARY KEY,
                run_date TEXT NOT NULL,
                post_id TEXT NOT NULL,
                like_count INTEGER NOT NULL DEFAULT 0,
                comment_count INTEGER NOT NULL DEFAULT 0,
                repost_count INTEGER NOT NULL DEFAULT 0,
                quality_score REAL NOT NULL DEFAULT 0,
                score_breakdown TEXT NOT NULL DEFAULT '{}',
                summary TEXT NOT NULL DEFAULT '',
                labels TEXT NOT NULL DEFAULT '[]',
                rank_no INTEGER,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS xq_daily_reports (
                run_id TEXT PRIMARY KEY,
                run_date TEXT NOT NULL,
                candidate_count INTEGER NOT NULL,
                selected_count INTEGER NOT NULL,
                markdown_path TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def save_run(
    *,
    config: XueqiuDailyConfig,
    candidate_count: int,
    selected_posts: list[PostCandidate],
    markdown_text: str,
) -> DailyRunResult:
    init_db(config)

    run_id = str(uuid.uuid4())
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    markdown_path = config.reports_dir / f"{timestamp}_xueqiu_top50_{run_id}.md"
    markdown_path.write_text(markdown_text, encoding="utf-8")

    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(config.db_path) as conn:
        for post in selected_posts:
            conn.execute(
                """
                INSERT INTO xq_authors (
                    author_id, author_name, author_url, follower_count, is_priority_author, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(author_id) DO UPDATE SET
                    author_name=excluded.author_name,
                    author_url=excluded.author_url,
                    follower_count=excluded.follower_count,
                    is_priority_author=excluded.is_priority_author,
                    updated_at=excluded.updated_at
                """,
                (
                    post.author_id,
                    post.author_name,
                    post.author_url,
                    post.follower_count,
                    1 if post.is_priority_author else 0,
                    now_text,
                ),
            )
            conn.execute(
                """
                INSERT INTO xq_posts (
                    post_id, title, url, author_id, publish_time, source_type,
                    content_text, content_length, topic_tags, extracted_symbols, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(post_id) DO UPDATE SET
                    title=excluded.title,
                    url=excluded.url,
                    author_id=excluded.author_id,
                    publish_time=excluded.publish_time,
                    source_type=excluded.source_type,
                    content_text=excluded.content_text,
                    content_length=excluded.content_length,
                    topic_tags=excluded.topic_tags,
                    extracted_symbols=excluded.extracted_symbols
                """,
                (
                    post.post_id,
                    post.title,
                    post.url,
                    post.author_id,
                    post.publish_time.isoformat(),
                    post.source_type,
                    post.content_text,
                    post.content_length,
                    json.dumps(post.topic_tags, ensure_ascii=False),
                    json.dumps(post.extracted_symbols, ensure_ascii=False),
                    now_text,
                ),
            )

        for index, post in enumerate(selected_posts, start=1):
            conn.execute(
                """
                INSERT INTO xq_post_metrics_daily (
                    id, run_date, post_id, like_count, comment_count, repost_count,
                    quality_score, score_breakdown, summary, labels, rank_no, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    config.run_date.isoformat(),
                    post.post_id,
                    post.like_count,
                    post.comment_count,
                    post.repost_count,
                    post.quality_score,
                    json.dumps(post.score_breakdown, ensure_ascii=False),
                    post.summary,
                    json.dumps(post.labels, ensure_ascii=False),
                    index,
                    now_text,
                ),
            )

        conn.execute(
            """
            INSERT INTO xq_daily_reports (
                run_id, run_date, candidate_count, selected_count, markdown_path, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                config.run_date.isoformat(),
                candidate_count,
                len(selected_posts),
                str(markdown_path),
                now_text,
            ),
        )
        conn.commit()

    return DailyRunResult(
        run_id=run_id,
        run_date=config.run_date.isoformat(),
        candidate_count=candidate_count,
        selected_count=len(selected_posts),
        markdown_path=str(markdown_path),
        top_posts=selected_posts,
    )
