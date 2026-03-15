"""Top10 云端缓存读取 — 从 GitHub data-archive 分支拉取 Stock_test 的 Top10 结果"""

import json
import logging
import base64
from datetime import date, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent.parent / "data" / "top10_cache"


def _get_config() -> dict:
    try:
        import streamlit as st
        return {
            "token": st.secrets.get("GITHUB_TOKEN", ""),
            "repo": st.secrets.get("GITHUB_ARCHIVE_REPO", "TianLin0509/Stock_test"),
            "branch": st.secrets.get("GITHUB_ARCHIVE_BRANCH", "data-archive"),
        }
    except Exception:
        return {"token": "", "repo": "", "branch": "data-archive"}


def _safe_filename(name: str) -> str:
    """将含 emoji/特殊字符的文件名转为安全本地名"""
    # 只保留 ASCII + 中文 + 基本符号
    safe = ""
    for ch in name:
        if ch.isascii() or '\u4e00' <= ch <= '\u9fff':
            safe += ch
        else:
            safe += "_"
    return safe


def pull_top10_cache() -> list[dict]:
    """从 GitHub 拉取最近的 Top10 缓存文件列表

    Returns:
        [{filename, safe_filename, git_url, sha, ...}, ...] 按日期倒序
    """
    import requests

    cfg = _get_config()
    token, repo, branch = cfg["token"], cfg["repo"], cfg["branch"]
    if not token:
        return []

    api = f"https://api.github.com/repos/{repo}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }

    try:
        r = requests.get(
            f"{api}/contents/cache/top10?ref={branch}",
            headers=headers, timeout=15,
        )
        if r.status_code != 200:
            return []

        files = []
        for item in r.json():
            name = item.get("name", "")
            if not name.endswith(".json") or name.startswith(("_", ".")):
                continue
            files.append({
                "filename": name,
                "safe_filename": _safe_filename(name),
                "git_url": item.get("git_url", ""),
                "sha": item.get("sha", ""),
            })

        files.sort(key=lambda x: x["filename"], reverse=True)
        return files
    except Exception as e:
        logger.debug("[top10_cloud] 拉取文件列表失败: %s", e)
        return []


def load_top10_data(file_info: dict) -> dict | None:
    """通过 Git Blob API 下载 Top10 数据并缓存到本地

    Args:
        file_info: pull_top10_cache() 返回的单个文件信息

    Returns:
        完整 JSON 数据 {results, summary, model, date, triggered_by, tokens_used}
    """
    import requests

    safe_name = file_info.get("safe_filename", file_info.get("filename", ""))
    git_url = file_info.get("git_url", "")

    # 先检查本地缓存
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    local_path = CACHE_DIR / safe_name
    if local_path.exists():
        try:
            return json.loads(local_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    # 通过 Git Blob API 下载（避免 URL 中 emoji/空格问题）
    if not git_url:
        return None

    cfg = _get_config()
    token = cfg["token"]
    if not token:
        return None

    try:
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        r = requests.get(git_url, headers=headers, timeout=30)
        if r.status_code != 200:
            logger.debug("[top10_cloud] Blob API 返回 %s", r.status_code)
            return None

        blob = r.json()
        content_b64 = blob.get("content", "")
        content_bytes = base64.b64decode(content_b64)
        data = json.loads(content_bytes.decode("utf-8"))

        # 缓存到本地
        local_path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return data
    except Exception as e:
        logger.debug("[top10_cloud] 下载失败: %s", e)
        return None
