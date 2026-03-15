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


def pull_top10_cache() -> list[dict]:
    """从 GitHub 拉取最近的 Top10 缓存文件列表

    Returns:
        [{filename, date, model, ...}, ...] 按日期倒序
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
                "download_url": item.get("download_url", ""),
                "sha": item.get("sha", ""),
            })

        # 按文件名（日期开头）倒序
        files.sort(key=lambda x: x["filename"], reverse=True)
        return files
    except Exception as e:
        logger.debug("[top10_cloud] 拉取文件列表失败: %s", e)
        return []


def load_top10_data(download_url: str, filename: str) -> dict | None:
    """下载并缓存 Top10 数据到本地

    Returns:
        完整 JSON 数据 {results, summary, model, date, triggered_by, tokens_used}
    """
    import requests

    # 先检查本地缓存
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    local_path = CACHE_DIR / filename
    if local_path.exists():
        try:
            return json.loads(local_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    # 从 GitHub 下载
    try:
        r = requests.get(download_url, timeout=30)
        if r.status_code == 200:
            data = r.json()
            # 缓存到本地
            local_path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            return data
    except Exception as e:
        logger.debug("[top10_cloud] 下载 %s 失败: %s", filename, e)

    return None


def get_latest_top10() -> dict | None:
    """获取最新的 Top10 数据（优先本地缓存，再远程拉取）

    Returns:
        完整 JSON 数据或 None
    """
    # 先检查本地是否有今天或昨天的缓存
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    for days_ago in range(3):  # 检查最近3天
        check_date = (date.today() - timedelta(days=days_ago)).isoformat()
        for f in sorted(CACHE_DIR.glob(f"{check_date}_*.json"), reverse=True):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                if data.get("results"):
                    return data
            except Exception:
                continue

    # 本地无缓存，从 GitHub 拉取
    files = pull_top10_cache()
    if not files:
        return None

    # 取最新的文件
    for f in files:
        data = load_top10_data(f["download_url"], f["filename"])
        if data and data.get("results"):
            return data

    return None
