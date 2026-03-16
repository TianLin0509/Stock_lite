"""炒股伙伴 — 记忆 CRUD（profile / memories / chat_history）"""

import json
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

DATA_DIR = Path(__file__).parent.parent / "data" / "companion"

# ── 上限控制 ─────────────────────────────────────────────────────────────
MAX_TRADES = 50
MAX_OBSERVATIONS = 30
MAX_LESSONS = 20
MAX_WATCHLIST = 15
MAX_CHAT_MESSAGES = 20


def _user_dir(username: str) -> Path:
    d = DATA_DIR / username
    d.mkdir(parents=True, exist_ok=True)
    return d


# ══════════════════════════════════════════════════════════════════════════
# Profile
# ══════════════════════════════════════════════════════════════════════════

_DEFAULT_PROFILE = {
    "template_id": "",
    "custom_system_prompt": "",
    "user_rules": [],
    "created_at": "",
}


def load_profile(username: str) -> dict | None:
    f = _user_dir(username) / "profile.json"
    if not f.exists():
        return None
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return None


def save_profile(username: str, profile: dict):
    f = _user_dir(username) / "profile.json"
    f.write_text(json.dumps(profile, ensure_ascii=False, indent=2), encoding="utf-8")


def create_profile(username: str, template_id: str,
                   rules: list[str], custom_prompt: str = "") -> dict:
    profile = {
        "template_id": template_id,
        "custom_system_prompt": custom_prompt,
        "user_rules": rules,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    save_profile(username, profile)
    return profile


def delete_profile(username: str):
    """删除用户所有伙伴数据"""
    d = _user_dir(username)
    for f in ["profile.json", "memories.json", "chat_history.json"]:
        (d / f).unlink(missing_ok=True)


# ══════════════════════════════════════════════════════════════════════════
# Memories
# ══════════════════════════════════════════════════════════════════════════

_DEFAULT_MEMORIES = {
    "watchlist": [],
    "trades": [],
    "observations": [],
    "lessons": [],
    "portfolio_summary": "",
}


def load_memories(username: str) -> dict:
    f = _user_dir(username) / "memories.json"
    if not f.exists():
        return dict(_DEFAULT_MEMORIES)
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return dict(_DEFAULT_MEMORIES)


def save_memories(username: str, memories: dict):
    f = _user_dir(username) / "memories.json"
    f.write_text(json.dumps(memories, ensure_ascii=False, indent=2), encoding="utf-8")


def add_trade(username: str, trade: dict):
    mem = load_memories(username)
    trade["timestamp"] = datetime.now().isoformat(timespec="seconds")
    mem["trades"].insert(0, trade)
    mem["trades"] = mem["trades"][:MAX_TRADES]
    save_memories(username, mem)


def add_watchlist(username: str, item: dict):
    mem = load_memories(username)
    # 去重
    mem["watchlist"] = [w for w in mem["watchlist"] if w.get("code") != item.get("code")]
    item["added_at"] = datetime.now().isoformat(timespec="seconds")
    mem["watchlist"].insert(0, item)
    mem["watchlist"] = mem["watchlist"][:MAX_WATCHLIST]
    save_memories(username, mem)


def add_observation(username: str, content: str, tags: list[str] | None = None):
    mem = load_memories(username)
    obs = {
        "content": content,
        "tags": tags or [],
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }
    mem["observations"].insert(0, obs)
    mem["observations"] = mem["observations"][:MAX_OBSERVATIONS]
    save_memories(username, mem)


def add_lesson(username: str, content: str, importance: str = "中"):
    mem = load_memories(username)
    lesson = {
        "content": content,
        "importance": importance,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    }
    mem["lessons"].insert(0, lesson)
    mem["lessons"] = mem["lessons"][:MAX_LESSONS]
    save_memories(username, mem)


def remove_watchlist(username: str, code: str):
    mem = load_memories(username)
    mem["watchlist"] = [w for w in mem["watchlist"] if w.get("code") != code]
    save_memories(username, mem)


# ══════════════════════════════════════════════════════════════════════════
# Chat History
# ══════════════════════════════════════════════════════════════════════════

_DEFAULT_HISTORY = {
    "messages": [],
    "summary_of_older": "",
}


def load_chat_history(username: str) -> dict:
    f = _user_dir(username) / "chat_history.json"
    if not f.exists():
        return dict(_DEFAULT_HISTORY)
    try:
        return json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        return dict(_DEFAULT_HISTORY)


def save_chat_history(username: str, history: dict):
    f = _user_dir(username) / "chat_history.json"
    f.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")


def append_message(username: str, role: str, content: str):
    hist = load_chat_history(username)
    hist["messages"].append({
        "role": role,
        "content": content,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
    })
    # 超过上限时需要摘要压缩（由 context.py 触发）
    save_chat_history(username, hist)


def clear_chat_history(username: str):
    save_chat_history(username, dict(_DEFAULT_HISTORY))


def needs_summary(username: str) -> bool:
    hist = load_chat_history(username)
    return len(hist["messages"]) > MAX_CHAT_MESSAGES
