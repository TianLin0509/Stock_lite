"""轻量用户系统 — JSON文件持久化，每用户一个文件"""

import json
import threading
from datetime import datetime
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "user_data"

_file_locks: dict[str, threading.Lock] = {}
_global_lock = threading.Lock()


def _get_lock(username: str) -> threading.Lock:
    with _global_lock:
        if username not in _file_locks:
            _file_locks[username] = threading.Lock()
        return _file_locks[username]


def _user_path(username: str) -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    return DATA_DIR / f"{username}.json"


def _default_user(username: str) -> dict:
    now = datetime.now().isoformat(timespec="seconds")
    return {
        "username": username,
        "created_at": now,
        "last_login": now,
        "preferences": {},
        "token_usage": {"total": 0, "prompt": 0, "completion": 0, "daily": {}},
        "history": [],
        "favorites": [],
    }


def load_user(username: str) -> dict:
    path = _user_path(username)
    lock = _get_lock(username)
    with lock:
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                pass
    return _default_user(username)


def save_user(data: dict):
    username = data["username"]
    path = _user_path(username)
    lock = _get_lock(username)
    # 限制历史条目
    if len(data.get("history", [])) > 100:
        data["history"] = data["history"][-100:]
    with lock:
        path.write_text(
            json.dumps(data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


def add_user_tokens(username: str, prompt: int, completion: int, total: int):
    if not username:
        return
    data = load_user(username)
    usage = data["token_usage"]
    usage["total"] += total
    usage["prompt"] += prompt
    usage["completion"] += completion
    today_str = datetime.now().strftime("%Y-%m-%d")
    daily = usage.setdefault("daily", {})
    day = daily.setdefault(today_str, {"prompt": 0, "completion": 0, "total": 0})
    day["prompt"] += prompt
    day["completion"] += completion
    day["total"] += total
    # 只保留最近30天
    if len(daily) > 30:
        for old in sorted(daily.keys())[:-30]:
            del daily[old]
    save_user(data)


def add_history_entry(username: str, stock_code: str, stock_name: str,
                      model: str, analyses_done: list, token_cost: int,
                      summary: str):
    if not username:
        return
    data = load_user(username)
    data["history"].append({
        "ts": datetime.now().isoformat(timespec="seconds"),
        "stock_code": stock_code,
        "stock_name": stock_name,
        "model": model,
        "analyses_done": analyses_done,
        "token_cost": token_cost,
        "summary": summary,
    })
    save_user(data)


