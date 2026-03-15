"""Tab: 🔮 玄学炒股 — 今日运势占卜（持久缓存，跨用户共享）"""

import json
import time
import streamlit as st
from pathlib import Path
from datetime import datetime

MYSTIC_DIR = Path(__file__).parent.parent.parent / "data" / "mystic"


def _load_cache(today_key: str) -> dict | None:
    """从磁盘读取今日缓存"""
    cache_file = MYSTIC_DIR / f"{today_key}.json"
    if cache_file.exists():
        try:
            return json.loads(cache_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    return None


def _save_cache(today_key: str, content: str, username: str, model: str):
    """保存到磁盘"""
    MYSTIC_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "date": today_key,
        "content": content,
        "username": username,
        "model": model,
        "created_at": datetime.now().isoformat(timespec="seconds"),
    }
    cache_file = MYSTIC_DIR / f"{today_key}.json"
    cache_file.write_text(
        json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8",
    )
    return record


def render_mystic_tab(client, cfg, selected_model):
    """渲染玄学炒股 Tab"""
    from ai.client import call_ai, get_ai_client

    client_m, cfg_m, _ = get_ai_client(selected_model)

    st.markdown("---")
    st.markdown("#### 🔮 玄学炒股 · 今日运势")

    now = datetime.now()
    today_str = now.strftime("%Y年%m月%d日")
    today_key = now.strftime("%Y-%m-%d")
    weekday = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"][now.weekday()]

    # ── 优先读取持久缓存（本地 → GitHub 回退）─────────────────
    cached = _load_cache(today_key)
    if not cached:
        try:
            from utils.cloud_archive import pull_mystic
            if pull_mystic(today_key):
                cached = _load_cache(today_key)
        except Exception:
            pass
    if cached:
        _by_user = cached.get("username", "未知")
        _by_model = cached.get("model", "")
        _by_time = cached.get("created_at", "")[:16].replace("T", " ")
        st.markdown(
            f'<div style="padding:6px 14px;background:linear-gradient(135deg,#faf5ff,#fff7ed);'
            f'border-radius:8px;border:1px solid #e9d5ff;margin-bottom:12px;'
            f'font-size:0.82rem;color:#7c3aed;">'
            f'🔮 今日运势由 <strong>{_by_user}</strong> 于 {_by_time} 使用 {_by_model} 推演'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.markdown(cached["content"])

        # 允许重新推演
        if st.button("🔄 重新推演今日运势", key="redo_mystic"):
            (MYSTIC_DIR / f"{today_key}.json").unlink(missing_ok=True)
            st.session_state.pop("_mystic_result", None)
            st.rerun()
        return

    # ── 无缓存，需要 AI 生成 ──────────────────────────────────
    if not client_m:
        st.warning("请先在左侧配置 AI 模型")
        return

    with st.status("🔮 正卦象推演中...", expanded=True) as status:
        st.write("📅 获取今日日期与天干地支...")
        time.sleep(0.3)
        st.write("🌙 查询黄历宜忌...")
        time.sleep(0.25)
        st.write("🎴 抽取今日塔罗牌...")
        time.sleep(0.25)
        st.write("🐉 推算生肖与五行运势...")
        time.sleep(0.2)
        st.write("🔮 综合推演炒股运势，请虔诚等待...")

        stock_name = st.session_state.get("stock_name", "")
        stock_extra = f"\n\n用户当前关注的股票：{stock_name}，请也对这只股票给出玄学点评。" if stock_name else ""

        prompt = f"""今天是 {today_str} {weekday}。

请你扮演一位精通易经八卦、紫微斗数、塔罗牌、黄历、星座的玄学大师，为今日的A股炒股运势做一次趣味占卜。

请联网搜索今天的真实黄历信息（天干地支、宜忌、冲煞等），然后结合以下维度给出有趣的分析：

## 要求输出格式（用 emoji 让内容生动有趣）：

### 📅 今日黄历
- 农历日期、天干地支、值神
- 宜：xxx  忌：xxx

### 🎯 今日炒股运势评级
给出一个明确的等级：大吉 / 吉 / 小吉 / 中平 / 小凶 / 凶 / 大凶
并配上一句有趣的点评（模仿古人口吻）

### 🐉 五行与板块
根据今日五行旺衰，推荐适合的板块（如：火旺利军工光伏、水旺利航运水利等）
也指出今日五行克制、应回避的板块

### 🎴 塔罗牌指引
随机抽一张塔罗牌，解读其对今日炒股的启示

### ⏰ 吉时与凶时
给出今日适合买入/卖出的吉时（用十二时辰+现代时间对照）
给出应该避开操作的凶时

### 🎲 今日幸运数字 & 尾号
给出今日幸运数字，以及适合关注的股票代码尾号

### ⚠️ 玄学大师忠告
用一段文言文风格的话总结今日建议，最后加一句现代吐槽（制造反差萌）
{stock_extra}

**注意：这是趣味内容，请在最后用小字提醒用户"以上内容纯属娱乐，不构成投资建议，请理性投资"。**"""

        system = (
            "你是一位学贯中西的玄学大师，精通易经、紫微斗数、塔罗牌、西方星座，"
            "同时对A股市场有深入了解。你的风格：专业中带着幽默，神秘中带着接地气，"
            "古典与现代混搭。请联网搜索今天的真实黄历数据来增强可信度。"
        )

        username = st.session_state.get("current_user", "匿名")
        result, err = call_ai(client_m, cfg_m, prompt, system=system, max_tokens=4000,
                              username=username)

        if err:
            status.update(label="❌ 卦象推演失败", state="error")
            st.error(f"玄学大师暂时失联：{err}")
            return

        st.write("✨ 卦象已成！")
        time.sleep(0.3)
        status.update(label="🔮 今日运势已揭晓！", state="complete")

    # 持久化到磁盘 + 推送 GitHub
    _save_cache(today_key, result, username, selected_model)
    try:
        from utils.cloud_archive import push_mystic_async
        push_mystic_async(today_key)
    except Exception:
        pass
    st.session_state["_mystic_result"] = {"date": today_str, "content": result}
    st.rerun()
