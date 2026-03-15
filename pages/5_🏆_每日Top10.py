"""🏆 每日 Top10 — 从 Stock_test 云端读取 AI 精选结果"""

import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

st.set_page_config(page_title="🏆 每日Top10", page_icon="🏆", layout="wide")

# 登录检查
if "current_user" not in st.session_state:
    st.warning("请先在主页登录")
    st.stop()

# 注入CSS + 侧边栏
from ui.styles import inject_css
from ui.sidebar import render_sidebar
inject_css()
render_sidebar(
    st.session_state["current_user"],
    lambda: (st.session_state.clear(), st.rerun()),
)

import pandas as pd
from top10.cloud_cache import pull_top10_cache, load_top10_data
from top10.cards import show_top10_cards

st.markdown("#### 🏆 每日 AI 精选 Top10")
st.caption("数据来自 Stock_test 每日自动分析，从人气+成交额双榜中 AI 评分筛选")

# ── 加载数据 ──────────────────────────────────────────────────

data = st.session_state.get("_top10_data")

if not data or not data.get("results"):
    with st.spinner("📡 正在从云端获取 Top10 数据..."):
        files = pull_top10_cache()
        for f in files:
            loaded = load_top10_data(f)
            if loaded and loaded.get("results"):
                data = loaded
                st.session_state["_top10_data"] = data
                break

if not data or not data.get("results"):
    st.info("暂无 Top10 数据。Stock_test 每日 22:00 自动分析，请稍后再来查看。")
    st.stop()

# ── 元信息展示 ────────────────────────────────────────────────

_date = data.get("date", "")
_model = data.get("model", "")
_user = data.get("triggered_by", "")
_tokens = data.get("tokens_used", 0)
_tokens_display = f"{_tokens / 10000:.1f}万" if _tokens >= 10000 else f"{_tokens:,}"

st.markdown(
    f'<div style="padding:8px 16px;background:linear-gradient(135deg,#eef2ff,#faf5ff);'
    f'border-radius:10px;border:1px solid #c7d2fe;margin-bottom:12px;'
    f'font-size:0.85rem;color:#4338ca;">'
    f'📅 <strong>{_date}</strong> &nbsp;·&nbsp; '
    f'🤖 {_model} &nbsp;·&nbsp; '
    f'👤 分析来自 <strong>{_user}</strong> &nbsp;·&nbsp; '
    f'🪙 {_tokens_display} tokens'
    f'</div>',
    unsafe_allow_html=True,
)

# ── 每日总结 ──────────────────────────────────────────────────

summary = data.get("summary", "")
if summary:
    with st.expander("📝 每日市场总结", expanded=False):
        st.markdown(summary)

# ── Top10 卡片 ────────────────────────────────────────────────

df = pd.DataFrame(data["results"])
if not df.empty:
    df = df.sort_values("综合评分", ascending=False).reset_index(drop=True)
    show_top10_cards(df)

# ── 刷新按钮 ──────────────────────────────────────────────────

st.markdown("---")
if st.button("🔄 刷新数据", key="refresh_top10"):
    st.session_state.pop("_top10_data", None)
    st.rerun()

st.caption("⚠️ Top10 数据由 Stock_test 自动生成，仅供参考，不构成投资建议。")
