"""Sidebar 逻辑 — Stock Lite 精简版"""

import streamlit as st
from config import MODEL_CONFIGS, MODEL_NAMES
from data.tushare_client import get_ts_error
from ui.auth import clear_login


def render_sidebar(current_user: str, on_logout) -> str:
    """渲染侧边栏，返回 selected_model"""
    with st.sidebar:
        st.markdown(f"**👤 {current_user}**")
        if st.button("🔄 切换用户", key="logout_btn"):
            clear_login()
            on_logout()

        st.markdown("---")
        st.markdown("### 🤖 选择分析模型")
        selected_model = st.selectbox(
            "当前模型", options=MODEL_NAMES, index=3,
            key="selected_model", label_visibility="collapsed",
        )
        cfg = MODEL_CONFIGS[selected_model]

        has_key = bool(cfg["api_key"])
        if has_key:
            search_tip = "🌐 联网搜索已开启" if cfg["supports_search"] else "📚 仅内部知识"
            st.markdown(
                f'<div class="model-badge ok">✅ {cfg["note"]} &nbsp;·&nbsp; {search_tip}</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown('<div class="model-badge err">⚠️ API Key 待配置</div>',
                        unsafe_allow_html=True)
            st.caption("暂无法使用AI分析，K线图仍可正常查看")

        st.markdown("### 📡 数据源状态")
        ts_error = get_ts_error()
        if not ts_error:
            st.markdown('<div class="model-badge ok">✅ Tushare 连接正常</div>',
                        unsafe_allow_html=True)
        else:
            st.markdown('<div class="model-badge ok">✅ 备用数据源就绪（akshare / 东方财富）</div>',
                        unsafe_allow_html=True)
            st.caption(f"Tushare 不可用：{ts_error}，已自动切换备用源")

        # ── 跨应用导航 ─────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("### 🔗 更多工具")
        st.caption("其他投研工具（开发中）")

        st.markdown("---")
        st.markdown("### 📖 使用方法")
        st.markdown("""
**① 选择分析模型**

**② 输入股票代码或名称**
> 例：`600519` 或 `贵州茅台`

**③ 点击「一键分析」**

**④ 按需点击分析按钮**
> 核心三项完成后可开启深度分析
""")

        # ── 分析历史 ──────────────────────────────────────────────────
        st.markdown("---")
        st.markdown("### 📜 分析历史")
        _cache_key = "_cached_user_data"
        if _cache_key not in st.session_state:
            from utils.user_store import load_user
            st.session_state[_cache_key] = load_user(current_user)
        _udata = st.session_state[_cache_key]
        _hist = _udata.get("history", [])
        if _hist:
            for _entry in reversed(_hist[-10:]):
                _date = _entry.get("ts", "")[:10]
                _sname = _entry.get("stock_name", "")
                _adone = _entry.get("analyses_done", [])
                _tcost = _entry.get("token_cost", 0)
                _tdisp = f"{_tcost/10000:.1f}万" if _tcost >= 10000 else f"{_tcost:,}"
                st.caption(f"{_date} · **{_sname}** ({len(_adone)}项) · {_tdisp} tokens")
        else:
            st.caption("暂无分析记录")

        st.markdown("---")
        st.markdown("""
<div class="disclaimer">
⚠️ <strong>免责声明</strong><br>
本工具仅供学习研究，不构成任何投资建议。A股市场风险较大，请独立判断，自行承担投资盈亏。
</div>
""", unsafe_allow_html=True)

    return selected_model
