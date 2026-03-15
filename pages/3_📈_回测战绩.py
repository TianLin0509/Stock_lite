"""📈 回测战绩 — AI 荐股历史回测"""

import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

st.set_page_config(page_title="📈 回测战绩", page_icon="📈", layout="wide")

# 登录检查（支持 localStorage 自动恢复）
from ui.auth import require_login
require_login()

# 注入CSS + 侧边栏
from ui.styles import inject_css
from ui.sidebar import render_sidebar
inject_css()
render_sidebar(
    st.session_state["current_user"],
    lambda: (st.session_state.clear(), st.rerun()),
)

# 渲染回测页面（不需要 AI 客户端）
from ui.tabs.backtest import render_backtest_tab
render_backtest_tab()
