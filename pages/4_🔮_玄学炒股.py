"""🔮 玄学炒股 — 今日运势占卜"""

import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

st.set_page_config(page_title="🔮 玄学炒股", page_icon="🔮", layout="wide")

# 登录检查（支持 localStorage 自动恢复）
from ui.auth import require_login
require_login()

# 注入CSS + 侧边栏
from ui.styles import inject_css
from ui.sidebar import render_sidebar
inject_css()
selected_model = render_sidebar(
    st.session_state["current_user"],
    lambda: (st.session_state.clear(), st.rerun()),
)

# 获取 AI 客户端
from ai.client import get_ai_client
client, cfg, _ = get_ai_client(selected_model) if selected_model else (None, None, None)

# 渲染玄学页面
from ui.tabs.mystic import render_mystic_tab
render_mystic_tab(client, cfg, selected_model)
