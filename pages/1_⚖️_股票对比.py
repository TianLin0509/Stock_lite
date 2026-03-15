"""⚖️ 股票对比 — 双股同屏对比分析"""

import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# 登录检查
if "current_user" not in st.session_state:
    st.warning("请先在主页登录")
    st.stop()

# 注入CSS
from ui.styles import inject_css
inject_css()

# 获取 AI 客户端
from ai.client import get_ai_client
selected_model = st.session_state.get("selected_model", "")
client, cfg, _ = get_ai_client(selected_model) if selected_model else (None, None, None)

# 渲染对比页面
from ui.tabs.compare import render_compare_tab
render_compare_tab(client, cfg, selected_model)
