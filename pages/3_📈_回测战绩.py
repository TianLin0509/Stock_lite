"""📈 回测战绩 — AI 荐股历史回测"""

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

# 渲染回测页面
from ui.tabs.backtest import render_backtest_tab
render_backtest_tab()
