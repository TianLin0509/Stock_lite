"""登录持久化 — 基于浏览器 localStorage 的免登录机制"""

import streamlit as st
import streamlit.components.v1 as components


def inject_auto_login():
    """注入 JS：从 localStorage 读取用户名，自动补 ?u=xxx 实现免登录"""
    components.html("""
    <script>
    (function() {
        const params = new URLSearchParams(window.parent.location.search);
        const saved = localStorage.getItem('stock_lite_user');
        if (saved && !params.get('u')) {
            params.set('u', saved);
            window.parent.location.search = params.toString();
        }
    })();
    </script>
    """, height=0)


def save_login(username: str):
    """将用户名写入浏览器 localStorage"""
    safe_name = username.replace("\\", "\\\\").replace("'", "\\'").replace('"', '\\"')
    components.html(f"""
    <script>
    localStorage.setItem('stock_lite_user', '{safe_name}');
    </script>
    """, height=0)


def clear_login():
    """清除浏览器 localStorage 中的登录记录"""
    components.html("""
    <script>
    localStorage.removeItem('stock_lite_user');
    </script>
    """, height=0)


def require_login():
    """子页面统一登录检查：先尝试 query_params 自动登录，再尝试 localStorage

    Returns True if logged in, calls st.stop() if not.
    """
    if "current_user" in st.session_state:
        return True

    # 尝试从 URL query params 恢复（主页登录后或 localStorage JS 跳转后）
    saved_user = st.query_params.get("u", "")
    if saved_user:
        from utils.user_store import load_user
        user_data = load_user(saved_user)
        st.session_state["current_user"] = saved_user
        st.session_state["_user_base_tokens"] = user_data["token_usage"]["total"]
        save_login(saved_user)
        return True

    # 无 query params → 注入 JS 尝试从 localStorage 恢复
    inject_auto_login()
    st.warning("请先在主页登录")
    st.stop()
    return False
