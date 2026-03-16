#!/usr/bin/env python3
"""
📈 Stock Lite v1.11 — 轻量投研助手
一键综合投研报告：全量数据 + 五维评分 + 执行摘要
"""

import logging
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

# 配置日志（DEBUG → stderr + 文件按日轮转）
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
try:
    from logging.handlers import TimedRotatingFileHandler
    _log_dir = Path(__file__).parent / "logs"
    _log_dir.mkdir(exist_ok=True)
    _file_handler = TimedRotatingFileHandler(
        _log_dir / "app.log", when="midnight", backupCount=7, encoding="utf-8",
    )
    _file_handler.setLevel(logging.INFO)
    _file_handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(name)s] %(levelname)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S",
    ))
    logging.getLogger().addHandler(_file_handler)
except Exception:
    pass

import streamlit as st

# ── Page Config ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Stock Lite v1.11 🌸",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="auto",
)
st.markdown(
    '<meta name="viewport" content="width=device-width, initial-scale=1.0, '
    'maximum-scale=1.0, user-scalable=no">',
    unsafe_allow_html=True,
)

# ── 内部模块导入 ──────────────────────────────────────────────────────────
from config import (
    MODEL_CONFIGS, MODEL_NAMES,
    CORE_KEYS, DEEP_KEYS, ALL_ANALYSIS_KEYS,
)
from ui.styles import inject_css
from data.tushare_client import (
    ts_ok, get_ts_error, get_data_source, resolve_stock, to_code6,
    get_basic_info, get_price_df, get_financial, get_valuation_history,
)
from ai.client import get_ai_client, get_token_usage
from ui.sidebar import render_sidebar
from ui.tabs.analysis import render_analysis_tab

inject_css()

logger = logging.getLogger(__name__)


from ui.auth import inject_auto_login, save_login, clear_login


def _show_login():
    """显示登录页面"""
    import re

    # 尝试从 localStorage 自动恢复登录
    inject_auto_login()

    st.markdown("""
<div class="app-header">
  <h1>📈 Stock Lite v1.11</h1>
  <p>一键综合投研报告 · 五维评分 · 执行摘要 — 轻量版</p>
</div>
""", unsafe_allow_html=True)

    username = st.text_input(
        "用户名", placeholder="例如：呆瓜方、章鱼哥...",
        key="_login_username", label_visibility="collapsed",
    )
    if st.button("🚀 登录", type="primary", use_container_width=True):
        name = username.strip()
        if not name or len(name) < 1 or len(name) > 10:
            st.warning("用户名长度 1-10 个字符")
            return
        if not re.match(r'^[\w\u4e00-\u9fff]+$', name):
            st.warning("仅支持字母、数字、下划线或中文")
            return
        from utils.user_store import load_user, save_user
        user_data = load_user(name)
        from datetime import datetime
        user_data["last_login"] = datetime.now().isoformat(timespec="seconds")
        save_user(user_data)
        st.session_state["current_user"] = name
        st.session_state["_user_base_tokens"] = user_data["token_usage"]["total"]
        st.query_params["u"] = name
        save_login(name)
        st.rerun()
    st.caption("无需注册，输入用户名即可使用。数据将按用户名保存。")


def _save_analysis_to_history():
    """保存当前分析到用户历史 + 完整归档"""
    username = st.session_state.get("current_user", "")
    stock_name = st.session_state.get("stock_name", "")
    stock_code = st.session_state.get("stock_code", "")
    if not username or not stock_name:
        return

    analyses = st.session_state.get("analyses", {})
    done_keys = [k for k in ["comprehensive", "expectation", "trend", "fundamentals",
                              "sentiment", "sector", "holders"] if analyses.get(k)]
    if not done_keys:
        return

    try:
        from utils.archive import save_archive
        save_archive(st.session_state)
        st.session_state["_archive_gen"] = st.session_state.get("_archive_gen", 0) + 1
    except Exception as e:
        logger.debug("[_auto_save] 归档失败: %s", e)

    parts = []
    label_map = {"comprehensive": "综合报告", "expectation": "预期差", "trend": "趋势",
                 "fundamentals": "基本面", "sentiment": "舆情", "sector": "板块", "holders": "股东"}
    for k in done_keys:
        text = analyses[k][:80].replace("\n", " ").strip()
        parts.append(f"{label_map.get(k, k)}: {text}")
    summary = " | ".join(parts)[:300]

    session_tokens = get_token_usage()["total"]

    from utils.user_store import add_history_entry
    add_history_entry(
        username=username,
        stock_code=stock_code,
        stock_name=stock_name,
        model=st.session_state.get("selected_model", ""),
        analyses_done=done_keys,
        token_cost=session_tokens,
        summary=summary,
    )
    st.session_state.pop("_cached_user_data", None)


def _build_stock_options() -> list[str]:
    """构建股票搜索候选列表，优先本地 CSV（毫秒级），避免阻塞首屏"""
    try:
        from data.tushare_client import load_stock_list
        _sl_df, _ = load_stock_list()
        if not _sl_df.empty:
            if "symbol" in _sl_df.columns:
                _codes = _sl_df["symbol"].astype(str).str.zfill(6)
            else:
                _codes = _sl_df["ts_code"].astype(str).str.split(".").str[0].str.zfill(6)
            _names = _sl_df.get("name", "").astype(str)
            _opts = sorted((_codes + " " + _names).tolist())
            st.session_state["_stock_options"] = _opts
            st.session_state["_stock_opts_ver"] = 2
            return _opts
    except Exception:
        pass
    st.session_state["_stock_options"] = []
    st.session_state["_stock_opts_ver"] = 2
    return []


def main():
    # ── 登录门（支持刷新保持登录） ────────────────────────────────────
    if "current_user" not in st.session_state:
        _saved_user = st.query_params.get("u", "")
        if _saved_user:
            from utils.user_store import load_user
            user_data = load_user(_saved_user)
            st.session_state["current_user"] = _saved_user
            st.session_state["_user_base_tokens"] = user_data["token_usage"]["total"]
            save_login(_saved_user)
        else:
            _show_login()
            return

    current_user = st.session_state["current_user"]

    # ── 启动时从 GitHub 拉取云端归档（仅一次） ──────────────────────────
    if "_cloud_synced" not in st.session_state:
        try:
            from utils.cloud_archive import sync_on_startup
            sync_on_startup()
        except Exception as e:
            logger.debug("[main] 云端同步失败: %s", e)
        st.session_state["_cloud_synced"] = True

    # ── 启动时清理过期归档（>30天，后台执行不阻塞UI） ─────────────────
    if "_archive_cleaned" not in st.session_state:
        import threading as _th
        def _do_cleanup():
            try:
                from utils.archive import cleanup_expired
                _removed = cleanup_expired(30)
                if _removed:
                    logger.info("[main] 已清理 %d 个过期归档文件", _removed)
            except Exception as e:
                logger.debug("[main] 归档清理失败: %s", e)
        _th.Thread(target=_do_cleanup, daemon=True).start()
        st.session_state["_archive_cleaned"] = True

    # ── 上方区域折叠控制 ──────────────────────────────────────────────────
    _upper_collapsed = st.session_state.get("_upper_collapsed", False)

    # ── Header ────────────────────────────────────────────────────────────
    if not _upper_collapsed:
        st.markdown("""
<div class="app-header">
  <h1>📈 Stock Lite v1.11</h1>
  <p>一键综合投研报告 · 五维评分 · 执行摘要 — 轻量版</p>
</div>
""", unsafe_allow_html=True)

    # ── Token 用量显示（右上角，含用户名） ─────────────────────────────
    usage = get_token_usage()
    session_tokens = usage["total"]
    user_base = st.session_state.get("_user_base_tokens", 0)
    total = user_base + session_tokens
    if total > 0:
        if total >= 10000:
            display = f"{total / 10000:.1f}万"
        else:
            display = f"{total:,}"
        st.markdown(
            f'<div class="token-badge">👤 {current_user} &nbsp;|&nbsp; 🪙 {display}</div>',
            unsafe_allow_html=True,
        )

    # ── Sidebar ───────────────────────────────────────────────────────────
    def _on_logout():
        _save_analysis_to_history()
        clear_login()
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.query_params.clear()
        st.rerun()

    selected_model = render_sidebar(current_user, _on_logout)

    # ── 数据源提示 ────────────────────────────────────────────────────────
    if get_ts_error():
        st.markdown("""<div class="status-banner warn">
  ⚠️ <strong>Tushare 不可用</strong>，已自动切换备用数据源（akshare / 东方财富）。部分数据（龙虎榜）可能缺失。
</div>""", unsafe_allow_html=True)

    # ── 折叠时：展开按钮 + 简化状态 ──────────────────────────────────────
    if _upper_collapsed:
        query = st.session_state.get("_last_query", "")
        _auto_search = False
        _go_clicked = False
        _sn = st.session_state.get("stock_name", "")
        _sc = st.session_state.get("stock_code", "")
        _expand_label = f"▽ {_sn}（{_sc}）" if _sn else "▽ 展开搜索区域"
        if st.button(_expand_label, key="btn_expand_upper", use_container_width=True):
            if "_last_query" in st.session_state:
                st.session_state["query_input"] = st.session_state["_last_query"]
            st.session_state["_upper_collapsed"] = False
            st.rerun()

    if not _upper_collapsed:
        # ══════════════════════════════════════════════════════════════════════
        # 搜索栏 + 开始分析 + 重置（同一行）
        # ══════════════════════════════════════════════════════════════════════
        _core_all_have = bool(
            st.session_state.get("analyses", {}).get("comprehensive")
        ) if st.session_state.get("stock_name") else False

        if _core_all_have:
            _go_label = "✅ 分析完成"
            _go_disabled = True
        else:
            _go_label = "🚀 一键分析"
            _go_disabled = False

        # 构建股票搜索候选列表（带缓存，不阻塞首屏）
        if "_stock_options" not in st.session_state or st.session_state.get("_stock_opts_ver") != 2:
            _stock_options = _build_stock_options()
        else:
            _stock_options = st.session_state["_stock_options"]

        # 搜索框（独占一行）
        if _stock_options:
            _default_idx = None
            _prev_q = st.session_state.get("query_input", "")
            if _prev_q and _prev_q in _stock_options:
                _default_idx = _stock_options.index(_prev_q)
            query = st.selectbox(
                "搜索股票", options=_stock_options,
                index=_default_idx, label_visibility="collapsed",
                placeholder="🔍 输入股票代码或名称搜索…",
                key="query_input",
            )
        else:
            query = st.text_input(
                "搜索股票", label_visibility="collapsed",
                placeholder="🔍 股票代码或名称…",
                key="query_input",
            )

        # 按钮行（两列平分）
        _go_col, _reset_col = st.columns(2)
        with _go_col:
            _go_clicked = st.button(_go_label, type="primary",
                                     use_container_width=True, key="btn_go",
                                     disabled=_go_disabled)
        with _reset_col:
            _reset_clicked = st.button("🔄 重置", type="secondary",
                                        use_container_width=True, key="btn_reset")

        # 重置：清除分析状态，保留登录
        if _reset_clicked:
            _save_analysis_to_history()
            _keep = {"current_user", "_user_base_tokens", "selected_model"}
            for k in list(st.session_state.keys()):
                if k not in _keep:
                    del st.session_state[k]
            st.rerun()

    # ══════════════════════════════════════════════════════════════════════
    # 股票解析 + 最少数据获取
    # ══════════════════════════════════════════════════════════════════════
    def _resolve_and_fetch(q: str):
        """解析股票 + 获取最少通用数据（info/K线/财务/估值），立即返回以启动分析"""
        q = q.strip()
        if " " in q:
            q = q.split()[0]
        _save_analysis_to_history()
        for k in ["analyses", "stock_fin",
                   "valuation_df",
                   "similarity_results", "_show_sim",
                   "active_view", "_auto_sim", "_jobs",
                   "_analyses_saved_keys", "_last_archive", "_last_archive_file",
                   "_shared_from", "_archive_lookup",
                   "_history_saved_this_stock",
                   "report_summary", "report_scores",
                   "blue_team_report", "final_verdict", "final_scores",
                   "_pending_blue_team", "_pending_verdict"]:
            st.session_state.pop(k, None)
        for k in list(st.session_state.keys()):
            if k.startswith("_confirm_redo_"):
                del st.session_state[k]
        st.session_state["analyses"] = {}
        for k in list(st.session_state.keys()):
            if k.startswith("_fig_kline_") or k.startswith("_fig_val_"):
                del st.session_state[k]

        with st.spinner("🔍 解析股票中..."):
            ts_code, name, resolve_warn = resolve_stock(q)
        if resolve_warn:
            st.markdown(f'<div class="status-banner warn">⚠️ {resolve_warn}</div>',
                        unsafe_allow_html=True)

        st.session_state["stock_code"] = ts_code
        st.session_state["stock_name"] = name
        data_errors = []

        with st.spinner(f"📥 正在获取 {name} 的核心数据..."):
            from concurrent.futures import ThreadPoolExecutor, as_completed
            _fetch_map = {
                "info": lambda: get_basic_info(ts_code),
                "price": lambda: get_price_df(ts_code),
                "fin": lambda: get_financial(ts_code),
                "val": lambda: get_valuation_history(ts_code),
            }
            _fetch_results = {}
            with ThreadPoolExecutor(max_workers=4) as _pool:
                _futs = {_pool.submit(fn): key for key, fn in _fetch_map.items()}
                for fut in as_completed(_futs):
                    _fetch_results[_futs[fut]] = fut.result()

            info, e = _fetch_results["info"]
            if e: data_errors.append(e)
            st.session_state["stock_info"] = info

            _cur_name = st.session_state.get("stock_name", "")
            if _cur_name and _cur_name.replace(".", "").isdigit():
                _real_name = info.get("名称", "") or info.get("name", "")
                if _real_name:
                    st.session_state["stock_name"] = _real_name

            df, e = _fetch_results["price"]
            if e: data_errors.append(e)
            st.session_state["price_df"] = df

            fin, e = _fetch_results["fin"]
            if e: data_errors.append(e)
            st.session_state["stock_fin"] = fin

            val_df, e = _fetch_results["val"]
            if e: data_errors.append(e)
            st.session_state["valuation_df"] = val_df

        if data_errors:
            st.markdown(f"""<div class="status-banner warn">
  ⚠️ <strong>部分数据获取受限</strong>：{' | '.join(data_errors[:3])}
</div>""", unsafe_allow_html=True)

        if not df.empty and len(df) < 20:
            st.warning(f"⚠️ 仅获取到 {len(df)} 天交易数据（建议至少 20 天），分析结果可能不准确。")

        # ── 智能归档恢复：先加载缓存，已有的 key 不再花 token ──
        from utils.archive import find_recent, load_archive
        _recent = find_recent(ts_code)
        if _recent:
            _recent_data = load_archive(_recent["file"])
            if _recent_data and _recent_data.get("analyses"):
                restored = _recent_data["analyses"]
                st.session_state["analyses"] = restored
                if _recent_data.get("report_summary"):
                    st.session_state["report_summary"] = _recent_data["report_summary"]
                _ts_short = _recent.get("ts", "")[11:16]
                _from_user = _recent.get("username", "")
                st.session_state["_shared_from"] = (
                    f"{_from_user} · {_recent.get('model', '')} · {_ts_short}"
                )
                st.session_state["_archive_restored"] = (
                    f"已从归档恢复 {len(restored)} 项分析"
                    f"（{_from_user} · {_recent.get('model', '')} · {_ts_short}）"
                )
                logger.debug("[resolve] 从归档恢复 %d 项分析: %s",
                             len(restored), list(restored.keys()))

    # ── 处理从 analysis tab 来的待解析请求 ──
    _pending_resolve = st.session_state.pop("_pending_resolve", None)
    _pending_key = st.session_state.pop("_pending_analysis_key", None)
    if _pending_resolve:
        _resolve_and_fetch(_pending_resolve)
        st.session_state["_last_query"] = _pending_resolve
        client, cfg_now, _ = get_ai_client(selected_model)
        st.session_state["active_view"] = _pending_key or "overview"
        st.rerun()

    # ══════════════════════════════════════════════════════════════════════
    # 获取 AI 客户端
    # ══════════════════════════════════════════════════════════════════════
    stock_ready = bool(st.session_state.get("stock_name"))
    analyses = st.session_state.get("analyses", {})

    # 归档恢复醒目提示
    _archive_msg = st.session_state.pop("_archive_restored", None)
    if _archive_msg:
        st.success(f"✅ {_archive_msg}")
    client, cfg_now, ai_err = get_ai_client(selected_model)

    # 搜索栏"一键分析"按钮触发逻辑
    if _go_clicked:
        if not query:
            st.toast("请先输入股票代码或名称")
        else:
            query = query.split()[0] if query and " " in query else query
            _last_q = st.session_state.get("_last_query", "")
            _need_fetch = not stock_ready or query != _last_q
            if _need_fetch:
                _resolve_and_fetch(query)
                st.session_state["_last_query"] = query
                stock_ready = True
                analyses = st.session_state.get("analyses", {})
            if client:
                if not analyses.get("comprehensive"):
                    st.session_state["_pending_comprehensive"] = True

            st.session_state["active_view"] = "overview"
            st.session_state["_upper_collapsed"] = True
            st.rerun()

    # ══════════════════════════════════════════════════════════════════════
    # 直接渲染分析页面（无 Tab）
    # ══════════════════════════════════════════════════════════════════════
    render_analysis_tab(client, cfg_now, selected_model)

    # ══════════════════════════════════════════════════════════════════════
    # 增量归档
    # ══════════════════════════════════════════════════════════════════════
    if stock_ready:
        _analyses_saved = st.session_state.get("_analyses_saved_keys", set())
        _analyses_now = set(k for k, v in analyses.items() if v and len(v) > 100)
        if _analyses_now - _analyses_saved:
            try:
                from utils.archive import save_archive
                _saved_file = save_archive(st.session_state)
                st.session_state["_analyses_saved_keys"] = _analyses_now.copy()
                st.session_state["_archive_gen"] = st.session_state.get("_archive_gen", 0) + 1
                # 异步推送到 GitHub
                if _saved_file:
                    from utils.cloud_archive import push_file_async
                    push_file_async(_saved_file)
            except Exception as e:
                logger.debug("[archive] 归档失败: %s", e)

            # 同步写入用户历史（分析完成或缓存加载后立即记录）
            if not st.session_state.get("_history_saved_this_stock"):
                _save_analysis_to_history()
                st.session_state["_history_saved_this_stock"] = True


if __name__ == "__main__":
    main()
