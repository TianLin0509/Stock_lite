"""Tab 1: 📊 智能分析 — 从 streamlit_app.py 提取"""

import streamlit as st
import pandas as pd

from config import CORE_KEYS, DEEP_KEYS, ALL_ANALYSIS_KEYS
from analysis.runner import run_analysis_sync
from ui.results import (
    _show_analysis_result,
    _show_similarity_section, render_radar_section,
)
from ui.charts import render_kline, render_valuation_bands
from data.tushare_client import to_code6


def _store_extra_data(extra: dict | None):
    """将趋势分析附带的资金流数据存入 session_state，供信号雷达使用"""
    if not extra:
        return
    cap = extra.get("capital_flow")
    if cap is not None:
        # cap 可能是 DataFrame 或字符串
        import pandas as _pd
        if isinstance(cap, _pd.DataFrame) and not cap.empty:
            st.session_state["capital_flow_df"] = cap
        elif isinstance(cap, str) and len(cap) > 20:
            st.session_state["stock_capital"] = cap
    nb = extra.get("northbound")
    if nb and isinstance(nb, str) and "暂无" not in nb:
        st.session_state["stock_northbound"] = nb
    margin = extra.get("margin")
    if margin and isinstance(margin, str) and "暂无" not in margin:
        st.session_state["stock_margin"] = margin


def _extract_conclusion(text: str, max_chars: int = 800) -> str:
    """从分析全文中提取结论/总结段落，保留 markdown 格式"""
    import re
    # 按优先级尝试匹配结论段落标题
    patterns = [
        r'(#{1,4}\s*.*(?:综合结论|最终结论|总结|操作建议|投资建议).*)',
        r'(#{1,4}\s*.*(?:三情景|概率估计|风险提示).*)',
        r'(#{1,4}\s*.*(?:中线展望|短线展望|趋势研判).*)',
        r'(#{1,4}\s*.*(?:基本面裁决|筛选结论|综合评分).*)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            # 从匹配位置截取到文末
            start = match.start()
            conclusion = text[start:start + max_chars]
            if len(text) > start + max_chars:
                conclusion += "\n\n..."
            return conclusion

    # 兜底：取最后 max_chars 字符（通常结论在末尾）
    if len(text) > max_chars:
        # 找一个段落分界
        tail = text[-max_chars:]
        newline_pos = tail.find('\n\n')
        if newline_pos > 0 and newline_pos < max_chars // 2:
            tail = tail[newline_pos + 2:]
        return "...\n\n" + tail
    return text


def _fmt_val(v) -> str:
    """格式化指标值：None/N/A/空 → —，数字保留合理精度"""
    if v is None or str(v).strip().lower() in ("none", "n/a", "nan", ""):
        return "—"
    try:
        f = float(v)
        return f"{f:.2f}" if abs(f) < 1000 else f"{f:.1f}"
    except (ValueError, TypeError):
        return str(v)[:14]


def _show_stock_overview_basic():
    """显示股票概览：紧凑一行式指标"""
    name = st.session_state["stock_name"]
    ts_code = st.session_state["stock_code"]
    info = st.session_state.get("stock_info", {})
    code6 = to_code6(ts_code)

    st.markdown(f"### {name} &nbsp; `{code6}`")

    # 紧凑指标条
    _price = _fmt_val(info.get("最新价(元)"))
    _pe = _fmt_val(info.get("市盈率TTM"))
    _pb = _fmt_val(info.get("市净率PB"))
    _turnover = _fmt_val(info.get("换手率(%)"))
    _industry = info.get("行业", "—") or "—"

    st.markdown(
        f'<div style="display:flex;flex-wrap:wrap;gap:6px 16px;font-size:0.85rem;'
        f'color:#374151;margin:2px 0 8px 0;">'
        f'<span><b>¥{_price}</b></span>'
        f'<span>PE <b>{_pe}</b></span>'
        f'<span>PB <b>{_pb}</b></span>'
        f'<span>换手 <b>{_turnover}%</b></span>'
        f'<span>{_industry}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )


def _extract_session_data():
    """从 session_state 提取分析所需数据（主线程调用）"""
    name = st.session_state.get("stock_name", "")
    tscode = st.session_state.get("stock_code", "")
    info = dict(st.session_state.get("stock_info", {}))
    fin = st.session_state.get("stock_fin", "")
    df = st.session_state.get("price_df", pd.DataFrame())
    if not df.empty:
        df = df.copy()
    username = st.session_state.get("current_user", "")
    return name, tscode, info, fin, df, username


_HEARTBEAT_TIPS = [
    "正在联网搜索最新资讯…",
    "正在整理分析数据…",
    "正在等待 AI 响应…",
    "AI 正在深度思考中…",
    "即将开始输出结果…",
    "AI 还在思考，请耐心等待…",
    "正在综合多维度数据…",
    "分析内容较多，稍等片刻…",
]


def _start_bg_analysis(keys, client, cfg_now, selected_model, label_map):
    """在后台线程中启动并行分析，结果写入 session_state。
    主线程通过轮询 _bg_analysis 状态来刷新 UI。
    """
    import threading
    from concurrent.futures import ThreadPoolExecutor, as_completed

    name, tscode, info, fin, df, username = _extract_session_data()
    analyses = st.session_state.get("analyses", {})

    # 后台状态对象（挂在 session_state 上，主线程可读）
    import time as _t
    bg = {
        "keys": list(keys),
        "label_map": dict(label_map),
        "total": len(keys),
        "done_keys": [],       # 已完成的 key
        "errors": {},          # key → error msg
        "finished": False,     # 全部完成标志
        "started_at": _t.time(),
    }
    st.session_state["_bg_analysis"] = bg

    def _worker():
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = {
                pool.submit(
                    run_analysis_sync, k, client, cfg_now, selected_model,
                    name, tscode, info, fin, df, username
                ): k for k in keys
            }
            for fut in as_completed(futures):
                k = futures[fut]
                result, err, extra = fut.result()
                if not err and result:
                    analyses[k] = result
                    st.session_state["analyses"] = analyses
                    if extra:
                        # 资金数据存入 session_state（线程安全：Streamlit dict 是普通 dict）
                        cap = extra.get("capital_flow")
                        if cap is not None:
                            import pandas as _pd
                            if isinstance(cap, _pd.DataFrame) and not cap.empty:
                                st.session_state["capital_flow_df"] = cap
                            elif isinstance(cap, str) and len(cap) > 20:
                                st.session_state["stock_capital"] = cap
                        nb = extra.get("northbound")
                        if nb and isinstance(nb, str) and "暂无" not in nb:
                            st.session_state["stock_northbound"] = nb
                        margin = extra.get("margin")
                        if margin and isinstance(margin, str) and "暂无" not in margin:
                            st.session_state["stock_margin"] = margin
                    bg["done_keys"].append(k)
                else:
                    bg["errors"][k] = err or "未知错误"
                    bg["done_keys"].append(k)
        bg["finished"] = True

    t = threading.Thread(target=_worker, daemon=True)
    t.start()


def _poll_bg_analysis():
    """检查后台分析状态，渲染进度条，完成时清理。
    返回 True 如果仍在运行（需要继续轮询）。
    """
    import time
    bg = st.session_state.get("_bg_analysis")
    if not bg:
        return False

    label_map = bg["label_map"]
    total = bg["total"]
    done_keys = bg["done_keys"]
    errors = bg["errors"]
    done_count = len(done_keys)

    if bg["finished"]:
        # 全部完成 → 清理状态，按钮已自动显示 ✅，无需额外提示
        st.session_state.pop("_bg_analysis", None)
        st.session_state.pop("_bg_tip_idx", None)
        # 有错误时简短提示
        if errors:
            for k, err in errors.items():
                st.toast(f"❌ {label_map.get(k, k)} 失败：{err}")
        # 深度分析完成时设置 auto_sim
        if any(k in ("sentiment", "sector", "holders") for k in bg["keys"]):
            st.session_state["_auto_sim"] = True
        return False

    # 仍在运行 → 显示进度 + 心跳
    import time as _t
    _elapsed = int(_t.time() - bg.get("started_at", _t.time()))
    _remaining = [label_map.get(k, k) for k in bg["keys"] if k not in done_keys]
    with st.status(f"⏳ 分析中（{done_count}/{total}）— 已等待 {_elapsed}s", expanded=True, state="running") as status:
        for k in done_keys:
            _lbl = label_map.get(k, k)
            if k in errors:
                st.write(f"❌ {_lbl} 失败")
            else:
                st.write(f"✅ **{_lbl}** 完成")
        if _remaining:
            tip_idx = st.session_state.get("_bg_tip_idx", 0)
            tip = _HEARTBEAT_TIPS[min(tip_idx, len(_HEARTBEAT_TIPS) - 1)]
            st.write(f"⏱️ 等待中 — {tip}（剩余：{'、'.join(_remaining)}）")
            st.session_state["_bg_tip_idx"] = tip_idx + 1

    time.sleep(4)
    st.rerun()
    return True  # 不会到达这里，rerun 会中断


def _run_single_analysis(key, label, client, cfg_now, selected_model, analyses):
    """启动单项后台分析"""
    _start_bg_analysis([key], client, cfg_now, selected_model, {key: label})


def _run_deep_analysis(client, cfg_now, selected_model, analyses):
    """启动深度分析（舆情+板块+股东）"""
    keys_to_run = [dk for dk in DEEP_KEYS if not analyses.get(dk)]
    if not keys_to_run:
        return
    label_map = {"sentiment": "舆情情绪", "sector": "板块联动", "holders": "股东动向"}
    _start_bg_analysis(keys_to_run, client, cfg_now, selected_model, label_map)


def render_analysis_tab(client, cfg_now, selected_model):
    """渲染智能分析页面的全部内容"""
    stock_ready = bool(st.session_state.get("stock_name"))
    analyses = st.session_state.get("analyses", {})
    current_user = st.session_state.get("current_user", "")

    # 是否有待执行的分析 / 正在后台运行的分析
    _pending_core = st.session_state.get("_pending_core_analysis", False)
    _pending_single = st.session_state.pop("_pending_single_key", None)
    _bg_running = st.session_state.get("_bg_analysis") is not None
    _is_analyzing = _pending_core or _pending_single is not None or _bg_running

    # ── active_view 初始化 ────────────────────────────────────
    if "active_view" not in st.session_state:
        st.session_state["active_view"] = "overview"
    active_view = st.session_state["active_view"]

    # ── 操作栏：4 按钮（预期差 / 趋势 / 基本面 / 总结）──
    _action_cols = st.columns(4)

    core_all_done = stock_ready and all(analyses.get(k) for k in CORE_KEYS)
    deep_all_done = all(analyses.get(k) for k in DEEP_KEYS)

    # Col 0-2: 核心三项按钮
    _view_map = [
        (0, "expectation", "预期差", "🔍"),
        (1, "trend", "趋势", "📈"),
        (2, "fundamentals", "基本面", "📋"),
    ]
    for col_idx, key, label, icon in _view_map:
        with _action_cols[col_idx]:
            done = bool(analyses.get(key))
            _btn_type = "primary" if active_view == key else "secondary"

            if _is_analyzing and not done:
                _btn_label = f"⏳ {label}"
            elif done:
                _btn_label = f"✅ {label}"
            else:
                _btn_label = f"{icon} {label}"

            query = st.session_state.get("query_input", "")
            # 分析进行中：已完成的按钮可点击查看，未完成的禁用
            _disabled = (_is_analyzing and not done) or (not stock_ready and not query)
            if st.button(_btn_label, type=_btn_type,
                         use_container_width=True, key=f"btn_{key}",
                         disabled=_disabled):
                st.session_state["active_view"] = key
                if not done:
                    if not stock_ready and query:
                        st.session_state["_pending_resolve"] = query
                        st.session_state["_pending_analysis_key"] = key
                        st.rerun()
                    if client and stock_ready:
                        # 标记待执行，下次 rerun 时在下方执行
                        st.session_state["_pending_single_key"] = key
                        st.rerun()
                else:
                    st.rerun()

    # Col 3: 总结按钮（核心三项完成后可用）
    with _action_cols[3]:
        _summary_type = "primary" if active_view == "summary" else "secondary"
        if core_all_done:
            _summary_label = "✅ 总结"
        else:
            _summary_label = "📊 总结"
        if st.button(_summary_label, type=_summary_type,
                     use_container_width=True, key="btn_summary",
                     disabled=not core_all_done):
            st.session_state["active_view"] = "summary"
            st.rerun()

    # 深度分析按钮：仅在核心三项完成后显示（独立行）
    if core_all_done or deep_all_done:
        if deep_all_done:
            st.button("✅ 舆情+板块+股东 深度分析已完成", disabled=True,
                      use_container_width=True, key="btn_deep")
        else:
            if st.button("🔬 开始深度分析（舆情+板块+股东）", use_container_width=True,
                         key="btn_deep", type="primary",
                         disabled=_is_analyzing):
                if client:
                    _run_deep_analysis(client, cfg_now, selected_model, analyses)
                    st.rerun()

    # ── 启动待处理的分析（后台线程）──────────────────────────
    if _pending_core and client and stock_ready and not _bg_running:
        st.session_state.pop("_pending_core_analysis", None)
        st.session_state.pop("_bg_tip_idx", None)
        keys_to_run = [k for k in CORE_KEYS if not analyses.get(k)]
        if keys_to_run:
            label_map = {"expectation": "预期差", "trend": "趋势", "fundamentals": "基本面"}
            _start_bg_analysis(keys_to_run, client, cfg_now, selected_model, label_map)
            st.rerun()

    if _pending_single is not None and client and stock_ready and not _bg_running:
        _single_labels = {"expectation": "预期差", "trend": "趋势", "fundamentals": "基本面",
                          "sentiment": "舆情", "sector": "板块", "holders": "股东"}
        _lbl = _single_labels.get(_pending_single, _pending_single)
        if not analyses.get(_pending_single):
            _start_bg_analysis([_pending_single], client, cfg_now, selected_model,
                               {_pending_single: _lbl})
            st.rerun()

    # ── 轮询后台分析进度（会 sleep+rerun 直到完成）──────────
    if _bg_running:
        _poll_bg_analysis()  # 会 sleep(4) + st.rerun() 直到 finished

    # ── 状态栏：仅在缓存来源或分析中时显示 ──────────────────
    active_view = st.session_state.get("active_view", "overview")
    _shared_from = st.session_state.get("_shared_from")

    if _shared_from and stock_ready:
        st.markdown(
            f'<div style="font-size:0.75rem;color:#f59e0b;margin:4px 0;">'
            f'📦 缓存来源：{_shared_from}</div>',
            unsafe_allow_html=True,
        )
        if st.button("🔄 忽略缓存，重新分析", key="btn_redo_fresh",
                     type="primary", use_container_width=True):
            st.session_state.pop("_shared_from", None)
            st.session_state["analyses"] = {}
            st.session_state.pop("similarity_results", None)
            st.session_state.pop("_auto_sim", None)
            st.session_state.pop("_analyses_saved_keys", None)
            if client:
                _run_core_analysis_all(client, cfg_now, selected_model)
            st.session_state["active_view"] = "overview"
            st.rerun()

    st.markdown("---")

    # ── 主内容区：按 active_view 条件渲染 ────────────────────
    if not stock_ready:
        st.info("请在上方输入股票代码/名称，点击「开始分析」")
    else:
        if active_view == "overview":
            _render_overview(client, cfg_now, analyses, core_all_done, current_user)
        elif active_view == "expectation":
            _render_expectation(analyses)
        elif active_view == "trend":
            _render_trend(analyses)
        elif active_view == "fundamentals":
            _render_fundamentals(analyses)
        elif active_view == "summary":
            _render_summary(analyses)

    # 邮件推送已移至总结视图内


def _run_core_analysis_all(client, cfg_now, selected_model):
    """启动核心三项后台分析（用于重新分析按钮）"""
    label_map = {"expectation": "预期差", "trend": "趋势", "fundamentals": "基本面"}
    st.session_state.pop("_bg_tip_idx", None)
    _start_bg_analysis(list(CORE_KEYS), client, cfg_now, selected_model, label_map)


# ══════════════════════════════════════════════════════════════════════════════
# 各视图的渲染函数
# ══════════════════════════════════════════════════════════════════════════════

def _render_overview(client, cfg_now, analyses, core_all_done, current_user):
    """overview 视图（基本指标 + 归档恢复 + 雷达）"""
    _show_stock_overview_basic()
    st.markdown("---")

    # 归档缓存检查（无分析结果时自动恢复 / 加载他人结果）
    from utils.archive import find_recent, find_today_others, load_archive

    if not analyses:
        _stock_code = st.session_state["stock_code"]

        # 用 session_state 缓存归档查询结果（Phase 1.3）
        _archive_gen = st.session_state.get("_archive_gen", 0)
        _cache = st.session_state.get("_archive_lookup", {})
        if _cache.get("gen") != _archive_gen or _cache.get("code") != _stock_code:
            _recent = find_recent(_stock_code)
            _others = find_today_others(_stock_code, exclude_user=current_user)
            st.session_state["_archive_lookup"] = {
                "gen": _archive_gen, "code": _stock_code,
                "recent": _recent, "others": _others,
            }
        else:
            _recent = _cache.get("recent")
            _others = _cache.get("others", [])

        # 1) 自动恢复
        if _recent:
            _recent_data = load_archive(_recent["file"])
            if _recent_data and _recent_data.get("analyses"):
                st.session_state["analyses"] = _recent_data["analyses"]
                _ts_short = _recent.get("ts", "")[11:16]
                _from_user = _recent.get("username", "")
                st.session_state["_shared_from"] = (
                    f"{_from_user} · {_recent.get('model', '')} · {_ts_short}"
                )
                st.rerun()

        # 2) 今日其他用户的归档（手动加载）
        if _others:
            for sh in _others:
                _ts_short = sh.get("ts", "")[11:16]
                _lbl_map = {
                    "expectation": "预期差", "trend": "趋势解读",
                    "fundamentals": "基本面", "sentiment": "舆情",
                    "sector": "板块", "holders": "股东",
                }
                keys_str = "、".join(
                    _lbl_map.get(k, k) for k in sh.get("analyses_done", [])
                )
                moe_tag = ""
                st.info(
                    f"📦 **{sh['username']}** 于 {_ts_short} 已用 "
                    f"{sh.get('model', '')} 分析过此股票（{keys_str}{moe_tag}）"
                )
                if st.button(
                    f"📥 加载 {sh['username']} 的分析结果（免费）",
                    key=f"load_arch_{sh['username']}_{sh.get('model', '')}",
                ):
                    _arch_data = load_archive(sh["file"])
                    if _arch_data:
                        st.session_state["analyses"] = _arch_data.get("analyses", {})
                        st.session_state["_shared_from"] = (
                            f"{sh['username']} · {sh.get('model', '')} · {_ts_short}"
                        )
                        st.session_state["_archive_gen"] = st.session_state.get("_archive_gen", 0) + 1
                        st.rerun()
            st.markdown("---")

    from ai.client import get_ai_client
    _, _, ai_err = get_ai_client(st.session_state.get("selected_model", ""))
    if ai_err:
        st.markdown(f"""<div class="status-banner warn">
  ⚠️ <strong>AI 模型暂不可用</strong>：{ai_err}，请在左侧切换其他模型。
</div>""", unsafe_allow_html=True)

    # ── 分析完成时直接显示核心结论摘要 ──
    if core_all_done:
        render_radar_section()
        _summary_items = [
            ("expectation", "🔍 预期差"),
            ("trend", "📈 趋势研判"),
            ("fundamentals", "📋 基本面"),
        ]
        for key, title in _summary_items:
            text = analyses.get(key, "")
            if text:
                conclusion = _extract_conclusion(text, max_chars=600)
                with st.expander(title, expanded=False):
                    st.markdown(conclusion)


def _render_expectation(analyses):
    """expectation 视图"""
    _has_deep_exp = bool(analyses.get("sentiment"))
    if analyses.get("expectation"):
        name = st.session_state.get("stock_name", "")
        if _has_deep_exp:
            with st.expander(f"🔍 {name} · 预期差分析结果", expanded=False):
                st.markdown(analyses["expectation"])
        else:
            st.markdown(f"#### 🔍 {name} · 预期差分析结果")
            with st.container(border=True):
                st.markdown(analyses["expectation"])
    else:
        st.info("预期差分析尚未执行，点击上方按钮开始分析")
    # 深度舆情追加
    if analyses.get("sentiment"):
        st.markdown("---")
        name = st.session_state.get("stock_name", "")
        st.markdown(f"#### 📣 {name} · 舆情情绪分析（深度）")
        with st.container(border=True):
            st.markdown(analyses["sentiment"])


def _render_trend(analyses):
    """trend 视图（K线图 + 趋势解读 + K线匹配）"""
    _t_name = st.session_state.get("stock_name", "")
    _t_code = st.session_state.get("stock_code", "")
    _t_df = st.session_state.get("price_df", pd.DataFrame())
    _has_deep_trend = (st.session_state.get("_auto_sim")
                       or st.session_state.get("similarity_results"))

    if analyses.get("trend"):
        if _has_deep_trend:
            with st.expander(f"📈 {_t_name} · 趋势解读结果（含K线图）", expanded=False):
                if not _t_df.empty:
                    render_kline(_t_df, _t_name, _t_code)
                    st.markdown("---")
                st.markdown(analyses["trend"])
        else:
            if not _t_df.empty:
                render_kline(_t_df, _t_name, _t_code)
                st.markdown("---")
            st.markdown(f"#### 📈 {_t_name} · 趋势解读结果")
            with st.container(border=True):
                st.markdown(analyses["trend"])
    else:
        if not _t_df.empty:
            render_kline(_t_df, _t_name, _t_code)
            st.markdown("---")
        st.info("趋势解读尚未执行，点击上方按钮开始分析")

    if _has_deep_trend:
        st.markdown("---")
        _show_similarity_section(
            st.session_state.get("stock_name", ""),
            st.session_state.get("stock_code", ""),
        )


def _render_fundamentals(analyses):
    """fundamentals 视图（估值分位 + 基本面 + 板块 + 股东）"""
    _f_name = st.session_state.get("stock_name", "")
    _f_val_df = st.session_state.get("valuation_df", pd.DataFrame())
    _has_deep_fund = bool(analyses.get("sector") or analyses.get("holders"))

    if analyses.get("fundamentals"):
        if _has_deep_fund:
            with st.expander(f"📋 {_f_name} · 基本面分析结果（含估值分位）", expanded=False):
                if not _f_val_df.empty:
                    st.markdown(f"#### 📊 估值历史分位")
                    render_valuation_bands(_f_val_df, _f_name)
                    st.markdown("---")
                st.markdown(analyses["fundamentals"])
        else:
            if not _f_val_df.empty:
                st.markdown(f"#### 📊 {_f_name} · 估值历史分位")
                render_valuation_bands(_f_val_df, _f_name)
                st.markdown("---")
            st.markdown(f"#### 📋 {_f_name} · 基本面分析结果")
            with st.container(border=True):
                st.markdown(analyses["fundamentals"])
    else:
        if not _f_val_df.empty:
            st.markdown(f"#### 📊 {_f_name} · 估值历史分位")
            render_valuation_bands(_f_val_df, _f_name)
            st.markdown("---")
        st.info("基本面分析尚未执行，点击上方按钮开始分析")

    # 深度板块追加
    if analyses.get("sector"):
        st.markdown("---")
        st.markdown(f"#### 🏭 {_f_name} · 板块联动分析（深度）")
        with st.container(border=True):
            st.markdown(analyses["sector"])
    # 深度股东追加
    if analyses.get("holders"):
        st.markdown("---")
        st.markdown(f"#### 👥 {_f_name} · 股东/机构动向（深度）")
        with st.container(border=True):
            st.markdown(analyses["holders"])


def _render_summary(analyses):
    """总结视图：价值投机雷达 + 三项核心分析结论提炼"""
    name = st.session_state.get("stock_name", "")
    ts_code = st.session_state.get("stock_code", "")

    st.markdown(f"#### 📊 {name} · 投资总结")

    # ── 价值投机雷达 ──
    render_radar_section()

    st.markdown("---")

    # ── 三项核心结论摘要 ──
    st.markdown(f"#### 📝 核心分析要点")

    _summary_map = [
        ("expectation", "🔍 预期差", "预期差分析"),
        ("trend", "📈 趋势研判", "趋势解读"),
        ("fundamentals", "📋 基本面", "基本面分析"),
    ]

    for key, title, fallback_label in _summary_map:
        text = analyses.get(key, "")
        if not text:
            st.caption(f"{title}：尚未完成")
            continue

        # 提取结论段落：优先找"综合结论""总结""操作建议"等关键段
        conclusion = _extract_conclusion(text)
        with st.expander(title, expanded=True):
            st.markdown(conclusion)

