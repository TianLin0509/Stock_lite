"""Tab 1: 📊 智能分析 — 综合报告模式 + 向后兼容旧三模块"""

import streamlit as st
import pandas as pd

from ui.charts import render_valuation_bands
from data.tushare_client import to_code6


def _store_extra_data(extra: dict | None):
    """将趋势分析附带的资金流数据存入 session_state，供信号雷达使用"""
    if not extra:
        return
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


def _fmt_val(v) -> str:
    """格式化指标值：None/N/A/空 -> —，数字保留合理精度"""
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


def render_analysis_tab(client, cfg_now, selected_model):
    """渲染智能分析页面 — 综合报告模式"""
    stock_ready = bool(st.session_state.get("stock_name"))
    analyses = st.session_state.get("analyses", {})
    current_user = st.session_state.get("current_user", "")

    if not stock_ready:
        st.info("请在上方输入股票代码/名称，点击「一键分析」")
        return

    # ── 股票概览 ──────────────────────────────────────────────────
    _show_stock_overview_basic()
    st.markdown("---")

    # ── 归档缓存恢复逻辑 ─────────────────────────────────────────
    _shared_from = st.session_state.get("_shared_from")
    if _shared_from:
        st.markdown(
            f'<div style="font-size:0.75rem;color:#f59e0b;margin:4px 0;">'
            f'📦 缓存来源：{_shared_from}</div>',
            unsafe_allow_html=True,
        )
        if st.button("🔄 忽略缓存，重新分析", key="btn_redo_fresh",
                     type="primary", use_container_width=True):
            st.session_state.pop("_shared_from", None)
            st.session_state["analyses"] = {}
            st.session_state.pop("report_summary", None)
            st.session_state.pop("report_scores", None)
            st.session_state.pop("similarity_results", None)
            st.session_state.pop("_analyses_saved_keys", None)
            st.session_state.pop("blue_team_report", None)
            st.session_state.pop("final_verdict", None)
            st.session_state.pop("final_scores", None)
            st.session_state["_pending_comprehensive"] = True
            st.rerun()

    # ── 归档自动恢复（无分析结果时）──────────────────────────────
    if not analyses:
        from utils.archive import find_recent, find_today_others, load_archive
        _stock_code = st.session_state["stock_code"]
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

        if _recent:
            _recent_data = load_archive(_recent["file"])
            if _recent_data and _recent_data.get("analyses"):
                st.session_state["analyses"] = _recent_data["analyses"]
                if _recent_data.get("report_summary"):
                    st.session_state["report_summary"] = _recent_data["report_summary"]
                if _recent_data.get("blue_team_report"):
                    st.session_state["blue_team_report"] = _recent_data["blue_team_report"]
                if _recent_data.get("final_verdict"):
                    st.session_state["final_verdict"] = _recent_data["final_verdict"]
                if _recent_data.get("final_scores"):
                    st.session_state["final_scores"] = _recent_data["final_scores"]
                _ts_short = _recent.get("ts", "")[11:16]
                _from_user = _recent.get("username", "")
                st.session_state["_shared_from"] = (
                    f"{_from_user} · {_recent.get('model', '')} · {_ts_short}"
                )
                st.rerun()

        if _others:
            for sh in _others:
                _ts_short = sh.get("ts", "")[11:16]
                _lbl_map = {
                    "expectation": "预期差", "trend": "趋势",
                    "fundamentals": "基本面", "comprehensive": "综合报告",
                    "sentiment": "舆情", "sector": "板块", "holders": "股东",
                }
                keys_str = "、".join(
                    _lbl_map.get(k, k) for k in sh.get("analyses_done", [])
                )
                st.info(
                    f"📦 **{sh['username']}** 于 {_ts_short} 已用 "
                    f"{sh.get('model', '')} 分析过此股票（{keys_str}）"
                )
                if st.button(
                    f"📥 加载 {sh['username']} 的分析结果（免费）",
                    key=f"load_arch_{sh['username']}_{sh.get('model', '')}",
                ):
                    _arch_data = load_archive(sh["file"])
                    if _arch_data:
                        st.session_state["analyses"] = _arch_data.get("analyses", {})
                        if _arch_data.get("report_summary"):
                            st.session_state["report_summary"] = _arch_data["report_summary"]
                        if _arch_data.get("blue_team_report"):
                            st.session_state["blue_team_report"] = _arch_data["blue_team_report"]
                        if _arch_data.get("final_verdict"):
                            st.session_state["final_verdict"] = _arch_data["final_verdict"]
                        if _arch_data.get("final_scores"):
                            st.session_state["final_scores"] = _arch_data["final_scores"]
                        st.session_state["_shared_from"] = (
                            f"{sh['username']} · {sh.get('model', '')} · {_ts_short}"
                        )
                        st.session_state["_archive_gen"] = st.session_state.get("_archive_gen", 0) + 1
                        st.rerun()
            st.markdown("---")

    # AI 客户端检查
    from ai.client import get_ai_client
    _, _, ai_err = get_ai_client(st.session_state.get("selected_model", ""))
    if ai_err:
        st.markdown(f"""<div class="status-banner warn">
  ⚠️ <strong>AI 模型暂不可用</strong>：{ai_err}，请在左侧切换其他模型。
</div>""", unsafe_allow_html=True)

    # 重新读取 analyses（可能被归档恢复更新了）
    analyses = st.session_state.get("analyses", {})

    # ── 执行待处理的综合报告 ──────────────────────────────────────
    _pending = st.session_state.pop("_pending_comprehensive", False)
    if _pending and client and stock_ready:
        from ui.deep_report import run_deep_report
        username = st.session_state.get("current_user", "")
        run_deep_report(client, cfg_now, selected_model, username)
        st.rerun()

    # ── 显示综合报告结果 ──────────────────────────────────────────
    has_comprehensive = bool(analyses.get("comprehensive"))

    if has_comprehensive:
        # 显示执行摘要
        summary = st.session_state.get("report_summary", "")
        if summary and len(summary) > 20:
            with st.container(border=True):
                st.caption("📊 执行摘要")
                st.markdown(summary)

        # 四维雷达
        from ui.results import render_radar_section_5d
        render_radar_section_5d()

        # 红蓝军对决区域（Tab切换：红军报告 / 蓝军挑战 / 终审裁决）
        from ui.challenge import render_challenge_section
        render_challenge_section(client, cfg_now, selected_model)

    # ── 向后兼容：旧三模块结果 ────────────────────────────────────
    elif _has_old_three_modules(analyses):
        from ui.results import render_radar_section
        render_radar_section()

        _summary_items = [
            ("expectation", "🔍 预期差"),
            ("trend", "📈 趋势研判"),
            ("fundamentals", "📋 基本面"),
        ]
        for key, title in _summary_items:
            text = analyses.get(key, "")
            if text:
                with st.expander(title, expanded=False):
                    st.markdown(text)

        # 深度分析结果
        for key, title in [("sentiment", "📣 舆情"), ("sector", "🏭 板块"), ("holders", "👥 股东")]:
            text = analyses.get(key, "")
            if text:
                with st.expander(title, expanded=False):
                    st.markdown(text)

    # 估值分位图
    _val_df = st.session_state.get("valuation_df", pd.DataFrame())
    if not _val_df.empty:
        st.markdown(f"#### 📊 估值历史分位")
        render_valuation_bands(_val_df, st.session_state.get("stock_name", ""))


def _has_old_three_modules(analyses: dict) -> bool:
    """检查是否有旧的三模块分析结果"""
    return all(analyses.get(k) for k in ["expectation", "trend", "fundamentals"])
