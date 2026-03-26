"""综合投研报告 UI 包装层。"""

import logging

import streamlit as st

from data.tushare_client import to_code6
from services.analysis_service import run_comprehensive_analysis

logger = logging.getLogger(__name__)


def run_deep_report(client, cfg, selected_model, username):
    """执行综合投研报告，并把结果写回 session_state。"""
    name = st.session_state.get("stock_name", "")
    ts_code = st.session_state.get("stock_code", "")
    code6 = to_code6(ts_code)

    if not name or not ts_code:
        st.error("请先选择股票")
        return

    summary_placeholder = st.empty()
    heartbeat_area = st.empty()

    st.markdown(f"#### 🧵 {name} · 综合投研报告")
    report_area = st.empty()

    with st.status(f"📗 正在采集 {name}（{code6}）全量数据...", expanded=True) as status:
        try:
            result = run_comprehensive_analysis(
                client=client,
                cfg=cfg,
                selected_model=selected_model,
                username=username,
                name=name,
                ts_code=ts_code,
                price_df=st.session_state.get("price_df"),
                data_progress_cb=lambda msg: st.write(msg),
                status_cb=lambda msg: heartbeat_area.info(msg),
                stream_cb=lambda text: report_area.markdown(text + "▌"),
            )
            status.update(label=f"✅ {name} 数据采集完成", state="complete")
        except Exception as exc:
            heartbeat_area.empty()
            st.error(str(exc))
            return

    heartbeat_area.empty()
    report_area.markdown(result.full_report)

    if result.summary and len(result.summary) > 20:
        with summary_placeholder.container():
            with st.container(border=True):
                st.caption("📳 执行摘要")
                st.markdown(result.summary)
    else:
        summary_placeholder.empty()
        st.warning("摘要生成异常")

    analyses = st.session_state.get("analyses", {})
    analyses["comprehensive"] = result.full_report
    st.session_state["analyses"] = analyses
    st.session_state["report_summary"] = result.summary
    st.session_state["report_scores"] = result.scores

    if result.stock_capital and len(result.stock_capital) > 20:
        st.session_state["stock_capital"] = result.stock_capital
    if result.stock_northbound and "暂无" not in result.stock_northbound:
        st.session_state["stock_northbound"] = result.stock_northbound
    if result.stock_margin and "暂无" not in result.stock_margin:
        st.session_state["stock_margin"] = result.stock_margin

    logger.info("[deep_report] %s 综合报告完成，评分=%s", name, result.scores)
