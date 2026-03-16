"""Tab: 🎯 六方会谈（MoE 多角色辩论）— 同步执行"""

import streamlit as st

from config import CORE_KEYS
from analysis.runner import run_moe_sync
from ui.results import _render_moe_results


def render_moe_tab(client, cfg_now, selected_model):
    """渲染六方会谈 Tab"""
    stock_ready = bool(st.session_state.get("stock_name"))
    analyses = st.session_state.get("analyses", {})

    # 支持综合报告模式 或 旧三模块模式
    _has_comprehensive = bool(analyses.get("comprehensive"))
    _has_old_three = all(analyses.get(k) for k in ["expectation", "trend", "fundamentals"])
    _core_done_moe = stock_ready and (_has_comprehensive or _has_old_three)

    if not stock_ready:
        st.markdown("#### 🎯 六方会谈 · 多角色辩论裁决")
        st.info("请先在「📊 智能分析」中输入股票并完成分析")
        st.caption("六方会谈需要综合投研报告或核心三项分析结果作为辩论素材")
    elif not _core_done_moe:
        st.markdown("#### 🎯 六方会谈 · 多角色辩论裁决")
        if analyses.get("comprehensive"):
            _progress_text = "✅ 综合报告"
        else:
            _done_labels = []
            _missing_labels = []
            _lbl = {"comprehensive": "综合报告", "expectation": "预期差",
                     "trend": "趋势解读", "fundamentals": "基本面"}
            for k in ["comprehensive", "expectation", "trend", "fundamentals"]:
                if analyses.get(k):
                    _done_labels.append(f"✅ {_lbl[k]}")
                else:
                    _missing_labels.append(_lbl[k])
            _progress_text = " &nbsp;|&nbsp; ".join(_done_labels)
            if _missing_labels:
                _progress_text += f" &nbsp;|&nbsp; ⬜ {'、'.join(_missing_labels)}"
        st.markdown(
            f'<div style="padding:1rem;background:linear-gradient(135deg,#faf5ff,#eff6ff);'
            f'border-radius:10px;border:1px solid #c4b5fd;text-align:center;">'
            f'<div style="font-size:0.95rem;color:#6b7280;margin-bottom:8px;">'
            f'完成综合报告或核心三项分析后即可启动六方会谈</div>'
            f'<div style="font-size:0.85rem;">{_progress_text}</div></div>',
            unsafe_allow_html=True,
        )
    else:
        _moe_name = st.session_state.get("stock_name", "")
        st.markdown(f"#### 🎯 {_moe_name} · 六方会谈")

        moe_done = st.session_state.get("moe_results", {}).get("done", False)
        if moe_done:
            _render_moe_results()
        else:
            st.caption(
                "六方会谈将召集5位不同角色的专家（价值投机手、技术派、基本面研究员、"
                "题材猎手、散户代表）对该股进行多角度辩论，最终由首席执行官综合裁决。"
            )
            if st.button("🎯 启动六方会谈", type="primary",
                         use_container_width=True, key="btn_moe_start"):
                if client:
                    name = st.session_state.get("stock_name", "")
                    tscode = st.session_state.get("stock_code", "")
                    username = st.session_state.get("current_user", "")

                    with st.status("🎯 六方会谈...", expanded=True) as status:
                        moe_data, err = run_moe_sync(
                            client, cfg_now, selected_model,
                            name, tscode, analyses,
                            username=username,
                            progress_cb=lambda msg: st.write(msg),
                        )
                        if err:
                            status.update(label="❌ 六方会谈失败", state="error")
                            st.error(f"六方会谈失败：{err}")
                        else:
                            st.session_state["moe_results"] = moe_data
                            status.update(label="✅ 六方会谈完成！", state="complete")
                    st.rerun()
