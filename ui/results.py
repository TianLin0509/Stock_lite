"""分析结果展示 — 同步模式，无轮询"""

import time
import streamlit as st
import pandas as pd
from analysis.moe import MOE_ROLES
from ai.client import call_ai
from ai.context import build_analysis_context


# ══════════════════════════════════════════════════════════════════════════════
# 结果渲染工具
# ══════════════════════════════════════════════════════════════════════════════

def _show_analysis_result(key: str, title: str, icon: str):
    """显示分析结果：已完成/未开始两态"""
    analyses = st.session_state.get("analyses", {})
    content = analyses.get(key, "")

    if content:
        name = st.session_state.get("stock_name", "")
        st.markdown(f"#### {icon} {name} · {title}结果")
        with st.container(border=True):
            st.markdown(content)
    else:
        st.info(f"{title}尚未执行，点击上方按钮开始分析")


# ══════════════════════════════════════════════════════════════════════════════
# 主展示函数
# ══════════════════════════════════════════════════════════════════════════════

def show_completed_results(client=None, cfg=None, model_name=""):
    """根据 active_tab 展示对应内容"""
    name     = st.session_state.get("stock_name", "")
    tscode   = st.session_state.get("stock_code", "")
    analyses = st.session_state.get("analyses", {})
    active_tab = st.session_state.get("active_tab", "")

    if not name or not active_tab:
        return

    st.markdown("---")

    if active_tab == "expectation":
        _show_analysis_result("expectation", "预期差分析", "🔍")

    elif active_tab == "trend":
        _show_analysis_result("trend", "K线趋势研判", "📈")

    elif active_tab == "similarity":
        _show_similarity_section(name, tscode)

    elif active_tab == "fundamentals":
        _show_analysis_result("fundamentals", "基本面分析", "📋")

    elif active_tab == "sentiment":
        _show_analysis_result("sentiment", "舆情情绪分析", "📣")

    elif active_tab == "sector":
        _show_analysis_result("sector", "板块联动分析", "🏭")

    elif active_tab == "holders":
        _show_analysis_result("holders", "股东/机构动向", "👥")


def _render_moe_results():
    moe = st.session_state.get("moe_results", {})
    if not moe.get("done"):
        return
    name = st.session_state.get("stock_name", "")
    st.markdown(f"#### 🎯 {name} · MoE 辩论裁决结果")
    for role in MOE_ROLES:
        text = moe["roles"].get(role["key"], "")
        st.markdown(f"""<div class="role-card {role['css']}">
  <div class="role-badge">{role['badge']}</div>
  <div class="role-content">{text}</div>
</div>""", unsafe_allow_html=True)
    st.markdown("---")
    st.markdown(f"""<div class="role-card r-ceo">
  <div class="role-badge">👔 首席执行官 · 最终裁决</div>
  <div class="role-content">{moe['ceo']}</div>
</div>""", unsafe_allow_html=True)



def render_radar_section():
    """渲染价值投机雷达仪表盘（核心三项完成后调用）"""
    from analysis.signal import compute_signal
    from ui.charts import render_radar

    signal = compute_signal(st.session_state)
    if not signal:
        return

    name = st.session_state.get("stock_name", "")
    st.markdown(f"#### 🎯 {name} · 价值投机雷达")
    if signal["resonance"]:
        st.markdown("""<div class="status-banner success">
  🔥 <strong>四维共振信号</strong> — 基本面、题材、技术、资金四维均达标（≥70），高置信度关注！
</div>""", unsafe_allow_html=True)

    col_radar, col_scores = st.columns([3, 2])
    with col_radar:
        render_radar(signal)
    with col_scores:
        for dim_name, dim_score, dim_icon in [
            ("基本面强度", signal["fundamental"], "📋"),
            ("题材正宗度", signal["catalyst"], "🔍"),
            ("技术启动度", signal["technical"], "📈"),
            ("资金关注度", signal["capital"], "💰"),
        ]:
            color = "#16a34a" if dim_score >= 70 else "#f59e0b" if dim_score >= 50 else "#ef4444"
            st.markdown(
                f'<div style="margin-bottom:0.7rem;">'
                f'<div style="font-size:0.8rem;color:#6b7280;margin-bottom:2px;">'
                f'{dim_icon} {dim_name}</div>'
                f'<div style="display:flex;align-items:center;gap:8px;">'
                f'<div style="flex:1;background:#f1f5f9;border-radius:6px;height:8px;overflow:hidden;">'
                f'<div style="width:{dim_score}%;height:100%;background:{color};'
                f'border-radius:6px;"></div></div>'
                f'<span style="font-size:0.9rem;font-weight:700;color:{color};'
                f'min-width:36px;text-align:right;">{dim_score}</span>'
                f'</div></div>',
                unsafe_allow_html=True,
            )
        verdict_color = "#16a34a" if signal["resonance"] else \
                       "#f59e0b" if signal["avg"] >= 60 else "#ef4444"
        st.markdown(
            f'<div style="margin-top:0.8rem;padding:0.7rem;'
            f'background:linear-gradient(135deg,#faf5ff,#fdf2f8);'
            f'border-radius:10px;border:1px solid #d8b4fe;text-align:center;">'
            f'<div style="font-size:0.75rem;color:#9ca3af;">综合评分</div>'
            f'<div style="font-size:1.6rem;font-weight:800;color:{verdict_color};">'
            f'{signal["avg"]}</div>'
            f'<div style="font-size:0.82rem;font-weight:600;color:{verdict_color};">'
            f'{signal["verdict"]}</div></div>',
            unsafe_allow_html=True,
        )
    st.markdown("---")



def render_radar_section_5d():
    """渲染五维综合投研雷达仪表盘（综合报告完成后调用）"""
    from analysis.signal import compute_signal_5d
    from ui.charts import render_radar_5d

    signal = compute_signal_5d(st.session_state)
    if not signal:
        return

    name = st.session_state.get("stock_name", "")
    st.markdown(f"#### 🎯 {name} · 四维投研雷达")

    # 致命缺陷警告
    if signal.get("fatal_flaw"):
        st.markdown(f"""<div class="status-banner warn">
  🚨 <strong>致命缺陷</strong> — {signal['fatal_flaw']}，综合评分已封顶。
</div>""", unsafe_allow_html=True)

    # 共振提示
    if signal["resonance"]:
        st.markdown("""<div class="status-banner success">
  🔥 <strong>四维共振信号</strong> — 基本面、预期差、技术、资金四维均达标（≥75），高置信度关注！
</div>""", unsafe_allow_html=True)

    col_radar, col_scores = st.columns([3, 2])
    with col_radar:
        render_radar_5d(signal)
    with col_scores:
        for dim_name, dim_key, dim_icon in [
            ("基本面", "fundamental", "📋"),
            ("预期差", "expectation", "🔍"),
            ("技术面", "technical", "📈"),
            ("资金面", "capital", "💰"),
        ]:
            dim_score = signal[dim_key]
            color = "#16a34a" if dim_score >= 70 else "#f59e0b" if dim_score >= 50 else "#ef4444"
            st.markdown(
                f'<div style="margin-bottom:0.7rem;">'
                f'<div style="font-size:0.8rem;color:#6b7280;margin-bottom:2px;">'
                f'{dim_icon} {dim_name}</div>'
                f'<div style="display:flex;align-items:center;gap:8px;">'
                f'<div style="flex:1;background:#f1f5f9;border-radius:6px;height:8px;overflow:hidden;">'
                f'<div style="width:{dim_score}%;height:100%;background:{color};'
                f'border-radius:6px;"></div></div>'
                f'<span style="font-size:0.9rem;font-weight:700;color:{color};'
                f'min-width:36px;text-align:right;">{dim_score}</span>'
                f'</div></div>',
                unsafe_allow_html=True,
            )

        # 综合评分
        composite = signal["composite"]
        verdict_color = "#16a34a" if signal["resonance"] else \
                       "#f59e0b" if composite >= 60 else "#ef4444"
        st.markdown(
            f'<div style="margin-top:0.8rem;padding:0.7rem;'
            f'background:linear-gradient(135deg,#faf5ff,#fdf2f8);'
            f'border-radius:10px;border:1px solid #d8b4fe;text-align:center;">'
            f'<div style="font-size:0.75rem;color:#9ca3af;">综合评分</div>'
            f'<div style="font-size:1.6rem;font-weight:800;color:{verdict_color};">'
            f'{composite}</div>'
            f'<div style="font-size:0.82rem;font-weight:600;color:{verdict_color};">'
            f'{signal["verdict"]}</div></div>',
            unsafe_allow_html=True,
        )

        # 短线攻击力 + 中线安全垫
        stp = signal.get("short_term_power", 0)
        mts = signal.get("mid_term_safety", 0)
        stp_color = "#16a34a" if stp >= 70 else "#f59e0b" if stp >= 50 else "#ef4444"
        mts_color = "#16a34a" if mts >= 70 else "#f59e0b" if mts >= 50 else "#ef4444"
        st.markdown(
            f'<div style="display:flex;gap:8px;margin-top:8px;">'
            f'<div style="flex:1;padding:6px;background:#f8fafc;border-radius:8px;text-align:center;">'
            f'<div style="font-size:0.7rem;color:#9ca3af;">短线攻击力</div>'
            f'<div style="font-size:1.1rem;font-weight:700;color:{stp_color};">{stp}</div></div>'
            f'<div style="flex:1;padding:6px;background:#f8fafc;border-radius:8px;text-align:center;">'
            f'<div style="font-size:0.7rem;color:#9ca3af;">中线安全垫</div>'
            f'<div style="font-size:1.1rem;font-weight:700;color:{mts_color};">{mts}</div></div>'
            f'</div>',
            unsafe_allow_html=True,
        )
    st.markdown("---")


# ══════════════════════════════════════════════════════════════════════════════
# K线相似走势匹配
# ══════════════════════════════════════════════════════════════════════════════

def _show_similarity_section(name: str, tscode: str):
    """独立的 K线匹配 tab"""
    from data.similarity import find_similar, HISTORY_FILE, HISTORY_DIR
    from ui.charts import render_similar_case
    import os, glob

    # 历史数据文件不存在则跳过
    has_parts = bool(glob.glob(os.path.join(HISTORY_DIR, "all_daily_part*.parquet")))
    if not has_parts and not os.path.exists(HISTORY_FILE):
        st.warning("历史K线数据文件不存在，K线匹配功能不可用")
        return

    price_df = st.session_state.get("price_df", pd.DataFrame())
    if price_df.empty or len(price_df) < 5:
        st.warning("K线数据不足，请先查询股票")
        return

    st.markdown("---")
    st.markdown(f"#### 📐 历史相似走势匹配 · {name}")

    # 参数选择
    col_opt1, col_opt2, col_opt3 = st.columns([1, 1, 2])
    with col_opt1:
        k_days = st.selectbox(
            "匹配天数", [5, 10, 15, 20, 30], index=1,
            key="sim_k_days", help="选择用最近多少个交易日的K线进行匹配",
        )
    with col_opt2:
        top_n = st.selectbox(
            "案例数", [1, 3, 5, 7], index=1,
            key="sim_top_n", help="返回最相似的前N个案例",
        )

    st.caption(
        f"基于最近 **{k_days}** 个交易日的五维K线特征（涨跌幅 · 振幅 · 量能节奏 · 上影线 · 下影线），"
        f"在全市场5年历史数据中搜索最相似的 Top {top_n} 走势，并展示匹配段前后各10天的完整走势供对比。"
    )

    if len(price_df) < k_days:
        st.warning(f"当前K线数据只有 {len(price_df)} 天，不足 {k_days} 天，请减小匹配天数")
        return

    # 检查缓存（需要 ts_code + k_days + top_n 一致）
    cached = st.session_state.get("similarity_results")
    if (cached and cached.get("ts_code") == tscode
            and cached.get("k_days") == k_days and cached.get("top_n") == top_n):
        _render_similarity_results(cached["results"], price_df, k_days)
        return

    if st.button("🔍 开始匹配历史走势", type="primary", key="btn_similarity"):
        progress_bar = st.progress(0, text="准备中...")
        status_container = st.status("📐 正在全市场搜索相似走势...", expanded=True)
        with status_container as status:
            st.write("📊 加载全市场5年日线数据（首次较慢）...")

            results = find_similar(
                target_df=price_df,
                k_days=k_days,
                top_n=top_n,
                context_days=10,
                exclude_code=tscode,
                exclude_recent_days=60,
                progress_callback=lambda cur, total: progress_bar.progress(
                    cur / total,
                    text=f"🔍 搜索中... {cur}/{total} 只股票 ({cur*100//total}%)"
                ),
            )

            progress_bar.progress(1.0, text="✅ 搜索完成！")

            if results:
                st.write(f"✅ 找到 {len(results)} 个高度相似的历史案例！")
                status.update(label="✅ 匹配完成！", state="complete")
            else:
                st.write("未找到足够相似的历史走势")
                status.update(label="⚠️ 未找到匹配", state="complete")

        # 缓存结果
        st.session_state["similarity_results"] = {
            "ts_code": tscode,
            "k_days": k_days,
            "top_n": top_n,
            "results": results,
        }

        if results:
            _render_similarity_results(results, price_df, k_days)


def _render_similarity_results(results: list, target_df: pd.DataFrame = None,
                               k_days: int = 10):
    """渲染相似走势匹配结果"""
    from ui.charts import render_similar_case

    if not results:
        st.info("未找到足够相似的历史走势案例")
        return

    # ── 全样本胜率统计（优先显示）──────────────────────────────────────
    match_stats = results[0].get("match_stats") if results else None
    if match_stats and match_stats["total_matches"] > 0:
        total = match_stats["total_matches"]
        wr = match_stats["win_rate_10d"]
        avg_r = match_stats["avg_return_10d"]
        med_r = match_stats["median_return_10d"]

        # 胜率颜色：>55% 绿色，<45% 红色，否则中性
        if wr > 55:
            wr_color = "#16a34a"
            wr_bg = "#f0fdf4"
            wr_border = "#bbf7d0"
            wr_label = "偏多"
        elif wr < 45:
            wr_color = "#dc2626"
            wr_bg = "#fef2f2"
            wr_border = "#fecaca"
            wr_label = "偏空"
        else:
            wr_color = "#d97706"
            wr_bg = "#fffbeb"
            wr_border = "#fde68a"
            wr_label = "中性"

        avg_color = "#16a34a" if avg_r > 0 else "#dc2626" if avg_r < 0 else "#6b7280"
        med_color = "#16a34a" if med_r > 0 else "#dc2626" if med_r < 0 else "#6b7280"

        st.markdown(
            f'<div style="padding:12px 16px;background:{wr_bg};border:1px solid {wr_border};'
            f'border-radius:10px;margin-bottom:12px;">'
            f'<div style="font-size:0.85rem;font-weight:700;color:#374151;margin-bottom:6px;">'
            f'全样本统计（相似度 > 45% 的所有匹配）</div>'
            f'<div style="display:flex;gap:24px;flex-wrap:wrap;">'
            f'<div><span style="color:#6b7280;font-size:0.78rem;">匹配总数</span><br>'
            f'<span style="font-size:1.1rem;font-weight:700;">{total}</span> 只</div>'
            f'<div><span style="color:#6b7280;font-size:0.78rem;">10日胜率</span><br>'
            f'<span style="font-size:1.1rem;font-weight:700;color:{wr_color};">'
            f'{wr}%</span> <span style="font-size:0.75rem;color:{wr_color};">{wr_label}</span></div>'
            f'<div><span style="color:#6b7280;font-size:0.78rem;">平均收益</span><br>'
            f'<span style="font-size:1.1rem;font-weight:700;color:{avg_color};">'
            f'{avg_r:+.2f}%</span></div>'
            f'<div><span style="color:#6b7280;font-size:0.78rem;">中位数收益</span><br>'
            f'<span style="font-size:1.1rem;font-weight:700;color:{med_color};">'
            f'{med_r:+.2f}%</span></div>'
            f'</div></div>',
            unsafe_allow_html=True,
        )

    # ── Top N 案例统计 ────────────────────────────────────────────────
    returns = [r["subsequent_return"] for r in results if r["subsequent_return"] is not None]
    if returns:
        avg_ret = sum(returns) / len(returns)
        up_count = sum(1 for r in returns if r > 0)
        color = "🔴" if avg_ret > 0 else "🟢"
        st.markdown(
            f"**Top {len(results)} 案例：** "
            f"{up_count} 个后续上涨、{len(returns) - up_count} 个下跌，"
            f"平均后续涨跌 {color} **{avg_ret:+.1f}%**"
        )
        st.caption("⚠️ 历史走势不代表未来表现，仅供参考")
        if target_df is not None:
            target_name = st.session_state.get("stock_name", "目标股")
            st.caption(f"📊 蓝色叠加K线为 **{target_name}** 最近 {k_days} 天走势，用于直观对比")

    for i, case in enumerate(results, 1):
        render_similar_case(case, i, target_df=target_df, k_days=k_days)
