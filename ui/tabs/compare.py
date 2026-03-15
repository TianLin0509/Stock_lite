"""Tab: ⚖️ 股票对比 — 双股同屏对比"""

import streamlit as st
import pandas as pd

from data.tushare_client import (
    resolve_stock, get_basic_info, get_price_df, to_code6,
)


def _format_info_for_ai(info: dict) -> str:
    """格式化股票信息供 AI 对比使用"""
    parts = []
    for key in ["最新价(元)", "市盈率TTM", "市净率PB", "市销率PS",
                "换手率(%)", "行业", "总市值(亿)"]:
        v = info.get(key, "")
        if v and str(v) != "N/A":
            parts.append(f"- {key}: {v}")
    return "\n".join(parts) if parts else "- 数据暂无"


def render_compare_tab(client, cfg, selected_model):
    """渲染股票对比 Tab"""
    from analysis.signal import compute_signal

    st.markdown("#### ⚖️ 双股对比分析")
    st.caption("输入两只股票，同屏对比关键指标和 AI 评价，帮你做选择")

    col_a, col_b = st.columns(2)
    with col_a:
        query_a = st.text_input("股票 A", placeholder="代码或名称，如 600519",
                                key="cmp_query_a")
    with col_b:
        query_b = st.text_input("股票 B", placeholder="代码或名称，如 000858",
                                key="cmp_query_b")

    if st.button("⚖️ 开始对比", type="primary", use_container_width=True, key="btn_compare"):
        if not query_a or not query_b:
            st.warning("请输入两只股票")
            return
        with st.spinner("正在获取两只股票数据..."):
            code_a, name_a, err_a = resolve_stock(query_a.strip())
            code_b, name_b, err_b = resolve_stock(query_b.strip())
        if err_a:
            st.warning(f"股票A：{err_a}")
        if err_b:
            st.warning(f"股票B：{err_b}")

        # 复用当前分析股票的数据
        _current_code = st.session_state.get("stock_code", "")

        with st.status("📥 获取对比数据...", expanded=True) as s:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            def _fetch_stock(code, name, is_current):
                if is_current:
                    info = st.session_state.get("stock_info", {})
                    df = st.session_state.get("price_df", pd.DataFrame())
                    if info and not df.empty:
                        return info, df
                info, _ = get_basic_info(code)
                df, _ = get_price_df(code, days=60)
                return info, df

            a_is_current = (code_a == _current_code)
            b_is_current = (code_b == _current_code)

            # 并行获取两只股票数据
            with ThreadPoolExecutor(max_workers=2) as pool:
                fut_a = pool.submit(_fetch_stock, code_a, name_a, a_is_current)
                fut_b = pool.submit(_fetch_stock, code_b, name_b, b_is_current)
                info_a, df_a = fut_a.result()
                info_b, df_b = fut_b.result()

            s.update(label="✅ 数据获取完成！", state="complete")

        st.session_state["_cmp_data"] = {
            "name_a": name_a, "code_a": code_a, "info_a": info_a, "df_a": df_a,
            "name_b": name_b, "code_b": code_b, "info_b": info_b, "df_b": df_b,
        }

    # 展示对比结果
    cmp = st.session_state.get("_cmp_data")
    if not cmp:
        return

    name_a, name_b = cmp["name_a"], cmp["name_b"]
    info_a, info_b = cmp["info_a"], cmp["info_b"]

    st.markdown("---")
    st.markdown(f"### {name_a} vs {name_b}")

    # ── 关键指标对比表 ─────────────────────────────────────
    metrics = [
        ("最新价(元)", "最新价"),
        ("市盈率TTM", "PE(TTM)"),
        ("市净率PB", "PB"),
        ("市销率PS", "PS"),
        ("换手率(%)", "换手率%"),
        ("行业", "行业"),
    ]
    rows = []
    for info_key, display_name in metrics:
        va = info_a.get(info_key, "—")
        vb = info_b.get(info_key, "—")
        better = ""
        try:
            fa, fb = float(va), float(vb)
            if info_key in ("市盈率TTM", "市净率PB", "市销率PS"):
                if 0 < fa < fb:
                    better = "← 优"
                elif 0 < fb < fa:
                    better = "优 →"
        except (ValueError, TypeError):
            pass
        rows.append({
            "指标": display_name,
            name_a: str(va)[:12],
            name_b: str(vb)[:12],
            "": better,
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # ── K线走势对比 ────────────────────────────────────────
    df_a, df_b = cmp.get("df_a", pd.DataFrame()), cmp.get("df_b", pd.DataFrame())
    if not df_a.empty and not df_b.empty:
        import plotly.graph_objects as go
        fig = go.Figure()
        if len(df_a) > 0:
            base_a = df_a["收盘"].iloc[0]
            fig.add_trace(go.Scatter(
                x=df_a["日期"], y=df_a["收盘"] / base_a * 100,
                name=name_a, line=dict(color="#6366f1", width=2),
            ))
        if len(df_b) > 0:
            base_b = df_b["收盘"].iloc[0]
            fig.add_trace(go.Scatter(
                x=df_b["日期"], y=df_b["收盘"] / base_b * 100,
                name=name_b, line=dict(color="#f59e0b", width=2),
            ))
        fig.update_layout(
            title="近60日走势对比（归一化）",
            yaxis_title="相对涨幅（起点=100）",
            height=350, margin=dict(l=0, r=0, t=40, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        fig.update_xaxes(type="category", tickangle=-45, nticks=10)
        st.plotly_chart(fig, use_container_width=True)

    # ── AI 对比点评 ────────────────────────────────────────
    if not client:
        st.caption("AI 模型不可用，无法生成对比点评")
        return

    cmp_key = f"_cmp_ai_{cmp['code_a']}_{cmp['code_b']}"
    cached_comment = st.session_state.get(cmp_key)
    if cached_comment:
        st.markdown("#### 🤖 AI 对比点评")
        st.markdown(cached_comment)
        if st.button("🔄 重新生成点评", key="redo_cmp_ai"):
            st.session_state.pop(cmp_key, None)
            st.rerun()
        return

    if st.button("🤖 生成 AI 对比点评", type="primary", key="btn_cmp_ai"):
        from ai.client import call_ai
        prompt = f"""请对比分析以下两只A股，帮助投资者做选择：

## 股票A：{name_a}（{to_code6(cmp['code_a'])}）
{_format_info_for_ai(info_a)}

## 股票B：{name_b}（{to_code6(cmp['code_b'])}）
{_format_info_for_ai(info_b)}

请从以下维度逐项对比，最后给出明确的选择建议：
1. **估值对比**：PE/PB/PS 谁更便宜，是否合理
2. **成长性**：行业前景、增长潜力
3. **技术面**：近期走势强弱
4. **资金面**：换手率、市场关注度
5. **风险对比**：各自主要风险点

## 最终建议
明确推荐哪只（或各自适合什么策略），给出理由。
"""
        system = "你是专业A股投资顾问，擅长对比分析。请联网搜索两只股票的最新消息辅助判断。回答要具体有数据，不要空泛。"
        with st.spinner("AI 正在对比分析..."):
            result, err = call_ai(client, cfg, prompt, system=system, max_tokens=4000,
                                  username=st.session_state.get("current_user", ""))
        if err:
            st.error(f"对比分析失败：{err}")
        else:
            st.session_state[cmp_key] = result
            st.rerun()
