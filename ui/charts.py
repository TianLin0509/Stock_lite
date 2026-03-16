"""K线图渲染 — 浅色风格（红涨绿跌）"""

import numpy as np
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from data.tushare_client import to_code6


# ══════════════════════════════════════════════════════════════════════════════
# 浅色主题配色（红涨绿跌）
# ══════════════════════════════════════════════════════════════════════════════
_UP     = "#ee3333"   # 涨 — 红
_DOWN   = "#00aa3b"   # 跌 — 绿
_MA_CLR = {
    5:  "#e8a633",    # MA5  — 黄
    10: "#33a3dc",    # MA10 — 蓝
    20: "#ee33ee",    # MA20 — 紫
    30: "#33bb33",    # MA30 — 绿
    60: "#999999",    # MA60 — 灰
}
_BG      = "#ffffff"  # 白色背景
_PAPER   = "#fafafa"  # 浅灰纸面
_GRID    = "#e8e8e8"  # 浅灰网格
_TEXT    = "#333333"   # 深色文字
_MATCH_BORDER = "rgba(30,100,220,0.5)"
_MATCH_BG     = "rgba(30,100,220,0.06)"

# 目标股叠加颜色 — 蓝/橙双色，与红绿形成强对比
_TGT_UP   = "#ff8c00"   # 目标涨 — 橙色
_TGT_DOWN = "#4169e1"   # 目标跌 — 皇家蓝


def render_kline(df: pd.DataFrame, name: str, ts_code: str) -> None:
    """主页 K 线图（浅色风格），缓存到 session_state"""
    if df.empty:
        st.warning("暂无K线数据")
        return

    # 图表缓存：相同股票+数据量 → 复用 figure 对象
    cache_key = f"_fig_kline_{ts_code}_{len(df)}"
    fig_cached = st.session_state.get(cache_key)
    if fig_cached is not None:
        st.plotly_chart(fig_cached, use_container_width=True,
                        config={"displayModeBar": False, "responsive": True,
                                "scrollZoom": False})
        return

    d = df.copy()
    d = d.reset_index(drop=True)
    x_idx = list(range(len(d)))
    tick_vals = x_idx[::max(1, len(d)//8)]
    tick_text = [d.iloc[i]["日期"] if i < len(d) else "" for i in tick_vals]

    for p in [5, 10, 20, 30, 60]:
        d[f"MA{p}"] = d["收盘"].rolling(p).mean()

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.02, row_heights=[0.72, 0.28])

    fig.add_trace(go.Candlestick(
        x=x_idx, open=d["开盘"], high=d["最高"],
        low=d["最低"], close=d["收盘"], name="K线",
        increasing=dict(line=dict(color=_UP, width=1), fillcolor=_UP),
        decreasing=dict(line=dict(color=_DOWN, width=1), fillcolor=_DOWN),
        whiskerwidth=0.4,
    ), row=1, col=1)

    for p, clr in [(5, _MA_CLR[5]), (20, _MA_CLR[20]), (60, _MA_CLR[60])]:
        fig.add_trace(go.Scatter(
            x=x_idx, y=d[f"MA{p}"], name=f"MA{p}",
            line=dict(color=clr, width=1.3), mode="lines",
        ), row=1, col=1)

    colors = [_UP if c >= o else _DOWN for c, o in zip(d["收盘"], d["开盘"])]
    fig.add_trace(go.Bar(x=x_idx, y=d["成交量"], name="成交量",
                          marker_color=colors, opacity=0.6), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=x_idx, y=d["成交量"].rolling(5).mean(), name="量MA5",
        line=dict(color=_MA_CLR[5], width=1.2), mode="lines", opacity=0.85,
    ), row=2, col=1)

    _apply_layout(fig, f"{name}（{to_code6(ts_code)}）日K线", 440,
                  tick_vals, tick_text, show_legend=True)
    st.session_state[cache_key] = fig
    st.plotly_chart(fig, use_container_width=True,
                    config={"displayModeBar": False, "responsive": True,
                            "scrollZoom": False})


# ══════════════════════════════════════════════════════════════════════════════
# 相似走势案例 K 线图（含目标股叠加对比）
# ══════════════════════════════════════════════════════════════════════════════

def _fmt_date(d) -> str:
    if isinstance(d, (float, int, np.floating, np.integer)) and d > 19000000:
        di = int(d)
        return f"{di // 10000}-{di % 10000 // 100:02d}-{di % 100:02d}"
    return str(d)


def render_similar_case(case: dict, idx: int, target_df: pd.DataFrame = None,
                        k_days: int = 10) -> None:
    """
    渲染单个相似走势案例 — 浅色风格 + 目标股叠加对比
    目标股用橙/蓝色 K 线叠加，与案例股红/绿形成鲜明对比
    """
    ctx = case["context_df"].copy()
    if ctx.empty:
        return

    code = case["ts_code"]
    stock_name = case.get("stock_name", "") or code
    sim = case["similarity"]
    kline_sim = case.get("kline_similarity", sim)
    vol_sim = case.get("vol_similarity", 0)
    start_d = _fmt_date(case["match_start_date"])
    end_d = _fmt_date(case["match_end_date"])
    ret = case["subsequent_return"]

    ret_text = f"后续走势 **{ret:+.1f}%**" if ret is not None else "后续数据不足"
    ret_color = ("🔴" if ret > 0 else "🟢" if ret < 0 else "⚪") if ret is not None else "⚪"

    # 最大回撤与最大涨幅
    max_dd = case.get("max_drawdown")
    max_g = case.get("max_gain")
    dd_text = ""
    if max_dd is not None and max_g is not None:
        dd_text = f" &nbsp;|&nbsp; 期间最大回撤 **{max_dd:+.1f}%** &nbsp; 最大涨幅 **{max_g:+.1f}%**"

    st.markdown(f"""
**案例 {idx}：{stock_name}（{code}）** &nbsp;
匹配区间 `{start_d}` ~ `{end_d}` &nbsp;
综合 **{sim}%** &nbsp;|&nbsp; K线 **{kline_sim}%** &nbsp;|&nbsp;
成交量 **{vol_sim}%** &nbsp; {ret_color} {ret_text}{dd_text}
""")

    detail = case.get("feature_detail", {})
    if detail:
        labels = {"pct_chg": "涨跌形态", "amplitude": "K线振幅", "vol_chg": "量能节奏",
                  "upper_shadow": "上影线", "lower_shadow": "下影线"}
        parts = [f"{labels.get(k, k)} {v}%" for k, v in detail.items()]
        st.caption(" · ".join(parts))

    # ── 准备数据 ─────────────────────────────────────────────────────────
    ctx = ctx.reset_index(drop=True)
    match_mask = ctx["is_match"].values
    n = len(ctx)
    x_idx = list(range(n))

    date_labels = [_fmt_date(d) for d in ctx["trade_date"].values]
    tick_vals = x_idx[::max(1, n // 6)]
    tick_text = [date_labels[i] for i in tick_vals]

    closes = ctx["close"].values.astype(float)
    opens = ctx["open"].values.astype(float)

    ma_data = {}
    for p in [5, 10, 20, 30]:
        if len(closes) >= p:
            ma_data[p] = pd.Series(closes).rolling(p).mean().values

    # ── 匹配段索引 ────────────────────────────────────────────────────────
    match_indices = [i for i, m in enumerate(match_mask) if m]
    match_start_idx = match_indices[0] if match_indices else 0
    match_end_idx = match_indices[-1] if match_indices else n - 1

    # ── 构建图表 ─────────────────────────────────────────────────────────
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.03, row_heights=[0.70, 0.30])

    # 案例股 K线（红绿）
    fig.add_trace(go.Candlestick(
        x=x_idx, open=ctx["open"], high=ctx["high"],
        low=ctx["low"], close=ctx["close"],
        name=stock_name,
        increasing=dict(line=dict(color=_UP, width=1), fillcolor=_UP),
        decreasing=dict(line=dict(color=_DOWN, width=1), fillcolor=_DOWN),
        whiskerwidth=0.4,
    ), row=1, col=1)

    # 均线
    for p in [5, 10, 20, 30]:
        if p in ma_data:
            fig.add_trace(go.Scatter(
                x=x_idx, y=ma_data[p], name=f"MA{p}",
                line=dict(color=_MA_CLR[p], width=1.1), mode="lines",
            ), row=1, col=1)

    # ── 叠加目标股 K线（橙/蓝色，与红/绿形成对比）──────────────────────
    if target_df is not None and len(target_df) >= k_days:
        tgt = target_df.tail(k_days).reset_index(drop=True)
        tgt_close = tgt["收盘"].values.astype(float)
        tgt_open = tgt["开盘"].values.astype(float)
        tgt_high = tgt["最高"].values.astype(float)
        tgt_low = tgt["最低"].values.astype(float)
        tgt_vol = tgt["成交量"].values.astype(float)

        # 价格归一化
        case_match_high = ctx["high"].values[match_start_idx:match_end_idx + 1].astype(float)
        case_match_low = ctx["low"].values[match_start_idx:match_end_idx + 1].astype(float)

        if len(case_match_high) > 0 and len(tgt_close) > 0:
            case_min = float(case_match_low.min())
            case_max = float(case_match_high.max())
            tgt_min = float(tgt_low.min())
            tgt_max = float(tgt_high.max())
            tgt_range = tgt_max - tgt_min if tgt_max != tgt_min else 1
            case_range = case_max - case_min if case_max != case_min else 1

            def _map_price(arr):
                return (arr - tgt_min) / tgt_range * case_range + case_min

            mapped_open = _map_price(tgt_open)
            mapped_high = _map_price(tgt_high)
            mapped_low = _map_price(tgt_low)
            mapped_close = _map_price(tgt_close)

            tgt_x = list(range(match_start_idx, match_start_idx + len(tgt_close)))

            # 目标股 K线 — 橙色涨 / 蓝色跌，粗线条，高辨识度
            fig.add_trace(go.Candlestick(
                x=tgt_x, open=mapped_open, high=mapped_high,
                low=mapped_low, close=mapped_close,
                name="目标股(叠加)",
                increasing=dict(line=dict(color=_TGT_UP, width=2),
                                fillcolor="rgba(255,140,0,0.35)"),
                decreasing=dict(line=dict(color=_TGT_DOWN, width=2),
                                fillcolor="rgba(65,105,225,0.35)"),
                whiskerwidth=0.6,
            ), row=1, col=1)

            # 成交量归一化叠加
            case_match_vol = ctx["vol"].values[match_start_idx:match_end_idx + 1].astype(float)
            if case_match_vol.max() > 0 and tgt_vol.max() > 0:
                vol_scale = case_match_vol.max() / tgt_vol.max()
                mapped_vol = tgt_vol * vol_scale
                tgt_vol_colors = [_TGT_UP if c >= o else _TGT_DOWN
                                  for c, o in zip(tgt_close, tgt_open)]
                fig.add_trace(go.Bar(
                    x=tgt_x, y=mapped_vol, name="目标股成交量",
                    marker_color=tgt_vol_colors, opacity=0.4,
                ), row=2, col=1)

    # 匹配段背景高亮
    if match_indices:
        fig.add_vrect(
            x0=match_start_idx - 0.5, x1=match_end_idx + 0.5,
            fillcolor=_MATCH_BG, line_width=1, line_color=_MATCH_BORDER,
            annotation_text="匹配段", annotation_position="top left",
            annotation_font_size=10, annotation_font_color="#3366cc",
            row=1, col=1,
        )
        fig.add_vrect(
            x0=match_start_idx - 0.5, x1=match_end_idx + 0.5,
            fillcolor=_MATCH_BG, line_width=0,
            row=2, col=1,
        )

    # 案例股成交量
    bar_colors = [_UP if c >= o else _DOWN for c, o in zip(closes, opens)]
    fig.add_trace(go.Bar(
        x=x_idx, y=ctx["vol"], name="成交量",
        marker_color=bar_colors, opacity=0.65,
    ), row=2, col=1)

    _apply_layout(fig, None, 420, tick_vals, tick_text, show_legend=True)
    st.plotly_chart(fig, use_container_width=True,
                    config={"displayModeBar": False, "responsive": True,
                            "scrollZoom": False})


# ══════════════════════════════════════════════════════════════════════════════
# 价值投机雷达图
# ══════════════════════════════════════════════════════════════════════════════

def render_radar(signal: dict) -> None:
    """渲染四维价值投机雷达图"""
    categories = ["基本面强度", "题材正宗度", "技术启动度", "资金关注度"]
    values = [
        signal["fundamental"],
        signal["catalyst"],
        signal["technical"],
        signal["capital"],
    ]
    # 闭合雷达图
    values_closed = values + [values[0]]
    categories_closed = categories + [categories[0]]

    # 70分参考线
    ref_line = [70] * 5

    fig = go.Figure()

    # 填充区域
    fig.add_trace(go.Scatterpolar(
        r=values_closed,
        theta=categories_closed,
        fill="toself",
        fillcolor="rgba(99,102,241,0.15)",
        line=dict(color="#6366f1", width=2.5),
        name="当前评分",
        marker=dict(size=8, color="#6366f1"),
    ))

    # 70分参考线
    fig.add_trace(go.Scatterpolar(
        r=ref_line,
        theta=categories_closed,
        fill=None,
        line=dict(color="#f59e0b", width=1, dash="dot"),
        name="共振线(70)",
        marker=dict(size=0),
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True, range=[0, 100],
                tickvals=[20, 40, 60, 80, 100],
                ticktext=["20", "40", "60", "80", "100"],
                gridcolor="#e8e8e8",
                tickfont=dict(size=9, color="#999"),
            ),
            angularaxis=dict(
                tickfont=dict(size=12, color=_TEXT, family="'Noto Sans SC',sans-serif"),
                gridcolor="#e8e8e8",
            ),
            bgcolor=_BG,
        ),
        template="plotly_white",
        height=320, autosize=True,
        paper_bgcolor=_PAPER,
        margin=dict(t=30, b=10, l=30, r=30),
        legend=dict(
            orientation="h", y=-0.05, x=0.5, xanchor="center",
            font=dict(size=10, color=_TEXT),
        ),
        dragmode=False,
    )

    st.plotly_chart(fig, use_container_width=True,
                    config={"displayModeBar": False, "responsive": True})


def render_radar_5d(signal: dict) -> None:
    """渲染四维综合投研雷达图"""
    categories = ["基本面", "预期差", "技术面", "资金面"]
    values = [
        signal["fundamental"],
        signal["expectation"],
        signal["technical"],
        signal["capital"],
    ]
    values_closed = values + [values[0]]
    categories_closed = categories + [categories[0]]

    ref_line = [75] * 5  # 共振线

    fig = go.Figure()

    fig.add_trace(go.Scatterpolar(
        r=values_closed,
        theta=categories_closed,
        fill="toself",
        fillcolor="rgba(99,102,241,0.15)",
        line=dict(color="#6366f1", width=2.5),
        name="当前评分",
        marker=dict(size=8, color="#6366f1"),
    ))

    fig.add_trace(go.Scatterpolar(
        r=ref_line,
        theta=categories_closed,
        fill=None,
        line=dict(color="#f59e0b", width=1, dash="dot"),
        name="共振线(75)",
        marker=dict(size=0),
    ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True, range=[0, 100],
                tickvals=[20, 40, 60, 80, 100],
                ticktext=["20", "40", "60", "80", "100"],
                gridcolor="#e8e8e8",
                tickfont=dict(size=9, color="#999"),
            ),
            angularaxis=dict(
                tickfont=dict(size=12, color=_TEXT, family="'Noto Sans SC',sans-serif"),
                gridcolor="#e8e8e8",
            ),
            bgcolor=_BG,
        ),
        template="plotly_white",
        height=320, autosize=True,
        paper_bgcolor=_PAPER,
        margin=dict(t=30, b=10, l=30, r=30),
        legend=dict(
            orientation="h", y=-0.05, x=0.5, xanchor="center",
            font=dict(size=10, color=_TEXT),
        ),
        dragmode=False,
    )

    st.plotly_chart(fig, use_container_width=True,
                    config={"displayModeBar": False, "responsive": True})


# ══════════════════════════════════════════════════════════════════════════════
# 估值历史分位图
# ══════════════════════════════════════════════════════════════════════════════

def render_valuation_bands(val_df: pd.DataFrame, name: str) -> None:
    """渲染 PE/PB 历史分位图 — 面积图 + 当前值标记，缓存到 session_state"""
    if val_df.empty:
        st.info("暂无历史估值数据")
        return

    df = val_df.copy()
    # 估值图缓存 key
    _val_cache_prefix = f"_fig_val_{name}_{len(val_df)}"
    # 确保有日期列
    date_col = "trade_date" if "trade_date" in df.columns else df.columns[0]
    df["date_str"] = df[date_col].astype(str)

    metrics = []
    if "pe_ttm" in df.columns and df["pe_ttm"].dropna().shape[0] > 50:
        metrics.append(("pe_ttm", "PE(TTM)", "#6366f1"))
    if "pb" in df.columns and df["pb"].dropna().shape[0] > 50:
        metrics.append(("pb", "PB", "#ec4899"))

    if not metrics:
        st.info("估值数据不足，无法生成分位图")
        return

    tabs = st.tabs([label for _, label, _ in metrics])

    for tab, (col, label, color) in zip(tabs, metrics):
        with tab:
            # 检查缓存
            _vcache_key = f"{_val_cache_prefix}_{col}"
            _vcached = st.session_state.get(_vcache_key)
            if _vcached is not None:
                c1, c2, c3 = st.columns(3)
                with c1: st.metric(f"当前{label}", _vcached["current"])
                with c2: st.metric("历史分位", _vcached["percentile"])
                with c3:
                    st.markdown(
                        f'<div style="text-align:center;padding-top:0.4rem;">'
                        f'<span style="font-size:0.72rem;color:#9ca3af;">估值状态</span><br>'
                        f'<span style="font-size:1.15rem;font-weight:800;color:{_vcached["status_color"]};">'
                        f'{_vcached["status"]}</span></div>',
                        unsafe_allow_html=True,
                    )
                st.plotly_chart(_vcached["fig"], use_container_width=True,
                                config={"displayModeBar": False, "responsive": True})
                continue

            series = df[col].dropna()
            if len(series) < 50:
                st.info(f"{label} 数据不足")
                continue

            # 过滤极端值（保留1-99百分位内数据，用于更合理的展示）
            q01, q99 = series.quantile(0.01), series.quantile(0.99)
            mask = (df[col] >= q01) & (df[col] <= q99)
            plot_df = df[mask].copy()

            current_val = df[col].iloc[-1]
            if pd.isna(current_val):
                st.info(f"当前{label}值不可用")
                continue

            # 计算当前值所处的历史百分位
            percentile = (series < current_val).sum() / len(series) * 100

            # 百分位线
            p10 = series.quantile(0.10)
            p25 = series.quantile(0.25)
            p50 = series.quantile(0.50)
            p75 = series.quantile(0.75)
            p90 = series.quantile(0.90)

            # 估值状态判断
            if percentile <= 20:
                status = "极度低估"
                status_color = "#16a34a"
            elif percentile <= 40:
                status = "相对低估"
                status_color = "#22c55e"
            elif percentile <= 60:
                status = "估值适中"
                status_color = "#f59e0b"
            elif percentile <= 80:
                status = "相对高估"
                status_color = "#f97316"
            else:
                status = "极度高估"
                status_color = "#ef4444"

            # 指标卡片
            c1, c2, c3 = st.columns(3)
            with c1:
                st.metric(f"当前{label}", f"{current_val:.2f}")
            with c2:
                st.metric("历史分位", f"{percentile:.0f}%")
            with c3:
                st.markdown(
                    f'<div style="text-align:center;padding-top:0.4rem;">'
                    f'<span style="font-size:0.72rem;color:#9ca3af;">估值状态</span><br>'
                    f'<span style="font-size:1.15rem;font-weight:800;color:{status_color};">{status}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

            # 绘制面积图
            x = list(range(len(plot_df)))
            tick_step = max(1, len(x) // 6)
            tick_vals = x[::tick_step]
            tick_text = [plot_df["date_str"].iloc[i][:7] for i in tick_vals]  # YYYY-MM

            fig = go.Figure()

            # 填充区域：10-90百分位
            fig.add_hline(y=p90, line_dash="dot", line_color="#fca5a5", line_width=0.8,
                         annotation_text="90%", annotation_position="right")
            fig.add_hline(y=p75, line_dash="dot", line_color="#fed7aa", line_width=0.8,
                         annotation_text="75%", annotation_position="right")
            fig.add_hline(y=p50, line_dash="dash", line_color="#e5e7eb", line_width=1,
                         annotation_text="中位数", annotation_position="right")
            fig.add_hline(y=p25, line_dash="dot", line_color="#bbf7d0", line_width=0.8,
                         annotation_text="25%", annotation_position="right")
            fig.add_hline(y=p10, line_dash="dot", line_color="#86efac", line_width=0.8,
                         annotation_text="10%", annotation_position="right")

            # 主线
            fig.add_trace(go.Scatter(
                x=x, y=plot_df[col].values, name=label,
                line=dict(color=color, width=1.5), mode="lines",
                fill="tozeroy", fillcolor=f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.08)",
            ))

            # 当前值标记
            fig.add_trace(go.Scatter(
                x=[x[-1]], y=[current_val],
                mode="markers+text", name="当前",
                marker=dict(color=status_color, size=10, symbol="diamond",
                           line=dict(color="white", width=2)),
                text=[f"{current_val:.2f}"],
                textposition="top center",
                textfont=dict(size=11, color=status_color, family="Nunito"),
            ))

            fig.update_layout(
                template="plotly_white",
                height=280, autosize=True,
                plot_bgcolor=_BG, paper_bgcolor=_PAPER,
                font=dict(family="'Noto Sans SC','Microsoft YaHei',sans-serif",
                          color=_TEXT, size=10),
                margin=dict(t=10, b=10, l=5, r=45),
                hovermode="x unified",
                dragmode=False,
                legend=dict(visible=False),
                xaxis=dict(
                    type="linear", tickvals=tick_vals, ticktext=tick_text,
                    gridcolor=_GRID, gridwidth=0.5, zeroline=False,
                    tickfont=dict(size=9, color="#666"),
                ),
                yaxis=dict(
                    gridcolor=_GRID, gridwidth=0.5, zeroline=False,
                    tickfont=dict(size=9, color="#666"),
                ),
            )

            # 缓存到 session_state
            st.session_state[_vcache_key] = {
                "fig": fig,
                "current": f"{current_val:.2f}",
                "percentile": f"{percentile:.0f}%",
                "status": status,
                "status_color": status_color,
            }
            st.plotly_chart(fig, use_container_width=True,
                            config={"displayModeBar": False, "responsive": True})


# ══════════════════════════════════════════════════════════════════════════════
# 浅色主题布局
# ══════════════════════════════════════════════════════════════════════════════

def _apply_layout(fig, title, height, tick_vals, tick_text, show_legend=False):
    """统一应用浅色主题布局"""
    fig.update_layout(
        title=dict(text=f"<b>{title}</b>" if title else "",
                   font=dict(size=12, color=_TEXT)) if title else {},
        template="plotly_white",
        height=height, autosize=True,
        xaxis_rangeslider_visible=False,
        plot_bgcolor=_BG, paper_bgcolor=_PAPER,
        font=dict(family="'Noto Sans SC','Microsoft YaHei',sans-serif",
                  color=_TEXT, size=10),
        legend=dict(
            orientation="h", y=1.06, x=0,
            font=dict(size=9, color=_TEXT),
            bgcolor="rgba(255,255,255,0.8)",
        ) if show_legend else dict(visible=False),
        margin=dict(t=35 if title else 25, b=10, l=5, r=5),
        hovermode="x unified",
        dragmode=False,
        bargap=0.15,
    )
    for ax_name in ["xaxis", "xaxis2"]:
        fig.update_layout(**{
            ax_name: dict(
                type="linear",
                tickvals=tick_vals, ticktext=tick_text,
                gridcolor=_GRID, gridwidth=0.5,
                zeroline=False, showgrid=True,
                tickfont=dict(size=9, color="#666"),
            )
        })
    fig.update_yaxes(gridcolor=_GRID, gridwidth=0.5, zeroline=False,
                     tickfont=dict(size=9, color="#666"))
