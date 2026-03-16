"""综合投研报告 — 流式输出 + 五维评分 + 执行摘要"""

import re
import logging
import streamlit as st

from config import MODEL_CONFIGS
from ai.client import call_ai_stream, call_ai, add_tokens
from ai.prompts_report import build_report_prompt, build_summary_prompt, SUMMARY_SYSTEM
from data.report_data import build_report_context
from data.indicators import compute_indicators, format_indicators_section
from data.tushare_client import price_summary, to_code6

logger = logging.getLogger(__name__)


def _cleanup_report_text(text: str) -> str:
    """清理报告文本：隐藏 SCORES 块 + 修复未闭合的 markdown 加粗"""
    # 1. 移除 <<<SCORES>>>...<<<END_SCORES>>> 块（已解析为评分）
    text = re.sub(r"<<<SCORES>>>.*?<<<END_SCORES>>>", "", text, flags=re.DOTALL)
    # 也移除单独的标记（如果 END 缺失）
    text = text.replace("<<<SCORES>>>", "").replace("<<<END_SCORES>>>", "")

    # 2. 修复未闭合的 ** 加粗（逐行检查）
    lines = text.split("\n")
    fixed = []
    for line in lines:
        count = line.count("**")
        if count % 2 == 1:
            # 奇数个 ** → 在末尾补一个
            line = line.rstrip() + "**"
        fixed.append(line)
    return "\n".join(fixed)


def get_summary_model_cfg(main_cfg: dict) -> dict:
    """为摘要生成选择更轻量的模型配置

    规则：doubao → 用 mini；openrouter → 用 gpt-4o；其他不变
    """
    provider = main_cfg.get("provider", "")

    if provider == "doubao":
        # 找 mini 版
        for name, cfg in MODEL_CONFIGS.items():
            if cfg.get("provider") == "doubao" and "mini" in cfg.get("model", "").lower():
                return cfg
        return main_cfg

    if provider == "openrouter":
        # 降级到 gpt-4o
        for name, cfg in MODEL_CONFIGS.items():
            if cfg.get("provider") == "openrouter" and "gpt-4o" in cfg.get("model", ""):
                return cfg
        return main_cfg

    # qwen / zhipu / deepseek — 保持原样
    return main_cfg


def parse_scores(text: str) -> dict | None:
    """从报告文本中解析 <<<SCORES>>> 块

    Returns:
        {"基本面": 7, "预期差": 6, "技术面": 8, "资金面": 5, "舆情情绪": 6} or None
    """
    m = re.search(r"<<<SCORES>>>(.*?)<<<END_SCORES>>>", text, re.DOTALL)
    if not m:
        return None

    block = m.group(1)
    scores = {}
    for line in block.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        # 匹配 "基本面: 7/10" 或 "基本面：7 / 10"
        match = re.match(r"(.+?)[：:]\s*(\d+(?:\.\d+)?)\s*/\s*10", line)
        if match:
            dim = match.group(1).strip()
            score = float(match.group(2))
            scores[dim] = score

    return scores if scores else None


def run_deep_report(client, cfg, selected_model, username):
    """执行综合投研报告的完整流程

    1. build_report_context() 获取全量数据
    2. 构建 prompt + 技术指标
    3. 流式输出报告正文
    4. 解析五维评分
    5. 流式生成执行摘要
    6. 存入 session_state
    """
    name = st.session_state.get("stock_name", "")
    ts_code = st.session_state.get("stock_code", "")
    code6 = to_code6(ts_code)

    if not name or not ts_code:
        st.error("请先选择股票")
        return

    # ── 1. 数据采集 ──────────────────────────────────────────────
    with st.status(f"📡 正在采集 {name}（{code6}）全量数据...", expanded=True) as status:
        context, raw_data = build_report_context(
            ts_code, name,
            progress_cb=lambda msg: st.write(msg),
        )
        status.update(label=f"✅ {name} 数据采集完成", state="complete")

    # ── 2. 构建 prompt ───────────────────────────────────────────
    price_df = raw_data.get("_price_df")
    import pandas as pd
    if price_df is None or (isinstance(price_df, pd.DataFrame) and price_df.empty):
        price_df = st.session_state.get("price_df", pd.DataFrame())

    price_snap = price_summary(price_df) if not price_df.empty else "暂无K线数据"
    indicators = compute_indicators(price_df)
    ind_section = format_indicators_section(indicators)

    user_prompt, system_prompt = build_report_prompt(
        name, ts_code, context, price_snap, ind_section,
    )

    # ── 3. 摘要占位符（先创建，稍后填充）──────────────────────────
    summary_placeholder = st.empty()

    # ── 4. 心跳 + 流式输出报告正文 ────────────────────────────────
    import time as _time
    import threading
    import queue

    _HEARTBEAT_TIPS = [
        f"🤖 正在连接 {selected_model}...",
        "📡 正在发送分析请求...",
        "🌐 AI 正在联网搜索最新资讯...",
        "🧠 AI 正在深度思考中...",
        "📊 正在整理多维度数据...",
        "✍️ 即将开始输出报告...",
        "⏳ AI 还在思考，请耐心等待...",
        "📝 分析内容较多，稍等片刻...",
        "🔍 正在交叉验证各维度信号...",
        "📈 报告即将生成，请稍候...",
    ]

    # 用后台线程消费流，通过 queue 传 chunk 到主线程
    _chunk_queue = queue.Queue()
    _SENTINEL = object()  # 结束标记
    _stream_error = [None]

    def _stream_worker():
        try:
            raw = call_ai_stream(
                client, cfg, user_prompt,
                system=system_prompt,
                max_tokens=12000,
                username=username,
            )
            for chunk in raw:
                _chunk_queue.put(chunk)
            if hasattr(raw, 'error') and raw.error:
                _stream_error[0] = raw.error
        except Exception as e:
            _stream_error[0] = str(e)
        finally:
            _chunk_queue.put(_SENTINEL)

    worker = threading.Thread(target=_stream_worker, daemon=True)
    worker.start()

    # Phase A: 心跳等待 — 主线程每3秒检查 queue，没有 chunk 就刷心跳
    heartbeat_area = st.empty()
    tip_idx = 0
    start_time = _time.time()
    got_first = False
    full_text = ""

    while not got_first:
        try:
            chunk = _chunk_queue.get(timeout=3)
            if chunk is _SENTINEL:
                break
            full_text += chunk
            got_first = True
        except queue.Empty:
            # 3秒没收到 → 更新心跳
            elapsed = int(_time.time() - start_time)
            tip = _HEARTBEAT_TIPS[min(tip_idx, len(_HEARTBEAT_TIPS) - 1)]
            heartbeat_area.info(f"{tip}（已等待 {elapsed}s）")
            tip_idx += 1

    heartbeat_area.empty()  # 收到第一个 chunk，清除心跳

    # Phase B: 逐 chunk 渲染到页面（带打字光标）
    st.markdown(f"#### 📋 {name} · 综合投研报告")
    report_area = st.empty()

    if got_first:
        report_area.markdown(full_text + "▌")

        while True:
            try:
                chunk = _chunk_queue.get(timeout=120)
            except queue.Empty:
                break
            if chunk is _SENTINEL:
                break
            full_text += chunk
            report_area.markdown(full_text + "▌")

    report_area.markdown(full_text)  # 最终渲染（去掉光标）

    if _stream_error[0]:
        st.error(f"报告生成出错：{_stream_error[0]}")
        return

    if not full_text or len(full_text) < 100:
        st.warning("报告生成内容过短，模型可能响应异常")
        return

    # ── 5. 解析评分 + 清理文本 ───────────────────────────────────
    scores = parse_scores(full_text)
    # 清理正文：移除 SCORES 块（已解析），修复未闭合加粗
    full_text = _cleanup_report_text(full_text)

    # ── 6. 生成执行摘要（直接用主模型，避免降级导致空响应）──────
    summary_prompt = build_summary_prompt(full_text)

    _heartbeat_summary = st.empty()
    _heartbeat_summary.info("📝 正在生成执行摘要...")

    summary_text, summary_err = call_ai(
        client, cfg, summary_prompt,
        system=SUMMARY_SYSTEM,
        max_tokens=1500,
        username=username,
    )
    _heartbeat_summary.empty()

    if summary_err:
        summary_text = f"摘要生成失败：{summary_err}"

    # 填充摘要占位符
    if summary_text and "未返回内容" not in summary_text and "失败" not in summary_text:
        with summary_placeholder.container():
            with st.container(border=True):
                st.caption("📊 执行摘要")
                st.markdown(summary_text)
    else:
        summary_placeholder.empty()

    # ── 7. 存入 session_state ─────────────────────────────────────
    analyses = st.session_state.get("analyses", {})
    analyses["comprehensive"] = full_text
    st.session_state["analyses"] = analyses
    st.session_state["report_summary"] = summary_text
    st.session_state["report_scores"] = scores

    # 存储额外数据供信号计算使用
    cap = context.get("capital", "")
    if cap and len(cap) > 20:
        st.session_state["stock_capital"] = cap
    nb = context.get("northbound", "")
    if nb and "暂无" not in nb:
        st.session_state["stock_northbound"] = nb
    margin = context.get("margin", "")
    if margin and "暂无" not in margin:
        st.session_state["stock_margin"] = margin

    logger.info("[deep_report] %s 综合报告完成，评分: %s", name, scores)
