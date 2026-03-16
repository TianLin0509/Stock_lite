"""红蓝军对决 UI — 蓝军挑战 + 终审裁决"""

import re
import streamlit as st
import threading
import queue
import time as _time

from ai.client import call_ai_stream
from ai.prompts_challenge import build_blue_team_prompt, build_verdict_prompt


def _stream_with_heartbeat(client, cfg, user_prompt, system_prompt,
                           max_tokens, username, selected_model, role_label):
    """通用心跳 + 流式渲染（复用 deep_report 的模式）"""
    _TIPS = [
        f"🤖 正在连接 {selected_model}...",
        f"📡 {role_label}正在分析中...",
        f"🧠 {role_label}深度思考中...",
        f"✍️ 即将开始输出...",
        f"⏳ 请耐心等待...",
    ]

    _chunk_queue = queue.Queue()
    _SENTINEL = object()
    _error = [None]

    def _worker():
        try:
            raw = call_ai_stream(client, cfg, user_prompt,
                                 system=system_prompt,
                                 max_tokens=max_tokens,
                                 username=username)
            for chunk in raw:
                _chunk_queue.put(chunk)
            if hasattr(raw, 'error') and raw.error:
                _error[0] = raw.error
        except Exception as e:
            _error[0] = str(e)
        finally:
            _chunk_queue.put(_SENTINEL)

    worker = threading.Thread(target=_worker, daemon=True)
    worker.start()

    # 心跳等待
    heartbeat = st.empty()
    tip_idx = 0
    start = _time.time()
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
            elapsed = int(_time.time() - start)
            tip = _TIPS[min(tip_idx, len(_TIPS) - 1)]
            heartbeat.info(f"{tip}（已等待 {elapsed}s）")
            tip_idx += 1

    heartbeat.empty()

    # 流式渲染
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

    report_area.markdown(full_text)

    if _error[0]:
        st.error(f"{role_label}生成出错：{_error[0]}")
        return None

    return full_text


def run_blue_team(client, cfg, selected_model, username):
    """蓝军挑战流式生成"""
    red_report = st.session_state["analyses"]["comprehensive"]
    user_prompt, system_prompt = build_blue_team_prompt(red_report)

    st.markdown("#### 🔵 蓝军挑战报告")
    result = _stream_with_heartbeat(
        client, cfg, user_prompt, system_prompt,
        max_tokens=4000, username=username,
        selected_model=selected_model, role_label="蓝军",
    )

    if result:
        st.session_state["blue_team_report"] = result


def run_final_verdict(client, cfg, selected_model, username):
    """终审裁决流式生成"""
    red_report = st.session_state["analyses"]["comprehensive"]
    blue_report = st.session_state["blue_team_report"]
    user_prompt, system_prompt = build_verdict_prompt(red_report, blue_report)

    st.markdown("#### 👨‍⚖️ 终审裁决")
    result = _stream_with_heartbeat(
        client, cfg, user_prompt, system_prompt,
        max_tokens=3000, username=username,
        selected_model=selected_model, role_label="终审",
    )

    if result:
        st.session_state["final_verdict"] = result
        # 解析终审评分
        _parse_final_scores(result)


def _parse_final_scores(text):
    """从终审文本解析 <<<FINAL_SCORES>>> 块"""
    m = re.search(r"<<<FINAL_SCORES>>>(.*?)<<<END_FINAL_SCORES>>>", text, re.DOTALL)
    if not m:
        return
    block = m.group(1)
    scores = {}
    for line in block.strip().split("\n"):
        line = line.strip()
        if not line or line == "---":
            continue
        match = re.match(r"(.+?)[：:]\s*(\d+(?:\.\d+)?)\s*/\s*10", line)
        if match:
            scores[match.group(1).strip()] = float(match.group(2))
    if scores:
        st.session_state["final_scores"] = scores


def render_challenge_section(client, cfg_now, selected_model):
    """渲染红蓝军对决区域（Tab + 按钮）"""
    red_report = st.session_state.get("analyses", {}).get("comprehensive")
    if not red_report:
        return

    blue_report = st.session_state.get("blue_team_report")
    verdict = st.session_state.get("final_verdict")
    username = st.session_state.get("current_user", "")

    # ── pending 触发（在 Tab 渲染前执行）──
    if st.session_state.pop("_pending_blue_team", False) and client:
        run_blue_team(client, cfg_now, selected_model, username)
        st.rerun()

    if st.session_state.pop("_pending_verdict", False) and client:
        run_final_verdict(client, cfg_now, selected_model, username)
        st.rerun()

    # ── Tab 构建 ──
    tab_labels = ["📊 红军分析"]
    if blue_report:
        tab_labels.append("🔵 蓝军挑战")
    if verdict:
        tab_labels.append("👨‍⚖️ 终审裁决")

    tabs = st.tabs(tab_labels)

    with tabs[0]:
        st.markdown(red_report)

    if blue_report and len(tabs) > 1:
        with tabs[1]:
            st.markdown(blue_report)

    if verdict and len(tabs) > 2:
        with tabs[2]:
            st.markdown(verdict)

    # ── 按钮区（Tab 外部）──
    col1, col2 = st.columns(2)

    with col1:
        if not blue_report:
            if st.button("🔵 发起蓝军挑战", type="secondary", use_container_width=True,
                         help="让 AI 从质疑者视角审查分析报告"):
                st.session_state["_pending_blue_team"] = True
                st.rerun()
        else:
            st.button("✅ 蓝军已完成", disabled=True, use_container_width=True)

    with col2:
        if blue_report and not verdict:
            if st.button("👨‍⚖️ 请求终审裁决", type="secondary", use_container_width=True,
                         help="综合红蓝双方意见做最终判断"):
                st.session_state["_pending_verdict"] = True
                st.rerun()
        elif verdict:
            st.button("✅ 终审已完成", disabled=True, use_container_width=True)
        else:
            st.button("👨‍⚖️ 终审裁决", disabled=True, use_container_width=True,
                      help="请先完成蓝军挑战")

    if verdict:
        st.caption("✅ 红蓝军对决完成。三轮结果可在上方 Tab 中切换查看。")
