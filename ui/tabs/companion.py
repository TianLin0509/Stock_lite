"""Tab: 🤖 炒股伙伴 — 设置流程 + 聊天界面 + 快捷按钮"""

import re
import streamlit as st
from companion.templates import TEMPLATES, TEMPLATE_IDS
from companion.memory import (
    load_profile, create_profile, save_profile, delete_profile,
    load_memories, add_trade, add_watchlist, add_observation, add_lesson,
    append_message, clear_chat_history, load_chat_history, needs_summary,
)
from companion.context import build_companion_context, compress_chat_history


# ══════════════════════════════════════════════════════════════════════════
# 智能记忆提取 — 从聊天内容中自动检测并记录
# ══════════════════════════════════════════════════════════════════════════

def _auto_extract_memory(username: str, text: str):
    """从用户消息中自动提取交易/关注等信息"""
    toasts = []

    # 检测交易：买了/卖了 + 股票名 + 可选价格/数量
    buy_match = re.search(r'买了?[入进]?\s*(\S+?)\s*(\d+)\s*股\s*[@＠]?\s*(\d+\.?\d*)?', text)
    sell_match = re.search(r'卖了?[出掉]?\s*(\S+?)\s*(\d+)\s*股\s*[@＠]?\s*(\d+\.?\d*)?', text)

    if buy_match:
        trade = {
            "action": "买入",
            "stock": buy_match.group(1),
            "quantity": buy_match.group(2),
            "price": buy_match.group(3) or "",
            "reasoning": text[:100],
            "emotion_tag": "",
        }
        add_trade(username, trade)
        toasts.append(f"📝 已记录买入 {trade['stock']}")

    if sell_match:
        trade = {
            "action": "卖出",
            "stock": sell_match.group(1),
            "quantity": sell_match.group(2),
            "price": sell_match.group(3) or "",
            "reasoning": text[:100],
            "emotion_tag": "",
        }
        add_trade(username, trade)
        toasts.append(f"📝 已记录卖出 {trade['stock']}")

    # 检测教训
    lesson_match = re.search(r'教训[是：:]\s*(.+)', text)
    if lesson_match:
        add_lesson(username, lesson_match.group(1).strip())
        toasts.append("💡 已记录投资教训")

    return toasts


# ══════════════════════════════════════════════════════════════════════════
# 设置流程 — 首次创建伙伴
# ══════════════════════════════════════════════════════════════════════════

def _render_setup(username: str):
    """首次使用 — 选择模板创建伙伴"""
    st.markdown("### 🤖 创建你的炒股伙伴")
    st.markdown("选择一个最适合你投资风格的 AI 伙伴，它会记住你的操作、监督你的纪律、陪你一起成长。")
    st.markdown("---")

    # 模板卡片
    cols = st.columns(len(TEMPLATE_IDS))
    selected = st.session_state.get("_companion_template", "")

    for i, tid in enumerate(TEMPLATE_IDS):
        t = TEMPLATES[tid]
        with cols[i]:
            is_selected = selected == tid
            border = "2px solid #6366f1" if is_selected else "1px solid #e5e7eb"
            bg = "#f0f0ff" if is_selected else "#ffffff"
            st.markdown(
                f'<div style="border:{border};border-radius:12px;padding:16px;'
                f'background:{bg};text-align:center;min-height:180px;">'
                f'<div style="font-size:2.5rem;">{t["icon"]}</div>'
                f'<div style="font-weight:bold;margin:8px 0;">{t["name"]}</div>'
                f'<div style="font-size:0.8rem;color:#666;">{t["desc"]}</div>'
                f'<div style="font-size:0.75rem;color:#999;margin-top:6px;">风险偏好：{t["risk"]}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )
            if st.button(f"选择{t['name']}", key=f"sel_{tid}", use_container_width=True):
                st.session_state["_companion_template"] = tid
                st.rerun()

    if not selected:
        return

    st.markdown("---")
    t = TEMPLATES[selected]
    st.markdown(f"#### {t['icon']} {t['name']} — 投资纪律设置")

    # 默认纪律（可编辑）
    rules = []
    for j, rule in enumerate(t["default_rules"]):
        checked = st.checkbox(rule, value=True, key=f"rule_{j}")
        if checked:
            rules.append(rule)

    custom_rule = st.text_input("➕ 添加自定义纪律", placeholder="例如：不在情绪激动时交易",
                                key="custom_rule_input")
    if custom_rule:
        rules.append(custom_rule.strip())

    # 高级选项
    with st.expander("⚙️ 高级选项：自定义 System Prompt"):
        st.caption("修改后将覆盖默认模板。保留 `{memory_block}` 和 `{rules_block}` 占位符。")
        custom_prompt = st.text_area(
            "System Prompt", value=t["system_prompt"], height=200,
            key="custom_prompt_input",
        )

    if st.button("🚀 创建伙伴", type="primary", use_container_width=True):
        final_prompt = custom_prompt if custom_prompt != t["system_prompt"] else ""
        create_profile(username, selected, rules, final_prompt)
        st.session_state.pop("_companion_template", None)
        st.toast(f"{t['icon']} {t['name']} 已就绪！开始聊天吧")
        st.rerun()


# ══════════════════════════════════════════════════════════════════════════
# 快捷按钮行
# ══════════════════════════════════════════════════════════════════════════

def _render_quick_buttons(username: str):
    """渲染快捷操作按钮"""
    cols = st.columns(5)

    with cols[0]:
        if st.button("📝 记录交易", use_container_width=True, key="qb_trade"):
            st.session_state["_cmp_panel"] = "trade"
    with cols[1]:
        if st.button("⭐ 添加自选", use_container_width=True, key="qb_watch"):
            st.session_state["_cmp_panel"] = "watchlist"
    with cols[2]:
        if st.button("💡 记录感悟", use_container_width=True, key="qb_obs"):
            st.session_state["_cmp_panel"] = "observation"
    with cols[3]:
        if st.button("📊 导入分析", use_container_width=True, key="qb_import"):
            st.session_state["_cmp_panel"] = "import"
    with cols[4]:
        if st.button("⚙️ 设置", use_container_width=True, key="qb_settings"):
            st.session_state["_cmp_panel"] = "settings"

    panel = st.session_state.get("_cmp_panel", "")

    if panel == "trade":
        with st.expander("📝 记录交易", expanded=True):
            tc1, tc2 = st.columns(2)
            with tc1:
                t_stock = st.text_input("股票名称/代码", key="t_stock")
                t_action = st.selectbox("操作", ["买入", "卖出"], key="t_action")
            with tc2:
                t_price = st.text_input("价格", key="t_price")
                t_qty = st.text_input("数量（股）", key="t_qty")
            t_reason = st.text_input("理由", key="t_reason", placeholder="为什么做这笔交易？")
            t_emotion = st.selectbox("情绪标签", ["", "冷静", "冲动", "恐慌", "贪婪", "犹豫"],
                                     key="t_emotion")
            if st.button("✅ 保存交易", key="save_trade"):
                if t_stock:
                    add_trade(username, {
                        "action": t_action, "stock": t_stock,
                        "price": t_price, "quantity": t_qty,
                        "reasoning": t_reason, "emotion_tag": t_emotion,
                    })
                    st.toast(f"📝 已记录{t_action} {t_stock}")
                    st.session_state.pop("_cmp_panel", None)
                    st.rerun()

    elif panel == "watchlist":
        with st.expander("⭐ 添加自选股", expanded=True):
            w_code = st.text_input("股票代码", key="w_code")
            w_name = st.text_input("股票名称", key="w_name")
            w_reason = st.text_input("关注原因", key="w_reason")
            if st.button("✅ 添加", key="save_watch"):
                if w_code:
                    add_watchlist(username, {
                        "code": w_code, "name": w_name,
                        "reason": w_reason, "status": "关注中",
                    })
                    st.toast(f"⭐ 已添加自选 {w_name or w_code}")
                    st.session_state.pop("_cmp_panel", None)
                    st.rerun()

    elif panel == "observation":
        with st.expander("💡 记录感悟", expanded=True):
            o_content = st.text_area("你的观察或感悟", key="o_content", height=100)
            if st.button("✅ 保存", key="save_obs"):
                if o_content:
                    add_observation(username, o_content.strip())
                    st.toast("💡 已记录感悟")
                    st.session_state.pop("_cmp_panel", None)
                    st.rerun()

    elif panel == "import":
        with st.expander("📊 导入当前分析", expanded=True):
            analyses = st.session_state.get("analyses", {})
            stock_name = st.session_state.get("stock_name", "")
            if analyses and stock_name:
                label_map = {
                    "expectation": "预期差", "trend": "趋势", "fundamentals": "基本面",
                    "sentiment": "舆情", "sector": "板块", "holders": "股东",
                }
                parts = []
                for k, v in analyses.items():
                    if v:
                        parts.append(f"[{label_map.get(k, k)}] {v[:150]}")
                summary = f"关于 {stock_name} 的分析：\n" + "\n".join(parts)
                st.text_area("将导入以下内容", value=summary[:500], height=120,
                             disabled=True, key="import_preview")
                if st.button("✅ 导入为观察记忆", key="do_import"):
                    add_observation(username, summary[:500], tags=[stock_name])
                    st.toast(f"📊 已导入 {stock_name} 的分析")
                    st.session_state.pop("_cmp_panel", None)
                    st.rerun()
            else:
                st.info("当前没有可导入的分析结果。请先在主页分析一只股票。")

    elif panel == "settings":
        _render_settings(username)


def _render_settings(username: str):
    """设置面板"""
    with st.expander("⚙️ 伙伴设置", expanded=True):
        profile = load_profile(username)
        if not profile:
            return

        tid = profile.get("template_id", "")
        t = TEMPLATES.get(tid, {})
        st.markdown(f"当前伙伴：**{t.get('icon','')} {t.get('name', tid)}**")

        # 编辑纪律
        st.markdown("**投资纪律：**")
        rules = profile.get("user_rules", [])
        new_rules = []
        for j, r in enumerate(rules):
            keep = st.checkbox(r, value=True, key=f"sr_{j}")
            if keep:
                new_rules.append(r)
        new_rule = st.text_input("➕ 新增纪律", key="new_rule_settings")
        if new_rule:
            new_rules.append(new_rule.strip())

        # 编辑 prompt
        custom = profile.get("custom_system_prompt", "") or t.get("system_prompt", "")
        new_prompt = st.text_area("System Prompt", value=custom, height=150,
                                  key="settings_prompt")

        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            if st.button("💾 保存设置", key="save_settings"):
                profile["user_rules"] = new_rules
                default_prompt = t.get("system_prompt", "")
                profile["custom_system_prompt"] = new_prompt if new_prompt != default_prompt else ""
                save_profile(username, profile)
                st.toast("✅ 设置已保存")
                st.session_state.pop("_cmp_panel", None)
                st.rerun()
        with sc2:
            if st.button("🗑️ 清空聊天", key="clear_chat"):
                clear_chat_history(username)
                st.session_state.pop("_cmp_chat_msgs", None)
                st.toast("聊天记录已清空")
                st.rerun()
        with sc3:
            if st.button("❌ 删除伙伴", type="secondary", key="delete_companion"):
                st.session_state["_confirm_delete"] = True

        if st.session_state.get("_confirm_delete"):
            st.warning("⚠️ 确定要删除伙伴吗？所有记忆和聊天记录都将丢失。")
            dc1, dc2 = st.columns(2)
            with dc1:
                if st.button("确认删除", type="primary", key="confirm_del"):
                    delete_profile(username)
                    st.session_state.pop("_confirm_delete", None)
                    st.session_state.pop("_cmp_chat_msgs", None)
                    st.toast("伙伴已删除")
                    st.rerun()
            with dc2:
                if st.button("取消", key="cancel_del"):
                    st.session_state.pop("_confirm_delete", None)
                    st.rerun()


# ══════════════════════════════════════════════════════════════════════════
# 聊天界面
# ══════════════════════════════════════════════════════════════════════════

def _render_chat(username: str, client, cfg, selected_model: str):
    """日常使用 — 聊天界面"""
    from ai.client import call_ai_chat, call_ai

    profile = load_profile(username)
    tid = profile.get("template_id", "")
    t = TEMPLATES.get(tid, {})

    # 顶部：伙伴信息
    st.markdown(f"### {t.get('icon','')} {t.get('name', '炒股伙伴')}")

    # 快捷按钮行
    _render_quick_buttons(username)

    st.markdown("---")

    # 加载聊天记录到 session_state
    if "_cmp_chat_msgs" not in st.session_state:
        hist = load_chat_history(username)
        st.session_state["_cmp_chat_msgs"] = [
            {"role": m["role"], "content": m["content"]}
            for m in hist.get("messages", [])
        ]

    # 显示聊天消息
    chat_msgs = st.session_state["_cmp_chat_msgs"]
    for msg in chat_msgs:
        avatar = t.get("icon", "🤖") if msg["role"] == "assistant" else "👤"
        with st.chat_message(msg["role"], avatar=avatar):
            st.markdown(msg["content"])

    # 欢迎消息
    if not chat_msgs:
        with st.chat_message("assistant", avatar=t.get("icon", "🤖")):
            st.markdown(f"你好！我是你的{t.get('name', '炒股伙伴')}。"
                        f"有什么想聊的？可以和我讨论股票、记录交易、或者让我帮你复盘。")

    # 输入框
    user_input = st.chat_input("和伙伴聊聊...", key="companion_chat_input")

    if user_input:
        if not client:
            st.error("请先在左侧选择 AI 模型")
            return

        # 显示用户消息
        with st.chat_message("user", avatar="👤"):
            st.markdown(user_input)

        # 自动提取记忆
        toasts = _auto_extract_memory(username, user_input)
        for t_msg in toasts:
            st.toast(t_msg)

        # 保存用户消息
        append_message(username, "user", user_input)
        chat_msgs.append({"role": "user", "content": user_input})

        # 构建上下文并调用 AI
        system_prompt, api_messages = build_companion_context(username)

        # 组装完整消息列表
        full_messages = [{"role": "system", "content": system_prompt}]
        full_messages.extend(api_messages)

        with st.chat_message("assistant", avatar=t.get("icon", "🤖")):
            with st.spinner("思考中..."):
                reply, err = call_ai_chat(
                    client, cfg, full_messages,
                    max_tokens=2000, username=username,
                )

            if err:
                st.error(f"AI 回复失败：{err}")
                return

            st.markdown(reply)

        # 保存 AI 回复
        append_message(username, "assistant", reply)
        chat_msgs.append({"role": "assistant", "content": reply})

        # 检查是否需要压缩聊天记录
        if needs_summary(username):
            compress_chat_history(username, client, cfg, call_ai)

        st.rerun()


# ══════════════════════════════════════════════════════════════════════════
# 入口
# ══════════════════════════════════════════════════════════════════════════

def render_companion_tab(client, cfg, selected_model: str):
    """渲染炒股伙伴页面"""
    username = st.session_state.get("current_user", "")
    if not username:
        st.warning("请先登录")
        return

    profile = load_profile(username)
    if not profile:
        _render_setup(username)
    else:
        _render_chat(username, client, cfg, selected_model)
