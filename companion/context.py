"""炒股伙伴 — 构建完整 system prompt + 消息列表"""

import logging
from companion.templates import TEMPLATES
from companion.memory import (
    load_profile, load_memories, load_chat_history,
    save_chat_history, MAX_CHAT_MESSAGES,
)

logger = logging.getLogger(__name__)


def _build_memory_block(memories: dict) -> str:
    """将结构化记忆转为 system prompt 中的文本块"""
    parts = []

    # 自选股
    wl = memories.get("watchlist", [])
    if wl:
        items = [f"  - {w['code']} {w.get('name','')}：{w.get('reason','')}"
                 for w in wl[:10]]
        parts.append("【自选股】\n" + "\n".join(items))

    # 持仓快照
    ps = memories.get("portfolio_summary", "")
    if ps:
        parts.append(f"【持仓概况】\n  {ps}")

    # 近期交易（最近10条）
    trades = memories.get("trades", [])
    if trades:
        items = []
        for t in trades[:10]:
            emotion = f"（{t['emotion_tag']}）" if t.get("emotion_tag") else ""
            items.append(
                f"  - {t.get('timestamp','')[:10]} "
                f"{t.get('action','')}{t.get('stock','')} "
                f"@{t.get('price','')} x{t.get('quantity','')} "
                f"{t.get('reasoning','')}{emotion}"
            )
        parts.append("【近期交易】\n" + "\n".join(items))

    # 市场观察（最近5条）
    obs = memories.get("observations", [])
    if obs:
        items = [f"  - {o.get('timestamp','')[:10]} {o['content']}"
                 for o in obs[:5]]
        parts.append("【市场观察】\n" + "\n".join(items))

    # 教训（全部）
    lessons = memories.get("lessons", [])
    if lessons:
        items = [f"  - [{l.get('importance','中')}] {l['content']}"
                 for l in lessons]
        parts.append("【投资教训】\n" + "\n".join(items))

    if not parts:
        return "（用户尚未记录任何投资信息）"
    return "\n\n".join(parts)


def _build_rules_block(rules: list[str]) -> str:
    """将投资纪律转为 system prompt 中的规则块"""
    if not rules:
        return "（用户尚未设定投资纪律）"
    lines = [f"  {i+1}. {r}" for i, r in enumerate(rules)]
    block = "【用户投资纪律 — 你必须监督执行】\n" + "\n".join(lines)
    block += "\n\n⚠️ 当用户的操作或想法违反以上纪律时，你必须直接、明确地指出，不要含糊其辞。"
    return block


def build_companion_context(username: str) -> tuple[str, list[dict]]:
    """
    构建完整的 system prompt 和消息列表。
    返回 (system_prompt, messages_for_api)
    """
    profile = load_profile(username)
    if not profile:
        return "", []

    template_id = profile.get("template_id", "")
    template = TEMPLATES.get(template_id)
    if not template:
        return "", []

    # 构建 memory_block 和 rules_block
    memories = load_memories(username)
    memory_block = _build_memory_block(memories)
    rules_block = _build_rules_block(profile.get("user_rules", []))

    # 用户自定义 prompt 优先，否则用模板
    base_prompt = profile.get("custom_system_prompt") or template["system_prompt"]
    system_prompt = base_prompt.replace("{memory_block}", memory_block).replace("{rules_block}", rules_block)

    # 截断 system prompt（目标 ≤ 2000 字）
    if len(system_prompt) > 2000:
        system_prompt = system_prompt[:1950] + "\n\n...（记忆过长已截断）"

    # 构建消息列表
    history = load_chat_history(username)
    messages = []

    # 历史摘要作为首条 system 消息
    summary = history.get("summary_of_older", "")
    if summary:
        messages.append({
            "role": "system",
            "content": f"以下是之前对话的摘要：\n{summary}",
        })

    # 近期消息
    for msg in history.get("messages", [])[-MAX_CHAT_MESSAGES:]:
        messages.append({
            "role": msg["role"],
            "content": msg["content"],
        })

    return system_prompt, messages


def compress_chat_history(username: str, client, cfg, ai_call_fn):
    """
    当消息超过上限时，用 AI 压缩旧消息为摘要。
    ai_call_fn 签名: (client, cfg, prompt, system, max_tokens, username) -> (text, err)
    """
    history = load_chat_history(username)
    msgs = history.get("messages", [])
    if len(msgs) <= MAX_CHAT_MESSAGES:
        return

    # 将超出部分压缩
    old_msgs = msgs[:-MAX_CHAT_MESSAGES]
    keep_msgs = msgs[-MAX_CHAT_MESSAGES:]

    old_text = "\n".join(
        f"{'用户' if m['role']=='user' else '伙伴'}: {m['content']}"
        for m in old_msgs
    )

    prev_summary = history.get("summary_of_older", "")
    prompt = f"""请将以下对话内容压缩为一段简洁的摘要（不超过500字），保留关键信息（股票操作、投资决策、重要教训）。

{f'之前的摘要：{prev_summary}' if prev_summary else ''}

需要压缩的新对话：
{old_text}

请直接输出摘要，不要加标题或前缀。"""

    text, err = ai_call_fn(
        client, cfg, prompt,
        system="你是一个对话摘要助手，专注提取投资相关的关键信息。",
        max_tokens=800, username=username,
    )

    if not err and text:
        history["summary_of_older"] = text.strip()
        history["messages"] = keep_msgs
        save_chat_history(username, history)
        logger.info("[compress] 用户 %s 对话压缩完成，摘要 %d 字", username, len(text))
