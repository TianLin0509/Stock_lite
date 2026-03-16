"""同步分析执行 — 无轮询、无后台线程，直接返回结果"""

import logging
import pandas as pd
from ai.client import call_ai, add_tokens
from ai.context import build_analysis_context
from ai.prompts import (
    build_expectation_prompt,
    build_trend_prompt,
    build_fundamentals_prompt,
    build_sentiment_prompt,
    build_sector_prompt,
    build_holders_prompt,
)
from data.tushare_client import price_summary, to_code6

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# 数据自取 + prompt 构建
# ══════════════════════════════════════════════════════════════════════════════

def _build_trend(name, tscode, df, progress_cb=None):
    """趋势分析：计算技术指标 + 并行获取资金/龙虎榜/北向/融资融券数据"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from data.tushare_client import (
        get_capital_flow, get_dragon_tiger,
        get_northbound_flow, get_margin_trading,
    )
    from data.indicators import compute_indicators, format_indicators_section

    if progress_cb:
        progress_cb("📊 计算K线技术指标 & 并行获取资金数据...")
    psmry = price_summary(df)

    # 计算 RSI / MACD / 布林带
    indicators = compute_indicators(df)
    ind_section = format_indicators_section(indicators)

    _data_fns = {
        "cap": lambda: get_capital_flow(tscode),
        "dragon": lambda: get_dragon_tiger(tscode),
        "nb": lambda: get_northbound_flow(tscode),
        "margin": lambda: get_margin_trading(tscode),
    }
    _results = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futs = {pool.submit(fn): key for key, fn in _data_fns.items()}
        for fut in as_completed(futs):
            _results[futs[fut]] = fut.result()

    cap, _ = _results["cap"]
    dragon, _ = _results["dragon"]
    nb, _ = _results["nb"]
    margin, _ = _results["margin"]
    if progress_cb:
        progress_cb("✅ 资金数据获取完成")

    prompt_result = build_trend_prompt(name, tscode, psmry, cap, dragon, nb, margin,
                                       indicators_section=ind_section)
    # 返回 (prompt, system, extra_data) — extra_data 供主线程存入 session_state
    p, s = prompt_result
    extra = {"capital_flow": cap, "northbound": nb, "margin": margin}
    return p, s, extra


def _build_sector(name, tscode, info, progress_cb=None):
    """板块分析：获取同行业对比数据"""
    from data.tushare_client import get_sector_peers
    if progress_cb:
        progress_cb("🏭 获取同行业个股对比数据...")
    sector_data, _ = get_sector_peers(tscode)
    return build_sector_prompt(name, tscode, info, sector_data)


def _build_holders(name, tscode, info, progress_cb=None):
    """股东分析：获取股东/质押/基金持仓数据"""
    from data.tushare_client import (
        get_holders_info, get_pledge_info, get_fund_holdings,
    )
    if progress_cb:
        progress_cb("👥 获取十大股东数据...")
    holders, _ = get_holders_info(tscode)
    if progress_cb:
        progress_cb("⚠️ 获取股权质押数据...")
    pledge, _ = get_pledge_info(tscode)
    if progress_cb:
        progress_cb("🏛️ 获取基金持仓数据...")
    fund, _ = get_fund_holdings(tscode)
    return build_holders_prompt(name, tscode, info, holders, pledge, fund)


# ══════════════════════════════════════════════════════════════════════════════
# 同步分析入口
# ══════════════════════════════════════════════════════════════════════════════

def run_analysis_sync(key, client, cfg, model_name, name, tscode, info, fin, df,
                      username="", progress_cb=None):
    """同步分析：构建prompt → call_ai → 返回 (result, error, extra_data)

    extra_data: 趋势分析时返回资金流数据供主线程存入 session_state，其他分析为 None。
    可从 ThreadPoolExecutor worker 调用（不要在 worker 中传 st.write 作为 progress_cb）。
    """
    # 分析调度表：key → (label, build_fn, build_args)
    dispatch = {
        "expectation":  ("预期差分析", build_expectation_prompt, (name, tscode, info, fin, df)),
        "trend":        ("K线趋势研判", _build_trend, (name, tscode, df)),
        "fundamentals": ("基本面剖析", build_fundamentals_prompt, (name, tscode, info, fin)),
        "sentiment":    ("舆情情绪分析", build_sentiment_prompt, (name, tscode, info)),
        "sector":       ("板块联动分析", _build_sector, (name, tscode, info)),
        "holders":      ("股东/机构动向分析", _build_holders, (name, tscode, info)),
    }

    if key not in dispatch:
        return None, f"未知分析类型: {key}", None

    label, build_fn, build_args = dispatch[key]

    try:
        if progress_cb:
            progress_cb(f"📡 正在连接 {model_name}...")

        extra_data = None

        # 趋势分析返回 (prompt, system, extra_data)
        if key == "trend":
            p, s, extra_data = build_fn(*build_args, progress_cb=progress_cb)
        elif key in ("sector", "holders"):
            p, s = build_fn(*build_args, progress_cb=progress_cb)
        else:
            p, s = build_fn(*build_args)

        if progress_cb:
            progress_cb(f"🤖 AI 正在进行{label}...")

        text, err = call_ai(client, cfg, p, system=s, max_tokens=8000,
                            username=username)

        if err:
            return None, err, None

        if progress_cb:
            progress_cb(f"✅ {label}完成！")

        return text, None, extra_data

    except Exception as e:
        logger.debug("[run_analysis_sync/%s] 异常: %s", key, e)
        return None, f"{label}异常：{e}", None


# ══════════════════════════════════════════════════════════════════════════════
# MoE 同步辩论
# ══════════════════════════════════════════════════════════════════════════════

def run_moe_sync(client, cfg, model_name, name, tscode, analyses,
                 username="", progress_cb=None):
    """同步 MoE 辩论（串行调用，避免豆包并发空响应）

    moe_data = {"roles": {...}, "ceo": "...", "done": True}
    """
    from analysis.moe import MOE_ROLES, CEO_SYSTEM
    code6 = to_code6(tscode)

    try:
        if progress_cb:
            progress_cb("📋 汇总分析结果作为辩论素材...")
        context = build_analysis_context(analyses)

        role_results = {}
        for idx, role in enumerate(MOE_ROLES, 1):
            if progress_cb:
                progress_cb(f"🎙️ [{idx}/{len(MOE_ROLES)}] {role['badge']} 正在发表观点...")

            prompt = f"""辩论标的：{name}（{code6}）

## 分析背景
{context}

---
从你的角色视角给出明确判断，控制在250字以内：

**核心判断：** 看多/看空/中性/观望
**判断依据（3条，引用上方分析中的具体数据）：**
1.
2.
3.
**操作建议：**（具体操作+入场价+止损价+目标价）
**最大风险：**（1条，具体描述）

保持角色特色和语言风格。"""
            text, err = call_ai(client, cfg, prompt,
                                system=role["system"], max_tokens=800,
                                username=username)
            if err:
                text = f"⚠️ 该角色分析失败：{err}"
            role_results[role["key"]] = text
            if progress_cb:
                progress_cb(f"  ✓ {role['badge']} 观点已提交")

        if progress_cb:
            progress_cb("👔 首席执行官正在综合五方观点，做最终裁决...")

        roles_text = "\n\n".join(
            f"【{r['badge']}】\n{role_results.get(r['key'], '')}"
            for r in MOE_ROLES
        )

        ceo_prompt = f"""标的：{name}（{code6}）

## 五位专家观点
{roles_text}

## 原始分析摘要
{context}

---
综合以上信息给出最终操作裁决。
⚠️ **散户（韭菜代表）的观点是反向指标，逆向参考。**
💡 **特别关注价值投机手的「三维共振」判断，若基本面+题材+技术三者不共振，需降级评价。**

## 🎯 最终操作结论

**操作评级：** 强烈买入/买入/谨慎介入/持有观察/减持/回避

**裁决逻辑（3-4句，说明为什么这样判断）：**

**目标价体系：**
| 维度 | 价格 | 依据 |
|-----|-----|-----|
| 当前股价 | ___元 | — |
| 短线目标（1-2周）| | |
| 中线目标（1-3月）| | |
| 止损位 | | |

**仓位策略：** 建议仓位___%, 介入方式：___

**核心逻辑（2条）：**
1.
2.

**核心风险（2条）：**
1.
2.

**策略有效期：** ___个交易日，若___则策略失效。"""

        ceo_text, ceo_err = call_ai(client, cfg, ceo_prompt,
                                     system=CEO_SYSTEM, max_tokens=2000,
                                     username=username)
        if ceo_err:
            ceo_text = f"⚠️ CEO裁决生成失败：{ceo_err}\n\n建议切换其他模型后重新尝试。"

        if progress_cb:
            progress_cb("✅ MoE 六方会谈完成！")

        moe_data = {"roles": role_results, "ceo": ceo_text, "done": True}
        return moe_data, None

    except Exception as e:
        logger.debug("[run_moe_sync] 异常: %s", e)
        return None, f"MoE 辩论异常：{e}"
