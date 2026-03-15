"""价值投机雷达 — 四维评分 + 综合买入信号"""

import re
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# 数据驱动评分辅助函数
# ══════════════════════════════════════════════════════════════════════════════

def _compute_technical_from_data(price_df: pd.DataFrame) -> int | None:
    """
    纯数据驱动的技术评分 (0-100)。
    基于均线排列、RSI、量价关系、价格在60日区间位置。
    如果数据不足返回 None。
    """
    if price_df.empty or len(price_df) < 60:
        return None

    try:
        closes = price_df["收盘"].astype(float).values
        vols = price_df["成交量"].astype(float).values
        score = 50  # 基准

        # ── 1. 均线排列 (最高 +20 / -20) ──
        ma5 = pd.Series(closes).rolling(5).mean().iloc[-1]
        ma20 = pd.Series(closes).rolling(20).mean().iloc[-1]
        ma60 = pd.Series(closes).rolling(60).mean().iloc[-1]
        current = closes[-1]

        if current > ma5 > ma20 > ma60:
            # 完美多头：price > MA5 > MA20 > MA60
            score += 20
        elif ma5 > ma20 > ma60:
            # 均线多头但股价回踩MA5
            score += 12
        elif current < ma5 < ma20 < ma60:
            # 完美空头
            score -= 20
        elif ma5 < ma20 < ma60:
            # 均线空头
            score -= 12
        elif current > ma20:
            # 股价在MA20上方，趋势偏多
            score += 5
        else:
            score -= 5

        # ── 2. RSI(14) 位置 ──
        delta = pd.Series(closes).diff()
        gain = delta.clip(lower=0).rolling(14).mean().iloc[-1]
        loss = (-delta.clip(upper=0)).rolling(14).mean().iloc[-1]
        if loss > 0:
            rsi = 100 - (100 / (1 + gain / loss))
        else:
            rsi = 100.0

        if rsi >= 70:
            score += 5   # 强势但接近超买，适度加分
        elif rsi >= 60:
            score += 10  # 健康强势区
        elif 40 <= rsi < 60:
            score += 0   # 中性
        elif 30 <= rsi < 40:
            score -= 5   # 偏弱
        elif rsi < 30:
            score += 5   # 超卖反弹潜力

        # ── 3. 量价关系：上涨日放量为蓄势 (+15 max) ──
        if len(closes) >= 20 and len(vols) >= 20:
            changes = pd.Series(closes).pct_change().values[-20:]
            recent_vols = vols[-20:]
            # 上涨日平均成交量 vs 下跌日平均成交量
            up_mask = changes > 0
            down_mask = changes < 0
            up_vol = recent_vols[up_mask].mean() if up_mask.any() else 0
            down_vol = recent_vols[down_mask].mean() if down_mask.any() else 0

            if down_vol > 0:
                vol_ratio = up_vol / down_vol
                if vol_ratio > 1.5:
                    score += 15  # 明显放量上攻、缩量回调 = 蓄势
                elif vol_ratio > 1.2:
                    score += 8
                elif vol_ratio < 0.7:
                    score -= 10  # 放量下跌、缩量上涨 = 出货
                elif vol_ratio < 0.85:
                    score -= 5

        # ── 4. 价格在60日区间的位置 ──
        high_60 = price_df["最高"].astype(float).values[-60:].max()
        low_60 = price_df["最低"].astype(float).values[-60:].min()
        range_60 = high_60 - low_60
        if range_60 > 0:
            position = (current - low_60) / range_60
            if position >= 0.8:
                score += 10  # 顶部20% — 强势
            elif position >= 0.6:
                score += 5
            elif position <= 0.2:
                score -= 10  # 底部20% — 弱势
            elif position <= 0.4:
                score -= 5

        return max(0, min(100, score))

    except Exception as e:
        logger.debug("[_compute_technical_from_data] 计算异常: %s", e)
        return None


def _compute_capital_from_data(session_state: dict) -> int | None:
    """
    尝试从 session_state 中获取资金流向数据并计算评分 (0-100)。
    查找 capital_flow_df（如果存储了 DataFrame）或解析 stock_capital 文本数据。
    如果无可用数据返回 None。
    """
    score = 50

    # 方式1：检查是否有 capital_flow_df（DataFrame 形式的资金流数据）
    cap_df = session_state.get("capital_flow_df")
    if cap_df is not None and isinstance(cap_df, pd.DataFrame) and not cap_df.empty:
        try:
            if "net_mf_amount" in cap_df.columns:
                # net_mf_amount = 主力净流入金额
                recent = cap_df.tail(5)["net_mf_amount"].astype(float)
                total_net = recent.sum()
                if total_net > 0:
                    score += min(20, int(total_net / 10000))  # 按万元级别加分
                else:
                    score -= min(20, int(abs(total_net) / 10000))

                # 趋势：净流入是否在增加
                if len(recent) >= 3:
                    if recent.iloc[-1] > recent.iloc[-3]:
                        score += 5  # 资金流入趋势向好
                    elif recent.iloc[-1] < recent.iloc[-3]:
                        score -= 5

            return max(0, min(100, score))
        except Exception as e:
            logger.debug("[_compute_capital_from_data] DataFrame解析异常: %s", e)

    # 方式2：尝试从 stock_capital 文本中提取数值
    cap_text = session_state.get("stock_capital", "")
    if cap_text and "暂无" not in cap_text and len(cap_text) > 20:
        try:
            # 尝试解析 net_mf_amount 列的数值
            net_values = re.findall(r"[-+]?\d+\.?\d*", cap_text)
            if net_values:
                # 文本中有数值，说明有实际资金数据
                score += 3  # 有数据本身就是信号
            # 检查大单/超大单关键词
            if re.search(r"buy_elg_amount|buy_lg_amount", cap_text):
                # 原始数据列名存在，说明有详细资金流数据
                score += 2
            return max(0, min(100, score))
        except Exception:
            pass

    return None


# ══════════════════════════════════════════════════════════════════════════════
# 文本评分函数（原有逻辑保留）
# ══════════════════════════════════════════════════════════════════════════════

def _score_technical_from_text(text: str) -> int:
    """从AI趋势分析文本中提取技术评分 (0-100)，纯文本方法"""
    score = 50

    # AI 评分提取
    m = re.search(r"(?:综合评分|技术面评分)[：:]\s*(\d+(?:\.\d+)?)\s*/\s*10", text)
    if m:
        score = int(float(m.group(1)) * 10)

    # 关键词信号
    if re.search(r"看多|上涨|突破|放量上攻|启动", text):
        score += 8
    if re.search(r"看空|下跌|破位|缩量|下行", text):
        score -= 8
    if re.search(r"多头排列", text):
        score += 5
    if re.search(r"空头排列", text):
        score -= 5
    if re.search(r"金叉", text):
        score += 5
    if re.search(r"死叉", text):
        score -= 5

    # 通过/回避信号
    if "✅" in text and re.search(r"技术面.*买入|支持买入", text):
        score = max(score, 65)
    if "❌" in text and re.search(r"技术面.*回避|建议回避", text):
        score = min(score, 35)

    return max(0, min(100, score))


# ══════════════════════════════════════════════════════════════════════════════
# 主评分函数
# ══════════════════════════════════════════════════════════════════════════════

def compute_signal(session_state: dict) -> dict | None:
    """
    基于已完成的分析结果，计算四维评分。
    返回 {"fundamental": 0-100, "catalyst": 0-100, "technical": 0-100,
           "capital": 0-100, "verdict": str, "resonance": bool}
    如果数据不足则返回 None。
    """
    analyses = session_state.get("analyses", {})
    moe = session_state.get("moe_results", {})

    # 至少需要三项分析完成
    if not all(analyses.get(k) for k in ["expectation", "trend", "fundamentals"]):
        return None

    info = session_state.get("stock_info", {})
    df = session_state.get("price_df", pd.DataFrame())

    # ── 1. 基本面强度 (0-100) ──────────────────────────────────
    fundamental = _score_fundamental(analyses["fundamentals"], info)

    # ── 2. 题材正宗度 (0-100) ──────────────────────────────────
    catalyst = _score_catalyst(analyses["expectation"])

    # ── 3. 技术启动度 (0-100) ──────────────────────────────────
    technical = _score_technical(analyses["trend"], df)

    # ── 4. 资金关注度 (0-100) ──────────────────────────────────
    capital = _score_capital(analyses["trend"], session_state)

    # ── 综合判断 ───────────────────────────────────────────────
    scores = [fundamental, catalyst, technical, capital]
    avg = sum(scores) / 4
    resonance = all(s >= 70 for s in scores)

    if resonance:
        verdict = "四维共振 — 强烈关注"
    elif avg >= 75:
        verdict = "综合优秀 — 值得关注"
    elif avg >= 60:
        verdict = "部分达标 — 择机介入"
    elif avg >= 45:
        verdict = "信号偏弱 — 观望为主"
    else:
        verdict = "条件不足 — 暂时回避"

    return {
        "fundamental": fundamental,
        "catalyst": catalyst,
        "technical": technical,
        "capital": capital,
        "avg": round(avg, 1),
        "verdict": verdict,
        "resonance": resonance,
    }


def _score_fundamental(text: str, info: dict) -> int:
    """从基本面分析文本 + 估值数据中提取评分"""
    score = 50  # 基准分

    # 从AI文本中提取评分（如 "综合评分：7/10" 或 "财务健康评分：8 / 10"）
    m = re.search(r"(?:综合评分|财务健康评分)[：:]\s*(\d+(?:\.\d+)?)\s*/\s*10", text)
    if m:
        ai_score = float(m.group(1))
        score = int(ai_score * 10)  # 转换到0-100

    # 通过/不通过信号
    if "✅" in text and "通过" in text:
        score = max(score, 65)
    if "❌" in text and "不通过" in text:
        score = min(score, 35)

    # PE估值加分/减分
    try:
        pe = float(info.get("市盈率TTM", 0))
        if 0 < pe < 20:
            score += 10
        elif 0 < pe < 30:
            score += 5
        elif pe > 80:
            score -= 10
    except (ValueError, TypeError):
        pass

    # 关键词信号
    positive_keywords = ["ROE.*1[5-9]|2[0-9]|3[0-9]", "持续增长", "现金流健康",
                        "护城河", "行业龙头", "低估"]
    negative_keywords = ["高负债", "现金流恶化", "业绩下滑", "商誉减值",
                        "财务造假", "ST"]

    for kw in positive_keywords:
        if re.search(kw, text):
            score += 5
    for kw in negative_keywords:
        if re.search(kw, text):
            score -= 8

    return max(0, min(100, score))


def _score_catalyst(text: str) -> int:
    """从预期差分析中提取题材正宗度评分"""
    score = 50

    # 超预期信号
    positive_count = len(re.findall(r"🟢|超预期|正向惊喜|催化", text))
    negative_count = len(re.findall(r"🔴|低预期|负向惊喜|风险", text))

    score += positive_count * 5
    score -= negative_count * 4

    # 炒作逻辑强度
    if re.search(r"核心叙事.*(?:业绩拐点|政策催化|产业趋势|国产替代|AI|新能源)", text):
        score += 10
    if re.search(r"逻辑.*(?:强|持续|明确|扎实)", text):
        score += 8
    if re.search(r"逻辑.*(?:弱|不确定|短暂|蹭概念)", text):
        score -= 10

    # 乐观情景概率
    m = re.search(r"乐观.*?(\d{2,3})%", text)
    if m:
        opt_prob = int(m.group(1))
        if opt_prob >= 40:
            score += 8
        elif opt_prob >= 30:
            score += 4

    # 催化事件密度
    event_count = len(re.findall(r"\d{4}年\d{1,2}月", text))
    if event_count >= 3:
        score += 8
    elif event_count >= 1:
        score += 4

    return max(0, min(100, score))


def _score_technical(text: str, df: pd.DataFrame) -> int:
    """
    技术启动度评分 (0-100)。
    混合策略：50% 数据驱动 + 50% 文本驱动，提高鲁棒性。
    如果数据不足则 100% 依赖文本。
    """
    data_score = _compute_technical_from_data(df)
    text_score = _score_technical_from_text(text)

    if data_score is not None:
        # 50/50 混合：数据评分 + 文本评分
        blended = int(data_score * 0.5 + text_score * 0.5)
        return max(0, min(100, blended))
    else:
        # 无足够数据，100% 依赖文本（保持原有行为）
        return text_score


def _score_capital(text: str, session_state: dict) -> int:
    """
    资金关注度评分 (0-100)。
    混合策略：数据评分 + 文本评分，提高鲁棒性。
    """
    # ── 文本评分部分（保留原有逻辑） ──
    text_score = 50

    # AI文本信号
    if re.search(r"主力.*(?:净流入|加仓|买入|吸筹|建仓)", text):
        text_score += 12
    if re.search(r"主力.*(?:净流出|减仓|卖出|出货|派发)", text):
        text_score -= 12
    if re.search(r"龙虎榜.*(?:买入|机构|知名游资)", text):
        text_score += 8

    # 北向资金信号
    nb = session_state.get("stock_northbound", "")
    if nb and "暂无" not in nb and "不可用" not in nb:
        if re.search(r"增持|加仓", nb):
            text_score += 10
        elif re.search(r"减持", nb):
            text_score -= 8
        else:
            text_score += 3  # 有北向数据本身就是关注信号

    # 融资融券信号
    margin = session_state.get("stock_margin", "")
    if margin and "暂无" not in margin and "不可用" not in margin:
        text_score += 3  # 有数据说明是两融标的

    text_score = max(0, min(100, text_score))

    # ── 数据评分部分 ──
    data_score = _compute_capital_from_data(session_state)

    if data_score is not None:
        # 50/50 混合
        blended = int(data_score * 0.5 + text_score * 0.5)
        return max(0, min(100, blended))
    else:
        return text_score
