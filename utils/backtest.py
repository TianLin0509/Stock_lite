"""回测战绩 — 对比历史 AI 推荐 vs 实际涨跌"""

import re
import json
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path

ARCHIVE_DIR = Path(__file__).parent.parent / "data" / "archive"
INDEX_FILE = ARCHIVE_DIR / "_index.jsonl"


# ══════════════════════════════════════════════════════════════════════════════
# 从 AI 分析文本中提取推荐方向
# ══════════════════════════════════════════════════════════════════════════════

# MoE CEO 裁决的操作评级
_RATING_PATTERNS = [
    (r"强烈买入|强烈推荐", "强烈买入"),
    (r"(?<!谨慎)买入(?!.*回避)", "买入"),
    (r"谨慎介入|谨慎买入|逢低介入|适量介入", "谨慎介入"),
    (r"持有观察|观望|持有", "观望"),
    (r"减持|逢高减持", "减持"),
    (r"回避|不建议|远离", "回避"),
]

# 从三维分析中提取信号词
_BULLISH_WORDS = [
    "看多", "看涨", "买入", "介入", "建仓", "加仓", "强势", "突破",
    "利好", "超预期", "低估", "值得关注", "推荐", "积极",
]
_BEARISH_WORDS = [
    "看空", "看跌", "回避", "减持", "卖出", "清仓", "弱势", "破位",
    "利空", "高估", "风险较大", "谨慎", "不建议",
]


def extract_recommendation(archive: dict) -> dict:
    """从归档数据中提取 AI 推荐方向和评级

    Returns:
        {
            "rating": "强烈买入" | "买入" | "谨慎介入" | "观望" | "减持" | "回避" | "未知",
            "direction": "bullish" | "bearish" | "neutral" | "unknown",
            "confidence": 0-100,   # 基于多少维度给出了一致信号
            "source": "moe" | "analyses",
        }
    """
    # 优先从 MoE CEO 裁决中提取
    moe = archive.get("moe_results")
    if moe and moe.get("ceo"):
        ceo_text = moe["ceo"]
        for pattern, label in _RATING_PATTERNS:
            if re.search(pattern, ceo_text):
                direction = "bullish" if label in ("强烈买入", "买入", "谨慎介入") \
                    else "bearish" if label in ("减持", "回避") \
                    else "neutral"
                return {
                    "rating": label,
                    "direction": direction,
                    "confidence": 80,
                    "source": "moe",
                }

    # 从分析文本中综合判断
    analyses = archive.get("analyses", {})

    # 优先从 comprehensive 报告提取（新版深度报告）
    comp_text = analyses.get("comprehensive", "")
    if comp_text:
        # comprehensive 报告包含"操作评级"字段，直接匹配
        for pattern, label in _RATING_PATTERNS:
            # 在结论部分搜索（最后 2000 字）
            tail = comp_text[-2000:]
            if re.search(pattern, tail):
                direction = "bullish" if label in ("强烈买入", "买入", "谨慎介入") \
                    else "bearish" if label in ("减持", "回避") \
                    else "neutral"
                return {
                    "rating": label,
                    "direction": direction,
                    "confidence": 70,
                    "source": "comprehensive",
                }
        # 兜底：用多空信号词从 comprehensive 结论部分判断
        tail = comp_text[-1500:]
        b = sum(1 for w in _BULLISH_WORDS if w in tail)
        s = sum(1 for w in _BEARISH_WORDS if w in tail)
        if b > s:
            return {"rating": "谨慎介入", "direction": "bullish",
                    "confidence": 50, "source": "comprehensive"}
        elif s > b:
            return {"rating": "观望", "direction": "bearish",
                    "confidence": 50, "source": "comprehensive"}

    # 从旧版三模块分析中综合判断
    bull_count, bear_count = 0, 0
    for key in ["expectation", "trend", "fundamentals"]:
        text = analyses.get(key, "")
        if not text:
            continue
        tail = text[-500:]
        b = sum(1 for w in _BULLISH_WORDS if w in tail)
        s = sum(1 for w in _BEARISH_WORDS if w in tail)
        if b > s:
            bull_count += 1
        elif s > b:
            bear_count += 1

    if bull_count + bear_count == 0:
        return {"rating": "未知", "direction": "unknown", "confidence": 0, "source": "analyses"}

    if bull_count > bear_count:
        rating = "买入" if bull_count >= 2 else "谨慎介入"
        return {"rating": rating, "direction": "bullish",
                "confidence": bull_count * 30, "source": "analyses"}
    elif bear_count > bull_count:
        rating = "回避" if bear_count >= 2 else "观望"
        return {"rating": rating, "direction": "bearish",
                "confidence": bear_count * 30, "source": "analyses"}
    else:
        return {"rating": "观望", "direction": "neutral",
                "confidence": 30, "source": "analyses"}


# ══════════════════════════════════════════════════════════════════════════════
# 获取分析后实际涨跌
# ══════════════════════════════════════════════════════════════════════════════

def get_subsequent_returns(ts_code: str, archive_date: str,
                           periods: list[int] = None) -> dict:
    """获取归档日期之后 N 个交易日的实际涨跌幅

    Args:
        ts_code: 股票代码 (如 "600519.SH")
        archive_date: 归档日期 "YYYY-MM-DD"
        periods: 计算哪些天数的收益率，默认 [5, 10, 20]

    Returns:
        {5: 2.3, 10: -1.5, 20: 5.2}  # 百分比
        值为 None 表示数据不足
    """
    if periods is None:
        periods = [5, 10, 20]

    try:
        from data.tushare_client import _pro
        if not _pro:
            return {p: None for p in periods}

        # 从归档日期开始往后取足够多的交易日
        start = archive_date.replace("-", "")
        max_period = max(periods)
        # 多取一些天数以确保覆盖足够的交易日
        end_date = datetime.strptime(archive_date, "%Y-%m-%d") + timedelta(days=max_period * 2)
        end = end_date.strftime("%Y%m%d")

        df = _pro.daily(ts_code=ts_code, start_date=start, end_date=end)
        if df is None or df.empty:
            return {p: None for p in periods}

        df = df.sort_values("trade_date").reset_index(drop=True)

        # 第一行是归档当天（或之后最近的交易日）
        if len(df) < 2:
            return {p: None for p in periods}

        base_close = df.iloc[0]["close"]
        results = {}
        for p in periods:
            if len(df) > p:
                future_close = df.iloc[p]["close"]
                results[p] = round((future_close - base_close) / base_close * 100, 2)
            else:
                results[p] = None
        return results
    except Exception:
        return {p: None for p in periods}


# ══════════════════════════════════════════════════════════════════════════════
# 批量回测
# ══════════════════════════════════════════════════════════════════════════════

def load_all_archives() -> list[dict]:
    """加载所有归档记录（轻量索引 + 按需读取详情）"""
    if not INDEX_FILE.exists():
        # 无索引文件时，扫描 JSON 文件
        if not ARCHIVE_DIR.exists():
            return []
        archives = []
        for f in sorted(ARCHIVE_DIR.glob("*.json")):
            if f.name.startswith("_"):
                continue
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                data["_file"] = f.name
                archives.append(data)
            except Exception:
                continue
        return archives

    # 有索引文件时从索引加载
    records = []
    for line in INDEX_FILE.read_text(encoding="utf-8").strip().split("\n"):
        if not line:
            continue
        try:
            entry = json.loads(line)
            records.append(entry)
        except Exception:
            continue
    return records


def run_backtest(progress_callback=None) -> pd.DataFrame:
    """执行完整回测，返回结果 DataFrame

    Columns:
        date, stock_code, stock_name, username, model,
        close_at_analysis,  # 分析时收盘价
        rating, direction, confidence, rating_source,
        return_5d, return_10d, return_20d,
        hit_5d, hit_10d, hit_20d  # 方向是否正确
    """
    archives = load_all_archives()
    if not archives:
        return pd.DataFrame()

    # 去重：同一天同一股票只保留最新的
    seen = {}
    for a in archives:
        key = f"{a.get('date', a.get('archive_date', ''))}_{a.get('stock_code', '')}"
        seen[key] = a
    unique = list(seen.values())

    # 只回测至少 5 个交易日前的记录
    cutoff = (date.today() - timedelta(days=8)).isoformat()
    eligible = [a for a in unique if a.get("date", a.get("archive_date", "")) <= cutoff]

    if not eligible:
        return pd.DataFrame()

    rows = []
    total = len(eligible)
    for i, entry in enumerate(eligible):
        if progress_callback:
            progress_callback(i + 1, total)

        # 索引记录 vs 完整归档的字段兼容
        archive_date = entry.get("date", entry.get("archive_date", ""))
        stock_code = entry.get("stock_code", "")
        stock_name = entry.get("stock_name", "")
        close = entry.get("close", 0)

        # 需要完整归档来提取推荐
        if "analyses" in entry:
            full = entry
        else:
            filename = entry.get("file", "")
            if not filename:
                continue
            filepath = ARCHIVE_DIR / filename
            if not filepath.exists():
                continue
            try:
                full = json.loads(filepath.read_text(encoding="utf-8"))
            except Exception:
                continue

        if not close:
            ps = full.get("price_snapshot", {})
            close = ps.get("close", 0)

        rec = extract_recommendation(full)
        returns = get_subsequent_returns(stock_code, archive_date)

        row = {
            "date": archive_date,
            "stock_code": stock_code,
            "stock_name": stock_name,
            "username": entry.get("username", full.get("username", "")),
            "model": entry.get("model", full.get("model", "")),
            "close_at_analysis": close,
            "rating": rec["rating"],
            "direction": rec["direction"],
            "confidence": rec["confidence"],
            "rating_source": rec["source"],
        }

        for p in [5, 10, 20]:
            ret = returns.get(p)
            row[f"return_{p}d"] = ret
            # 判断方向是否正确
            if ret is not None and rec["direction"] in ("bullish", "bearish"):
                if rec["direction"] == "bullish":
                    row[f"hit_{p}d"] = ret > 0
                else:
                    row[f"hit_{p}d"] = ret < 0
            else:
                row[f"hit_{p}d"] = None

        rows.append(row)

    return pd.DataFrame(rows)


def compute_stats(df: pd.DataFrame) -> dict:
    """计算回测统计指标"""
    if df.empty:
        return {}

    total = len(df)
    stats = {"total_records": total}

    # 按方向分
    bullish = df[df["direction"] == "bullish"]
    bearish = df[df["direction"] == "bearish"]
    stats["bullish_count"] = len(bullish)
    stats["bearish_count"] = len(bearish)

    for period in [5, 10, 20]:
        col_ret = f"return_{period}d"
        col_hit = f"hit_{period}d"

        valid = df[df[col_ret].notna()]
        if valid.empty:
            stats[f"avg_return_{period}d"] = None
            stats[f"win_rate_{period}d"] = None
            continue

        stats[f"avg_return_{period}d"] = round(valid[col_ret].mean(), 2)

        # 胜率：只算有明确方向的
        directed = valid[valid[col_hit].notna()]
        if len(directed) > 0:
            stats[f"win_rate_{period}d"] = round(
                directed[col_hit].sum() / len(directed) * 100, 1
            )
            stats[f"directed_count_{period}d"] = len(directed)
        else:
            stats[f"win_rate_{period}d"] = None

    # 按评级分组统计
    rating_stats = []
    for rating in ["强烈买入", "买入", "谨慎介入", "观望", "减持", "回避"]:
        sub = df[df["rating"] == rating]
        if sub.empty:
            continue
        r = {"rating": rating, "count": len(sub)}
        for p in [5, 10, 20]:
            col = f"return_{p}d"
            v = sub[col].dropna()
            r[f"avg_{p}d"] = round(v.mean(), 2) if len(v) > 0 else None
        rating_stats.append(r)
    stats["by_rating"] = rating_stats

    return stats
