"""Human-readable pattern semantics for K-line snapshots."""

from __future__ import annotations

TAG_LABELS = {
    "trend_tag": {
        "trend_up": "趋势偏强",
        "trend_down": "趋势偏弱",
        "trend_flat": "趋势走平",
    },
    "momentum_tag": {
        "momentum_up": "短线动能上行",
        "momentum_down": "短线动能回落",
        "momentum_flat": "短线动能走平",
    },
    "volume_tag": {
        "volume_expand": "量能放大",
        "volume_contract": "量能收缩",
        "volume_neutral": "量能平稳",
    },
    "rsi_tag": {
        "rsi_hot": "RSI偏热",
        "rsi_cold": "RSI偏冷",
        "rsi_mid": "RSI中性",
    },
    "candle_tag": {
        "lower_shadow": "下影较长，低位承接较强",
        "upper_shadow": "上影较长，上方抛压明显",
        "wide_bull": "实体偏强，多头主导",
        "wide_bear": "实体偏弱，空头主导",
        "neutral_body": "实体中性，多空暂时均衡",
    },
    "breakout_tag": {
        "near_high": "位置靠近阶段高位",
        "near_low": "位置靠近阶段低位",
        "mid_range": "位置处于区间中部",
    },
}

TAG_TITLES = {
    "trend_tag": "趋势状态",
    "momentum_tag": "动量状态",
    "volume_tag": "量能状态",
    "rsi_tag": "RSI状态",
    "candle_tag": "当日K线",
    "breakout_tag": "所处位置",
}

PATTERN_TAG_ORDER = list(TAG_TITLES.keys())


def build_pattern_details(row) -> list[dict]:
    details = []
    for key in PATTERN_TAG_ORDER:
        raw_value = row.get(key, "N/A")
        label = TAG_LABELS.get(key, {}).get(raw_value, raw_value)
        details.append(
            {
                "key": key,
                "raw": raw_value,
                "title": TAG_TITLES[key],
                "label": label,
            }
        )
    return details


def pattern_summary_text(details: list[dict]) -> str:
    return "，".join(item["label"] for item in details)
