from datetime import datetime
from zoneinfo import ZoneInfo

from services.top100_review_service import (
    _normalize_ts_code,
    _pick_compare_trade_date,
    render_top100_review_markdown,
)


SH_TZ = ZoneInfo("Asia/Shanghai")


def test_pick_compare_trade_date_before_open_uses_same_day_close():
    open_trade_dates = ["20260324", "20260325"]
    generated_at = datetime(2026, 3, 24, 8, 59, tzinfo=SH_TZ)
    assert _pick_compare_trade_date(generated_at, open_trade_dates) == "20260324"


def test_pick_compare_trade_date_after_close_uses_next_trade_day():
    open_trade_dates = ["20260324", "20260325", "20260326"]
    generated_at = datetime(2026, 3, 24, 15, 1, tzinfo=SH_TZ)
    assert _pick_compare_trade_date(generated_at, open_trade_dates) == "20260325"


def test_render_top100_review_markdown_contains_requested_columns():
    review = {
        "source_file": "demo.json",
        "generated_at": "2026-03-24 23:30:00",
        "compare_trade_date": "20260325",
        "model": "Seed 2.0 Pro",
        "tokens_used": 123456,
        "rows": [
            {
                "rank": 1,
                "stock_name": "比亚迪",
                "ts_code": "002594.SZ",
                "match_score": 83.75,
                "short_term": "低吸",
                "daily_pct_chg": 2.31,
                "open_buy_pct": 1.12,
                "market_pct_chg": 0.58,
            }
        ],
    }

    markdown = render_top100_review_markdown(review)

    assert "# Top100 次日表现复盘（20260325）" in markdown
    assert "| 股票名 | 代码 | 排名 | 综合匹配度 | 短线建议 | 当天涨跌幅 | 开盘买入策略 | 上证指数涨跌幅 |" in markdown
    assert "| 比亚迪 | 002594.SZ | 1 | 83.75 | 低吸 | 2.31% | 1.12% | 0.58% |" in markdown


def test_normalize_ts_code_appends_exchange_suffix():
    assert _normalize_ts_code("300308") == "300308.SZ"
    assert _normalize_ts_code("600519") == "600519.SH"
    assert _normalize_ts_code("430001") == "430001.BJ"
    assert _normalize_ts_code("002594.SZ") == "002594.SZ"
