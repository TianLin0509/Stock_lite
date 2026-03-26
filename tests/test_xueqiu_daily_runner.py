from __future__ import annotations

from datetime import date
from pathlib import Path

from xueqiu_daily.collector.mock import MockCollector
from xueqiu_daily.config import XueqiuDailyConfig
from xueqiu_daily.runner import run_daily_top50


def test_run_daily_top50_creates_report(tmp_path: Path) -> None:
    config = XueqiuDailyConfig(run_date=date(2026, 3, 24), base_dir=tmp_path)

    result = run_daily_top50(config=config, collector=MockCollector())

    assert result.candidate_count == 3
    assert result.selected_count == 3
    assert Path(result.markdown_path).exists()
    report_text = Path(result.markdown_path).read_text(encoding="utf-8")
    assert "雪球高质量观点 Top3 日报" in report_text
    assert "白酒龙头的现金流与提价逻辑" in report_text
