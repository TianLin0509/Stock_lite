from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from analysis.kline_research import ResearchConfig
from data.history_dataset_builder import _merge_history, _write_partitioned_history, _fmt_trade_date


def test_merge_history_keeps_latest_and_joins_stock_info():
    existing = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": 20240102, "open": 10, "high": 11, "low": 9, "close": 10.5, "vol": 100, "pct_chg": 1.0, "amount": 1000},
        ]
    )
    fetched = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "trade_date": 20240102, "open": 10, "high": 11.2, "low": 9, "close": 10.8, "vol": 120, "pct_chg": 2.0, "amount": 1100},
            {"ts_code": "000002.SZ", "trade_date": 20240102, "open": 20, "high": 21, "low": 19.5, "close": 20.2, "vol": 90, "pct_chg": 0.5, "amount": 900},
        ]
    )
    stock_list = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "symbol": "000001", "name": "平安银行", "industry": "银行", "list_date": 19910403},
            {"ts_code": "000002.SZ", "symbol": "000002", "name": "万科A", "industry": "房地产开发", "list_date": 19910129},
        ]
    )

    merged = _merge_history(existing, fetched, stock_list)
    assert len(merged) == 2
    assert merged.loc[merged["ts_code"] == "000001.SZ", "close"].iloc[0] == 10.8
    assert merged.loc[merged["ts_code"] == "000002.SZ", "name"].iloc[0] == "万科A"


def test_write_partitioned_history_creates_parquet_parts(tmp_path: Path):
    history = pd.DataFrame(
        [
            {"ts_code": f"{idx:06d}.SZ", "trade_date": 20240102 + idx, "open": 10, "high": 11, "low": 9, "close": 10.5, "vol": 100, "pct_chg": 1.0, "amount": 1000}
            for idx in range(20)
        ]
    )

    from data import history_dataset_builder as builder

    old_dir = builder.HISTORY_DIR
    builder.HISTORY_DIR = tmp_path
    try:
        _write_partitioned_history(history, parts=4)
        files = sorted(tmp_path.glob("all_daily_part*.parquet"))
        assert len(files) == 4
        rows = sum(len(pd.read_parquet(file)) for file in files)
        assert rows == len(history)
    finally:
        builder.HISTORY_DIR = old_dir


def test_fmt_trade_date_formats_numeric_values():
    assert _fmt_trade_date(20260312.0) == "2026-03-12"
