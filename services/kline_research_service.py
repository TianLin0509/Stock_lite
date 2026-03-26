"""Service helpers for running K-line research on the local history dataset."""

from __future__ import annotations

from dataclasses import asdict

from analysis.kline_research import (
    DEFAULT_FEATURE_COLUMNS,
    ResearchConfig,
    build_research_dataset,
    build_stock_research_snapshot,
    summarize_rule_patterns,
    walk_forward_evaluate,
)
from data.similarity import load_history
from data.tushare_client import load_stock_list, resolve_stock


def run_kline_research(
    stock_query: str,
    *,
    config: ResearchConfig | None = None,
    horizon: int = 5,
    top_patterns: int = 15,
) -> dict:
    """Run the full 6-step research pipeline on the built-in history dataset."""
    cfg = config or ResearchConfig()
    ts_code, resolved_name, err = resolve_stock(stock_query)
    if not ts_code:
        raise ValueError(err or f"unable to resolve stock: {stock_query}")

    history = load_history()
    dataset = build_research_dataset(history, config=cfg)
    if dataset.empty:
        raise ValueError("research dataset is empty")

    latest = build_stock_research_snapshot(
        dataset,
        stock_code=ts_code,
        horizon=horizon,
        feature_names=DEFAULT_FEATURE_COLUMNS,
        min_rule_samples=cfg.min_rule_samples,
    )
    walk_forward = walk_forward_evaluate(
        dataset,
        horizon=horizon,
        feature_names=DEFAULT_FEATURE_COLUMNS,
    )
    patterns = summarize_rule_patterns(
        dataset,
        horizon=horizon,
        min_samples=cfg.min_rule_samples,
    ).head(top_patterns)

    stock_list, _ = load_stock_list()
    stock_names = {}
    if stock_list is not None and not stock_list.empty:
        stock_names = dict(zip(stock_list["ts_code"], stock_list["name"]))

    latest["stock_name"] = resolved_name or stock_names.get(ts_code, stock_query)
    latest["ts_code"] = ts_code
    return {
        "config": asdict(cfg),
        "dataset_rows": int(len(dataset)),
        "stock_signal": latest,
        "top_patterns": patterns.to_dict(orient="records"),
        "walk_forward": walk_forward,
    }
