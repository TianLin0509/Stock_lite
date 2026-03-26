from __future__ import annotations

import numpy as np
import pandas as pd

from analysis.kline_research import (
    DEFAULT_FEATURE_COLUMNS,
    LinearProbabilityModel,
    ResearchConfig,
    build_research_dataset,
    summarize_rule_patterns,
    train_probability_model,
    walk_forward_evaluate,
)


def _make_history(rows_per_stock: int = 180) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    frames: list[pd.DataFrame] = []

    for stock_idx, ts_code in enumerate(["000001.SZ", "000002.SZ", "000003.SZ"]):
        dates = pd.date_range("2023-01-01", periods=rows_per_stock, freq="B")
        close = [10.0 + stock_idx]
        for i in range(1, rows_per_stock):
            prev = close[-1]
            drift = 0.008 if i % 12 < 6 else -0.004
            noise = rng.normal(0, 0.003)
            close.append(prev * (1 + drift + noise))

        close_arr = np.array(close)
        open_arr = close_arr * (1 + rng.normal(0, 0.002, rows_per_stock))
        high_arr = np.maximum(open_arr, close_arr) * (1 + rng.uniform(0.002, 0.015, rows_per_stock))
        low_arr = np.minimum(open_arr, close_arr) * (1 - rng.uniform(0.002, 0.015, rows_per_stock))
        vol_arr = 1_000_000 * (1 + np.sin(np.arange(rows_per_stock) / 8) * 0.2 + stock_idx * 0.05)

        frame = pd.DataFrame(
            {
                "ts_code": ts_code,
                "trade_date": dates.strftime("%Y%m%d").astype(int),
                "open": open_arr,
                "high": high_arr,
                "low": low_arr,
                "close": close_arr,
                "vol": vol_arr,
                "pct_chg": pd.Series(close_arr).pct_change().fillna(0.0).to_numpy() * 100,
            }
        )
        frames.append(frame)

    return pd.concat(frames, ignore_index=True)


def test_build_research_dataset_creates_features_and_labels():
    history = _make_history()
    dataset = build_research_dataset(history, config=ResearchConfig(lookback=7, horizons=(5, 10), event_gap=3))

    assert not dataset.empty
    assert {"pattern_key", "forward_return_5d", "forward_up_5d"}.issubset(dataset.columns)
    assert dataset["ts_code"].nunique() == 3
    assert dataset["trade_date"].is_monotonic_increasing is False


def test_rule_summary_filters_and_sorts_patterns():
    history = _make_history()
    dataset = build_research_dataset(history, config=ResearchConfig(min_rule_samples=5, event_gap=2))
    summary = summarize_rule_patterns(dataset, horizon=5, min_samples=5)

    assert not summary.empty
    assert (summary["sample_count"] >= 5).all()
    assert "up_prob" in summary.columns


def test_probability_model_and_walk_forward_run_end_to_end():
    history = _make_history(rows_per_stock=220)
    dataset = build_research_dataset(history, config=ResearchConfig(event_gap=2, min_rule_samples=5))
    usable = dataset.dropna(subset=DEFAULT_FEATURE_COLUMNS + ["forward_up_5d"])

    model = train_probability_model(usable, horizon=5, feature_names=DEFAULT_FEATURE_COLUMNS)
    probs = model.predict_proba(usable.tail(10))

    assert len(probs) == 10
    assert np.all((probs >= 0) & (probs <= 1))

    evaluation = walk_forward_evaluate(
        usable,
        horizon=5,
        feature_names=DEFAULT_FEATURE_COLUMNS,
        min_train_rows=120,
        folds=3,
    )
    assert evaluation["summary"] is not None
    assert evaluation["summary"]["fold_count"] >= 1


def test_probability_model_supports_downsampled_training_path():
    history = _make_history(rows_per_stock=260)
    dataset = build_research_dataset(history, config=ResearchConfig(event_gap=1, min_rule_samples=5))
    usable = dataset.dropna(subset=DEFAULT_FEATURE_COLUMNS + ["forward_up_5d"])

    model = LinearProbabilityModel(
        feature_names=DEFAULT_FEATURE_COLUMNS,
        horizon=5,
        max_train_rows=30,
        epochs=20,
    ).fit(usable)
    probs = model.predict_proba(usable.tail(5))

    assert len(probs) == 5
    assert np.all((probs >= 0) & (probs <= 1))
