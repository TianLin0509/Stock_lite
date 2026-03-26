from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from data import window20_memory_builder as builder
from services import numeric_retrieval_service as numeric_service


def _make_history(rows_per_stock: int = 240) -> pd.DataFrame:
    rng = np.random.default_rng(29)
    frames: list[pd.DataFrame] = []
    codes = ["000001.SZ", "000002.SZ", "002594.SZ", "600519.SH"]

    for stock_idx, ts_code in enumerate(codes):
        dates = pd.date_range("2023-01-02", periods=rows_per_stock, freq="B")
        close = [12.0 + stock_idx * 7]
        for i in range(1, rows_per_stock):
            regime = 0.007 if (i // 28) % 2 == 0 else -0.003
            noise = rng.normal(0, 0.012)
            close.append(close[-1] * (1 + regime + noise))

        close_arr = np.array(close, dtype=float)
        open_arr = close_arr * (1 + rng.normal(0, 0.004, rows_per_stock))
        high_arr = np.maximum(open_arr, close_arr) * (1 + rng.uniform(0.002, 0.022, rows_per_stock))
        low_arr = np.minimum(open_arr, close_arr) * (1 - rng.uniform(0.002, 0.022, rows_per_stock))
        vol_base = 1_200_000 * (1 + stock_idx * 0.18)
        vol_arr = vol_base * (1 + np.sin(np.arange(rows_per_stock) / 10) * 0.22 + rng.normal(0, 0.04, rows_per_stock))
        vol_arr = np.clip(vol_arr, 1.0, None)

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
                "amount": vol_arr * close_arr,
                "name": f"Stock{stock_idx}",
                "industry": "Auto" if "002594" in ts_code else "Consume",
            }
        )
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


def _patch_paths(monkeypatch, tmp_path: Path) -> None:
    research_dir = tmp_path / "research"
    research_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(builder, "RESEARCH_DIR", research_dir)
    monkeypatch.setattr(builder, "MEMORY_DATASET_PATH", research_dir / "window20_memories.parquet")
    monkeypatch.setattr(builder, "MEMORY_META_PATH", research_dir / "window20_memories_meta.json")

    monkeypatch.setattr(numeric_service, "RESEARCH_DIR", research_dir)
    monkeypatch.setattr(numeric_service, "STATE_FEATURES_PATH", research_dir / "window20_state_features.npy")
    monkeypatch.setattr(numeric_service, "STATE_IDS_PATH", research_dir / "window20_state_ids.npy")
    monkeypatch.setattr(numeric_service, "STATE_STATS_PATH", research_dir / "window20_state_stats.json")
    monkeypatch.setattr(numeric_service, "STATE_INDEX_PATH", research_dir / "window20_state.index")
    monkeypatch.setattr(numeric_service, "PRICE_SHAPE_PATH", research_dir / "window20_price_shape.npy")
    monkeypatch.setattr(numeric_service, "PRICE_RET_SHAPE_PATH", research_dir / "window20_price_returns.npy")
    monkeypatch.setattr(numeric_service, "PRICE_IDS_PATH", research_dir / "window20_price_shape_ids.npy")
    monkeypatch.setattr(numeric_service, "PRICE_INDEX_PATH", research_dir / "window20_price_shape.index")
    monkeypatch.setattr(numeric_service, "PRICE_MP_PATH", research_dir / "window20_price_mp.npy")
    monkeypatch.setattr(numeric_service, "PRICE_MP_META_PATH", research_dir / "window20_price_mp_meta.json")
    monkeypatch.setattr(numeric_service, "VOLUME_SHAPE_PATH", research_dir / "window20_volume_shape.npy")
    monkeypatch.setattr(numeric_service, "VOLUME_FLOW_PATH", research_dir / "window20_volume_flow.npy")
    monkeypatch.setattr(numeric_service, "VOLUME_IDS_PATH", research_dir / "window20_volume_shape_ids.npy")
    monkeypatch.setattr(numeric_service, "VOLUME_INDEX_PATH", research_dir / "window20_volume_shape.index")
    monkeypatch.setattr(numeric_service, "LABEL_CONSISTENCY_PATH", research_dir / "window20_label_consistency.npy")
    monkeypatch.setattr(numeric_service, "LABEL_IDS_PATH", research_dir / "window20_label_ids.npy")
    monkeypatch.setattr(numeric_service, "ENSEMBLE_META_PATH", research_dir / "window20_numeric_ensemble_meta.json")


def test_numeric_ensemble_dataset_contains_sequence_columns(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    history = _make_history()

    stats = builder.build_window20_numeric_ensemble_dataset(history_frame=history)
    memories = builder.load_window20_numeric_ensemble_dataset()

    assert stats.memory_rows > 0
    expected = set(
        builder.PRICE_SEQUENCE_COLUMNS
        + builder.RETURN_SEQUENCE_COLUMNS
        + builder.VOLUME_SEQUENCE_COLUMNS
        + builder.PV_FLOW_SEQUENCE_COLUMNS
    )
    assert expected.issubset(memories.columns)

    sample = memories.iloc[0]
    close_seq = [float(sample[column]) for column in builder.PRICE_SEQUENCE_COLUMNS]
    vol_seq = [float(sample[column]) for column in builder.VOLUME_SEQUENCE_COLUMNS]
    assert len(close_seq) == builder.WINDOW_SIZE
    assert len(vol_seq) == builder.WINDOW_SIZE
    assert abs(float(np.mean(close_seq))) < 1e-5
    assert abs(float(np.mean(vol_seq))) < 1e-5
    assert pd.to_datetime(sample["window_end"]) > pd.to_datetime(sample["window_start"])


def test_build_numeric_ensemble_indexes_persists_channel_artifacts(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    builder.build_window20_numeric_ensemble_dataset(history_frame=_make_history())
    memories = builder.load_window20_numeric_ensemble_dataset()

    stats = numeric_service.build_window20_numeric_ensemble_indexes(memory_df=memories)

    assert stats.memory_rows > 0
    assert stats.state_dim == len(builder.WINDOW20_FEATURE_COLUMNS)
    assert stats.price_dim == len(builder.PRICE_SEQUENCE_COLUMNS) + len(builder.RETURN_SEQUENCE_COLUMNS)
    assert stats.volume_dim == len(builder.VOLUME_SEQUENCE_COLUMNS) + len(builder.PV_FLOW_SEQUENCE_COLUMNS)
    assert numeric_service.STATE_FEATURES_PATH.exists()
    assert numeric_service.PRICE_SHAPE_PATH.exists()
    assert numeric_service.VOLUME_SHAPE_PATH.exists()
    assert numeric_service.LABEL_CONSISTENCY_PATH.exists()

    meta = json.loads(numeric_service.ENSEMBLE_META_PATH.read_text(encoding="utf-8"))
    assert meta["memory_rows"] == stats.memory_rows
    assert meta["state_backend"] in {"faiss", "sklearn", "numpy"}


def test_query_window20_numeric_ensemble_returns_ranked_matches(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    history = _make_history(rows_per_stock=280)
    builder.build_window20_numeric_ensemble_dataset(history_frame=history)
    numeric_service.build_window20_numeric_ensemble_indexes(
        memory_df=builder.load_window20_numeric_ensemble_dataset()
    )

    query_df = history[history["ts_code"] == "002594.SZ"].copy()
    result = numeric_service.query_window20_numeric_ensemble(
        query_df,
        top_k=12,
        state_k=32,
        price_k=32,
        volume_k=32,
    )

    assert result["query_snapshot"]["ts_code"] == "002594.SZ"
    assert result["query_snapshot"]["pattern_summary"]
    assert result["query_regime_summary"]["trend_bucket"]
    assert result["query_regime_summary"]["volatility_bucket"]
    assert result["weight_profile"]
    assert abs(sum(result["weight_profile"].values()) - 1.0) < 1e-3
    assert result["distribution_stats"]["sample_count"] > 0
    assert len(result["top_memories"]) == result["distribution_stats"]["sample_count"]
    assert result["supporting_examples"]

    required_match_fields = {
        "state_score",
        "price_shape_score",
        "stumpy_price_score",
        "volume_shape_score",
        "label_consistency_score",
        "regime_match_score",
        "freshness_score",
        "outcome_quality_score",
        "anti_example_penalty",
        "ensemble_score",
        "ret_5",
        "ret_10",
        "ret_20",
        "up_5",
        "up_10",
        "up_20",
        "drawdown_5",
        "drawdown_10",
        "drawdown_20",
        "max_up_5",
        "max_up_10",
        "max_up_20",
    }
    assert required_match_fields.issubset(result["top_memories"][0].keys())

    query_end = pd.to_datetime(result["query_snapshot"]["window_end"])
    scores = [item["ensemble_score"] for item in result["top_memories"]]
    assert scores == sorted(scores, reverse=True)

    for item in result["top_memories"]:
        assert 0.0 <= item["state_score"] <= 1.0
        assert 0.0 <= item["price_shape_score"] <= 1.0
        assert 0.0 <= item["stumpy_price_score"] <= 1.0
        assert 0.0 <= item["volume_shape_score"] <= 1.0
        assert 0.0 <= item["label_consistency_score"] <= 1.0
        assert 0.0 <= item["regime_match_score"] <= 1.0
        assert 0.0 <= item["freshness_score"] <= 1.0
        assert 0.0 <= item["outcome_quality_score"] <= 1.0
        assert 0.0 <= item["anti_example_penalty"] <= 1.0
        assert 0.0 <= item["ensemble_score"] <= 1.0
        item_end = pd.to_datetime(item["window_end"])
        assert item_end < query_end
        if item["ts_code"] == "002594.SZ":
            assert abs((query_end - item_end).days) >= 60


def test_query_window20_numeric_ensemble_accepts_precomputed_single_row(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    history = _make_history(rows_per_stock=260)
    builder.build_window20_numeric_ensemble_dataset(history_frame=history)
    memories = builder.load_window20_numeric_ensemble_dataset()
    numeric_service.build_window20_numeric_ensemble_indexes(memory_df=memories)

    query_row = memories[memories["ts_code"] == "600519.SH"].sort_values("window_end").tail(1)
    result = numeric_service.query_window20_numeric_ensemble(query_row, top_k=8, state_k=24, price_k=24, volume_k=24)

    assert result["query_snapshot"]["ts_code"] == "600519.SH"
    assert result["distribution_stats"]["sample_count"] > 0
    assert result["channel_scores_summary"]["avg_ensemble_score"] > 0
    assert result["channel_scores_summary"]["avg_regime_match_score"] >= 0
