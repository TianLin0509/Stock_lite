from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from data import window20_memory_builder as builder
from services import memory_index_service as index_service


def _make_history(rows_per_stock: int = 220) -> pd.DataFrame:
    rng = np.random.default_rng(17)
    frames: list[pd.DataFrame] = []
    codes = ["000001.SZ", "000002.SZ", "002594.SZ", "600519.SH"]

    for stock_idx, ts_code in enumerate(codes):
        dates = pd.date_range("2023-01-02", periods=rows_per_stock, freq="B")
        close = [10.0 + stock_idx * 5]
        for i in range(1, rows_per_stock):
            regime = 0.006 if (i // 25) % 2 == 0 else -0.002
            noise = rng.normal(0, 0.01)
            close.append(close[-1] * (1 + regime + noise))

        close_arr = np.array(close, dtype=float)
        open_arr = close_arr * (1 + rng.normal(0, 0.003, rows_per_stock))
        high_arr = np.maximum(open_arr, close_arr) * (1 + rng.uniform(0.002, 0.02, rows_per_stock))
        low_arr = np.minimum(open_arr, close_arr) * (1 - rng.uniform(0.002, 0.02, rows_per_stock))
        vol_arr = 1000000 * (1 + stock_idx * 0.15 + np.sin(np.arange(rows_per_stock) / 9) * 0.2)

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


class FakeEmbeddingProvider:
    def embed_texts(self, texts: list[str]) -> np.ndarray:
        vectors = []
        for text in texts:
            base = np.zeros(8, dtype=np.float32)
            encoded = text.encode("utf-8")
            for idx, byte in enumerate(encoded):
                base[idx % len(base)] += byte / 255.0
            vectors.append(base)
        return np.asarray(vectors, dtype=np.float32)


def _patch_paths(monkeypatch, tmp_path: Path) -> None:
    research_dir = tmp_path / "research"
    research_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(builder, "RESEARCH_DIR", research_dir)
    monkeypatch.setattr(builder, "MEMORY_DATASET_PATH", research_dir / "window20_memories.parquet")
    monkeypatch.setattr(builder, "MEMORY_META_PATH", research_dir / "window20_memories_meta.json")

    monkeypatch.setattr(index_service, "RESEARCH_DIR", research_dir)
    monkeypatch.setattr(index_service, "NUMERIC_FEATURES_PATH", research_dir / "window20_numeric_features.npy")
    monkeypatch.setattr(index_service, "NUMERIC_MEAN_PATH", research_dir / "window20_numeric_mean.npy")
    monkeypatch.setattr(index_service, "NUMERIC_STD_PATH", research_dir / "window20_numeric_std.npy")
    monkeypatch.setattr(index_service, "NUMERIC_SAMPLE_IDS_PATH", research_dir / "window20_sample_ids.npy")
    monkeypatch.setattr(index_service, "NUMERIC_INDEX_PATH", research_dir / "window20_numeric.index")
    monkeypatch.setattr(index_service, "TEXT_EMBEDDINGS_PATH", research_dir / "window20_text_embeddings.npy")
    monkeypatch.setattr(index_service, "TEXT_SAMPLE_IDS_PATH", research_dir / "window20_text_sample_ids.npy")
    monkeypatch.setattr(index_service, "TEXT_INDEX_PATH", research_dir / "window20_text.index")
    monkeypatch.setattr(index_service, "INDEX_META_PATH", research_dir / "window20_indexes_meta.json")


def test_build_window20_memory_dataset_creates_memory_rows(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    history = _make_history()

    stats = builder.build_window20_memory_dataset(history_frame=history)
    memories = builder.load_window20_memory_dataset()

    assert stats.memory_rows > 0
    assert not memories.empty
    assert {"sample_id", "window_start", "window_end", "memory_text", "pattern_summary"}.issubset(memories.columns)
    assert (memories["window_end"] > memories["window_start"]).all()
    assert memories["memory_text"].str.contains("ret_5|up_5|drawdown_5").sum() == 0


def test_build_window20_indexes_supports_text_embedding_and_persists(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    memories = builder.build_window20_memory_dataset(history_frame=_make_history())
    assert memories.memory_rows > 0

    stats = index_service.build_window20_indexes(
        memory_df=builder.load_window20_memory_dataset(),
        embedding_provider=FakeEmbeddingProvider(),
    )

    assert stats.memory_rows > 0
    assert stats.numeric_dim == len(builder.WINDOW20_FEATURE_COLUMNS)
    assert stats.text_enabled is True
    assert index_service.NUMERIC_FEATURES_PATH.exists()
    assert index_service.TEXT_EMBEDDINGS_PATH.exists()


def test_build_window20_indexes_degrades_when_embedding_is_missing(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    builder.build_window20_memory_dataset(history_frame=_make_history())

    stats = index_service.build_window20_indexes(
        memory_df=builder.load_window20_memory_dataset(),
        embedding_provider=None,
    )

    assert stats.text_enabled is False
    assert index_service.NUMERIC_FEATURES_PATH.exists()
    assert not index_service.TEXT_EMBEDDINGS_PATH.exists()


def test_query_window20_memories_returns_hybrid_results_and_distribution(tmp_path, monkeypatch):
    _patch_paths(monkeypatch, tmp_path)
    history = _make_history(rows_per_stock=260)
    builder.build_window20_memory_dataset(history_frame=history)
    memories = builder.load_window20_memory_dataset()
    index_service.build_window20_indexes(memory_df=memories, embedding_provider=FakeEmbeddingProvider())

    query_df = history[history["ts_code"] == "002594.SZ"].copy()
    result = index_service.query_window20_memories(
        query_df,
        top_k=10,
        numeric_k=20,
        text_k=20,
        embedding_provider=FakeEmbeddingProvider(),
    )

    assert result["query_pattern_summary"]
    assert result["distribution_stats"]["sample_count"] <= 10
    assert result["distribution_stats"]["sample_count"] > 0
    assert len(result["top_memories"]) == result["distribution_stats"]["sample_count"]
    assert all("hybrid_score" in item for item in result["top_memories"])

    query_end = pd.to_datetime(result["query_snapshot"]["window_end"])
    query_code = result["query_snapshot"]["ts_code"]
    for item in result["top_memories"]:
        item_end = pd.to_datetime(item["window_end"])
        assert item_end < query_end
        if item["ts_code"] == query_code:
            assert abs((query_end - item_end).days) >= 60

    scores = [item["hybrid_score"] for item in result["top_memories"]]
    assert scores == sorted(scores, reverse=True)
