"""Build and query the dual-channel memory indexes for 20-day windows."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

import numpy as np
import pandas as pd

from data.window20_memory_builder import (
    LABEL_COLUMNS,
    WINDOW20_FEATURE_COLUMNS,
    build_query_window20_record,
    build_window20_memory_dataset,
    load_window20_memory_dataset,
    load_window20_memory_metadata,
)
from utils.app_config import get_secret


RESEARCH_DIR = Path(__file__).resolve().parent.parent / "data" / "research"
NUMERIC_FEATURES_PATH = RESEARCH_DIR / "window20_numeric_features.npy"
NUMERIC_MEAN_PATH = RESEARCH_DIR / "window20_numeric_mean.npy"
NUMERIC_STD_PATH = RESEARCH_DIR / "window20_numeric_std.npy"
NUMERIC_SAMPLE_IDS_PATH = RESEARCH_DIR / "window20_sample_ids.npy"
NUMERIC_INDEX_PATH = RESEARCH_DIR / "window20_numeric.index"
TEXT_EMBEDDINGS_PATH = RESEARCH_DIR / "window20_text_embeddings.npy"
TEXT_SAMPLE_IDS_PATH = RESEARCH_DIR / "window20_text_sample_ids.npy"
TEXT_INDEX_PATH = RESEARCH_DIR / "window20_text.index"
INDEX_META_PATH = RESEARCH_DIR / "window20_indexes_meta.json"

NUMERIC_WEIGHT = 0.65
TEXT_WEIGHT = 0.35
OUTPUT_LABEL_MAP = {
    "ret_5": "label_ret_5",
    "ret_10": "label_ret_10",
    "ret_20": "label_ret_20",
    "up_5": "label_up_5",
    "up_10": "label_up_10",
    "up_20": "label_up_20",
    "drawdown_5": "label_drawdown_5",
    "drawdown_10": "label_drawdown_10",
    "drawdown_20": "label_drawdown_20",
    "max_up_5": "label_max_up_5",
    "max_up_10": "label_max_up_10",
    "max_up_20": "label_max_up_20",
}


class EmbeddingProvider(Protocol):
    def embed_texts(self, texts: list[str]) -> np.ndarray:
        ...


@dataclass(slots=True)
class IndexBuildStats:
    memory_rows: int
    numeric_dim: int
    text_dim: int
    numeric_backend: str
    text_backend: str
    text_enabled: bool
    built_at: str


def build_window20_indexes(
    *,
    force_rebuild_dataset: bool = False,
    memory_df: pd.DataFrame | None = None,
    embedding_provider: EmbeddingProvider | None = None,
    text_batch_size: int = 128,
) -> IndexBuildStats:
    """Build the numeric and optional text indexes."""
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)

    if memory_df is None:
        memory_df = load_window20_memory_dataset()
    if memory_df.empty or force_rebuild_dataset:
        build_window20_memory_dataset()
        memory_df = load_window20_memory_dataset()
    if memory_df.empty:
        raise ValueError("window20 memory dataset is empty")

    numeric_matrix, mean_vec, std_vec = _prepare_numeric_matrix(memory_df)
    np.save(NUMERIC_FEATURES_PATH, numeric_matrix)
    np.save(NUMERIC_MEAN_PATH, mean_vec)
    np.save(NUMERIC_STD_PATH, std_vec)
    np.save(NUMERIC_SAMPLE_IDS_PATH, memory_df["sample_id"].to_numpy(dtype=str))
    numeric_backend = _maybe_write_faiss_index(NUMERIC_INDEX_PATH, numeric_matrix)

    text_enabled = False
    text_backend = "disabled"
    text_dim = 0
    provider = embedding_provider or _build_default_embedding_provider()
    if provider is not None:
        texts = memory_df["memory_text"].fillna("").astype(str).tolist()
        embeddings = _batch_embed(provider, texts, batch_size=text_batch_size)
        embeddings = _normalize_rows(embeddings.astype(np.float32, copy=False))
        np.save(TEXT_EMBEDDINGS_PATH, embeddings)
        np.save(TEXT_SAMPLE_IDS_PATH, memory_df["sample_id"].to_numpy(dtype=str))
        text_backend = _maybe_write_faiss_index(TEXT_INDEX_PATH, embeddings)
        text_enabled = True
        text_dim = int(embeddings.shape[1])
    else:
        if TEXT_EMBEDDINGS_PATH.exists():
            TEXT_EMBEDDINGS_PATH.unlink()
        if TEXT_SAMPLE_IDS_PATH.exists():
            TEXT_SAMPLE_IDS_PATH.unlink()
        if TEXT_INDEX_PATH.exists():
            TEXT_INDEX_PATH.unlink()

    stats = IndexBuildStats(
        memory_rows=int(len(memory_df)),
        numeric_dim=int(numeric_matrix.shape[1]),
        text_dim=text_dim,
        numeric_backend=numeric_backend,
        text_backend=text_backend,
        text_enabled=text_enabled,
        built_at=datetime.now().isoformat(timespec="seconds"),
    )
    INDEX_META_PATH.write_text(json.dumps(asdict(stats), ensure_ascii=False, indent=2), encoding="utf-8")
    return stats


def query_window20_memories(
    price_window_df,
    *,
    top_k: int = 30,
    numeric_k: int = 50,
    text_k: int = 50,
    embedding_provider: EmbeddingProvider | None = None,
) -> dict:
    """Query the historical memory index using a raw price frame or a prebuilt sample."""
    memories = load_window20_memory_dataset()
    if memories.empty:
        raise ValueError("window20 memory dataset is missing, run build_window20_memory_dataset first")

    query_record = build_query_window20_record(price_window_df)
    numeric_matrix = _load_required_matrix(NUMERIC_FEATURES_PATH, "numeric features index")
    mean_vec = np.load(NUMERIC_MEAN_PATH)
    std_vec = np.load(NUMERIC_STD_PATH)
    numeric_ids = np.load(NUMERIC_SAMPLE_IDS_PATH, allow_pickle=False)

    id_to_row = memories.set_index("sample_id", drop=False)
    query_numeric = _prepare_query_numeric(query_record, mean_vec, std_vec)
    numeric_results = _search_matrix(numeric_matrix, numeric_ids, query_numeric, top_k=numeric_k)

    text_results: list[dict] = []
    if TEXT_EMBEDDINGS_PATH.exists() and TEXT_SAMPLE_IDS_PATH.exists():
        provider = embedding_provider or _build_default_embedding_provider()
        if provider is not None:
            query_embedding = _batch_embed(provider, [query_record["memory_text"]], batch_size=1)[0]
            query_embedding = _normalize_rows(np.asarray([query_embedding], dtype=np.float32))[0]
            text_matrix = np.load(TEXT_EMBEDDINGS_PATH)
            text_ids = np.load(TEXT_SAMPLE_IDS_PATH, allow_pickle=False)
            text_results = _search_matrix(text_matrix, text_ids, query_embedding, top_k=text_k)

    merged = _merge_channel_results(
        numeric_results=numeric_results,
        text_results=text_results,
        memories=id_to_row,
        query_record=query_record,
    )
    merged = merged[:top_k]
    distribution_stats = _build_distribution_stats(merged)

    return {
        "query_snapshot": _build_query_snapshot(query_record),
        "query_pattern_summary": query_record["pattern_summary"],
        "top_memories": merged,
        "distribution_stats": distribution_stats,
        "supporting_examples": merged[: min(5, len(merged))],
        "memory_metadata": load_window20_memory_metadata(),
        "index_metadata": _load_index_metadata(),
    }


def _prepare_numeric_matrix(memory_df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    frame = memory_df[WINDOW20_FEATURE_COLUMNS].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    matrix = frame.to_numpy(dtype=np.float32, copy=True)
    mean_vec = matrix.mean(axis=0).astype(np.float32)
    std_vec = matrix.std(axis=0).astype(np.float32)
    std_vec[std_vec < 1e-6] = 1.0
    normalized = (matrix - mean_vec) / std_vec
    normalized = _normalize_rows(normalized)
    return normalized, mean_vec, std_vec


def _prepare_query_numeric(query_record: dict, mean_vec: np.ndarray, std_vec: np.ndarray) -> np.ndarray:
    values = []
    for column in WINDOW20_FEATURE_COLUMNS:
        try:
            values.append(float(query_record.get(column, 0.0)))
        except Exception:
            values.append(0.0)
    vector = np.asarray(values, dtype=np.float32)
    vector = (vector - mean_vec) / std_vec
    vector = _normalize_rows(vector.reshape(1, -1))[0]
    return vector


def _search_matrix(matrix: np.ndarray, sample_ids: np.ndarray, query_vector: np.ndarray, *, top_k: int) -> list[dict]:
    if matrix.size == 0:
        return []
    scores = matrix @ query_vector
    rank_idx = np.argsort(scores)[::-1][:top_k]
    return [
        {
            "sample_id": str(sample_ids[idx]),
            "score": float(scores[idx]),
        }
        for idx in rank_idx
    ]


def _merge_channel_results(
    *,
    numeric_results: list[dict],
    text_results: list[dict],
    memories: pd.DataFrame,
    query_record: dict,
) -> list[dict]:
    merged: dict[str, dict] = {}
    query_end = pd.to_datetime(query_record["window_end"])
    query_code = str(query_record.get("ts_code", "")).upper()
    query_id = query_record.get("sample_id", "")

    for item in numeric_results:
        merged.setdefault(item["sample_id"], {}).update({"numeric_raw": item["score"]})
    for item in text_results:
        merged.setdefault(item["sample_id"], {}).update({"text_raw": item["score"]})

    results = []
    for sample_id, scores in merged.items():
        if sample_id == query_id or sample_id not in memories.index:
            continue
        row = memories.loc[sample_id]
        row_end = pd.to_datetime(row["window_end"])
        if row_end >= query_end:
            continue
        if str(row["ts_code"]).upper() == query_code and abs((query_end - row_end).days) < 60:
            continue

        numeric_score = _score_to_unit(scores.get("numeric_raw"))
        text_score = _score_to_unit(scores.get("text_raw"))
        if numeric_score is None and text_score is None:
            continue

        if numeric_score is None:
            hybrid = text_score
        elif text_score is None:
            hybrid = numeric_score
        else:
            hybrid = NUMERIC_WEIGHT * numeric_score + TEXT_WEIGHT * text_score

        item = {
            "sample_id": sample_id,
            "ts_code": row["ts_code"],
            "name": row.get("name", ""),
            "window_start": pd.to_datetime(row["window_start"]).strftime("%Y-%m-%d"),
            "window_end": pd.to_datetime(row["window_end"]).strftime("%Y-%m-%d"),
            "pattern_summary": row.get("pattern_summary", ""),
            "memory_text": row.get("memory_text", ""),
            "numeric_score": round(float(numeric_score), 4) if numeric_score is not None else None,
            "text_score": round(float(text_score), 4) if text_score is not None else None,
            "hybrid_score": round(float(hybrid), 4),
        }
        for output_label, stored_label in OUTPUT_LABEL_MAP.items():
            item[output_label] = _to_native(row.get(stored_label))
        results.append(item)

    results.sort(key=lambda item: item["hybrid_score"], reverse=True)
    return results


def _build_distribution_stats(matches: list[dict]) -> dict:
    if not matches:
        return {
            "sample_count": 0,
            "up_prob_5": 0.0,
            "up_prob_10": 0.0,
            "up_prob_20": 0.0,
            "avg_ret_5": 0.0,
            "avg_ret_10": 0.0,
            "avg_ret_20": 0.0,
            "median_ret_5": 0.0,
            "median_ret_10": 0.0,
            "median_ret_20": 0.0,
            "avg_drawdown_5": 0.0,
            "avg_drawdown_10": 0.0,
            "avg_drawdown_20": 0.0,
        }

    frame = pd.DataFrame(matches)
    return {
        "sample_count": int(len(frame)),
        "up_prob_5": round(float(frame["up_5"].mean() * 100), 2),
        "up_prob_10": round(float(frame["up_10"].mean() * 100), 2),
        "up_prob_20": round(float(frame["up_20"].mean() * 100), 2),
        "avg_ret_5": round(float(frame["ret_5"].mean() * 100), 2),
        "avg_ret_10": round(float(frame["ret_10"].mean() * 100), 2),
        "avg_ret_20": round(float(frame["ret_20"].mean() * 100), 2),
        "median_ret_5": round(float(frame["ret_5"].median() * 100), 2),
        "median_ret_10": round(float(frame["ret_10"].median() * 100), 2),
        "median_ret_20": round(float(frame["ret_20"].median() * 100), 2),
        "avg_drawdown_5": round(float(frame["drawdown_5"].mean() * 100), 2),
        "avg_drawdown_10": round(float(frame["drawdown_10"].mean() * 100), 2),
        "avg_drawdown_20": round(float(frame["drawdown_20"].mean() * 100), 2),
    }


def _build_query_snapshot(query_record: dict) -> dict:
    return {
        "sample_id": query_record["sample_id"],
        "ts_code": query_record.get("ts_code", ""),
        "name": query_record.get("name", ""),
        "window_start": pd.to_datetime(query_record["window_start"]).strftime("%Y-%m-%d"),
        "window_end": pd.to_datetime(query_record["window_end"]).strftime("%Y-%m-%d"),
        "pattern_summary": query_record["pattern_summary"],
        "memory_text": query_record["memory_text"],
    }


def _build_default_embedding_provider() -> EmbeddingProvider | None:
    api_key = get_secret("MEMORY_EMBEDDING_API_KEY", "") or get_secret("OPENAI_API_KEY", "")
    base_url = get_secret("MEMORY_EMBEDDING_BASE_URL", "") or get_secret("OPENAI_BASE_URL", "")
    model = get_secret("MEMORY_EMBEDDING_MODEL", "") or get_secret("OPENAI_EMBEDDING_MODEL", "")
    if not api_key or not model:
        return None
    return _OpenAIEmbeddingProvider(api_key=api_key, model=model, base_url=base_url or None)


class _OpenAIEmbeddingProvider:
    def __init__(self, *, api_key: str, model: str, base_url: str | None = None) -> None:
        from openai import OpenAI

        kwargs = {"api_key": api_key}
        if base_url:
            kwargs["base_url"] = base_url
        self._client = OpenAI(**kwargs)
        self._model = model

    def embed_texts(self, texts: list[str]) -> np.ndarray:
        response = self._client.embeddings.create(model=self._model, input=texts)
        vectors = [item.embedding for item in response.data]
        return np.asarray(vectors, dtype=np.float32)


def _batch_embed(provider: EmbeddingProvider, texts: list[str], *, batch_size: int) -> np.ndarray:
    outputs = []
    for start in range(0, len(texts), batch_size):
        batch = texts[start : start + batch_size]
        outputs.append(provider.embed_texts(batch))
    return np.vstack(outputs) if outputs else np.zeros((0, 0), dtype=np.float32)


def _normalize_rows(matrix: np.ndarray) -> np.ndarray:
    arr = np.asarray(matrix, dtype=np.float32)
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms[norms < 1e-8] = 1.0
    return arr / norms


def _maybe_write_faiss_index(path: Path, matrix: np.ndarray) -> str:
    try:
        import faiss  # type: ignore

        index = faiss.IndexFlatIP(matrix.shape[1])
        index.add(matrix.astype(np.float32))
        faiss.write_index(index, str(path))
        return "faiss"
    except Exception:
        if path.exists():
            path.unlink()
        return "numpy"


def _load_required_matrix(path: Path, label: str) -> np.ndarray:
    if not path.exists():
        raise ValueError(f"{label} is missing, run build_window20_indexes first")
    return np.load(path)


def _score_to_unit(value: float | None) -> float | None:
    if value is None:
        return None
    return max(0.0, min(1.0, (float(value) + 1.0) / 2.0))


def _to_native(value):
    if isinstance(value, np.generic):
        return value.item()
    return value


def _load_index_metadata() -> dict:
    if not INDEX_META_PATH.exists():
        return {}
    return json.loads(INDEX_META_PATH.read_text(encoding="utf-8"))
