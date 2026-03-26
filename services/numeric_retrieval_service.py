"""Multi-channel numeric ensemble retrieval for 20-day stock windows."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from data.window20_memory_builder import (
    LABEL_COLUMNS,
    PRICE_SEQUENCE_COLUMNS,
    PV_FLOW_SEQUENCE_COLUMNS,
    RETURN_SEQUENCE_COLUMNS,
    VOLUME_SEQUENCE_COLUMNS,
    WINDOW20_FEATURE_COLUMNS,
    build_query_window20_record,
    build_window20_numeric_ensemble_dataset,
    load_window20_numeric_ensemble_dataset,
)


RESEARCH_DIR = Path(__file__).resolve().parent.parent / "data" / "research"

STATE_FEATURES_PATH = RESEARCH_DIR / "window20_state_features.npy"
STATE_IDS_PATH = RESEARCH_DIR / "window20_state_ids.npy"
STATE_STATS_PATH = RESEARCH_DIR / "window20_state_stats.json"
STATE_INDEX_PATH = RESEARCH_DIR / "window20_state.index"

PRICE_SHAPE_PATH = RESEARCH_DIR / "window20_price_shape.npy"
PRICE_RET_SHAPE_PATH = RESEARCH_DIR / "window20_price_returns.npy"
PRICE_IDS_PATH = RESEARCH_DIR / "window20_price_shape_ids.npy"
PRICE_INDEX_PATH = RESEARCH_DIR / "window20_price_shape.index"
PRICE_MP_PATH = RESEARCH_DIR / "window20_price_mp.npy"
PRICE_MP_META_PATH = RESEARCH_DIR / "window20_price_mp_meta.json"

VOLUME_SHAPE_PATH = RESEARCH_DIR / "window20_volume_shape.npy"
VOLUME_FLOW_PATH = RESEARCH_DIR / "window20_volume_flow.npy"
VOLUME_IDS_PATH = RESEARCH_DIR / "window20_volume_shape_ids.npy"
VOLUME_INDEX_PATH = RESEARCH_DIR / "window20_volume_shape.index"

LABEL_CONSISTENCY_PATH = RESEARCH_DIR / "window20_label_consistency.npy"
LABEL_IDS_PATH = RESEARCH_DIR / "window20_label_ids.npy"
ENSEMBLE_META_PATH = RESEARCH_DIR / "window20_numeric_ensemble_meta.json"

BASE_STATE_WEIGHT = 0.35
BASE_PRICE_WEIGHT = 0.35
BASE_VOLUME_WEIGHT = 0.15
BASE_LABEL_WEIGHT = 0.15
STUMPY_RERANK_LIMIT = 120

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


@dataclass(slots=True)
class IndexBuildStats:
    memory_rows: int
    state_dim: int
    price_dim: int
    volume_dim: int
    state_backend: str
    price_backend: str
    volume_backend: str
    built_at: str


def build_window20_numeric_ensemble_indexes(
    *,
    force_rebuild_dataset: bool = False,
    memory_df: pd.DataFrame | None = None,
    label_neighbors: int = 20,
) -> IndexBuildStats:
    RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
    if memory_df is None:
        memory_df = load_window20_numeric_ensemble_dataset()
    if memory_df.empty or force_rebuild_dataset:
        build_window20_numeric_ensemble_dataset()
        memory_df = load_window20_numeric_ensemble_dataset()
    if memory_df.empty:
        raise ValueError("numeric ensemble dataset is empty")

    state_raw = memory_df[WINDOW20_FEATURE_COLUMNS].apply(pd.to_numeric, errors="coerce").fillna(0.0).to_numpy(dtype=np.float32)
    state_mean = state_raw.mean(axis=0).astype(np.float32)
    state_std = state_raw.std(axis=0).astype(np.float32)
    state_std[state_std < 1e-6] = 1.0
    state_scaled = (state_raw - state_mean) / state_std
    state_cosine = _normalize_rows(state_scaled)
    inv_var = 1.0 / np.square(state_std)

    np.save(STATE_FEATURES_PATH, state_cosine)
    np.save(STATE_IDS_PATH, memory_df["sample_id"].to_numpy(dtype=str))
    STATE_STATS_PATH.write_text(
        json.dumps(
            {
                "feature_columns": WINDOW20_FEATURE_COLUMNS,
                "mean": state_mean.tolist(),
                "std": state_std.tolist(),
                "inv_var": inv_var.tolist(),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    state_backend = _maybe_write_faiss_index(STATE_INDEX_PATH, state_cosine)

    price_close = memory_df[PRICE_SEQUENCE_COLUMNS].apply(pd.to_numeric, errors="coerce").fillna(0.0).to_numpy(dtype=np.float32)
    price_ret = memory_df[RETURN_SEQUENCE_COLUMNS].apply(pd.to_numeric, errors="coerce").fillna(0.0).to_numpy(dtype=np.float32)
    price_shape = _normalize_rows(np.concatenate([price_close, price_ret], axis=1))
    np.save(PRICE_SHAPE_PATH, price_close)
    np.save(PRICE_RET_SHAPE_PATH, price_ret)
    np.save(PRICE_IDS_PATH, memory_df["sample_id"].to_numpy(dtype=str))
    price_backend = _maybe_write_faiss_index(PRICE_INDEX_PATH, price_shape)

    volume_shape = memory_df[VOLUME_SEQUENCE_COLUMNS].apply(pd.to_numeric, errors="coerce").fillna(0.0).to_numpy(dtype=np.float32)
    volume_flow = memory_df[PV_FLOW_SEQUENCE_COLUMNS].apply(pd.to_numeric, errors="coerce").fillna(0.0).to_numpy(dtype=np.float32)
    volume_search = _normalize_rows(np.concatenate([volume_shape, volume_flow], axis=1))
    np.save(VOLUME_SHAPE_PATH, volume_shape)
    np.save(VOLUME_FLOW_PATH, volume_flow)
    np.save(VOLUME_IDS_PATH, memory_df["sample_id"].to_numpy(dtype=str))
    volume_backend = _maybe_write_faiss_index(VOLUME_INDEX_PATH, volume_search)

    price_mp = _build_price_matrix_profile_proxy(price_close)
    np.save(PRICE_MP_PATH, price_mp)
    PRICE_MP_META_PATH.write_text(
        json.dumps({"method": "stumpy" if _has_stumpy() else "euclidean_proxy"}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    label_consistency = _compute_label_consistency(memory_df, state_cosine, neighbor_count=label_neighbors)
    np.save(LABEL_CONSISTENCY_PATH, label_consistency.astype(np.float32))
    np.save(LABEL_IDS_PATH, memory_df["sample_id"].to_numpy(dtype=str))

    stats = IndexBuildStats(
        memory_rows=int(len(memory_df)),
        state_dim=int(state_cosine.shape[1]),
        price_dim=int(price_shape.shape[1]),
        volume_dim=int(volume_search.shape[1]),
        state_backend=state_backend,
        price_backend=price_backend,
        volume_backend=volume_backend,
        built_at=datetime.now().isoformat(timespec="seconds"),
    )
    ENSEMBLE_META_PATH.write_text(json.dumps(asdict(stats), ensure_ascii=False, indent=2), encoding="utf-8")
    return stats


def query_window20_numeric_ensemble(
    price_window_df,
    *,
    top_k: int = 30,
    state_k: int = 200,
    price_k: int = 200,
    volume_k: int = 200,
) -> dict:
    memories = load_window20_numeric_ensemble_dataset()
    if memories.empty:
        raise ValueError("window20 numeric ensemble dataset is missing, run build_window20_numeric_ensemble_dataset first")

    query_record = build_query_window20_record(price_window_df)
    id_to_row = memories.set_index("sample_id", drop=False)
    query_regime = _build_query_regime_profile(query_record)
    weight_profile = _resolve_dynamic_weights(query_regime)

    state_stats = json.loads(STATE_STATS_PATH.read_text(encoding="utf-8"))
    state_cosine = np.load(STATE_FEATURES_PATH)
    state_ids = np.load(STATE_IDS_PATH, allow_pickle=False)
    state_scaled_query = _build_query_state_vector(query_record, state_stats)

    state_results = _search_state_channel(
        state_cosine=state_cosine,
        state_ids=state_ids,
        query_scaled=state_scaled_query,
        inv_var=np.asarray(state_stats["inv_var"], dtype=np.float32),
        top_k=state_k,
    )

    price_results = _search_price_channel(
        query_record=query_record,
        sample_ids=np.load(PRICE_IDS_PATH, allow_pickle=False),
        price_close=np.load(PRICE_SHAPE_PATH),
        price_ret=np.load(PRICE_RET_SHAPE_PATH),
        price_mp=np.load(PRICE_MP_PATH),
        top_k=price_k,
    )

    volume_results = _search_volume_channel(
        query_record=query_record,
        sample_ids=np.load(VOLUME_IDS_PATH, allow_pickle=False),
        volume_shape=np.load(VOLUME_SHAPE_PATH),
        volume_flow=np.load(VOLUME_FLOW_PATH),
        top_k=volume_k,
    )

    label_consistency = np.load(LABEL_CONSISTENCY_PATH)
    label_ids = np.load(LABEL_IDS_PATH, allow_pickle=False)
    label_map = {str(sample_id): float(score) for sample_id, score in zip(label_ids, label_consistency, strict=True)}

    merged = _merge_numeric_channel_results(
        state_results=state_results,
        price_results=price_results,
        volume_results=volume_results,
        label_map=label_map,
        memories=id_to_row,
        query_record=query_record,
        query_regime=query_regime,
        weight_profile=weight_profile,
    )
    merged = merged[:top_k]

    return {
        "query_snapshot": _build_query_snapshot(query_record),
        "query_regime_summary": query_regime,
        "weight_profile": weight_profile,
        "channel_scores_summary": _build_channel_scores_summary(merged),
        "top_memories": merged,
        "distribution_stats": _build_distribution_stats(merged),
        "supporting_examples": merged[: min(5, len(merged))],
        "index_metadata": _load_ensemble_metadata(),
    }


def _search_state_channel(
    *,
    state_cosine: np.ndarray,
    state_ids: np.ndarray,
    query_scaled: np.ndarray,
    inv_var: np.ndarray,
    top_k: int,
) -> list[dict]:
    query_cosine = _normalize_rows(query_scaled.reshape(1, -1))[0]
    cosine_scores = state_cosine @ query_cosine
    diff = state_cosine - query_scaled
    mahal = np.sqrt(np.sum(np.square(diff) * inv_var, axis=1))
    rank_idx = np.argsort(cosine_scores)[::-1][:top_k]
    return [
        {
            "sample_id": str(state_ids[idx]),
            "cosine": float(cosine_scores[idx]),
            "mahal": float(mahal[idx]),
        }
        for idx in rank_idx
    ]


def _search_price_channel(
    *,
    query_record: dict,
    sample_ids: np.ndarray,
    price_close: np.ndarray,
    price_ret: np.ndarray,
    price_mp: np.ndarray,
    top_k: int,
) -> list[dict]:
    query_close = _build_sequence_query(query_record, PRICE_SEQUENCE_COLUMNS)
    query_ret = _build_sequence_query(query_record, RETURN_SEQUENCE_COLUMNS)

    euclidean = np.linalg.norm(price_close - query_close, axis=1)
    dtw_scores = np.asarray([_dtw_distance(query_close, row) for row in price_close], dtype=np.float32)
    mp_scores = _matrix_profile_scores(query_close, price_close, price_mp)
    ret_shape_dist = np.linalg.norm(price_ret - query_ret, axis=1)

    coarse_rank_idx = np.argsort(euclidean)[: max(top_k, min(STUMPY_RERANK_LIMIT, len(sample_ids)))]
    stumpy_lookup = _stumpy_price_scores(query_close, price_close, coarse_rank_idx)

    coarse_price_score = np.asarray(
        [
            _combine_price_scores(
                float(euclidean[idx]),
                float(dtw_scores[idx]),
                float(mp_scores[idx]),
                float(ret_shape_dist[idx]),
                float(stumpy_lookup.get(int(idx), 0.0)),
            )
            for idx in range(len(sample_ids))
        ],
        dtype=np.float32,
    )
    rank_idx = np.argsort(coarse_price_score)[::-1][:top_k]
    return [
        {
            "sample_id": str(sample_ids[idx]),
            "price_euclidean": float(euclidean[idx]),
            "price_dtw": float(dtw_scores[idx]),
            "price_mp": float(mp_scores[idx]),
            "price_ret_shape_dist": float(ret_shape_dist[idx]),
            "stumpy_price_score": float(stumpy_lookup.get(int(idx), 0.0)),
        }
        for idx in rank_idx
    ]


def _search_volume_channel(
    *,
    query_record: dict,
    sample_ids: np.ndarray,
    volume_shape: np.ndarray,
    volume_flow: np.ndarray,
    top_k: int,
) -> list[dict]:
    query_vol = _build_sequence_query(query_record, VOLUME_SEQUENCE_COLUMNS)
    query_flow = _build_sequence_query(query_record, PV_FLOW_SEQUENCE_COLUMNS)

    euclidean = np.linalg.norm(volume_shape - query_vol, axis=1)
    dtw_scores = np.asarray([_dtw_distance(query_vol, row) for row in volume_shape], dtype=np.float32)
    flow_dist = np.linalg.norm(volume_flow - query_flow, axis=1)

    rank_idx = np.argsort(euclidean)[:top_k]
    return [
        {
            "sample_id": str(sample_ids[idx]),
            "volume_euclidean": float(euclidean[idx]),
            "volume_dtw": float(dtw_scores[idx]),
            "volume_flow_dist": float(flow_dist[idx]),
        }
        for idx in rank_idx
    ]


def _merge_numeric_channel_results(
    *,
    state_results: list[dict],
    price_results: list[dict],
    volume_results: list[dict],
    label_map: dict[str, float],
    memories: pd.DataFrame,
    query_record: dict,
    query_regime: dict,
    weight_profile: dict,
) -> list[dict]:
    merged: dict[str, dict] = {}
    query_end = pd.to_datetime(query_record["window_end"])
    query_code = str(query_record.get("ts_code", "")).upper()
    query_id = query_record.get("sample_id", "")
    query_close = _build_sequence_query(query_record, PRICE_SEQUENCE_COLUMNS)
    query_ret = _build_sequence_query(query_record, RETURN_SEQUENCE_COLUMNS)
    query_vol = _build_sequence_query(query_record, VOLUME_SEQUENCE_COLUMNS)
    query_flow = _build_sequence_query(query_record, PV_FLOW_SEQUENCE_COLUMNS)

    for item in state_results:
        merged.setdefault(item["sample_id"], {}).update(item)
    for item in price_results:
        merged.setdefault(item["sample_id"], {}).update(item)
    for item in volume_results:
        merged.setdefault(item["sample_id"], {}).update(item)

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

        _fill_missing_shape_scores(scores, row, query_close=query_close, query_ret=query_ret, query_vol=query_vol, query_flow=query_flow)
        state_score = _combine_state_scores(scores.get("cosine"), scores.get("mahal"))
        price_score = _combine_price_scores(
            scores.get("price_euclidean"),
            scores.get("price_dtw"),
            scores.get("price_mp"),
            scores.get("price_ret_shape_dist"),
            scores.get("stumpy_price_score"),
        )
        volume_score = _combine_volume_scores(
            scores.get("volume_euclidean"),
            scores.get("volume_dtw"),
            scores.get("volume_flow_dist"),
            volume_channel=True,
        )
        label_score = _clip01(float(np.nan_to_num(label_map.get(sample_id, 0.0), nan=0.0)))
        regime_match_score = _compute_regime_match_score(query_regime, row)
        freshness_score = _compute_freshness_score(query_end, row_end)
        outcome_quality_score = _compute_outcome_quality_score(row)
        anti_example_penalty = _compute_anti_example_penalty(row)

        base_score = (
            weight_profile["state"] * state_score
            + weight_profile["price"] * price_score
            + weight_profile["volume"] * volume_score
            + weight_profile["label"] * label_score
        )
        calibration_score = (
            0.45 * regime_match_score
            + 0.30 * outcome_quality_score
            + 0.25 * freshness_score
        )
        ensemble_score = _clip01((0.82 * base_score + 0.18 * calibration_score) * (1.0 - 0.22 * anti_example_penalty))

        item = {
            "sample_id": sample_id,
            "ts_code": row["ts_code"],
            "name": row.get("name", ""),
            "window_start": pd.to_datetime(row["window_start"]).strftime("%Y-%m-%d"),
            "window_end": pd.to_datetime(row["window_end"]).strftime("%Y-%m-%d"),
            "pattern_summary": row.get("pattern_summary", ""),
            "state_score": round(float(state_score), 4),
            "price_shape_score": round(float(price_score), 4),
            "stumpy_price_score": round(float(_clip01(float(scores.get("stumpy_price_score", 0.0)))), 4),
            "volume_shape_score": round(float(volume_score), 4),
            "label_consistency_score": round(float(label_score), 4),
            "regime_match_score": round(float(regime_match_score), 4),
            "freshness_score": round(float(freshness_score), 4),
            "outcome_quality_score": round(float(outcome_quality_score), 4),
            "anti_example_penalty": round(float(anti_example_penalty), 4),
            "ensemble_score": round(float(ensemble_score), 4),
        }
        for output_label, stored_label in OUTPUT_LABEL_MAP.items():
            item[output_label] = _to_native(row.get(stored_label))
        results.append(item)

    results.sort(key=lambda item: item["ensemble_score"], reverse=True)
    return results


def _fill_missing_shape_scores(
    scores: dict,
    row: pd.Series,
    *,
    query_close: np.ndarray,
    query_ret: np.ndarray,
    query_vol: np.ndarray,
    query_flow: np.ndarray,
) -> None:
    if "price_euclidean" not in scores or "price_dtw" not in scores or "price_ret_shape_dist" not in scores:
        candidate_close = np.asarray([float(row.get(column, 0.0) or 0.0) for column in PRICE_SEQUENCE_COLUMNS], dtype=np.float32)
        candidate_ret = np.asarray([float(row.get(column, 0.0) or 0.0) for column in RETURN_SEQUENCE_COLUMNS], dtype=np.float32)
        scores.setdefault("price_euclidean", float(np.linalg.norm(candidate_close - query_close)))
        scores.setdefault("price_dtw", float(_dtw_distance(query_close, candidate_close)))
        scores.setdefault("price_ret_shape_dist", float(np.linalg.norm(candidate_ret - query_ret)))
        if "price_mp" not in scores:
            scores["price_mp"] = float(_single_matrix_profile_score(query_close, candidate_close))
        if "stumpy_price_score" not in scores:
            scores["stumpy_price_score"] = float(_single_stumpy_price_score(query_close, candidate_close))

    if "volume_euclidean" not in scores or "volume_dtw" not in scores or "volume_flow_dist" not in scores:
        candidate_vol = np.asarray([float(row.get(column, 0.0) or 0.0) for column in VOLUME_SEQUENCE_COLUMNS], dtype=np.float32)
        candidate_flow = np.asarray([float(row.get(column, 0.0) or 0.0) for column in PV_FLOW_SEQUENCE_COLUMNS], dtype=np.float32)
        scores.setdefault("volume_euclidean", float(np.linalg.norm(candidate_vol - query_vol)))
        scores.setdefault("volume_dtw", float(_dtw_distance(query_vol, candidate_vol)))
        scores.setdefault("volume_flow_dist", float(np.linalg.norm(candidate_flow - query_flow)))


def _compute_label_consistency(memory_df: pd.DataFrame, state_cosine: np.ndarray, *, neighbor_count: int) -> np.ndarray:
    future_up = np.nan_to_num(
        memory_df[["label_up_5", "label_up_10", "label_up_20"]].to_numpy(dtype=np.float32),
        nan=0.0,
    )
    future_ret = np.nan_to_num(
        memory_df[["label_ret_5", "label_ret_10", "label_ret_20"]].to_numpy(dtype=np.float32),
        nan=0.0,
    )
    future_dd = np.nan_to_num(
        memory_df[["label_drawdown_5", "label_drawdown_10", "label_drawdown_20"]].to_numpy(dtype=np.float32),
        nan=0.0,
    )

    scores = np.zeros(len(memory_df), dtype=np.float32)
    similarity = state_cosine @ state_cosine.T
    for idx in range(len(memory_df)):
        neighbors = np.argsort(similarity[idx])[::-1]
        neighbors = neighbors[neighbors != idx][:neighbor_count]
        if len(neighbors) == 0:
            continue
        up_consistency = np.mean(np.abs(future_up[neighbors].mean(axis=0) - 0.5) * 2.0)
        ret_stability = np.mean(1.0 / (1.0 + np.std(future_ret[neighbors] * 100.0, axis=0)))
        dd_stability = np.mean(1.0 / (1.0 + np.std(future_dd[neighbors] * 100.0, axis=0)))
        expectancy = np.mean(1.0 / (1.0 + np.exp(-future_ret[neighbors].mean(axis=0) * 15.0)))
        downside = np.mean(np.clip(-future_ret[neighbors], 0.0, None))
        downside_control = 1.0 / (1.0 + downside * 25.0)
        raw_score = (
            0.28 * up_consistency
            + 0.22 * ret_stability
            + 0.18 * dd_stability
            + 0.20 * expectancy
            + 0.12 * downside_control
        )
        scores[idx] = float(np.clip(np.nan_to_num(raw_score, nan=0.0), 0.0, 1.0))
    return scores


def _build_query_state_vector(query_record: dict, stats: dict) -> np.ndarray:
    values = []
    for column in WINDOW20_FEATURE_COLUMNS:
        try:
            values.append(float(query_record.get(column, 0.0)))
        except Exception:
            values.append(0.0)
    vector = np.asarray(values, dtype=np.float32)
    mean = np.asarray(stats["mean"], dtype=np.float32)
    std = np.asarray(stats["std"], dtype=np.float32)
    return (vector - mean) / std


def _build_sequence_query(query_record: dict, columns: list[str]) -> np.ndarray:
    values = []
    for column in columns:
        try:
            values.append(float(query_record.get(column, 0.0)))
        except Exception:
            values.append(0.0)
    return np.asarray(values, dtype=np.float32)


def _build_price_matrix_profile_proxy(price_close: np.ndarray) -> np.ndarray:
    if _has_stumpy():
        try:
            import stumpy  # type: ignore

            profiles = []
            for row in price_close:
                profile = stumpy.stump(row, m=max(4, len(row) // 2))
                profiles.append(float(np.nanmean(profile[:, 0])) if len(profile) else 0.0)
            return np.asarray(profiles, dtype=np.float32)
        except Exception:
            pass
    return np.linalg.norm(np.diff(price_close, axis=1), axis=1).astype(np.float32)


def _matrix_profile_scores(query_close: np.ndarray, price_close: np.ndarray, price_mp: np.ndarray) -> np.ndarray:
    if _has_stumpy():
        try:
            import stumpy  # type: ignore

            scores = np.zeros(len(price_close), dtype=np.float32)
            for idx, candidate in enumerate(price_close):
                join = stumpy.mass(query_close, candidate)
                best = float(np.nanmin(join)) if len(join) else 0.0
                scores[idx] = 1.0 / (1.0 + best)
            return scores
        except Exception:
            pass
    query_proxy = float(np.linalg.norm(np.diff(query_close)))
    return 1.0 / (1.0 + np.abs(price_mp - query_proxy))


def _single_matrix_profile_score(query_close: np.ndarray, candidate_close: np.ndarray) -> float:
    if _has_stumpy():
        try:
            import stumpy  # type: ignore

            join = stumpy.mass(query_close.astype(np.float64), candidate_close.astype(np.float64))
            best = float(np.nanmin(join)) if len(join) else np.inf
            if not np.isfinite(best):
                best = np.inf
            return _clip01(1.0 / (1.0 + best))
        except Exception:
            pass
    query_proxy = float(np.linalg.norm(np.diff(query_close)))
    candidate_proxy = float(np.linalg.norm(np.diff(candidate_close)))
    return _clip01(1.0 / (1.0 + abs(query_proxy - candidate_proxy)))


def _combine_state_scores(cosine: float | None, mahal: float | None) -> float:
    cos_score = _clip01((float(cosine) + 1.0) / 2.0 if cosine is not None else 0.0)
    mahal_score = _clip01(1.0 / (1.0 + float(mahal)) if mahal is not None else 0.0)
    return (cos_score + mahal_score) / 2.0


def _combine_price_scores(
    euclidean: float | None,
    dtw: float | None,
    mp: float | None,
    ret_shape_dist: float | None,
    stumpy_price_score: float | None = None,
) -> float:
    euclid_score = _clip01(1.0 / (1.0 + float(euclidean)) if euclidean is not None else 0.0)
    dtw_score = _clip01(1.0 / (1.0 + float(dtw)) if dtw is not None else 0.0)
    mp_score = _clip01(float(mp) if mp is not None else 0.0)
    ret_score = _clip01(1.0 / (1.0 + float(ret_shape_dist)) if ret_shape_dist is not None else 0.0)
    stumpy_score = _clip01(float(stumpy_price_score) if stumpy_price_score is not None else 0.0)
    if stumpy_price_score is None:
        return (euclid_score + dtw_score + mp_score + ret_score) / 4.0
    return (
        0.22 * euclid_score
        + 0.18 * dtw_score
        + 0.18 * mp_score
        + 0.17 * ret_score
        + 0.25 * stumpy_score
    )


def _combine_volume_scores(
    euclidean: float | None,
    dtw: float | None,
    flow_dist: float | None,
    *,
    volume_channel: bool = False,
) -> float:
    euclid_score = _clip01(1.0 / (1.0 + float(euclidean)) if euclidean is not None else 0.0)
    dtw_score = _clip01(1.0 / (1.0 + float(dtw)) if dtw is not None else 0.0)
    flow_score = _clip01(1.0 / (1.0 + float(flow_dist)) if flow_dist is not None else 0.0)
    score = (euclid_score + dtw_score + flow_score) / 3.0
    return score if volume_channel else _clip01(score)


def _dtw_distance(a: np.ndarray, b: np.ndarray) -> float:
    a = np.asarray(a, dtype=np.float32)
    b = np.asarray(b, dtype=np.float32)
    dp = np.full((len(a) + 1, len(b) + 1), np.inf, dtype=np.float32)
    dp[0, 0] = 0.0
    for i in range(1, len(a) + 1):
        for j in range(1, len(b) + 1):
            cost = abs(a[i - 1] - b[j - 1])
            dp[i, j] = cost + min(dp[i - 1, j], dp[i, j - 1], dp[i - 1, j - 1])
    return float(dp[len(a), len(b)] / max(len(a), len(b)))


def _stumpy_price_scores(query_close: np.ndarray, price_close: np.ndarray, candidate_indices: np.ndarray) -> dict[int, float]:
    candidate_indices = np.asarray(candidate_indices, dtype=int)
    if len(candidate_indices) == 0:
        return {}

    if _has_stumpy():
        try:
            import stumpy  # type: ignore

            scores: dict[int, float] = {}
            for idx in candidate_indices.tolist():
                candidate = price_close[idx]
                mass_profile = stumpy.mass(query_close.astype(np.float64), candidate.astype(np.float64))
                best = float(np.nanmin(mass_profile)) if len(mass_profile) else np.inf
                if not np.isfinite(best):
                    best = np.inf
                scores[int(idx)] = _clip01(1.0 / (1.0 + best))
            return scores
        except Exception:
            pass

    fallback_scores: dict[int, float] = {}
    for idx in candidate_indices.tolist():
        candidate = price_close[idx]
        fallback_scores[int(idx)] = _single_stumpy_price_score(query_close, candidate)
    return fallback_scores


def _single_stumpy_price_score(query_close: np.ndarray, candidate_close: np.ndarray) -> float:
    if _has_stumpy():
        try:
            import stumpy  # type: ignore

            mass_profile = stumpy.mass(query_close.astype(np.float64), candidate_close.astype(np.float64))
            best = float(np.nanmin(mass_profile)) if len(mass_profile) else np.inf
            if not np.isfinite(best):
                best = np.inf
            return _clip01(1.0 / (1.0 + best))
        except Exception:
            pass
    return _clip01(1.0 / (1.0 + np.linalg.norm(query_close - candidate_close)))


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
        try:
            from sklearn.neighbors import NearestNeighbors  # type: ignore

            # Validation only; actual persisted fallback stays numpy-based.
            NearestNeighbors(metric="cosine").fit(matrix)
            if path.exists():
                path.unlink()
            return "sklearn"
        except Exception:
            if path.exists():
                path.unlink()
            return "numpy"


def _build_query_snapshot(query_record: dict) -> dict:
    return {
        "sample_id": query_record["sample_id"],
        "ts_code": query_record.get("ts_code", ""),
        "name": query_record.get("name", ""),
        "window_start": pd.to_datetime(query_record["window_start"]).strftime("%Y-%m-%d"),
        "window_end": pd.to_datetime(query_record["window_end"]).strftime("%Y-%m-%d"),
        "pattern_summary": query_record["pattern_summary"],
    }


def _build_query_regime_profile(query_record: dict) -> dict:
    trend_20 = float(query_record.get("window_return_20", 0.0) or 0.0)
    vol_20 = float(query_record.get("window_volatility_20", 0.0) or 0.0)
    rel_5 = float(query_record.get("relative_strength_5", 0.0) or 0.0)
    volume_trend = float(query_record.get("volume_trend_20", 0.0) or 0.0)
    dist_high = float(query_record.get("dist_high_20", 0.0) or 0.0)
    dist_low = float(query_record.get("dist_low_20", 0.0) or 0.0)

    if trend_20 >= 0.08:
        trend_bucket = "strong_up"
    elif trend_20 <= -0.08:
        trend_bucket = "strong_down"
    elif abs(trend_20) <= 0.025:
        trend_bucket = "sideways"
    else:
        trend_bucket = "moderate"

    if vol_20 >= 0.045:
        volatility_bucket = "high"
    elif vol_20 <= 0.02:
        volatility_bucket = "low"
    else:
        volatility_bucket = "medium"

    if dist_high <= 0.08:
        location_bucket = "near_high"
    elif dist_low <= 0.08:
        location_bucket = "near_low"
    else:
        location_bucket = "mid_range"

    return {
        "trend_20": round(trend_20, 6),
        "volatility_20": round(vol_20, 6),
        "relative_strength_5": round(rel_5, 6),
        "volume_trend_20": round(volume_trend, 6),
        "dist_high_20": round(dist_high, 6),
        "dist_low_20": round(dist_low, 6),
        "trend_bucket": trend_bucket,
        "volatility_bucket": volatility_bucket,
        "location_bucket": location_bucket,
    }


def _resolve_dynamic_weights(query_regime: dict) -> dict:
    state = BASE_STATE_WEIGHT
    price = BASE_PRICE_WEIGHT
    volume = BASE_VOLUME_WEIGHT
    label = BASE_LABEL_WEIGHT

    trend_20 = abs(float(query_regime.get("trend_20", 0.0)))
    volatility_20 = float(query_regime.get("volatility_20", 0.0))
    volume_trend = abs(float(query_regime.get("volume_trend_20", 0.0)))

    if trend_20 >= 0.08:
        price += 0.04
        state -= 0.02
        label -= 0.01
    if volatility_20 >= 0.045:
        label += 0.04
        price -= 0.02
        state -= 0.01
        volume -= 0.01
    if volume_trend >= 0.15:
        volume += 0.04
        state -= 0.01
        price -= 0.02
        label -= 0.01

    weights = np.asarray([state, price, volume, label], dtype=np.float32)
    weights = np.clip(weights, 0.05, None)
    weights = weights / weights.sum()
    return {
        "state": round(float(weights[0]), 4),
        "price": round(float(weights[1]), 4),
        "volume": round(float(weights[2]), 4),
        "label": round(float(weights[3]), 4),
    }


def _compute_regime_match_score(query_regime: dict, row: pd.Series) -> float:
    candidate = {
        "trend_20": float(row.get("window_return_20", 0.0) or 0.0),
        "volatility_20": float(row.get("window_volatility_20", 0.0) or 0.0),
        "relative_strength_5": float(row.get("relative_strength_5", 0.0) or 0.0),
        "volume_trend_20": float(row.get("volume_trend_20", 0.0) or 0.0),
        "dist_high_20": float(row.get("dist_high_20", 0.0) or 0.0),
        "dist_low_20": float(row.get("dist_low_20", 0.0) or 0.0),
    }
    feature_scales = {
        "trend_20": 0.12,
        "volatility_20": 0.05,
        "relative_strength_5": 0.08,
        "volume_trend_20": 0.20,
        "dist_high_20": 0.20,
        "dist_low_20": 0.20,
    }
    component_scores = []
    for key, scale in feature_scales.items():
        diff = abs(float(query_regime.get(key, 0.0)) - candidate[key])
        component_scores.append(1.0 / (1.0 + diff / max(scale, 1e-6)))

    bucket_bonus = 0.0
    query_location = query_regime.get("location_bucket", "")
    candidate_location = "near_high" if candidate["dist_high_20"] <= 0.08 else "near_low" if candidate["dist_low_20"] <= 0.08 else "mid_range"
    if query_location == candidate_location:
        bucket_bonus += 0.08
    query_trend_bucket = query_regime.get("trend_bucket", "")
    candidate_trend_bucket = "strong_up" if candidate["trend_20"] >= 0.08 else "strong_down" if candidate["trend_20"] <= -0.08 else "sideways" if abs(candidate["trend_20"]) <= 0.025 else "moderate"
    if query_trend_bucket == candidate_trend_bucket:
        bucket_bonus += 0.08

    return _clip01(float(np.mean(component_scores)) + bucket_bonus)


def _compute_freshness_score(query_end: pd.Timestamp, row_end: pd.Timestamp) -> float:
    days_gap = max((query_end - row_end).days, 1)
    return _clip01(float(np.exp(-days_gap / 900.0)))


def _compute_outcome_quality_score(row: pd.Series) -> float:
    ret_values = np.asarray(
        [
            float(row.get("label_ret_5", 0.0) or 0.0),
            float(row.get("label_ret_10", 0.0) or 0.0),
            float(row.get("label_ret_20", 0.0) or 0.0),
        ],
        dtype=np.float32,
    )
    drawdown_values = np.asarray(
        [
            float(row.get("label_drawdown_5", 0.0) or 0.0),
            float(row.get("label_drawdown_10", 0.0) or 0.0),
            float(row.get("label_drawdown_20", 0.0) or 0.0),
        ],
        dtype=np.float32,
    )
    max_up_values = np.asarray(
        [
            float(row.get("label_max_up_5", 0.0) or 0.0),
            float(row.get("label_max_up_10", 0.0) or 0.0),
            float(row.get("label_max_up_20", 0.0) or 0.0),
        ],
        dtype=np.float32,
    )

    expectancy = float(1.0 / (1.0 + np.exp(-ret_values.mean() * 18.0)))
    drawdown_control = float(1.0 / (1.0 + abs(drawdown_values.mean()) * 18.0))
    upside_capture = float(1.0 / (1.0 + np.exp(-max_up_values.mean() * 12.0)))
    return _clip01(0.45 * expectancy + 0.30 * drawdown_control + 0.25 * upside_capture)


def _compute_anti_example_penalty(row: pd.Series) -> float:
    ret_values = np.asarray(
        [
            float(row.get("label_ret_5", 0.0) or 0.0),
            float(row.get("label_ret_10", 0.0) or 0.0),
            float(row.get("label_ret_20", 0.0) or 0.0),
        ],
        dtype=np.float32,
    )
    drawdown_values = np.asarray(
        [
            float(row.get("label_drawdown_5", 0.0) or 0.0),
            float(row.get("label_drawdown_10", 0.0) or 0.0),
            float(row.get("label_drawdown_20", 0.0) or 0.0),
        ],
        dtype=np.float32,
    )
    downside = np.clip(-ret_values, 0.0, None).mean()
    drawdown_tail = np.clip(np.abs(drawdown_values) - 0.08, 0.0, None).mean()
    return _clip01(float(np.clip(downside * 8.0 + drawdown_tail * 6.0, 0.0, 1.0)))


def _build_channel_scores_summary(matches: list[dict]) -> dict:
    if not matches:
        return {
            "avg_state_score": 0.0,
            "avg_price_shape_score": 0.0,
            "avg_volume_shape_score": 0.0,
            "avg_label_consistency_score": 0.0,
            "avg_regime_match_score": 0.0,
            "avg_freshness_score": 0.0,
            "avg_outcome_quality_score": 0.0,
            "avg_anti_example_penalty": 0.0,
            "avg_ensemble_score": 0.0,
        }
    frame = pd.DataFrame(matches)
    return {
        "avg_state_score": round(float(frame["state_score"].mean()), 4),
        "avg_price_shape_score": round(float(frame["price_shape_score"].mean()), 4),
        "avg_volume_shape_score": round(float(frame["volume_shape_score"].mean()), 4),
        "avg_label_consistency_score": round(float(frame["label_consistency_score"].mean()), 4),
        "avg_regime_match_score": round(float(frame["regime_match_score"].mean()), 4),
        "avg_freshness_score": round(float(frame["freshness_score"].mean()), 4),
        "avg_outcome_quality_score": round(float(frame["outcome_quality_score"].mean()), 4),
        "avg_anti_example_penalty": round(float(frame["anti_example_penalty"].mean()), 4),
        "avg_ensemble_score": round(float(frame["ensemble_score"].mean()), 4),
    }


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
            "avg_max_up_5": 0.0,
            "avg_max_up_10": 0.0,
            "avg_max_up_20": 0.0,
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
        "avg_max_up_5": round(float(frame["max_up_5"].mean() * 100), 2),
        "avg_max_up_10": round(float(frame["max_up_10"].mean() * 100), 2),
        "avg_max_up_20": round(float(frame["max_up_20"].mean() * 100), 2),
    }


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, value))


def _to_native(value):
    if isinstance(value, np.generic):
        return value.item()
    return value


def _has_stumpy() -> bool:
    try:
        import stumpy  # type: ignore  # noqa: F401

        return True
    except Exception:
        return False


def _load_ensemble_metadata() -> dict:
    if not ENSEMBLE_META_PATH.exists():
        return {}
    return json.loads(ENSEMBLE_META_PATH.read_text(encoding="utf-8"))
