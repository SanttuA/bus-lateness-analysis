from __future__ import annotations

import sqlite3
from datetime import time
from pathlib import Path

import pandas as pd

from analysis._shared import (
    DELAY_METRIC_COLUMNS,
    QUALIFIED_DELAY_FILTER_SQL,
    aggregate_delay_buckets,
    apply_quality_filter,
    base_quality_query,
    summarize_delay_metrics,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "foli.db"
DEFAULT_GTFS_ROOT = PROJECT_ROOT / "data" / "gtfs"
DEFAULT_TIMEZONE = "Europe/Helsinki"

METRIC_LABELS = {
    "p90_delay_min": "P90 delay (min)",
    "median_delay_min": "Median delay (min)",
    "p75_delay_min": "P75 delay (min)",
    "p95_delay_min": "P95 delay (min)",
    "signed_mean_delay_min": "Signed mean delay (min)",
    "pct_over_3_min_late": "Over 3 min late (%)",
    "pct_over_5_min_late": "Over 5 min late (%)",
    "pct_early": "Early buckets (%)",
    "pct_over_1_min_early": "Over 1 min early (%)",
    "pct_over_3_min_early": "Over 3 min early (%)",
    "bucket_count": "Bucket count",
    "raw_poll_count": "Raw poll count",
}

DIVERGING_METRICS = set(DELAY_METRIC_COLUMNS)


def resolve_project_path(path: Path | str) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    return resolved.resolve()


def latest_gtfs_dir(root: Path | str = DEFAULT_GTFS_ROOT) -> Path | None:
    gtfs_root = resolve_project_path(root)
    if not gtfs_root.exists():
        return None

    candidates = [
        path
        for path in gtfs_root.iterdir()
        if path.is_dir() and (path / "stops.txt").exists()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.name)


def connect_readonly_db(path: Path | str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    db_path = resolve_project_path(path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    uri = f"file:{db_path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def load_observations(
    db_path: Path | str = DEFAULT_DB_PATH,
    *,
    limit: int | None = None,
) -> pd.DataFrame:
    query = base_quality_query(where=QUALIFIED_DELAY_FILTER_SQL)
    params: list[object] = []
    if limit is not None:
        query += "\nLIMIT ?"
        params.append(limit)

    with connect_readonly_db(db_path) as con:
        return pd.read_sql_query(query, con, params=params)


def load_stop_metadata(gtfs_dir: Path | str | None = None) -> pd.DataFrame:
    resolved_gtfs_dir = resolve_project_path(gtfs_dir) if gtfs_dir else latest_gtfs_dir()
    if resolved_gtfs_dir is None:
        raise FileNotFoundError(f"No GTFS stops.txt found below {DEFAULT_GTFS_ROOT}")

    stops_path = resolved_gtfs_dir / "stops.txt"
    if not stops_path.exists():
        raise FileNotFoundError(f"GTFS stops.txt not found: {stops_path}")

    stops = pd.read_csv(stops_path, dtype={"stop_id": "string"})
    required = {"stop_id", "stop_name", "stop_lat", "stop_lon"}
    missing = required.difference(stops.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"{stops_path} is missing required column(s): {missing_text}")

    stops = stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]].copy()
    stops["stop_id"] = stops["stop_id"].astype("string")
    stops["stop_lat"] = pd.to_numeric(stops["stop_lat"], errors="coerce")
    stops["stop_lon"] = pd.to_numeric(stops["stop_lon"], errors="coerce")
    return stops.rename(columns={"stop_name": "gtfs_stop_name"})


def prepare_observations(
    observations: pd.DataFrame,
    stops: pd.DataFrame,
    *,
    timezone: str = DEFAULT_TIMEZONE,
) -> pd.DataFrame:
    if observations.empty:
        return _empty_prepared_frame()

    result = apply_quality_filter(observations, quality_mode="conservative")
    result = aggregate_delay_buckets(result, bucket="trip-stop", timezone=timezone)
    if result.empty:
        return _empty_prepared_frame()

    result["recorded_at_utc"] = pd.to_datetime(
        result["recorded_at_utc"],
        utc=True,
        errors="coerce",
    )
    result["delay_seconds"] = pd.to_numeric(result["delay_seconds"], errors="coerce")
    result = result.dropna(subset=["recorded_at_utc", "delay_seconds", "line_ref"])

    result["delay_min"] = result["delay_seconds"] / 60.0
    result["line_ref"] = result["line_ref"].astype("string")
    result["direction_ref"] = result["direction_ref"].astype("string").fillna("Unknown")
    result["published_line_name"] = (
        result["published_line_name"].astype("string").fillna(result["line_ref"])
    )
    result["stop_id"] = result["next_stop_point_ref"].astype("string")
    result["local_minute_of_day"] = result["local_minutes"]

    stop_columns = ["stop_id", "gtfs_stop_name", "stop_lat", "stop_lon"]
    if stops.empty:
        stop_metadata = pd.DataFrame(columns=stop_columns)
    else:
        stop_metadata = stops[stop_columns].copy()
        stop_metadata["stop_id"] = stop_metadata["stop_id"].astype("string")

    result = result.merge(stop_metadata, how="left", on="stop_id")
    result["next_stop_point_name"] = result["next_stop_point_name"].astype("string")
    result["stop_name"] = result["gtfs_stop_name"].combine_first(
        result["next_stop_point_name"]
    )
    result["stop_lat"] = pd.to_numeric(result["stop_lat"], errors="coerce")
    result["stop_lon"] = pd.to_numeric(result["stop_lon"], errors="coerce")
    return result


def filter_observations(
    df: pd.DataFrame,
    *,
    start_date: object | None = None,
    end_date: object | None = None,
    line_refs: list[str] | tuple[str, ...] | None = None,
    direction_refs: list[str] | tuple[str, ...] | None = None,
    day_filter: str = "All days",
    start_time: time | None = None,
    end_time: time | None = None,
) -> pd.DataFrame:
    result = df
    if start_date is not None:
        result = result[result["local_date"] >= start_date]
    if end_date is not None:
        result = result[result["local_date"] <= end_date]
    if line_refs:
        selected_lines = {str(line_ref) for line_ref in line_refs}
        result = result[result["line_ref"].astype(str).isin(selected_lines)]
    if direction_refs:
        selected_directions = {str(direction_ref) for direction_ref in direction_refs}
        result = result[result["direction_ref"].astype(str).isin(selected_directions)]
    if day_filter == "Weekdays":
        result = result[result["is_weekday"]]
    elif day_filter == "Weekends":
        result = result[~result["is_weekday"]]
    if start_time is not None or end_time is not None:
        start_minute = 0 if start_time is None else _minute_of_day(start_time)
        end_minute = (24 * 60) - 1 if end_time is None else _minute_of_day(end_time)
        if start_minute > end_minute:
            raise ValueError("start_time must be before or equal to end_time")
        result = result[
            (result["local_minute_of_day"] >= start_minute)
            & (result["local_minute_of_day"] <= end_minute)
        ]
    return result


def build_hourly_line_metrics(
    df: pd.DataFrame,
    *,
    min_observations: int = 1,
) -> pd.DataFrame:
    if df.empty:
        return _empty_metric_frame(["line_ref", "local_hour", "line_name"])

    grouped = summarize_delay_metrics(
        df,
        ["line_ref", "local_hour"],
        min_observations=min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )
    return grouped.reset_index(drop=True)


def build_stop_metrics(
    df: pd.DataFrame,
    *,
    min_observations: int = 1,
) -> pd.DataFrame:
    if df.empty:
        return _empty_metric_frame(["stop_id", "stop_name", "stop_lat", "stop_lon"])

    grouped = summarize_delay_metrics(
        df,
        ["stop_id", "stop_name", "stop_lat", "stop_lon"],
        min_observations=min_observations,
        extra_aggs={"line_count": ("line_ref", "nunique")},
    )
    return grouped.reset_index(drop=True)


def build_stop_heatmap_weights(
    stop_metrics: pd.DataFrame,
    metric_key: str,
    *,
    delay_direction: str = "late",
) -> pd.DataFrame:
    result = stop_metrics.dropna(subset=["stop_lat", "stop_lon"]).copy()
    if result.empty:
        result["heat_weight"] = pd.Series(dtype="float64")
        return result

    count_column = "bucket_count" if "bucket_count" in result.columns else "obs_count"
    if metric_key in DIVERGING_METRICS:
        if delay_direction == "late":
            result["heat_weight"] = result[metric_key].clip(lower=0) * result[count_column]
        elif delay_direction == "early":
            result["heat_weight"] = (-result[metric_key]).clip(lower=0) * result[count_column]
        else:
            raise ValueError("delay_direction must be 'late' or 'early'")
    elif metric_key.startswith("pct_"):
        result["heat_weight"] = result[metric_key] / 100.0 * result[count_column]
    elif metric_key in ("bucket_count", "raw_poll_count", "obs_count"):
        result["heat_weight"] = result[metric_key]
    else:
        raise ValueError(f"Unsupported heatmap metric: {metric_key}")

    result["heat_weight"] = pd.to_numeric(result["heat_weight"], errors="coerce")
    return result[result["heat_weight"] > 0].reset_index(drop=True)


def summarize_observations(df: pd.DataFrame) -> dict[str, float | int]:
    if df.empty:
        return {
            "bucket_count": 0,
            "raw_poll_count": 0,
            "line_count": 0,
            "stop_count": 0,
            "median_delay_min": 0.0,
            "p90_delay_min": 0.0,
            "pct_over_5_min_late": 0.0,
        }

    return {
        "bucket_count": int(len(df)),
        "raw_poll_count": int(df["raw_poll_count"].sum()),
        "line_count": int(df["line_ref"].nunique(dropna=True)),
        "stop_count": int(df["stop_id"].nunique(dropna=True)),
        "median_delay_min": float(df["delay_min"].median()),
        "p90_delay_min": float(df["delay_min"].quantile(0.90)),
        "pct_over_5_min_late": float((df["delay_seconds"] > 300).mean() * 100.0),
    }


def rank_late_stops(stop_metrics: pd.DataFrame, *, limit: int = 20) -> pd.DataFrame:
    return stop_metrics.sort_values(
        ["p90_delay_min", "pct_over_5_min_late", "bucket_count"],
        ascending=[False, False, False],
    ).head(limit)


def rank_early_stops(stop_metrics: pd.DataFrame, *, limit: int = 20) -> pd.DataFrame:
    return stop_metrics.sort_values(
        ["p90_early_min_abs", "pct_over_3_min_early", "bucket_count"],
        ascending=[False, False, False],
    ).head(limit)


def metric_label(metric_key: str) -> str:
    return METRIC_LABELS.get(metric_key, metric_key)


def _empty_prepared_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "recorded_at_utc",
            "line_ref",
            "direction_ref",
            "published_line_name",
            "delay_seconds",
            "next_stop_point_ref",
            "next_stop_point_name",
            "delay_min",
            "raw_poll_count",
            "representative_time_utc",
            "bucket_mode",
            "stop_id",
            "local_time",
            "local_date",
            "local_hour",
            "local_minute_of_day",
            "local_minutes",
            "local_weekday",
            "is_weekday",
            "day_type",
            "gtfs_stop_name",
            "stop_lat",
            "stop_lon",
            "stop_name",
        ]
    )


def _minute_of_day(value: time) -> int:
    return value.hour * 60 + value.minute


def _empty_metric_frame(group_columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            *group_columns,
            "bucket_count",
            "raw_poll_count",
            "line_count",
            "signed_mean_delay_min",
            "median_delay_min",
            "p75_delay_min",
            "p90_delay_min",
            "p95_delay_min",
            "pct_over_3_min_late",
            "pct_over_5_min_late",
            "pct_early",
            "pct_over_1_min_early",
            "pct_over_3_min_early",
            "median_early_min_abs",
            "p90_early_min_abs",
        ]
    )
