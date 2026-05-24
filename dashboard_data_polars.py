from __future__ import annotations

from datetime import time
from pathlib import Path

import polars as pl

from analysis.polars._shared import (
    DEFAULT_TIMEZONE,
    QUALIFIED_DELAY_FILTER_SQL,
    aggregate_delay_buckets,
    apply_quality_filter,
    assign_gtfs_feed_dates,
    base_quality_query,
    gtfs_metadata_fingerprint,
    latest_gtfs_dir as shared_latest_gtfs_dir,
    load_gtfs_stop_metadata,
    read_sql,
    summarize_delay_metrics,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "foli.db"
DEFAULT_GTFS_ROOT = PROJECT_ROOT / "data" / "gtfs"

DELAY_METRIC_COLUMNS = [
    "signed_mean_delay_min",
    "median_delay_min",
    "p75_delay_min",
    "p90_delay_min",
    "p95_delay_min",
]

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
    return shared_latest_gtfs_dir(resolve_project_path(root))


def gtfs_stop_metadata_fingerprint(root: Path | str = DEFAULT_GTFS_ROOT) -> str:
    return gtfs_metadata_fingerprint(resolve_project_path(root), filenames=("stops.txt",))


def load_observations(
    db_path: Path | str = DEFAULT_DB_PATH,
    *,
    limit: int | None = None,
) -> pl.DataFrame:
    query = base_quality_query(where=QUALIFIED_DELAY_FILTER_SQL)
    params: list[object] = []
    if limit is not None:
        query += "\nLIMIT ?"
        params.append(limit)
    return read_sql(resolve_project_path(db_path), query, params)


def load_stop_metadata(
    gtfs_dir: Path | str | None = None,
    *,
    gtfs_root: Path | str = DEFAULT_GTFS_ROOT,
) -> pl.DataFrame:
    stops = load_gtfs_stop_metadata(
        gtfs_dir=resolve_project_path(gtfs_dir) if gtfs_dir else None,
        gtfs_root=resolve_project_path(gtfs_root),
    )
    if stops.is_empty():
        source = gtfs_dir if gtfs_dir is not None else gtfs_root
        raise FileNotFoundError(f"No date-aware GTFS stops.txt found below {source}")
    if "gtfs_feed_date" in stops.columns:
        return stops.sort("gtfs_feed_date", "stop_id")
    return stops.sort("stop_id")


def prepare_observations(
    observations: pl.DataFrame,
    stops: pl.DataFrame,
    *,
    timezone: str = DEFAULT_TIMEZONE,
) -> pl.DataFrame:
    if observations.is_empty():
        return _empty_prepared_frame()

    result = apply_quality_filter(observations, quality_mode="conservative")
    result = aggregate_delay_buckets(result, bucket="trip-stop", timezone=timezone)
    if result.is_empty():
        return _empty_prepared_frame()

    result = result.with_columns(
        pl.col("recorded_at_utc").cast(pl.Datetime(time_zone="UTC"), strict=False),
        pl.col("delay_seconds").cast(pl.Float64, strict=False),
        pl.col("line_ref").cast(pl.Utf8),
        pl.col("direction_ref").cast(pl.Utf8).fill_null("Unknown"),
        pl.coalesce(pl.col("published_line_name").cast(pl.Utf8), pl.col("line_ref").cast(pl.Utf8))
        .alias("published_line_name"),
        pl.col("next_stop_point_ref").cast(pl.Utf8).alias("stop_id"),
        pl.col("local_minutes").alias("local_minute_of_day"),
    ).filter(
        pl.col("recorded_at_utc").is_not_null()
        & pl.col("delay_seconds").is_not_null()
        & pl.col("line_ref").is_not_null()
    )
    if result.is_empty():
        return _empty_prepared_frame()

    result, date_aware_stop_metadata = _join_stop_metadata(result, stops)
    result = result.with_columns(
        pl.col("next_stop_point_name").cast(pl.Utf8),
        pl.coalesce(pl.col("gtfs_stop_name"), pl.col("next_stop_point_name")).alias("stop_name"),
        pl.col("stop_lat").cast(pl.Float64, strict=False),
        pl.col("stop_lon").cast(pl.Float64, strict=False),
        pl.col("gtfs_stop_name").is_not_null().alias("has_gtfs_stop_metadata"),
    )
    if not date_aware_stop_metadata and "gtfs_feed_date" not in result.columns:
        result = result.with_columns(pl.lit(None, dtype=pl.Date).alias("gtfs_feed_date"))
    return result.select([column for column in _empty_prepared_frame().columns if column in result.columns])


def _join_stop_metadata(
    observations: pl.DataFrame,
    stops: pl.DataFrame,
) -> tuple[pl.DataFrame, bool]:
    stop_columns = ["stop_id", "gtfs_stop_name", "stop_lat", "stop_lon"]
    date_aware = "gtfs_feed_date" in stops.columns
    result = observations
    if result.is_empty():
        return result, date_aware

    if stops.is_empty():
        return (
            result.with_columns(
                pl.lit(None, dtype=pl.Date).alias("gtfs_feed_date"),
                pl.lit(None, dtype=pl.Utf8).alias("gtfs_stop_name"),
                pl.lit(None, dtype=pl.Float64).alias("stop_lat"),
                pl.lit(None, dtype=pl.Float64).alias("stop_lon"),
            ),
            date_aware,
        )

    if date_aware:
        metadata = (
            stops.select("gtfs_feed_date", *stop_columns)
            .with_columns(pl.col("stop_id").cast(pl.Utf8))
            .unique(["gtfs_feed_date", "stop_id"], keep="first")
        )
        feeds = metadata.select("gtfs_feed_date").unique()
        result = result.with_columns(assign_gtfs_feed_dates(result, feeds))
        return result.join(metadata, on=["gtfs_feed_date", "stop_id"], how="left"), True

    metadata = (
        stops.select(stop_columns)
        .with_columns(pl.col("stop_id").cast(pl.Utf8))
        .unique(["stop_id"], keep="first")
    )
    result = result.with_columns(pl.lit(None, dtype=pl.Date).alias("gtfs_feed_date"))
    return result.join(metadata, on="stop_id", how="left"), False


def filter_observations(
    df: pl.DataFrame,
    *,
    start_date: object | None = None,
    end_date: object | None = None,
    line_refs: list[str] | tuple[str, ...] | None = None,
    direction_refs: list[str] | tuple[str, ...] | None = None,
    day_filter: str = "All days",
    start_time: time | None = None,
    end_time: time | None = None,
) -> pl.DataFrame:
    result = df
    if start_date is not None:
        result = result.filter(pl.col("local_date") >= start_date)
    if end_date is not None:
        result = result.filter(pl.col("local_date") <= end_date)
    if line_refs:
        selected_lines = [str(line_ref) for line_ref in line_refs]
        result = result.filter(pl.col("line_ref").cast(pl.Utf8).is_in(selected_lines))
    if direction_refs:
        selected_directions = [str(direction_ref) for direction_ref in direction_refs]
        result = result.filter(pl.col("direction_ref").cast(pl.Utf8).is_in(selected_directions))
    if day_filter == "Weekdays":
        result = result.filter(pl.col("is_weekday"))
    elif day_filter == "Weekends":
        result = result.filter(~pl.col("is_weekday"))
    if start_time is not None or end_time is not None:
        start_minute = 0 if start_time is None else _minute_of_day(start_time)
        end_minute = (24 * 60) - 1 if end_time is None else _minute_of_day(end_time)
        if start_minute > end_minute:
            raise ValueError("start_time must be before or equal to end_time")
        result = result.filter(
            (pl.col("local_minute_of_day") >= start_minute)
            & (pl.col("local_minute_of_day") <= end_minute)
        )
    return result


def build_hourly_line_metrics(
    df: pl.DataFrame,
    *,
    min_observations: int = 1,
) -> pl.DataFrame:
    if df.is_empty():
        return _empty_metric_frame(["line_ref", "local_hour", "line_name"])

    return summarize_delay_metrics(
        df,
        ["line_ref", "local_hour"],
        min_observations=min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )


def build_stop_metrics(
    df: pl.DataFrame,
    *,
    min_observations: int = 1,
) -> pl.DataFrame:
    if df.is_empty():
        return _empty_metric_frame(["stop_id", "stop_name", "stop_lat", "stop_lon"])

    metrics = summarize_delay_metrics(
        df,
        ["stop_id", "stop_name", "stop_lat", "stop_lon"],
        min_observations=min_observations,
    )
    if metrics.is_empty():
        return _empty_metric_frame(["stop_id", "stop_name", "stop_lat", "stop_lon"])

    line_counts = df.group_by("stop_id", "stop_name", "stop_lat", "stop_lon").agg(
        pl.col("line_ref").n_unique().alias("line_count")
    )
    return metrics.join(
        line_counts,
        on=["stop_id", "stop_name", "stop_lat", "stop_lon"],
        how="left",
    ).select(
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "line_count",
        *[column for column in metrics.columns if column not in {"stop_id", "stop_name", "stop_lat", "stop_lon"}],
    )


def build_stop_heatmap_weights(
    stop_metrics: pl.DataFrame,
    metric_key: str,
    *,
    delay_direction: str = "late",
) -> pl.DataFrame:
    result = stop_metrics.drop_nulls(subset=["stop_lat", "stop_lon"])
    if result.is_empty():
        return result.with_columns(pl.lit(None, dtype=pl.Float64).alias("heat_weight"))

    count_column = "bucket_count" if "bucket_count" in result.columns else "obs_count"
    if metric_key in DIVERGING_METRICS:
        if delay_direction == "late":
            heat_expr = pl.col(metric_key).clip(lower_bound=0) * pl.col(count_column)
        elif delay_direction == "early":
            heat_expr = (-pl.col(metric_key)).clip(lower_bound=0) * pl.col(count_column)
        else:
            raise ValueError("delay_direction must be 'late' or 'early'")
    elif metric_key.startswith("pct_"):
        heat_expr = pl.col(metric_key) / 100.0 * pl.col(count_column)
    elif metric_key in ("bucket_count", "raw_poll_count", "obs_count"):
        heat_expr = pl.col(metric_key)
    else:
        raise ValueError(f"Unsupported heatmap metric: {metric_key}")

    return (
        result.with_columns(heat_expr.cast(pl.Float64, strict=False).alias("heat_weight"))
        .filter(pl.col("heat_weight") > 0)
    )


def summarize_observations(df: pl.DataFrame) -> dict[str, float | int]:
    if df.is_empty():
        return {
            "bucket_count": 0,
            "raw_poll_count": 0,
            "line_count": 0,
            "stop_count": 0,
            "median_delay_min": 0.0,
            "p90_delay_min": 0.0,
            "pct_over_5_min_late": 0.0,
        }

    summary = df.select(
        pl.len().alias("bucket_count"),
        pl.col("raw_poll_count").sum().alias("raw_poll_count"),
        pl.col("line_ref").n_unique().alias("line_count"),
        pl.col("stop_id").n_unique().alias("stop_count"),
        pl.col("delay_min").median().alias("median_delay_min"),
        pl.col("delay_min").quantile(0.90, interpolation="linear").alias("p90_delay_min"),
        (pl.col("delay_seconds") > 300).mean().mul(100.0).alias("pct_over_5_min_late"),
    ).row(0, named=True)
    return {
        "bucket_count": int(summary["bucket_count"] or 0),
        "raw_poll_count": int(summary["raw_poll_count"] or 0),
        "line_count": int(summary["line_count"] or 0),
        "stop_count": int(summary["stop_count"] or 0),
        "median_delay_min": float(summary["median_delay_min"] or 0.0),
        "p90_delay_min": float(summary["p90_delay_min"] or 0.0),
        "pct_over_5_min_late": float(summary["pct_over_5_min_late"] or 0.0),
    }


def rank_late_stops(stop_metrics: pl.DataFrame, *, limit: int = 20) -> pl.DataFrame:
    return stop_metrics.sort(
        ["p90_delay_min", "pct_over_5_min_late", "bucket_count"],
        descending=[True, True, True],
    ).head(limit)


def rank_early_stops(stop_metrics: pl.DataFrame, *, limit: int = 20) -> pl.DataFrame:
    return stop_metrics.sort(
        ["p90_early_min_abs", "pct_over_3_min_early", "bucket_count"],
        descending=[True, True, True],
    ).head(limit)


def metric_label(metric_key: str) -> str:
    return METRIC_LABELS.get(metric_key, metric_key)


def _empty_prepared_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "recorded_at_utc": pl.Datetime(time_zone="UTC"),
            "line_ref": pl.Utf8,
            "direction_ref": pl.Utf8,
            "published_line_name": pl.Utf8,
            "delay_seconds": pl.Float64,
            "next_stop_point_ref": pl.Utf8,
            "next_stop_point_name": pl.Utf8,
            "delay_min": pl.Float64,
            "raw_poll_count": pl.Int64,
            "representative_time_utc": pl.Datetime(time_zone="UTC"),
            "bucket_mode": pl.Utf8,
            "stop_id": pl.Utf8,
            "local_time": pl.Datetime(time_zone=DEFAULT_TIMEZONE),
            "local_date": pl.Date,
            "local_hour": pl.Int64,
            "local_minute_of_day": pl.Int64,
            "local_minutes": pl.Int64,
            "local_weekday": pl.Int64,
            "is_weekday": pl.Boolean,
            "day_type": pl.Utf8,
            "gtfs_feed_date": pl.Date,
            "has_gtfs_stop_metadata": pl.Boolean,
            "gtfs_stop_name": pl.Utf8,
            "stop_lat": pl.Float64,
            "stop_lon": pl.Float64,
            "stop_name": pl.Utf8,
        }
    )


def _minute_of_day(value: time) -> int:
    return value.hour * 60 + value.minute


def _empty_metric_frame(group_columns: list[str]) -> pl.DataFrame:
    schema: dict[str, pl.DataType] = {column: pl.Utf8 for column in group_columns}
    if "local_hour" in schema:
        schema["local_hour"] = pl.Int64
    if "stop_lat" in schema:
        schema["stop_lat"] = pl.Float64
    if "stop_lon" in schema:
        schema["stop_lon"] = pl.Float64
    schema.update(
        {
            "bucket_count": pl.Int64,
            "raw_poll_count": pl.Int64,
            "line_count": pl.Int64,
            "signed_mean_delay_min": pl.Float64,
            "median_delay_min": pl.Float64,
            "p75_delay_min": pl.Float64,
            "p90_delay_min": pl.Float64,
            "p95_delay_min": pl.Float64,
            "pct_over_3_min_late": pl.Float64,
            "pct_over_5_min_late": pl.Float64,
            "pct_early": pl.Float64,
            "pct_over_1_min_early": pl.Float64,
            "pct_over_3_min_early": pl.Float64,
            "median_early_min_abs": pl.Float64,
            "p90_early_min_abs": pl.Float64,
        }
    )
    return pl.DataFrame(schema=schema)
