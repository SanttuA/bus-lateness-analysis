from __future__ import annotations

import json
import sqlite3
import tempfile
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

import polars as pl
import polars.selectors as cs

try:
    from ._shared import (
        BUCKET_MODES,
        DEFAULT_ANALYSIS_CACHE_DIR,
        DEFAULT_BUCKET_MODE,
        DEFAULT_DB_PATH,
        DEFAULT_GTFS_ROOT,
        DEFAULT_QUALITY_MODE,
        DEFAULT_RUSH_WINDOWS,
        DEFAULT_TIMEZONE,
        QUALITY_FLAG_COLUMNS,
        QUALITY_MODES,
        CONSERVATIVE_EXCLUSION_COLUMNS,
        add_quality_pass,
        aggregate_delay_buckets_lazy,
        assign_gtfs_feed_dates,
        base_quality_query,
        base_quality_query_without_collector,
        gtfs_dir_fingerprint,
        gtfs_feed_date_for_timestamp,
        gtfs_metadata_fingerprint,
        load_gtfs_route_metadata,
        load_gtfs_stop_metadata,
        metric_aggs,
        parse_rush_windows,
        parse_timestamp,
        read_sql,
        representative_time_sql,
        resolve_project_path,
        round_numeric,
        rush_period_expr,
        rush_window_values,
        sort_robust_delay_metrics,
        summarize_delay_metrics,
    )
except ImportError:  # pragma: no cover - used when called as analysis/polars/*.py.
    from _shared import (
        BUCKET_MODES,
        DEFAULT_ANALYSIS_CACHE_DIR,
        DEFAULT_BUCKET_MODE,
        DEFAULT_DB_PATH,
        DEFAULT_GTFS_ROOT,
        DEFAULT_QUALITY_MODE,
        DEFAULT_RUSH_WINDOWS,
        DEFAULT_TIMEZONE,
        QUALITY_FLAG_COLUMNS,
        QUALITY_MODES,
        CONSERVATIVE_EXCLUSION_COLUMNS,
        add_quality_pass,
        aggregate_delay_buckets_lazy,
        assign_gtfs_feed_dates,
        base_quality_query,
        base_quality_query_without_collector,
        gtfs_dir_fingerprint,
        gtfs_feed_date_for_timestamp,
        gtfs_metadata_fingerprint,
        load_gtfs_route_metadata,
        load_gtfs_stop_metadata,
        metric_aggs,
        parse_rush_windows,
        parse_timestamp,
        read_sql,
        representative_time_sql,
        resolve_project_path,
        round_numeric,
        rush_period_expr,
        rush_window_values,
        sort_robust_delay_metrics,
        summarize_delay_metrics,
    )


CACHE_VERSION = 1
DEFAULT_CACHE_DIR = DEFAULT_ANALYSIS_CACHE_DIR
DEFAULT_REPORT_PATH = Path("reports/generated/overall-results-polars.md")
MANIFEST_NAME = "manifest.json"
QUALITY_ROWS_NAME = "quality_rows"
DELAY_BUCKETS_NAME = "delay_buckets"
DELAY_CACHE_SUMMARY_NAME = "delay_cache_summary"
QUALITY_SOURCE_ID_BATCH_SIZE = 250_000
DELAY_BUCKET_PARTITIONS = 16

RESULT_TABLES = [
    "quality_summary",
    "quality_by_line",
    "line_late_rankings",
    "line_early_rankings",
    "context_delay_metrics",
    "hourly_delay_profile",
    "rush_impact",
    "stop_midpoint_change",
    "service_alert_grouped",
    "service_alert_by_line",
    "collector_blackouts",
    "collector_missing_summary",
    "collector_missing_spots",
]


@dataclass(frozen=True)
class ReportSettings:
    db: Path = DEFAULT_DB_PATH
    cache_dir: Path = DEFAULT_CACHE_DIR
    quality_mode: str = DEFAULT_QUALITY_MODE
    bucket: str = DEFAULT_BUCKET_MODE
    timezone: str = DEFAULT_TIMEZONE
    limit: int = 20
    min_observations: int = 30
    exclude_stop_call_disagreement: bool = False
    rush_windows: tuple[str, ...] = DEFAULT_RUSH_WINDOWS
    include_weekends: bool = False
    gtfs_dir: Path | None = None
    gtfs_root: Path = DEFAULT_GTFS_ROOT

    def resolved(self) -> ReportSettings:
        return ReportSettings(
            db=resolve_project_path(self.db),
            cache_dir=resolve_project_path(self.cache_dir),
            quality_mode=self.quality_mode,
            bucket=self.bucket,
            timezone=self.timezone,
            limit=self.limit,
            min_observations=self.min_observations,
            exclude_stop_call_disagreement=self.exclude_stop_call_disagreement,
            rush_windows=tuple(self.rush_windows),
            include_weekends=self.include_weekends,
            gtfs_dir=resolve_project_path(self.gtfs_dir) if self.gtfs_dir else None,
            gtfs_root=resolve_project_path(self.gtfs_root),
        )

    def validate(self) -> None:
        if self.quality_mode not in QUALITY_MODES:
            raise ValueError(f"quality_mode must be one of: {', '.join(QUALITY_MODES)}")
        if self.bucket not in BUCKET_MODES:
            raise ValueError(f"bucket must be one of: {', '.join(BUCKET_MODES)}")
        if self.limit < 1:
            raise ValueError("limit must be at least 1")
        if self.min_observations < 1:
            raise ValueError("min_observations must be at least 1")
        parse_rush_windows(tuple(self.rush_windows))


@dataclass(frozen=True)
class CacheResult:
    status: str
    cache_db: Path
    manifest: dict[str, Any]
    timings: dict[str, float] = field(default_factory=dict)


def settings_from_args(args: object) -> ReportSettings:
    return ReportSettings(
        db=getattr(args, "db", DEFAULT_DB_PATH),
        cache_dir=getattr(args, "cache_dir", DEFAULT_CACHE_DIR),
        quality_mode=getattr(args, "quality_mode", DEFAULT_QUALITY_MODE),
        bucket=getattr(args, "bucket", DEFAULT_BUCKET_MODE),
        timezone=getattr(args, "timezone", DEFAULT_TIMEZONE),
        limit=getattr(args, "limit", 20),
        min_observations=getattr(args, "min_observations", 30),
        exclude_stop_call_disagreement=getattr(args, "exclude_stop_call_disagreement", False),
        rush_windows=tuple(rush_window_values(getattr(args, "rush_window", None))),
        include_weekends=getattr(args, "include_weekends", False),
        gtfs_dir=getattr(args, "gtfs_dir", None),
        gtfs_root=getattr(args, "gtfs_root", DEFAULT_GTFS_ROOT),
    )


def ensure_analysis_cache(
    settings: ReportSettings,
    *,
    force: bool = False,
    progress: Callable[[str], None] | None = None,
) -> CacheResult:
    started = time.perf_counter()
    settings = settings.resolved()
    settings.validate()
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    _progress(progress, "Checking base cache")
    manifest_path = settings.cache_dir / MANIFEST_NAME
    current = _read_manifest(manifest_path)
    file_metadata = collect_db_file_metadata(settings.db)
    if (
        not force
        and _manifest_file_matches(current, file_metadata)
        and _manifest_settings_match(current, settings, base_only=True)
        and _has_tables(settings.cache_dir, [QUALITY_ROWS_NAME, DELAY_BUCKETS_NAME])
    ):
        _progress(progress, "Reusing base cache")
        return CacheResult(
            "reused",
            settings.cache_dir,
            current,
            timings={"base_cache_seconds": time.perf_counter() - started},
        )

    _progress(progress, "Building base cache")
    db_metadata = collect_db_metadata(settings.db)
    _build_base_tables(settings, db_metadata, progress=progress)

    manifest = {
        **_expected_manifest(settings, db_metadata, base_only=True),
        "built_at_utc": _utc_now_iso(),
        "cache_dir": str(settings.cache_dir),
        "base_tables": [QUALITY_ROWS_NAME, DELAY_BUCKETS_NAME, DELAY_CACHE_SUMMARY_NAME],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return CacheResult(
        "rebuilt",
        settings.cache_dir,
        manifest,
        timings={"base_cache_seconds": time.perf_counter() - started},
    )


def ensure_report_cache(
    settings: ReportSettings,
    *,
    force: bool = False,
    progress: Callable[[str], None] | None = None,
) -> CacheResult:
    started = time.perf_counter()
    settings = settings.resolved()
    settings.validate()
    settings.cache_dir.mkdir(parents=True, exist_ok=True)
    _progress(progress, "Checking report cache")
    manifest_path = settings.cache_dir / MANIFEST_NAME
    current = _read_manifest(manifest_path)
    file_metadata = collect_db_file_metadata(settings.db)
    if (
        not force
        and _manifest_file_matches(current, file_metadata)
        and _manifest_settings_match(current, settings, base_only=False)
        and _has_tables(settings.cache_dir, [QUALITY_ROWS_NAME, DELAY_BUCKETS_NAME, *RESULT_TABLES])
    ):
        _progress(progress, "Reusing report cache")
        return CacheResult(
            "reused",
            settings.cache_dir,
            current,
            timings={"cache_build_seconds": time.perf_counter() - started},
        )

    _progress(progress, "Rebuilding report cache")
    base = ensure_analysis_cache(settings, force=force, progress=progress)
    db_metadata = base.manifest.get("db_metadata") or collect_db_metadata(settings.db)
    _progress(progress, "Building result tables")
    _build_result_tables(settings)

    manifest = {
        **_expected_manifest(settings, db_metadata, base_only=False),
        "built_at_utc": _utc_now_iso(),
        "cache_dir": str(settings.cache_dir),
        "base_tables": [QUALITY_ROWS_NAME, DELAY_BUCKETS_NAME, DELAY_CACHE_SUMMARY_NAME],
        "result_tables": RESULT_TABLES,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return CacheResult(
        "rebuilt",
        settings.cache_dir,
        manifest,
        timings={"cache_build_seconds": time.perf_counter() - started},
    )


def read_table(cache_dir: Path, table_name: str) -> pl.DataFrame:
    path = resolve_project_path(cache_dir) / f"{table_name}.parquet"
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)


def read_result_table(cache_dir: Path, table_name: str) -> pl.DataFrame:
    return read_table(cache_dir, table_name)


def write_markdown_report(
    settings: ReportSettings,
    cache_result: CacheResult,
    output_path: Path = DEFAULT_REPORT_PATH,
) -> Path:
    settings = settings.resolved()
    output_path = resolve_project_path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(_render_report_lines(settings, cache_result)).rstrip() + "\n")
    return output_path


def _progress(callback: Callable[[str], None] | None, message: str) -> None:
    if callback is not None:
        callback(message)


def collect_db_file_metadata(db_path: Path) -> dict[str, Any]:
    db_path = resolve_project_path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    stat = db_path.stat()
    return {
        "db_path": str(db_path),
        "db_size_bytes": stat.st_size,
        "db_mtime_ns": stat.st_mtime_ns,
    }


def collect_db_metadata(db_path: Path) -> dict[str, Any]:
    file_metadata = collect_db_file_metadata(db_path)
    db_path = Path(file_metadata["db_path"])
    with sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True) as con:
        vehicle = con.execute(
            """
            SELECT COUNT(*), MAX(id), MIN(recorded_at_utc), MAX(recorded_at_utc)
            FROM vehicle_observations
            """
        ).fetchone()
        analysis = con.execute(
            """
            SELECT COUNT(*), MAX(id), MIN(recorded_at_utc), MAX(recorded_at_utc)
            FROM vehicle_observations v
            WHERE v.is_gtfs_matchable = 1
              AND v.delay_seconds IS NOT NULL
              AND v.line_ref IS NOT NULL
            """
        ).fetchone()
        collector_count = _table_count(con, "collector_polls")
        alert_count = _table_count(con, "service_alerts")
    return {
        **file_metadata,
        "vehicle_observation_count": int(vehicle[0] or 0),
        "vehicle_observation_max_id": int(vehicle[1] or 0),
        "vehicle_observation_start_utc": vehicle[2],
        "vehicle_observation_end_utc": vehicle[3],
        "analysis_row_count": int(analysis[0] or 0),
        "analysis_row_max_id": int(analysis[1] or 0),
        "analysis_start_utc": analysis[2],
        "analysis_end_utc": analysis[3],
        "collector_poll_count": collector_count,
        "service_alert_count": alert_count,
    }


def _build_base_tables(
    settings: ReportSettings,
    db_metadata: dict[str, Any],
    *,
    progress: Callable[[str], None] | None = None,
) -> None:
    with tempfile.TemporaryDirectory(
        dir=settings.cache_dir,
        prefix=".polars-report-build-",
    ) as temp_name:
        temp_dir = Path(temp_name)
        quality_path = temp_dir / f"{QUALITY_ROWS_NAME}.parquet"
        buckets_path = temp_dir / f"{DELAY_BUCKETS_NAME}.parquet"
        summary_path = temp_dir / f"{DELAY_CACHE_SUMMARY_NAME}.parquet"

        _progress(progress, "Building quality row fragments")
        _build_quality_rows_table(settings, db_metadata, quality_path)
        _progress(progress, "Building delay bucket fragments")
        _build_delay_buckets_table(settings, quality_path, buckets_path)
        _write_lazy_parquet(
            build_delay_cache_summary_lazy(pl.scan_parquet(buckets_path)),
            summary_path,
        )

        _replace_file(quality_path, settings.cache_dir / f"{QUALITY_ROWS_NAME}.parquet")
        _replace_file(buckets_path, settings.cache_dir / f"{DELAY_BUCKETS_NAME}.parquet")
        _replace_file(summary_path, settings.cache_dir / f"{DELAY_CACHE_SUMMARY_NAME}.parquet")


def _build_quality_rows_table(
    settings: ReportSettings,
    db_metadata: dict[str, Any],
    output_path: Path,
    *,
    batch_size: int = QUALITY_SOURCE_ID_BATCH_SIZE,
) -> None:
    fragments_dir = output_path.parent / f"{QUALITY_ROWS_NAME}-fragments"
    fragments_dir.mkdir(parents=True, exist_ok=True)
    fragments: list[Path] = []
    max_id = int(db_metadata.get("vehicle_observation_max_id") or 0)
    query_builder = (
        base_quality_query
        if _db_table_exists(settings.db, "collector_polls")
        else base_quality_query_without_collector
    )

    part = 0
    for start_id in range(1, max_id + 1, batch_size):
        end_id = start_id + batch_size
        where = f"""
        v.id >= ?
        AND v.id < ?
        AND v.is_gtfs_matchable = 1
        AND v.delay_seconds IS NOT NULL
        AND v.line_ref IS NOT NULL
        """
        rows = read_sql(settings.db, query_builder(where=where), [start_id, end_id])
        if rows.is_empty():
            continue
        quality_rows = add_quality_pass(
            rows,
            quality_mode=settings.quality_mode,
            exclude_stop_call_disagreement=settings.exclude_stop_call_disagreement,
        )
        fragment_path = fragments_dir / f"part-{part:05d}.parquet"
        quality_rows.write_parquet(fragment_path)
        fragments.append(fragment_path)
        part += 1

    if fragments:
        _write_lazy_parquet(pl.scan_parquet(fragments), output_path)
        return

    _build_empty_quality_rows(settings).write_parquet(output_path)


def _build_empty_quality_rows(settings: ReportSettings) -> pl.DataFrame:
    query_builder = (
        base_quality_query
        if _db_table_exists(settings.db, "collector_polls")
        else base_quality_query_without_collector
    )
    rows = read_sql(settings.db, query_builder(where="1 = 0"))
    return add_quality_pass(
        rows,
        quality_mode=settings.quality_mode,
        exclude_stop_call_disagreement=settings.exclude_stop_call_disagreement,
    )


def _build_delay_buckets_table(
    settings: ReportSettings,
    quality_rows_path: Path,
    output_path: Path,
    *,
    partition_count: int = DELAY_BUCKET_PARTITIONS,
) -> None:
    fragments_dir = output_path.parent / f"{DELAY_BUCKETS_NAME}-fragments"
    fragments_dir.mkdir(parents=True, exist_ok=True)
    quality_rows = pl.scan_parquet(quality_rows_path).filter(pl.col("quality_pass"))

    if settings.bucket == "poll":
        _write_lazy_parquet(
            aggregate_delay_buckets_lazy(
                quality_rows,
                bucket=settings.bucket,
                timezone=settings.timezone,
            ),
            output_path,
        )
        return

    fragments: list[Path] = []
    for partition_index in range(partition_count):
        fragment_path = fragments_dir / f"part-{partition_index:03d}.parquet"
        _write_lazy_parquet(
            aggregate_delay_buckets_lazy(
                quality_rows,
                bucket=settings.bucket,
                timezone=settings.timezone,
                partition_count=partition_count,
                partition_index=partition_index,
            ),
            fragment_path,
        )
        fragments.append(fragment_path)

    _write_lazy_parquet(pl.scan_parquet(fragments), output_path)


def build_delay_cache_summary(delay_buckets: pl.DataFrame) -> pl.DataFrame:
    if delay_buckets.is_empty():
        return pl.DataFrame(
            {
                "bucket_count": [0],
                "raw_poll_count": [0],
                "line_count": [0],
                "observation_start_utc": [None],
                "observation_end_utc": [None],
            }
        )
    return delay_buckets.select(
        pl.len().alias("bucket_count"),
        pl.col("raw_poll_count").sum().alias("raw_poll_count"),
        pl.col("line_ref").n_unique().alias("line_count"),
        pl.col("representative_time_utc").min().alias("observation_start_utc"),
        pl.col("representative_time_utc").max().alias("observation_end_utc"),
    )


def build_delay_cache_summary_lazy(delay_buckets: pl.LazyFrame) -> pl.LazyFrame:
    return delay_buckets.select(
        pl.len().alias("bucket_count"),
        pl.col("raw_poll_count").sum().alias("raw_poll_count"),
        pl.col("line_ref").n_unique().alias("line_count"),
        pl.col("representative_time_utc").min().alias("observation_start_utc"),
        pl.col("representative_time_utc").max().alias("observation_end_utc"),
    )


def build_quality_summary(quality_rows: pl.DataFrame) -> pl.DataFrame:
    if quality_rows.is_empty():
        return pl.DataFrame({"quality_check": [], "row_count": [], "pct_rows": []})
    total = quality_rows.height
    rows = [{"quality_check": "analysis_rows", "row_count": total, "pct_rows": 100.0}]
    for column in QUALITY_FLAG_COLUMNS:
        count = int(quality_rows.select(pl.col(column).sum()).item() or 0)
        rows.append(
            {
                "quality_check": column,
                "row_count": count,
                "pct_rows": count / total * 100.0,
            }
        )
    conservative = quality_rows.select(
        pl.any_horizontal([pl.col(column) for column in CONSERVATIVE_EXCLUSION_COLUMNS]).sum()
    ).item()
    rows.append(
        {
            "quality_check": "conservative_excluded_default",
            "row_count": int(conservative or 0),
            "pct_rows": (conservative or 0) / total * 100.0,
        }
    )
    with_stop_call = quality_rows.select(
        (
            pl.any_horizontal([pl.col(column) for column in CONSERVATIVE_EXCLUSION_COLUMNS])
            | pl.col("has_stop_call_disagreement")
        ).sum()
    ).item()
    rows.append(
        {
            "quality_check": "conservative_excluded_with_stop_call_disagreement",
            "row_count": int(with_stop_call or 0),
            "pct_rows": (with_stop_call or 0) / total * 100.0,
        }
    )
    return round_numeric(pl.DataFrame(rows))


def build_quality_by_line(
    quality_rows: pl.DataFrame,
    *,
    min_observations: int,
    limit: int,
) -> pl.DataFrame:
    if quality_rows.is_empty():
        return pl.DataFrame()
    grouped = quality_rows.with_columns(
        pl.any_horizontal([pl.col(column) for column in CONSERVATIVE_EXCLUSION_COLUMNS]).alias(
            "conservative_excluded_default"
        )
    ).group_by("line_ref").agg(
        pl.len().alias("row_count"),
        pl.col("published_line_name").first().alias("line_name"),
        pl.col("is_implausible_delay").sum().alias("implausible_delay_rows"),
        pl.col("is_stale_observation").sum().alias("stale_rows"),
        pl.col("is_pre_trip_observation").sum().alias("pre_trip_rows"),
        pl.col("is_post_trip_observation").sum().alias("post_trip_rows"),
        pl.col("has_stop_call_disagreement").sum().alias("stop_call_disagreement_rows"),
        pl.col("conservative_excluded_default").sum().alias("conservative_excluded_rows"),
    )
    grouped = grouped.filter(pl.col("row_count") >= min_observations)
    if grouped.is_empty():
        return grouped
    return round_numeric(
        grouped.with_columns(
            (
                pl.col("conservative_excluded_rows") / pl.col("row_count") * 100.0
            ).alias("conservative_excluded_pct")
        )
        .sort(
            ["conservative_excluded_pct", "conservative_excluded_rows", "line_ref"],
            descending=[True, True, False],
        )
        .head(limit)
    )


def build_line_rankings(
    delay_buckets: pl.DataFrame,
    ranking: str,
    *,
    min_observations: int,
    limit: int,
) -> pl.DataFrame:
    metrics = summarize_delay_metrics(
        delay_buckets,
        ["line_ref"],
        min_observations=min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )
    if metrics.is_empty():
        return metrics
    if ranking == "early":
        return round_numeric(
            metrics.sort(
                ["p90_early_min_abs", "pct_over_3_min_early", "bucket_count", "line_ref"],
                descending=[True, True, True, False],
            )
            .head(limit)
            .select(
                [
                    "line_ref",
                    "line_name",
                    "bucket_count",
                    "raw_poll_count",
                    "signed_mean_delay_min",
                    "median_delay_min",
                    "pct_early",
                    "pct_over_1_min_early",
                    "pct_over_3_min_early",
                    "median_early_min_abs",
                    "p90_early_min_abs",
                ]
            )
        )
    return round_numeric(
        metrics.sort(
            ["p90_delay_min", "pct_over_5_min_late", "bucket_count", "line_ref"],
            descending=[True, True, True, False],
        )
        .head(limit)
        .select(
            [
                "line_ref",
                "line_name",
                "bucket_count",
                "raw_poll_count",
                "signed_mean_delay_min",
                "median_delay_min",
                "p75_delay_min",
                "p90_delay_min",
                "p95_delay_min",
                "pct_over_3_min_late",
                "pct_over_5_min_late",
            ]
        )
    )


def build_context_delay_metrics(
    delay_buckets: pl.DataFrame,
    *,
    line_ref: str | None = None,
    direction_ref: str | None = None,
    day_type: str = "all",
    min_observations: int,
    limit: int,
) -> pl.DataFrame:
    buckets = delay_buckets
    if line_ref:
        buckets = buckets.filter(pl.col("line_ref") == line_ref)
    if direction_ref:
        buckets = buckets.filter(pl.col("direction_ref") == direction_ref)
    if day_type != "all":
        buckets = buckets.filter(pl.col("day_type") == day_type)
    metrics = summarize_delay_metrics(
        buckets,
        ["line_ref", "direction_ref", "local_hour", "day_type"],
        min_observations=min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )
    if metrics.is_empty():
        return metrics
    return round_numeric(
        sort_robust_delay_metrics(metrics, limit=limit).with_columns(
            (pl.col("local_hour").cast(pl.Utf8).str.zfill(2) + pl.lit(":00")).alias(
                "hour_local"
            )
        ).select(
            [
                "line_ref",
                "line_name",
                "direction_ref",
                "hour_local",
                "day_type",
                "bucket_count",
                "raw_poll_count",
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
    )


def build_hourly_delay_profile(
    delay_buckets: pl.DataFrame,
    *,
    line_ref: str | None = None,
    min_observations: int,
    limit: int,
) -> pl.DataFrame:
    buckets = delay_buckets.filter(pl.col("line_ref") == line_ref) if line_ref else delay_buckets
    metrics = summarize_delay_metrics(
        buckets,
        ["local_hour"],
        min_observations=min_observations,
    )
    if metrics.is_empty():
        return metrics
    return round_numeric(
        sort_robust_delay_metrics(metrics, limit=limit).with_columns(
            (pl.col("local_hour").cast(pl.Utf8).str.zfill(2) + pl.lit(":00")).alias(
                "hour_local"
            )
        ).select(
            [
                "hour_local",
                "bucket_count",
                "raw_poll_count",
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
            ]
        )
    )


def build_rush_impact(
    delay_buckets: pl.DataFrame,
    *,
    rush_windows: tuple[str, ...],
    include_weekends: bool,
    min_observations: int,
    limit: int,
) -> pl.DataFrame:
    if delay_buckets.is_empty():
        return pl.DataFrame()
    marked = delay_buckets.with_columns(
        rush_period_expr(parse_rush_windows(rush_windows), include_weekends=include_weekends)
    )
    grouped = summarize_delay_metrics(
        marked,
        ["line_ref", "is_rush"],
        min_observations=min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )
    if grouped.is_empty():
        return grouped
    rush = grouped.filter(pl.col("is_rush")).drop("is_rush")
    non_rush = grouped.filter(~pl.col("is_rush")).drop("is_rush")
    result = non_rush.join(rush, on="line_ref", how="inner", suffix="_rush")
    if result.is_empty():
        return result
    result = result.rename(
        {
            "bucket_count": "bucket_count_non_rush",
            "raw_poll_count": "raw_poll_count_non_rush",
            "median_delay_min": "median_delay_min_non_rush",
            "p90_delay_min": "p90_delay_min_non_rush",
            "pct_over_5_min_late": "pct_over_5_min_late_non_rush",
            "line_name": "line_name_non_rush",
            "bucket_count_rush": "bucket_count_rush",
            "raw_poll_count_rush": "raw_poll_count_rush",
            "median_delay_min_rush": "median_delay_min_rush",
            "p90_delay_min_rush": "p90_delay_min_rush",
            "pct_over_5_min_late_rush": "pct_over_5_min_late_rush",
            "line_name_rush": "line_name_rush",
        }
    )
    return round_numeric(
        result.with_columns(
            pl.coalesce("line_name_rush", "line_name_non_rush").alias("line_name"),
            (pl.col("median_delay_min_rush") - pl.col("median_delay_min_non_rush")).alias(
                "rush_median_delay_lift_min"
            ),
            (pl.col("p90_delay_min_rush") - pl.col("p90_delay_min_non_rush")).alias(
                "rush_p90_delay_lift_min"
            ),
            (
                pl.col("pct_over_5_min_late_rush")
                - pl.col("pct_over_5_min_late_non_rush")
            ).alias("rush_over_5_min_late_pct_point_lift"),
        )
        .sort(
            [
                "rush_p90_delay_lift_min",
                "rush_over_5_min_late_pct_point_lift",
                "bucket_count_rush",
            ],
            descending=[True, True, True],
        )
        .head(limit)
        .select(
            [
                "line_ref",
                "line_name",
                "bucket_count_non_rush",
                "bucket_count_rush",
                "raw_poll_count_non_rush",
                "raw_poll_count_rush",
                "median_delay_min_non_rush",
                "median_delay_min_rush",
                "rush_median_delay_lift_min",
                "p90_delay_min_non_rush",
                "p90_delay_min_rush",
                "rush_p90_delay_lift_min",
                "pct_over_5_min_late_non_rush",
                "pct_over_5_min_late_rush",
                "rush_over_5_min_late_pct_point_lift",
            ]
        )
    )


def build_stop_midpoint_change(settings: ReportSettings, delay_buckets: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    buckets = delay_buckets.filter(pl.col("next_stop_point_ref").is_not_null())
    if buckets.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    start = buckets["representative_time_utc"].min()
    end = buckets["representative_time_utc"].max()
    if start is None or end is None or start >= end:
        return pl.DataFrame(), pl.DataFrame()
    midpoint = start + ((end - start) / 2)
    summary = pl.DataFrame(
        {
            "baseline_start_utc": [start],
            "baseline_end_utc": [midpoint],
            "comparison_start_utc": [midpoint],
            "comparison_end_utc": [end],
        }
    )
    rows = buckets.with_columns(
        pl.when(
            (pl.col("representative_time_utc") >= start)
            & (pl.col("representative_time_utc") < midpoint)
        )
        .then(pl.lit("baseline"))
        .when(
            (pl.col("representative_time_utc") >= midpoint)
            & (pl.col("representative_time_utc") <= end)
        )
        .then(pl.lit("comparison"))
        .otherwise(None)
        .alias("period")
    ).filter(pl.col("period").is_not_null())
    rows = enrich_stops(
        rows,
        load_gtfs_stop_metadata(gtfs_dir=settings.gtfs_dir, gtfs_root=settings.gtfs_root),
        pl.DataFrame({"stop_id": [], "city_part": []}),
    )
    rows = matched_context_rows(rows, ["stop_id"])
    if rows.is_empty():
        return pl.DataFrame(), summary
    baseline = summarize_period(rows.filter(pl.col("period") == "baseline"), ["stop_id"], "baseline")
    comparison = summarize_period(
        rows.filter(pl.col("period") == "comparison"),
        ["stop_id"],
        "comparison",
    )
    result = baseline.join(comparison, on="stop_id", how="inner", suffix="_comparison")
    if result.is_empty():
        return result, summary
    for column in ("stop_name", "city_part", "stop_lat", "stop_lon"):
        comparison_column = f"{column}_comparison"
        if comparison_column in result.columns:
            if column in result.columns:
                result = result.with_columns(
                    pl.coalesce(pl.col(column), pl.col(comparison_column)).alias(column)
                ).drop(comparison_column)
            else:
                result = result.rename({comparison_column: column})
    result = result.filter(
        (pl.col("baseline_bucket_count") >= settings.min_observations)
        & (pl.col("comparison_bucket_count") >= settings.min_observations)
    )
    if result.is_empty():
        return result, summary
    result = result.with_columns(
        (pl.col("comparison_median_delay_min") - pl.col("baseline_median_delay_min")).alias(
            "median_delay_change_min"
        ),
        (pl.col("comparison_p90_delay_min") - pl.col("baseline_p90_delay_min")).alias(
            "p90_delay_change_min"
        ),
        (
            pl.col("comparison_pct_over_5_min_late")
            - pl.col("baseline_pct_over_5_min_late")
        ).alias("over_5_min_late_pct_point_change"),
    )
    ordered = [
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "baseline_bucket_count",
        "comparison_bucket_count",
        "baseline_raw_poll_count",
        "comparison_raw_poll_count",
        "baseline_median_delay_min",
        "comparison_median_delay_min",
        "median_delay_change_min",
        "baseline_p90_delay_min",
        "comparison_p90_delay_min",
        "p90_delay_change_min",
        "baseline_pct_over_5_min_late",
        "comparison_pct_over_5_min_late",
        "over_5_min_late_pct_point_change",
        "baseline_pct_over_3_min_early",
        "comparison_pct_over_3_min_early",
    ]
    present = [column for column in ordered if column in result.columns]
    return round_numeric(
        result.with_columns(pl.col("p90_delay_change_min").abs().alias("_abs_change"))
        .sort(["_abs_change", "comparison_bucket_count"], descending=[True, True])
        .drop("_abs_change")
        .head(settings.limit)
        .select(present)
    ), summary


def enrich_stops(
    df: pl.DataFrame,
    stops: pl.DataFrame,
    city_parts: pl.DataFrame,
) -> pl.DataFrame:
    result = df.with_columns(pl.col("next_stop_point_ref").cast(pl.Utf8).alias("stop_id"))
    if not stops.is_empty():
        stops = stops.with_columns(pl.col("stop_id").cast(pl.Utf8))
        if "gtfs_feed_date" in stops.columns:
            feeds = stops.select("gtfs_feed_date").unique()
            result = result.with_columns(assign_gtfs_feed_dates(result, feeds))
            result = result.join(stops, on=["gtfs_feed_date", "stop_id"], how="left")
        else:
            result = result.with_columns(pl.lit(None).alias("gtfs_feed_date"))
            result = result.join(stops, on="stop_id", how="left")
    else:
        result = result.with_columns(
            pl.lit(None).alias("gtfs_feed_date"),
            pl.lit(None).alias("gtfs_stop_name"),
            pl.lit(None).alias("stop_lat"),
            pl.lit(None).alias("stop_lon"),
        )
    result = result.with_columns(
        pl.col("gtfs_stop_name").is_not_null().alias("has_gtfs_stop_metadata"),
        pl.coalesce("gtfs_stop_name", "next_stop_point_name").alias("stop_name"),
    )
    if not city_parts.is_empty():
        result = result.join(
            city_parts.with_columns(pl.col("stop_id").cast(pl.Utf8)),
            on="stop_id",
            how="left",
        )
    elif "city_part" not in result.columns:
        result = result.with_columns(pl.lit(None).alias("city_part"))
    return result


def matched_context_rows(df: pl.DataFrame, group_keys: list[str]) -> pl.DataFrame:
    context_keys = group_keys + ["line_ref", "direction_ref", "local_weekday", "local_hour"]
    baseline = df.filter(pl.col("period") == "baseline").select(context_keys).unique()
    comparison = df.filter(pl.col("period") == "comparison").select(context_keys).unique()
    matched = baseline.join(comparison, on=context_keys, how="inner")
    if matched.is_empty():
        return pl.DataFrame(schema=df.schema)
    return df.join(matched, on=context_keys, how="inner")


def summarize_period(df: pl.DataFrame, keys: list[str], prefix: str) -> pl.DataFrame:
    extra_aggs: dict[str, tuple[str, str]] = {}
    for column in ("stop_name", "city_part", "stop_lat", "stop_lon"):
        if column in df.columns and column not in keys:
            extra_aggs[column] = (column, "first")
    grouped = summarize_delay_metrics(df, keys, min_observations=1, extra_aggs=extra_aggs)
    rename = {
        column: f"{prefix}_{column}"
        for column in grouped.columns
        if column not in keys and column not in extra_aggs
    }
    return grouped.rename(rename)


def load_collector_polls(settings: ReportSettings, *, source: str | None = None) -> pl.DataFrame:
    if not _db_table_exists(settings.db, "collector_polls"):
        return _empty_collector_polls()

    where = "1 = 1"
    params: list[object] = []
    if source:
        where += " AND source = ?"
        params.append(source)
    return read_sql(
        settings.db,
        f"""
        SELECT source, attempted_at_utc, collected_at_utc, status, ok, row_count,
               gap_seconds_since_previous_success
        FROM collector_polls
        WHERE {where}
        ORDER BY source, attempted_at_utc
        """,
        params,
    )


def build_collector_blackouts(polls: pl.DataFrame, limit: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    if polls.is_empty():
        return pl.DataFrame()
    for source in polls["source"].unique().to_list():
        source_df = polls.filter(pl.col("source") == source)
        successful = source_df.filter(pl.col("ok") == 1)
        gaps = successful.filter(pl.col("gap_seconds_since_previous_success") > 0)[
            "gap_seconds_since_previous_success"
        ]
        if gaps.is_empty():
            expected = None
            blackout = successful.head(0)
        else:
            expected = float(gaps.median())
            blackout = successful.filter(pl.col("gap_seconds_since_previous_success") > 2 * expected)
        avg_rows = float(successful["row_count"].mean() or 0.0) if not successful.is_empty() else 0.0
        durations = (
            (blackout["gap_seconds_since_previous_success"] - expected).clip(0)
            if expected is not None and not blackout.is_empty()
            else pl.Series([], dtype=pl.Float64)
        )
        missed_polls = durations / expected if expected else pl.Series([], dtype=pl.Float64)
        rows.append(
            {
                "source": source,
                "poll_count": source_df.height,
                "success_count": successful.height,
                "failed_count": source_df.filter(pl.col("ok") != 1).height,
                "expected_cadence_seconds": expected,
                "blackout_count": blackout.height,
                "total_blackout_min": float(durations.sum() or 0.0) / 60.0,
                "largest_blackout_min": float(durations.max() or 0.0) / 60.0,
                "estimated_missed_polls": float(missed_polls.sum() or 0.0),
                "estimated_missed_rows": float((missed_polls * avg_rows).sum() or 0.0),
            }
        )
    return round_numeric(
        pl.DataFrame(rows).sort(
            ["total_blackout_min", "blackout_count", "source"],
            descending=[True, True, False],
        ).head(limit)
    )


def build_missing_spots(
    polls: pl.DataFrame,
    *,
    gap_multiplier: float = 2.0,
    min_missing_minutes: float = 0.0,
) -> pl.DataFrame:
    if polls.is_empty():
        return pl.DataFrame()
    polls = polls.with_columns(
        pl.col("attempted_at_utc").cast(pl.Utf8).str.to_datetime(time_zone="UTC", strict=False),
        pl.col("collected_at_utc").cast(pl.Utf8).str.to_datetime(time_zone="UTC", strict=False),
    )
    rows: list[dict[str, object]] = []
    for source in polls["source"].unique().to_list():
        source_df = polls.filter(pl.col("source") == source).sort("attempted_at_utc")
        successful = source_df.filter(pl.col("ok") == 1).sort("collected_at_utc")
        gaps = successful.filter(pl.col("gap_seconds_since_previous_success") > 0)[
            "gap_seconds_since_previous_success"
        ]
        if gaps.is_empty():
            continue
        expected = float(gaps.median())
        avg_rows = float(successful["row_count"].mean() or 0.0)
        previous = None
        for success in successful.iter_rows(named=True):
            current = success["collected_at_utc"]
            if current is None:
                continue
            if previous is None:
                previous = current
                continue
            gap_seconds = (current - previous).total_seconds()
            missing_seconds = max(0.0, gap_seconds - expected)
            if (
                gap_seconds > gap_multiplier * expected
                and missing_seconds / 60.0 >= min_missing_minutes
            ):
                failed = source_df.filter(
                    (pl.col("attempted_at_utc") > previous)
                    & (pl.col("attempted_at_utc") < current)
                    & (pl.col("ok") != 1)
                )
                missed_polls = missing_seconds / expected
                rows.append(
                    {
                        "source": source,
                        "gap_start_utc": previous.isoformat(),
                        "gap_end_utc": current.isoformat(),
                        "gap_min": gap_seconds / 60.0,
                        "expected_cadence_seconds": expected,
                        "missing_min": missing_seconds / 60.0,
                        "estimated_missed_polls": missed_polls,
                        "estimated_missed_rows": missed_polls * avg_rows,
                        "failed_attempt_count": failed.height,
                        "next_success_status": success["status"],
                    }
                )
            previous = current
    if not rows:
        return pl.DataFrame()
    return round_numeric(
        pl.DataFrame(rows).sort(["missing_min", "source"], descending=[True, False])
    )


def summarize_missing_spots(spots: pl.DataFrame, polls: pl.DataFrame) -> pl.DataFrame:
    if polls.is_empty():
        return pl.DataFrame()
    rows: list[dict[str, object]] = []
    for source in polls["source"].unique().to_list():
        source_df = polls.filter(pl.col("source") == source)
        source_spots = spots.filter(pl.col("source") == source) if not spots.is_empty() else spots
        rows.append(
            {
                "source": source,
                "poll_count": source_df.height,
                "success_count": source_df.filter(pl.col("ok") == 1).height,
                "failed_count": source_df.filter(pl.col("ok") != 1).height,
                "missing_spot_count": source_spots.height,
                "total_missing_min": float(source_spots["missing_min"].sum() or 0.0)
                if not source_spots.is_empty()
                else 0.0,
                "largest_missing_min": float(source_spots["missing_min"].max() or 0.0)
                if not source_spots.is_empty()
                else 0.0,
                "estimated_missed_polls": float(
                    source_spots["estimated_missed_polls"].sum() or 0.0
                )
                if not source_spots.is_empty()
                else 0.0,
                "estimated_missed_rows": float(source_spots["estimated_missed_rows"].sum() or 0.0)
                if not source_spots.is_empty()
                else 0.0,
            }
        )
    return round_numeric(
        pl.DataFrame(rows).sort(
            ["total_missing_min", "missing_spot_count", "source"],
            descending=[True, True, False],
        )
    )


ALERT_GROUP_COLUMNS = ["cause", "effect", "priority", "alert_scope"]
MATCH_CONTEXT_COLUMNS = ["line_ref", "direction_ref", "local_hour", "day_type"]


def load_alerts(
    settings: ReportSettings,
    window: tuple[datetime | None, datetime | None] | None = None,
) -> pl.DataFrame:
    if not _db_table_exists(settings.db, "service_alerts"):
        return _empty_alerts()

    where = "is_active = 1"
    params: list[object] = []
    if window is not None:
        start, end = window
        if end is not None:
            where += " AND COALESCE(validity_start_utc, created_at_utc) < ?"
            params.append(end.strftime("%Y-%m-%dT%H:%M:%SZ"))
        if start is not None:
            where += " AND (validity_end_utc IS NULL OR validity_end_utc >= ?)"
            params.append(start.strftime("%Y-%m-%dT%H:%M:%SZ"))
    return read_sql(
        settings.db,
        f"""
        SELECT source_alert_id, line_ref, cause, effect, priority, is_active,
               validity_start_utc, validity_end_utc, affected_routes_json,
               affected_stops_json, created_at_utc
        FROM service_alerts
        WHERE {where}
        """,
        params,
    )


def build_alert_results(
    settings: ReportSettings,
    observations: pl.DataFrame,
    *,
    alert_kind: str = "any",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if observations.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    observations = observations.with_columns(
        pl.col("line_ref").cast(pl.Utf8),
        pl.col("direction_ref").cast(pl.Utf8),
        pl.col("next_stop_point_ref").cast(pl.Utf8),
    )
    start = observations["representative_time_utc"].min()
    end = observations["representative_time_utc"].max()
    alerts = load_alerts(settings, (start, end))
    routes = load_gtfs_route_metadata(gtfs_dir=settings.gtfs_dir, gtfs_root=settings.gtfs_root)
    targets = build_alert_targets(
        alerts,
        routes,
        start,
        end,
        include_routes=alert_kind in ("route", "any"),
        include_stops=alert_kind in ("stop", "any"),
        timezone=settings.timezone,
    )
    if targets.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    grouped_rows: list[pl.DataFrame] = []
    line_rows: list[pl.DataFrame] = []
    for group_values in targets.select(ALERT_GROUP_COLUMNS).unique().iter_rows(named=False):
        intervals = targets.filter(
            pl.all_horizontal(
                [
                    pl.col(column) == value
                    for column, value in zip(ALERT_GROUP_COLUMNS, group_values, strict=True)
                ]
            )
        )
        active_mask = mark_active_for_group(observations, intervals)
        active, controls = matched_control_rows(observations, active_mask)
        if active.is_empty() or controls.is_empty():
            continue
        group_data = dict(zip(ALERT_GROUP_COLUMNS, group_values, strict=True))
        grouped = summarize_alert_lift(
            active,
            controls,
            min_observations=settings.min_observations,
        )
        if not grouped.is_empty():
            grouped_rows.append(grouped.with_columns([pl.lit(v).alias(k) for k, v in group_data.items()]))
        by_line = summarize_alert_lift(
            active,
            controls,
            min_observations=settings.min_observations,
            group_keys=["line_ref"],
        )
        if not by_line.is_empty():
            line_rows.append(by_line.with_columns([pl.lit(v).alias(k) for k, v in group_data.items()]))
    grouped_result = _format_alert_result(
        pl.concat(grouped_rows, how="vertical_relaxed") if grouped_rows else pl.DataFrame(),
        settings.limit,
    )
    line_result = _format_alert_result(
        pl.concat(line_rows, how="vertical_relaxed") if line_rows else pl.DataFrame(),
        settings.limit,
        include_line=True,
    )
    return grouped_result, line_result


def build_alert_targets(
    alerts: pl.DataFrame,
    routes: pl.DataFrame,
    obs_start: datetime,
    obs_end: datetime,
    *,
    include_routes: bool,
    include_stops: bool,
    timezone: str,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for alert in alerts.iter_rows(named=True):
        start = _alert_timestamp(alert.get("validity_start_utc")) or _alert_timestamp(
            alert.get("created_at_utc")
        ) or obs_start
        end = _alert_timestamp(alert.get("validity_end_utc")) or (obs_end + timedelta_microsecond())
        base = {
            "cause": _clean_alert_value(alert.get("cause")),
            "effect": _clean_alert_value(alert.get("effect")),
            "priority": int(alert["priority"]) if alert.get("priority") is not None else -1,
            "start_utc": start,
            "end_utc": end,
        }
        if include_routes:
            lines = set()
            if alert.get("line_ref") is not None:
                lines.add(str(alert["line_ref"]))
            for route_ref in json_list(alert.get("affected_routes_json")):
                lines.add(resolve_route_short_name(routes, route_ref, start, timezone))
            for line_ref in lines:
                rows.append({**base, "alert_scope": "route", "target_ref": line_ref})
        if include_stops:
            for stop_id in set(json_list(alert.get("affected_stops_json"))):
                rows.append({**base, "alert_scope": "stop", "target_ref": stop_id})
    if not rows:
        return pl.DataFrame(schema={**{c: pl.Utf8 for c in ALERT_GROUP_COLUMNS}, "target_ref": pl.Utf8})
    return pl.DataFrame(rows).unique()


def resolve_route_short_name(
    routes: pl.DataFrame,
    route_ref: object,
    timestamp: object,
    timezone: str,
) -> str:
    route_ref_text = str(route_ref)
    if routes.is_empty():
        return route_ref_text
    candidates = routes.with_columns(
        pl.col("route_id").cast(pl.Utf8),
        pl.col("route_short_name").cast(pl.Utf8),
    )
    if "gtfs_feed_date" in candidates.columns:
        feed_date = gtfs_feed_date_for_timestamp(
            timestamp,
            candidates.select("gtfs_feed_date").unique(),
            timezone=timezone,
        )
        if feed_date is None:
            return route_ref_text
        candidates = candidates.filter(pl.col("gtfs_feed_date") == feed_date)
    matches = candidates.filter(pl.col("route_id") == route_ref_text)
    if matches.is_empty() or matches["route_short_name"][0] is None:
        return route_ref_text
    return str(matches["route_short_name"][0])


def mark_active_for_group(observations: pl.DataFrame, intervals: pl.DataFrame) -> pl.Series:
    active = pl.Series("active", [False] * observations.height)
    if intervals.is_empty():
        return active
    key = "line_ref" if intervals["alert_scope"][0] == "route" else "next_stop_point_ref"
    for interval in intervals.iter_rows(named=True):
        mask = observations.select(
            (
                (pl.col(key).cast(pl.Utf8) == str(interval["target_ref"]))
                & (pl.col("representative_time_utc") >= interval["start_utc"])
                & (pl.col("representative_time_utc") <= interval["end_utc"])
            )
            .fill_null(False)
            .alias("active")
        )["active"]
        active = active | mask
    return active


def matched_control_rows(
    observations: pl.DataFrame,
    active_mask: pl.Series,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    active = observations.filter(active_mask)
    if active.is_empty():
        return active, pl.DataFrame(schema=observations.schema)
    contexts = active.select(MATCH_CONTEXT_COLUMNS).unique()
    controls = observations.filter(~active_mask).join(contexts, on=MATCH_CONTEXT_COLUMNS, how="inner")
    return active, controls


def summarize_alert_lift(
    active: pl.DataFrame,
    controls: pl.DataFrame,
    *,
    min_observations: int,
    group_keys: list[str] | None = None,
) -> pl.DataFrame:
    group_keys = group_keys or []
    extra = {"line_name": ("published_line_name", "first")} if "line_ref" in group_keys else None
    active_metrics = summarize_delay_metrics(
        active,
        group_keys,
        min_observations=min_observations,
        extra_aggs=extra,
    )
    control_metrics = summarize_delay_metrics(
        controls,
        group_keys,
        min_observations=min_observations,
        extra_aggs=extra,
    )
    if active_metrics.is_empty() or control_metrics.is_empty():
        return pl.DataFrame()
    if group_keys:
        result = control_metrics.join(active_metrics, on=group_keys, how="inner", suffix="_alert")
        rename = {
            column: f"{column}_control"
            for column in control_metrics.columns
            if column not in group_keys
        }
        result = result.rename(rename)
    else:
        result = pl.concat(
            [
                control_metrics.rename({c: f"{c}_control" for c in control_metrics.columns}),
                active_metrics.rename({c: f"{c}_alert" for c in active_metrics.columns}),
            ],
            how="horizontal",
        )
    if result.is_empty():
        return result
    if "line_name_control" in result.columns:
        result = result.with_columns(
            pl.coalesce("line_name_alert", "line_name_control").alias("line_name")
        ).drop(["line_name_control", "line_name_alert"])
    return result.with_columns(
        (pl.col("median_delay_min_alert") - pl.col("median_delay_min_control")).alias(
            "median_delay_lift_min"
        ),
        (pl.col("p90_delay_min_alert") - pl.col("p90_delay_min_control")).alias(
            "p90_delay_lift_min"
        ),
        (
            pl.col("pct_over_5_min_late_alert")
            - pl.col("pct_over_5_min_late_control")
        ).alias("over_5_min_late_pct_point_lift"),
    )


def _format_alert_result(df: pl.DataFrame, limit: int, *, include_line: bool = False) -> pl.DataFrame:
    if df.is_empty():
        return df
    ordered = ALERT_GROUP_COLUMNS.copy()
    if include_line:
        ordered.extend(["line_ref", "line_name"])
    ordered.extend(
        [
            "bucket_count_control",
            "bucket_count_alert",
            "raw_poll_count_control",
            "raw_poll_count_alert",
            "median_delay_min_control",
            "median_delay_min_alert",
            "median_delay_lift_min",
            "p90_delay_min_control",
            "p90_delay_min_alert",
            "p90_delay_lift_min",
            "pct_over_5_min_late_control",
            "pct_over_5_min_late_alert",
            "over_5_min_late_pct_point_lift",
            "pct_over_3_min_early_control",
            "pct_over_3_min_early_alert",
        ]
    )
    return round_numeric(
        df.sort(
            ["p90_delay_lift_min", "over_5_min_late_pct_point_lift", "bucket_count_alert"],
            descending=[True, True, True],
        )
        .head(limit)
        .select([column for column in ordered if column in df.columns])
    )


def json_list(value: object) -> list[str]:
    if value is None or str(value).strip() == "":
        return []
    try:
        decoded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(decoded, list):
        return []
    return [str(item) for item in decoded if item is not None and not isinstance(item, dict | list)]


def build_quality_summary_lazy(quality_rows: pl.LazyFrame) -> pl.LazyFrame:
    conservative_default = pl.any_horizontal(
        [pl.col(column) for column in CONSERVATIVE_EXCLUSION_COLUMNS]
    )
    conservative_with_stop_call = conservative_default | pl.col("has_stop_call_disagreement")
    return (
        quality_rows.select(
            pl.len().alias("analysis_rows"),
            *[pl.col(column).sum().alias(column) for column in QUALITY_FLAG_COLUMNS],
            conservative_default.sum().alias("conservative_excluded_default"),
            conservative_with_stop_call.sum().alias(
                "conservative_excluded_with_stop_call_disagreement"
            ),
        )
        .unpivot(index=[], variable_name="quality_check", value_name="row_count")
        .with_columns(
            pl.when(pl.col("quality_check") == "analysis_rows")
            .then(pl.col("row_count").cast(pl.Float64))
            .otherwise(
                pl.col("row_count").cast(pl.Float64)
                / pl.col("row_count").filter(pl.col("quality_check") == "analysis_rows").first()
                * 100.0
            )
            .alias("pct_rows")
        )
        .with_columns(
            pl.when(pl.col("quality_check") == "analysis_rows")
            .then(pl.lit(100.0))
            .otherwise(pl.col("pct_rows"))
            .alias("pct_rows")
        )
        .with_columns(pl.col("row_count").cast(pl.Int64))
    )


def build_quality_by_line_lazy(
    quality_rows: pl.LazyFrame,
    *,
    min_observations: int,
    limit: int,
) -> pl.LazyFrame:
    conservative_default = pl.any_horizontal(
        [pl.col(column) for column in CONSERVATIVE_EXCLUSION_COLUMNS]
    )
    grouped = (
        quality_rows.with_columns(
            conservative_default.alias("conservative_excluded_default")
        )
        .group_by("line_ref")
        .agg(
            pl.len().alias("row_count"),
            pl.col("published_line_name").sort_by("recorded_at_utc").first().alias("line_name"),
            pl.col("is_implausible_delay").sum().alias("implausible_delay_rows"),
            pl.col("is_stale_observation").sum().alias("stale_rows"),
            pl.col("is_pre_trip_observation").sum().alias("pre_trip_rows"),
            pl.col("is_post_trip_observation").sum().alias("post_trip_rows"),
            pl.col("has_stop_call_disagreement").sum().alias("stop_call_disagreement_rows"),
            pl.col("conservative_excluded_default").sum().alias("conservative_excluded_rows"),
        )
        .filter(pl.col("row_count") >= min_observations)
    )
    return round_numeric_lazy(
        grouped.with_columns(
            (
                pl.col("conservative_excluded_rows") / pl.col("row_count") * 100.0
            ).alias("conservative_excluded_pct")
        )
        .sort(
            ["conservative_excluded_pct", "conservative_excluded_rows", "line_ref"],
            descending=[True, True, False],
        )
        .head(limit)
    )


def build_line_rankings_lazy(
    delay_buckets: pl.LazyFrame,
    ranking: str,
    *,
    min_observations: int,
    limit: int,
) -> pl.LazyFrame:
    metrics = summarize_delay_metrics_lazy(
        delay_buckets,
        ["line_ref"],
        min_observations=min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )
    if ranking == "early":
        return round_numeric_lazy(
            metrics.sort(
                ["p90_early_min_abs", "pct_over_3_min_early", "bucket_count", "line_ref"],
                descending=[True, True, True, False],
            )
            .head(limit)
            .select(
                [
                    "line_ref",
                    "line_name",
                    "bucket_count",
                    "raw_poll_count",
                    "signed_mean_delay_min",
                    "median_delay_min",
                    "pct_early",
                    "pct_over_1_min_early",
                    "pct_over_3_min_early",
                    "median_early_min_abs",
                    "p90_early_min_abs",
                ]
            )
        )
    return round_numeric_lazy(
        metrics.sort(
            ["p90_delay_min", "pct_over_5_min_late", "bucket_count", "line_ref"],
            descending=[True, True, True, False],
        )
        .head(limit)
        .select(
            [
                "line_ref",
                "line_name",
                "bucket_count",
                "raw_poll_count",
                "signed_mean_delay_min",
                "median_delay_min",
                "p75_delay_min",
                "p90_delay_min",
                "p95_delay_min",
                "pct_over_3_min_late",
                "pct_over_5_min_late",
            ]
        )
    )


def build_context_delay_metrics_lazy(
    delay_buckets: pl.LazyFrame,
    *,
    line_ref: str | None = None,
    direction_ref: str | None = None,
    day_type: str = "all",
    min_observations: int,
    limit: int,
) -> pl.LazyFrame:
    buckets = delay_buckets
    if line_ref:
        buckets = buckets.filter(pl.col("line_ref") == line_ref)
    if direction_ref:
        buckets = buckets.filter(pl.col("direction_ref") == direction_ref)
    if day_type != "all":
        buckets = buckets.filter(pl.col("day_type") == day_type)
    metrics = summarize_delay_metrics_lazy(
        buckets,
        ["line_ref", "direction_ref", "local_hour", "day_type"],
        min_observations=min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )
    return round_numeric_lazy(
        sort_robust_delay_metrics_lazy(metrics, limit=limit)
        .with_columns(
            (pl.col("local_hour").cast(pl.Utf8).str.zfill(2) + pl.lit(":00")).alias(
                "hour_local"
            )
        )
        .select(
            [
                "line_ref",
                "line_name",
                "direction_ref",
                "hour_local",
                "day_type",
                "bucket_count",
                "raw_poll_count",
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
    )


def build_hourly_delay_profile_lazy(
    delay_buckets: pl.LazyFrame,
    *,
    line_ref: str | None = None,
    min_observations: int,
    limit: int,
) -> pl.LazyFrame:
    buckets = delay_buckets.filter(pl.col("line_ref") == line_ref) if line_ref else delay_buckets
    metrics = summarize_delay_metrics_lazy(
        buckets,
        ["local_hour"],
        min_observations=min_observations,
    )
    return round_numeric_lazy(
        sort_robust_delay_metrics_lazy(metrics, limit=limit)
        .with_columns(
            (pl.col("local_hour").cast(pl.Utf8).str.zfill(2) + pl.lit(":00")).alias(
                "hour_local"
            )
        )
        .select(
            [
                "hour_local",
                "bucket_count",
                "raw_poll_count",
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
            ]
        )
    )


def build_rush_impact_lazy(
    delay_buckets: pl.LazyFrame,
    *,
    rush_windows: tuple[str, ...],
    include_weekends: bool,
    min_observations: int,
    limit: int,
) -> pl.LazyFrame:
    marked = delay_buckets.with_columns(
        rush_period_expr(parse_rush_windows(rush_windows), include_weekends=include_weekends)
    )
    grouped = summarize_delay_metrics_lazy(
        marked,
        ["line_ref", "is_rush"],
        min_observations=min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )
    rush = grouped.filter(pl.col("is_rush")).drop("is_rush")
    non_rush = grouped.filter(~pl.col("is_rush")).drop("is_rush")
    result = non_rush.join(rush, on="line_ref", how="inner", suffix="_rush").rename(
        {
            "bucket_count": "bucket_count_non_rush",
            "raw_poll_count": "raw_poll_count_non_rush",
            "median_delay_min": "median_delay_min_non_rush",
            "p90_delay_min": "p90_delay_min_non_rush",
            "pct_over_5_min_late": "pct_over_5_min_late_non_rush",
            "line_name": "line_name_non_rush",
            "bucket_count_rush": "bucket_count_rush",
            "raw_poll_count_rush": "raw_poll_count_rush",
            "median_delay_min_rush": "median_delay_min_rush",
            "p90_delay_min_rush": "p90_delay_min_rush",
            "pct_over_5_min_late_rush": "pct_over_5_min_late_rush",
            "line_name_rush": "line_name_rush",
        }
    )
    return round_numeric_lazy(
        result.with_columns(
            pl.coalesce("line_name_rush", "line_name_non_rush").alias("line_name"),
            (pl.col("median_delay_min_rush") - pl.col("median_delay_min_non_rush")).alias(
                "rush_median_delay_lift_min"
            ),
            (pl.col("p90_delay_min_rush") - pl.col("p90_delay_min_non_rush")).alias(
                "rush_p90_delay_lift_min"
            ),
            (
                pl.col("pct_over_5_min_late_rush")
                - pl.col("pct_over_5_min_late_non_rush")
            ).alias("rush_over_5_min_late_pct_point_lift"),
        )
        .sort(
            [
                "rush_p90_delay_lift_min",
                "rush_over_5_min_late_pct_point_lift",
                "bucket_count_rush",
            ],
            descending=[True, True, True],
        )
        .head(limit)
        .select(
            [
                "line_ref",
                "line_name",
                "bucket_count_non_rush",
                "bucket_count_rush",
                "raw_poll_count_non_rush",
                "raw_poll_count_rush",
                "median_delay_min_non_rush",
                "median_delay_min_rush",
                "rush_median_delay_lift_min",
                "p90_delay_min_non_rush",
                "p90_delay_min_rush",
                "rush_p90_delay_lift_min",
                "pct_over_5_min_late_non_rush",
                "pct_over_5_min_late_rush",
                "rush_over_5_min_late_pct_point_lift",
            ]
        )
    )


def summarize_delay_metrics_lazy(
    df: pl.LazyFrame,
    group_keys: list[str],
    *,
    min_observations: int = 1,
    extra_aggs: dict[str, tuple[str, str]] | None = None,
) -> pl.LazyFrame:
    working = df.with_columns(pl.col("delay_seconds").cast(pl.Float64, strict=False))
    if "raw_poll_count" not in working.collect_schema().names():
        working = working.with_columns(pl.lit(1, dtype=pl.Int64).alias("raw_poll_count"))
    keys = group_keys.copy()
    if not keys:
        working = working.with_columns(pl.lit("overall").alias("_scope"))
        keys = ["_scope"]

    aggs: list[pl.Expr] = metric_aggs()
    for output, (column, how) in (extra_aggs or {}).items():
        if how != "first":
            raise ValueError("Polars extra_aggs currently supports only first aggregations.")
        if "representative_time_utc" in working.collect_schema().names():
            aggs.append(pl.col(column).sort_by("representative_time_utc").first().alias(output))
        else:
            aggs.append(pl.col(column).first().alias(output))

    grouped = working.group_by(keys).agg(*aggs).filter(
        pl.col("bucket_count") >= min_observations
    )
    if "_scope" in grouped.collect_schema().names():
        grouped = grouped.drop("_scope")
    return grouped


def sort_robust_delay_metrics_lazy(
    df: pl.LazyFrame,
    *,
    limit: int | None = None,
    ascending: bool = False,
) -> pl.LazyFrame:
    result = df.sort(
        ["p90_delay_min", "pct_over_5_min_late", "bucket_count"],
        descending=[not ascending, not ascending, ascending],
    )
    return result.head(limit) if limit is not None else result


def round_numeric_lazy(df: pl.LazyFrame, digits: int = 2) -> pl.LazyFrame:
    return df.with_columns(cs.numeric().round(digits))


def build_stop_midpoint_change_lazy(
    settings: ReportSettings,
    delay_buckets: pl.LazyFrame,
) -> tuple[pl.LazyFrame, pl.DataFrame]:
    buckets = delay_buckets.filter(pl.col("next_stop_point_ref").is_not_null())
    bounds = buckets.select(
        pl.col("representative_time_utc").min().alias("start"),
        pl.col("representative_time_utc").max().alias("end"),
    ).collect()
    start = bounds["start"][0] if bounds.height else None
    end = bounds["end"][0] if bounds.height else None
    if start is None or end is None or start >= end:
        return pl.DataFrame().lazy(), pl.DataFrame()

    midpoint = start + ((end - start) / 2)
    summary = pl.DataFrame(
        {
            "baseline_start_utc": [start],
            "baseline_end_utc": [midpoint],
            "comparison_start_utc": [midpoint],
            "comparison_end_utc": [end],
        }
    )
    period_rows = (
        buckets.with_columns(
            pl.col("next_stop_point_ref").cast(pl.Utf8).alias("stop_id"),
            pl.when(
                (pl.col("representative_time_utc") >= start)
                & (pl.col("representative_time_utc") < midpoint)
            )
            .then(pl.lit("baseline"))
            .when(
                (pl.col("representative_time_utc") >= midpoint)
                & (pl.col("representative_time_utc") <= end)
            )
            .then(pl.lit("comparison"))
            .otherwise(None)
            .alias("period"),
        )
        .filter(pl.col("period").is_not_null())
    )
    rows = matched_context_rows_lazy(period_rows, ["stop_id"])
    rows = enrich_stops_lazy(
        rows,
        load_gtfs_stop_metadata(gtfs_dir=settings.gtfs_dir, gtfs_root=settings.gtfs_root),
    )
    baseline = summarize_period_lazy(
        rows.filter(pl.col("period") == "baseline"),
        ["stop_id"],
        "baseline",
        min_observations=settings.min_observations,
    )
    comparison = summarize_period_lazy(
        rows.filter(pl.col("period") == "comparison"),
        ["stop_id"],
        "comparison",
        min_observations=settings.min_observations,
    )
    result = baseline.join(comparison, on="stop_id", how="inner", suffix="_comparison")
    result_columns = set(result.collect_schema().names())
    for column in ("stop_name", "city_part", "stop_lat", "stop_lon"):
        comparison_column = f"{column}_comparison"
        if comparison_column not in result_columns:
            continue
        if column in result_columns:
            result = result.with_columns(
                pl.coalesce(pl.col(column), pl.col(comparison_column)).alias(column)
            ).drop(comparison_column)
        else:
            result = result.rename({comparison_column: column})
        result_columns = set(result.collect_schema().names())

    result = result.with_columns(
        (pl.col("comparison_median_delay_min") - pl.col("baseline_median_delay_min")).alias(
            "median_delay_change_min"
        ),
        (pl.col("comparison_p90_delay_min") - pl.col("baseline_p90_delay_min")).alias(
            "p90_delay_change_min"
        ),
        (
            pl.col("comparison_pct_over_5_min_late")
            - pl.col("baseline_pct_over_5_min_late")
        ).alias("over_5_min_late_pct_point_change"),
    )
    ordered = [
        "stop_id",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "baseline_bucket_count",
        "comparison_bucket_count",
        "baseline_raw_poll_count",
        "comparison_raw_poll_count",
        "baseline_median_delay_min",
        "comparison_median_delay_min",
        "median_delay_change_min",
        "baseline_p90_delay_min",
        "comparison_p90_delay_min",
        "p90_delay_change_min",
        "baseline_pct_over_5_min_late",
        "comparison_pct_over_5_min_late",
        "over_5_min_late_pct_point_change",
        "baseline_pct_over_3_min_early",
        "comparison_pct_over_3_min_early",
    ]
    present = [column for column in ordered if column in result.collect_schema().names()]
    return round_numeric_lazy(
        result.with_columns(pl.col("p90_delay_change_min").abs().alias("_abs_change"))
        .sort(["_abs_change", "comparison_bucket_count"], descending=[True, True])
        .drop("_abs_change")
        .head(settings.limit)
        .select(present)
    ), summary


def enrich_stops_lazy(df: pl.LazyFrame, stops: pl.DataFrame) -> pl.LazyFrame:
    result = df.with_columns(pl.col("next_stop_point_ref").cast(pl.Utf8).alias("stop_id"))
    if not stops.is_empty():
        stops = stops.with_columns(pl.col("stop_id").cast(pl.Utf8))
        if "gtfs_feed_date" in stops.columns:
            feeds = stops.select("gtfs_feed_date").unique().sort("gtfs_feed_date")
            result = (
                result.sort("local_date")
                .join_asof(
                    feeds.lazy(),
                    left_on="local_date",
                    right_on="gtfs_feed_date",
                    strategy="backward",
                )
                .join(stops.lazy(), on=["gtfs_feed_date", "stop_id"], how="left")
            )
        else:
            result = result.with_columns(pl.lit(None).alias("gtfs_feed_date")).join(
                stops.lazy(),
                on="stop_id",
                how="left",
            )
    else:
        result = result.with_columns(
            pl.lit(None).alias("gtfs_feed_date"),
            pl.lit(None).alias("gtfs_stop_name"),
            pl.lit(None).alias("stop_lat"),
            pl.lit(None).alias("stop_lon"),
        )
    return result.with_columns(
        pl.col("gtfs_stop_name").is_not_null().alias("has_gtfs_stop_metadata"),
        pl.coalesce("gtfs_stop_name", "next_stop_point_name").alias("stop_name"),
        pl.lit(None).alias("city_part"),
    )


def matched_context_rows_lazy(df: pl.LazyFrame, group_keys: list[str]) -> pl.LazyFrame:
    context_keys = group_keys + ["line_ref", "direction_ref", "local_weekday", "local_hour"]
    baseline = df.filter(pl.col("period") == "baseline").select(context_keys).unique()
    comparison = df.filter(pl.col("period") == "comparison").select(context_keys).unique()
    matched = baseline.join(comparison, on=context_keys, how="inner")
    return df.join(matched, on=context_keys, how="inner")


def summarize_period_lazy(
    df: pl.LazyFrame,
    keys: list[str],
    prefix: str,
    *,
    min_observations: int,
) -> pl.LazyFrame:
    schema_names = set(df.collect_schema().names())
    extra_aggs: dict[str, tuple[str, str]] = {}
    for column in ("stop_name", "city_part", "stop_lat", "stop_lon"):
        if column in schema_names and column not in keys:
            extra_aggs[column] = (column, "first")
    grouped = summarize_delay_metrics_lazy(
        df,
        keys,
        min_observations=min_observations,
        extra_aggs=extra_aggs,
    )
    rename = {
        column: f"{prefix}_{column}"
        for column in grouped.collect_schema().names()
        if column not in keys and column not in extra_aggs
    }
    return grouped.rename(rename)


def build_alert_results_lazy(
    settings: ReportSettings,
    observations: pl.LazyFrame,
    *,
    alert_kind: str = "any",
) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    observations = observations.with_columns(
        pl.col("line_ref").cast(pl.Utf8),
        pl.col("direction_ref").cast(pl.Utf8),
        pl.col("next_stop_point_ref").cast(pl.Utf8),
    )
    bounds = observations.select(
        pl.col("representative_time_utc").min().alias("start"),
        pl.col("representative_time_utc").max().alias("end"),
    ).collect()
    start = bounds["start"][0] if bounds.height else None
    end = bounds["end"][0] if bounds.height else None
    if start is None or end is None:
        return pl.DataFrame().lazy(), pl.DataFrame().lazy()

    alerts = load_alerts(settings, (start, end))
    routes = load_gtfs_route_metadata(gtfs_dir=settings.gtfs_dir, gtfs_root=settings.gtfs_root)
    targets = build_alert_targets(
        alerts,
        routes,
        start,
        end,
        include_routes=alert_kind in ("route", "any"),
        include_stops=alert_kind in ("stop", "any"),
        timezone=settings.timezone,
    )
    if targets.is_empty():
        return pl.DataFrame().lazy(), pl.DataFrame().lazy()

    target_rows = targets.lazy().with_columns(
        pl.col("target_ref").cast(pl.Utf8),
        pl.col("start_utc").cast(pl.Datetime(time_zone="UTC")),
        pl.col("end_utc").cast(pl.Datetime(time_zone="UTC")),
    )
    route_active = active_alert_buckets_lazy(
        target_rows.filter(pl.col("alert_scope") == "route"),
        observations,
        observation_key="line_ref",
    )
    stop_active = active_alert_buckets_lazy(
        target_rows.filter(pl.col("alert_scope") == "stop"),
        observations,
        observation_key="next_stop_point_ref",
    )
    active_buckets = pl.concat([route_active, stop_active], how="vertical_relaxed").unique()
    active = active_buckets.join(observations, on="bucket_id", how="inner")
    contexts = active.select([*ALERT_GROUP_COLUMNS, *MATCH_CONTEXT_COLUMNS]).unique()
    controls = (
        contexts.join(observations, on=MATCH_CONTEXT_COLUMNS, how="inner")
        .join(active_buckets, on=[*ALERT_GROUP_COLUMNS, "bucket_id"], how="anti")
    )
    grouped = _format_alert_result_lazy(
        summarize_alert_lift_lazy(
            active,
            controls,
            min_observations=settings.min_observations,
            group_keys=ALERT_GROUP_COLUMNS,
        ),
        settings.limit,
    )
    by_line = _format_alert_result_lazy(
        summarize_alert_lift_lazy(
            active,
            controls,
            min_observations=settings.min_observations,
            group_keys=[*ALERT_GROUP_COLUMNS, "line_ref"],
        ),
        settings.limit,
        include_line=True,
    )
    return grouped, by_line


def active_alert_buckets_lazy(
    targets: pl.LazyFrame,
    observations: pl.LazyFrame,
    *,
    observation_key: str,
) -> pl.LazyFrame:
    return (
        targets.join(observations, left_on="target_ref", right_on=observation_key, how="inner")
        .filter(
            (pl.col("representative_time_utc") >= pl.col("start_utc"))
            & (pl.col("representative_time_utc") <= pl.col("end_utc"))
        )
        .select([*ALERT_GROUP_COLUMNS, "bucket_id"])
    )


def summarize_alert_lift_lazy(
    active: pl.LazyFrame,
    controls: pl.LazyFrame,
    *,
    min_observations: int,
    group_keys: list[str],
) -> pl.LazyFrame:
    extra = {"line_name": ("published_line_name", "first")} if "line_ref" in group_keys else None
    active_metrics = summarize_delay_metrics_lazy(
        active,
        group_keys,
        min_observations=min_observations,
        extra_aggs=extra,
    )
    control_metrics = summarize_delay_metrics_lazy(
        controls,
        group_keys,
        min_observations=min_observations,
        extra_aggs=extra,
    )
    result = control_metrics.join(active_metrics, on=group_keys, how="inner", suffix="_alert")
    rename = {
        column: f"{column}_control"
        for column in control_metrics.collect_schema().names()
        if column not in group_keys
    }
    result = result.rename(rename)
    if "line_name_control" in result.collect_schema().names():
        result = result.with_columns(
            pl.coalesce("line_name_alert", "line_name_control").alias("line_name")
        ).drop(["line_name_control", "line_name_alert"])
    return result.with_columns(
        (pl.col("median_delay_min_alert") - pl.col("median_delay_min_control")).alias(
            "median_delay_lift_min"
        ),
        (pl.col("p90_delay_min_alert") - pl.col("p90_delay_min_control")).alias(
            "p90_delay_lift_min"
        ),
        (
            pl.col("pct_over_5_min_late_alert")
            - pl.col("pct_over_5_min_late_control")
        ).alias("over_5_min_late_pct_point_lift"),
    )


def _format_alert_result_lazy(
    df: pl.LazyFrame,
    limit: int,
    *,
    include_line: bool = False,
) -> pl.LazyFrame:
    ordered = ALERT_GROUP_COLUMNS.copy()
    if include_line:
        ordered.extend(["line_ref", "line_name"])
    ordered.extend(
        [
            "bucket_count_control",
            "bucket_count_alert",
            "raw_poll_count_control",
            "raw_poll_count_alert",
            "median_delay_min_control",
            "median_delay_min_alert",
            "median_delay_lift_min",
            "p90_delay_min_control",
            "p90_delay_min_alert",
            "p90_delay_lift_min",
            "pct_over_5_min_late_control",
            "pct_over_5_min_late_alert",
            "over_5_min_late_pct_point_lift",
            "pct_over_3_min_early_control",
            "pct_over_3_min_early_alert",
        ]
    )
    present = [column for column in ordered if column in df.collect_schema().names()]
    return round_numeric_lazy(
        df.sort(
            ["p90_delay_lift_min", "over_5_min_late_pct_point_lift", "bucket_count_alert"],
            descending=[True, True, True],
        )
        .head(limit)
        .select(present)
    )


def _build_result_tables(settings: ReportSettings) -> None:
    quality_rows = _scan_table(settings.cache_dir, QUALITY_ROWS_NAME)
    delay_buckets = _scan_table(settings.cache_dir, DELAY_BUCKETS_NAME)

    with tempfile.TemporaryDirectory(
        dir=settings.cache_dir,
        prefix=".polars-report-results-",
    ) as temp_name:
        temp_dir = Path(temp_name)
        _write_result_table(
            settings.cache_dir,
            "quality_summary",
            build_quality_summary_lazy(quality_rows),
            temp_dir,
        )
        _write_result_table(
            settings.cache_dir,
            "quality_by_line",
            build_quality_by_line_lazy(
                quality_rows,
                min_observations=settings.min_observations,
                limit=settings.limit,
            ),
            temp_dir,
        )
        _write_result_table(
            settings.cache_dir,
            "line_late_rankings",
            build_line_rankings_lazy(
                delay_buckets,
                "late",
                min_observations=settings.min_observations,
                limit=settings.limit,
            ),
            temp_dir,
        )
        _write_result_table(
            settings.cache_dir,
            "line_early_rankings",
            build_line_rankings_lazy(
                delay_buckets,
                "early",
                min_observations=settings.min_observations,
                limit=settings.limit,
            ),
            temp_dir,
        )
        _write_result_table(
            settings.cache_dir,
            "context_delay_metrics",
            build_context_delay_metrics_lazy(
                delay_buckets,
                min_observations=settings.min_observations,
                limit=settings.limit,
            ),
            temp_dir,
        )
        _write_result_table(
            settings.cache_dir,
            "hourly_delay_profile",
            build_hourly_delay_profile_lazy(
                delay_buckets,
                min_observations=settings.min_observations,
                limit=settings.limit,
            ),
            temp_dir,
        )
        _write_result_table(
            settings.cache_dir,
            "rush_impact",
            build_rush_impact_lazy(
                delay_buckets,
                rush_windows=settings.rush_windows,
                include_weekends=settings.include_weekends,
                min_observations=settings.min_observations,
                limit=settings.limit,
            ),
            temp_dir,
        )

        stop_change, midpoint_summary = build_stop_midpoint_change_lazy(settings, delay_buckets)
        _write_table_atomic(settings.cache_dir, "midpoint_summary", midpoint_summary, temp_dir)
        _write_result_table(settings.cache_dir, "stop_midpoint_change", stop_change, temp_dir)

        polls = load_collector_polls(settings)
        _write_result_table(
            settings.cache_dir,
            "collector_blackouts",
            build_collector_blackouts(polls, settings.limit),
            temp_dir,
        )
        spots = build_missing_spots(polls)
        _write_result_table(
            settings.cache_dir,
            "collector_missing_summary",
            summarize_missing_spots(spots, polls),
            temp_dir,
        )
        _write_result_table(
            settings.cache_dir,
            "collector_missing_spots",
            spots.head(settings.limit) if not spots.is_empty() else spots,
            temp_dir,
        )

        grouped_alerts, line_alerts = build_alert_results_lazy(settings, delay_buckets)
        _write_result_table(settings.cache_dir, "service_alert_grouped", grouped_alerts, temp_dir)
        _write_result_table(settings.cache_dir, "service_alert_by_line", line_alerts, temp_dir)


def _build_quality_rows(settings: ReportSettings) -> pl.DataFrame:
    query = (
        base_quality_query()
        if _db_table_exists(settings.db, "collector_polls")
        else base_quality_query_without_collector()
    )
    rows = read_sql(settings.db, query)
    return add_quality_pass(
        rows,
        quality_mode=settings.quality_mode,
        exclude_stop_call_disagreement=settings.exclude_stop_call_disagreement,
    )


def _write_table(cache_dir: Path, table_name: str, df: pl.DataFrame) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    df.write_parquet(cache_dir / f"{table_name}.parquet")


def _write_csv(cache_dir: Path, table_name: str, df: pl.DataFrame) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    df.write_csv(cache_dir / f"{table_name}.csv")


def _scan_table(cache_dir: Path, table_name: str) -> pl.LazyFrame:
    return pl.scan_parquet(resolve_project_path(cache_dir) / f"{table_name}.parquet")


def _write_result_table(
    cache_dir: Path,
    table_name: str,
    table: pl.DataFrame | pl.LazyFrame,
    temp_dir: Path,
) -> None:
    df = table.collect() if isinstance(table, pl.LazyFrame) else table
    _write_table_atomic(cache_dir, table_name, df, temp_dir)
    csv_path = temp_dir / f"{table_name}.csv"
    df.write_csv(csv_path)
    _replace_file(csv_path, cache_dir / f"{table_name}.csv")


def _write_table_atomic(
    cache_dir: Path,
    table_name: str,
    df: pl.DataFrame,
    temp_dir: Path,
) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    table_path = temp_dir / f"{table_name}.parquet"
    df.write_parquet(table_path)
    _replace_file(table_path, cache_dir / f"{table_name}.parquet")


def _write_lazy_parquet(lf: pl.LazyFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lf.sink_parquet(path)


def _replace_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    source.replace(destination)


def _render_report_lines(settings: ReportSettings, cache_result: CacheResult) -> list[str]:
    render_started = time.perf_counter()
    manifest = cache_result.manifest
    db_meta = manifest["db_metadata"]
    lines = [
        "# Overall Bus Lateness Results (Polars)",
        "",
        f"Generated at: {_utc_now_iso()}",
        f"Cache status: {cache_result.status}",
        f"Database: `{db_meta['db_path']}`",
        (
            "Observation range: "
            f"{db_meta.get('analysis_start_utc') or 'unknown'} to "
            f"{db_meta.get('analysis_end_utc') or 'unknown'} UTC"
        ),
        (
            "Rows: "
            f"{_format_int(db_meta.get('analysis_row_count', 0))} analysis rows from "
            f"{_format_int(db_meta.get('vehicle_observation_count', 0))} raw vehicle observations"
        ),
        (
            "Settings: "
            f"quality `{settings.quality_mode}`, bucket `{settings.bucket}`, "
            f"timezone `{settings.timezone}`, minimum observations {settings.min_observations}"
        ),
        "",
    ]
    timing_index = len(lines) - 1
    summary = read_table(settings.cache_dir, DELAY_CACHE_SUMMARY_NAME)
    if not summary.is_empty():
        row = summary.row(0, named=True)
        lines.extend(
            [
                "## Cached Analysis Scope",
                "",
                (
                    f"- Buckets: {_format_int(row.get('bucket_count', 0))}; "
                    f"raw polls represented: {_format_int(row.get('raw_poll_count', 0))}"
                ),
                f"- Lines represented: {_format_int(row.get('line_count', 0))}",
                (
                    "- Representative time range: "
                    f"{_format_value(row.get('observation_start_utc'))} to "
                    f"{_format_value(row.get('observation_end_utc'))}"
                ),
                "",
            ]
        )
    midpoint = read_table(settings.cache_dir, "midpoint_summary")
    if not midpoint.is_empty():
        row = midpoint.row(0, named=True)
        lines.extend(
            [
                "## Automatic Midpoint Split",
                "",
                (
                    "- Baseline: "
                    f"{_format_value(row.get('baseline_start_utc'))} to "
                    f"{_format_value(row.get('baseline_end_utc'))}"
                ),
                (
                    "- Comparison: "
                    f"{_format_value(row.get('comparison_start_utc'))} to "
                    f"{_format_value(row.get('comparison_end_utc'))}"
                ),
                "",
            ]
        )
    sections = [
        ("Data Quality Summary", "quality_summary"),
        ("Worst Data Quality By Line", "quality_by_line"),
        ("Most Late Lines", "line_late_rankings"),
        ("Most Early Lines", "line_early_rankings"),
        ("Context Delay Metrics", "context_delay_metrics"),
        ("Hourly Delay Profile", "hourly_delay_profile"),
        ("Rush-Time Impact", "rush_impact"),
        ("Stop-Level Midpoint Changes", "stop_midpoint_change"),
        ("Service Alert Matched-Control Groups", "service_alert_grouped"),
        ("Service Alert Matched-Control By Line", "service_alert_by_line"),
        ("Collector Blackouts", "collector_blackouts"),
        ("Collector Missing-Data Summary", "collector_missing_summary"),
        ("Collector Missing-Data Spots", "collector_missing_spots"),
    ]
    for title, table_name in sections:
        lines.extend(_render_table_section(settings.cache_dir, title, table_name))
    lines.extend(
        [
            "## Caveats",
            "",
            "- SIRI VM delay is estimated vehicle-monitoring state, not measured arrival truth.",
            "- Raw vehicle-monitoring rows are repeated polls; default results use trip-stop buckets.",
            "- Conservative filtering excludes implausible, stale, pre-trip, and post-trip rows.",
            "- This report is generated by the Polars secondary analysis path.",
            "",
        ]
    )
    _record_render_timing(cache_result, time.perf_counter() - render_started)
    lines[timing_index : timing_index + 1] = _render_run_timing(cache_result)
    return lines


def _record_render_timing(cache_result: CacheResult, elapsed_seconds: float) -> None:
    cache_result.timings["report_render_seconds"] = elapsed_seconds
    cache_seconds = cache_result.timings.get("cache_build_seconds")
    if cache_seconds is None:
        cache_result.timings["total_report_seconds"] = elapsed_seconds
    else:
        cache_result.timings["total_report_seconds"] = cache_seconds + elapsed_seconds


def _render_run_timing(cache_result: CacheResult) -> list[str]:
    timings = cache_result.timings
    return [
        "",
        "## Run Timing",
        "",
        f"- Cache/build: {_format_seconds(timings.get('cache_build_seconds'))}",
        f"- Report render: {_format_seconds(timings.get('report_render_seconds'))}",
        f"- Total report run: {_format_seconds(timings.get('total_report_seconds'))}",
        "",
    ]


def _render_table_section(cache_dir: Path, title: str, table_name: str) -> list[str]:
    df = read_result_table(cache_dir, table_name)
    csv_path = cache_dir / f"{table_name}.csv"
    lines = [f"## {title}", ""]
    if csv_path.exists():
        lines.append(f"Cached CSV: `{csv_path}`")
        lines.append("")
    lines.extend(_markdown_table(df))
    lines.append("")
    return lines


def _markdown_table(df: pl.DataFrame) -> list[str]:
    if df.is_empty():
        return ["_No matching rows._"]
    columns = df.columns
    lines = [
        "| " + " | ".join(_escape_markdown(column) for column in columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in df.iter_rows(named=True):
        lines.append(
            "| "
            + " | ".join(_escape_markdown(_format_value(row[column])) for column in columns)
            + " |"
        )
    return lines


def _expected_manifest(
    settings: ReportSettings,
    db_metadata: dict[str, Any],
    *,
    base_only: bool,
) -> dict[str, Any]:
    return {
        "cache_version": CACHE_VERSION,
        "engine": "polars",
        "db_metadata": db_metadata,
        "settings": _settings_manifest(settings, base_only=base_only),
    }


def _settings_manifest(settings: ReportSettings, *, base_only: bool) -> dict[str, Any]:
    values: dict[str, Any] = {
        "quality_mode": settings.quality_mode,
        "bucket": settings.bucket,
        "timezone": settings.timezone,
        "exclude_stop_call_disagreement": settings.exclude_stop_call_disagreement,
    }
    if not base_only:
        values.update(
            {
                "limit": settings.limit,
                "min_observations": settings.min_observations,
                "rush_windows": list(settings.rush_windows),
                "include_weekends": settings.include_weekends,
                "gtfs_dir": str(settings.gtfs_dir) if settings.gtfs_dir else None,
                "gtfs_dir_metadata": gtfs_dir_fingerprint(settings.gtfs_dir),
                "gtfs_root": str(settings.gtfs_root),
                "gtfs_metadata": gtfs_metadata_fingerprint(settings.gtfs_root),
            }
        )
    return values


def _manifest_file_matches(manifest: dict[str, Any], file_metadata: dict[str, Any]) -> bool:
    db_metadata = manifest.get("db_metadata", {})
    return all(db_metadata.get(key) == value for key, value in file_metadata.items())


def _manifest_settings_match(
    manifest: dict[str, Any],
    settings: ReportSettings,
    *,
    base_only: bool,
) -> bool:
    expected = _settings_manifest(settings, base_only=base_only)
    current = manifest.get("settings", {})
    settings_match = (
        all(current.get(key) == value for key, value in expected.items())
        if base_only
        else current == expected
    )
    return (
        manifest.get("cache_version") == CACHE_VERSION
        and manifest.get("engine") == "polars"
        and settings_match
    )


def _has_tables(cache_dir: Path, table_names: list[str]) -> bool:
    return all((cache_dir / f"{table_name}.parquet").exists() for table_name in table_names)


def _read_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}


def _table_count(con: sqlite3.Connection, table_name: str) -> int:
    if not _table_exists_sqlite(con, table_name):
        return 0
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def _db_table_exists(db_path: Path, table_name: str) -> bool:
    db_path = resolve_project_path(db_path)
    with sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True) as con:
        return _table_exists_sqlite(con, table_name)


def _table_exists_sqlite(con: sqlite3.Connection, table_name: str) -> bool:
    return (
        con.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            [table_name],
        ).fetchone()
        is not None
    )


def _empty_collector_polls() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "source": pl.Utf8,
            "attempted_at_utc": pl.Utf8,
            "collected_at_utc": pl.Utf8,
            "status": pl.Utf8,
            "ok": pl.Int64,
            "row_count": pl.Int64,
            "gap_seconds_since_previous_success": pl.Float64,
        }
    )


def _empty_alerts() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "source_alert_id": pl.Utf8,
            "line_ref": pl.Utf8,
            "cause": pl.Utf8,
            "effect": pl.Utf8,
            "priority": pl.Int64,
            "is_active": pl.Int64,
            "validity_start_utc": pl.Utf8,
            "validity_end_utc": pl.Utf8,
            "affected_routes_json": pl.Utf8,
            "affected_stops_json": pl.Utf8,
            "created_at_utc": pl.Utf8,
        }
    )


def _alert_timestamp(value: object) -> datetime | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return parse_timestamp(value, "UTC")
    except ValueError:
        return None


def timedelta_microsecond():
    from datetime import timedelta

    return timedelta(microseconds=1)


def _clean_alert_value(value: object) -> str:
    if value is None or str(value).strip() == "":
        return "Unknown"
    return str(value)


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _format_int(value: object) -> str:
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "0"


def _format_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _format_seconds(value: object) -> str:
    if value is None:
        return "not recorded"
    try:
        return f"{float(value):.2f}s"
    except (TypeError, ValueError):
        return "not recorded"


def _escape_markdown(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")
