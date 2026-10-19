from __future__ import annotations

from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
import json
from pathlib import Path
import sqlite3
import tempfile
import time
from typing import Any

import numpy as np
import polars as pl
import polars.selectors as cs

try:
    from ._shared import (
        BUCKET_MODES,
        CONSERVATIVE_EXCLUSION_COLUMNS,
        DATA_FAULT_COLUMNS,
        DEFAULT_ANALYSIS_CACHE_DIR,
        DEFAULT_BUCKET_MODE,
        DEFAULT_DB_PATH,
        DEFAULT_GTFS_ROOT,
        DEFAULT_OFFPEAK_WINDOWS,
        DEFAULT_QUALITY_MODE,
        DEFAULT_RUSH_WINDOWS,
        DEFAULT_TIMEZONE,
        METRIC_COLUMNS,
        QUALITY_FLAG_COLUMNS,
        QUALITY_MODES,
        add_quality_pass,
        aggregate_delay_buckets_lazy,
        apply_no_realtime_trip_flag_lazy,
        no_realtime_trip_stats_lazy,
        assign_gtfs_feed_dates,
        base_quality_query,
        base_quality_query_without_collector,
        gtfs_dir_fingerprint,
        gtfs_metadata_fingerprint,
        parse_rush_windows,
        read_sql,
        resolve_project_path,
        round_numeric,
        rush_window_values,
        sort_robust_delay_metrics_lazy,
        summarize_delay_metrics,
        summarize_delay_metrics_lazy,
        time_window_expr,
    )
except ImportError:  # pragma: no cover - used when called as analysis/polars/*.py.
    from _shared import (
        BUCKET_MODES,
        CONSERVATIVE_EXCLUSION_COLUMNS,
        DATA_FAULT_COLUMNS,
        DEFAULT_ANALYSIS_CACHE_DIR,
        DEFAULT_BUCKET_MODE,
        DEFAULT_DB_PATH,
        DEFAULT_GTFS_ROOT,
        DEFAULT_OFFPEAK_WINDOWS,
        DEFAULT_QUALITY_MODE,
        DEFAULT_RUSH_WINDOWS,
        DEFAULT_TIMEZONE,
        METRIC_COLUMNS,
        QUALITY_FLAG_COLUMNS,
        QUALITY_MODES,
        add_quality_pass,
        aggregate_delay_buckets_lazy,
        apply_no_realtime_trip_flag_lazy,
        no_realtime_trip_stats_lazy,
        assign_gtfs_feed_dates,
        base_quality_query,
        base_quality_query_without_collector,
        gtfs_dir_fingerprint,
        gtfs_metadata_fingerprint,
        parse_rush_windows,
        read_sql,
        resolve_project_path,
        round_numeric,
        rush_window_values,
        sort_robust_delay_metrics_lazy,
        summarize_delay_metrics,
        summarize_delay_metrics_lazy,
        time_window_expr,
    )

try:
    from ..alert_analysis import (
        ALERT_GROUP_COLUMNS,
        ALERT_RESULT_COLUMNS,
        MATCH_CONTEXT_COLUMNS,
        build_alert_targets,
        finalize_alert_rows,
        make_route_resolver,
        summarize_alert_group,
        summarize_cancellations,
    )
    from ..collector_gaps import (
        blackout_rows,
        daily_coverage,
        missing_spots,
        missing_summary_rows,
        source_summaries,
    )
    from ..service_calendar import GtfsCalendar, cached_calendar, calendar_fingerprint
    from ..uncertainty import (
        DEFAULT_BOOTSTRAP_REPS,
        EARLY_RANKING_INTERVAL_COLUMNS,
        LATE_RANKING_INTERVAL_COLUMNS,
        line_ranking_intervals,
        rush_lift_interval,
    )
except ImportError:  # pragma: no cover - used when called as analysis/polars/*.py.
    import sys

    _ANALYSIS_DIR = str(Path(__file__).resolve().parents[1])
    if _ANALYSIS_DIR not in sys.path:
        sys.path.append(_ANALYSIS_DIR)
    from alert_analysis import (
        ALERT_GROUP_COLUMNS,
        ALERT_RESULT_COLUMNS,
        MATCH_CONTEXT_COLUMNS,
        build_alert_targets,
        finalize_alert_rows,
        make_route_resolver,
        summarize_alert_group,
        summarize_cancellations,
    )
    from collector_gaps import (
        blackout_rows,
        daily_coverage,
        missing_spots,
        missing_summary_rows,
        source_summaries,
    )
    from service_calendar import GtfsCalendar, cached_calendar, calendar_fingerprint
    from uncertainty import (
        DEFAULT_BOOTSTRAP_REPS,
        EARLY_RANKING_INTERVAL_COLUMNS,
        LATE_RANKING_INTERVAL_COLUMNS,
        line_ranking_intervals,
        rush_lift_interval,
    )


CACHE_VERSION = 2
DEFAULT_CACHE_DIR = DEFAULT_ANALYSIS_CACHE_DIR
DEFAULT_REPORT_PATH = Path("reports/generated/overall-results-polars.md")
MANIFEST_NAME = "manifest.json"
QUALITY_ROWS_NAME = "quality_rows"
DELAY_BUCKETS_NAME = "delay_buckets"
DELAY_CACHE_SUMMARY_NAME = "delay_cache_summary"
QUALITY_SOURCE_ID_BATCH_SIZE = 250_000
DELAY_BUCKET_PARTITIONS = 16
DEFAULT_MIN_TRIPS = 10
FULL_COVERAGE_PCT = 95.0
FULL_DAY_HOURS = 23.0
LOW_QUALITY_FAULT_PCT = 20.0
LOW_QUALITY_TRIP_COVERAGE_PCT = 80.0

RESULT_TABLES = [
    "quality_summary",
    "quality_by_line",
    "line_quality",
    "line_late_rankings",
    "line_early_rankings",
    "context_delay_metrics",
    "hourly_delay_profile",
    "rush_impact",
    "daily_network_profile",
    "service_alert_grouped",
    "service_alert_by_line",
    "service_alert_inventory",
    "service_cancellations",
    "collector_blackouts",
    "collector_missing_summary",
    "collector_missing_spots",
]

LATE_RANKING_COLUMNS = [
    "line_ref",
    "line_name",
    "bucket_count",
    "raw_poll_count",
    "trip_count",
    "day_count",
    "signed_mean_delay_min",
    "median_delay_min",
    "p75_delay_min",
    "p90_delay_min",
    "p90_delay_ci_lo",
    "p90_delay_ci_hi",
    "p95_delay_min",
    "pct_on_time",
    "pct_over_3_min_late",
    "pct_over_5_min_late",
    "pct_over_5_min_late_ci_lo",
    "pct_over_5_min_late_ci_hi",
]
EARLY_RANKING_COLUMNS = [
    "line_ref",
    "line_name",
    "bucket_count",
    "raw_poll_count",
    "trip_count",
    "day_count",
    "signed_mean_delay_min",
    "median_delay_min",
    "pct_on_time",
    "pct_early",
    "pct_over_1_min_early",
    "pct_over_3_min_early",
    "pct_over_3_min_early_ci_lo",
    "pct_over_3_min_early_ci_hi",
    "median_early_min_abs",
    "p90_early_min_abs",
]
RUSH_SIDE_COLUMNS = [
    "bucket_count",
    "raw_poll_count",
    "trip_count",
    "median_delay_min",
    "p90_delay_min",
    "pct_over_5_min_late",
]
RUSH_COLUMNS = [
    "line_ref",
    "line_name",
    "bucket_count_offpeak",
    "bucket_count_rush",
    "trip_count_offpeak",
    "trip_count_rush",
    "raw_poll_count_offpeak",
    "raw_poll_count_rush",
    "median_delay_min_offpeak",
    "median_delay_min_rush",
    "rush_median_delay_lift_min",
    "p90_delay_min_offpeak",
    "p90_delay_min_rush",
    "rush_p90_delay_lift_min",
    "pct_over_5_min_late_offpeak",
    "pct_over_5_min_late_rush",
    "rush_over_5_min_late_pct_point_lift",
    "rush_over_5_min_late_lift_ci_lo",
    "rush_over_5_min_late_lift_ci_hi",
]
DAILY_COLUMNS = [
    "local_date",
    "local_weekday",
    "day_type",
    "is_holiday",
    "bucket_count",
    "trip_count",
    "median_delay_min",
    "p90_delay_min",
    "pct_on_time",
    "pct_over_5_min_late",
    "pct_over_3_min_early",
    "collector_coverage_pct",
    "collector_missing_min",
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
    min_trips: int = DEFAULT_MIN_TRIPS
    exclude_stop_call_disagreement: bool = False
    rush_windows: tuple[str, ...] = DEFAULT_RUSH_WINDOWS
    offpeak_windows: tuple[str, ...] = DEFAULT_OFFPEAK_WINDOWS
    include_weekends: bool = False
    gtfs_dir: Path | None = None
    gtfs_root: Path = DEFAULT_GTFS_ROOT
    bootstrap_reps: int = DEFAULT_BOOTSTRAP_REPS

    def resolved(self) -> ReportSettings:
        return ReportSettings(
            db=resolve_project_path(self.db),
            cache_dir=resolve_project_path(self.cache_dir),
            quality_mode=self.quality_mode,
            bucket=self.bucket,
            timezone=self.timezone,
            limit=self.limit,
            min_observations=self.min_observations,
            min_trips=self.min_trips,
            exclude_stop_call_disagreement=self.exclude_stop_call_disagreement,
            rush_windows=tuple(self.rush_windows),
            offpeak_windows=tuple(self.offpeak_windows),
            include_weekends=self.include_weekends,
            gtfs_dir=resolve_project_path(self.gtfs_dir) if self.gtfs_dir else None,
            gtfs_root=resolve_project_path(self.gtfs_root),
            bootstrap_reps=self.bootstrap_reps,
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
        if self.min_trips < 1:
            raise ValueError("min_trips must be at least 1")
        parse_rush_windows(tuple(self.rush_windows))
        parse_rush_windows(tuple(self.offpeak_windows))

    @property
    def effective_min_trips(self) -> int:
        """Line-hour buckets aggregate many trips and carry no trip key to count."""
        return 1 if self.bucket == "line-hour" else self.min_trips

    def gtfs_calendar(self) -> GtfsCalendar:
        return cached_calendar(gtfs_root=self.gtfs_root, gtfs_dir=self.gtfs_dir)

    def holiday_dates(self) -> tuple[str, ...]:
        return tuple(day.isoformat() for day in self.gtfs_calendar().holiday_dates())


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
        min_trips=getattr(args, "min_trips", None) or DEFAULT_MIN_TRIPS,
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
    _build_result_tables(settings, progress=progress)

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
    with closing(sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)) as con:
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
    """Read the source in id batches, then add trip-level flags in one streaming pass."""
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
        where = """
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
            trip_flags=False,
        )
        fragment_path = fragments_dir / f"part-{part:05d}.parquet"
        quality_rows.write_parquet(fragment_path)
        fragments.append(fragment_path)
        part += 1

    if not fragments:
        _build_empty_quality_rows(settings).write_parquet(output_path)
        return
    # Trip-level flags need every row of a trip, which can span id batches. The per-trip
    # summary is small, so it is computed once and joined onto each fragment separately.
    trip_stats = no_realtime_trip_stats_lazy(
        pl.scan_parquet(fragments).select(
            "trip_match_key", "vehicle_id", "delay_seconds", "next_stop_point_ref"
        )
    ).collect(engine="streaming")
    flagged_fragments = []
    for fragment in fragments:
        flagged_path = fragment.with_name(f"flagged-{fragment.name}")
        _write_lazy_parquet(
            apply_no_realtime_trip_flag_lazy(
                pl.scan_parquet(fragment),
                quality_mode=settings.quality_mode,
                exclude_stop_call_disagreement=settings.exclude_stop_call_disagreement,
                trip_stats=trip_stats.lazy(),
            ),
            flagged_path,
        )
        fragment.unlink()
        flagged_fragments.append(flagged_path)
    _write_lazy_parquet(pl.scan_parquet(flagged_fragments), output_path)


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
    holidays = settings.holiday_dates()

    if settings.bucket == "poll":
        _write_lazy_parquet(
            aggregate_delay_buckets_lazy(
                quality_rows,
                bucket=settings.bucket,
                timezone=settings.timezone,
                holidays=holidays,
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
                holidays=holidays,
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
    return build_delay_cache_summary_lazy(delay_buckets.lazy()).collect()


def build_delay_cache_summary_lazy(delay_buckets: pl.LazyFrame) -> pl.LazyFrame:
    return delay_buckets.select(
        pl.len().alias("bucket_count"),
        pl.col("raw_poll_count").sum().alias("raw_poll_count"),
        pl.col("line_ref").n_unique().alias("line_count"),
        pl.col("representative_time_utc").min().alias("observation_start_utc"),
        pl.col("representative_time_utc").max().alias("observation_end_utc"),
    )


# --- Quality tables -----------------------------------------------------------------


def _conservative_default_expr() -> pl.Expr:
    return pl.any_horizontal([pl.col(column) for column in CONSERVATIVE_EXCLUSION_COLUMNS])


def build_quality_summary(quality_rows: pl.DataFrame) -> pl.DataFrame:
    if quality_rows.is_empty():
        return pl.DataFrame({"quality_check": [], "row_count": [], "pct_rows": []})
    return round_numeric(build_quality_summary_lazy(quality_rows.lazy()).collect())


def build_quality_summary_lazy(quality_rows: pl.LazyFrame) -> pl.LazyFrame:
    conservative_default = _conservative_default_expr()
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
            pl.col("row_count")
            .filter(pl.col("quality_check") == "analysis_rows")
            .first()
            .alias("_analysis_row_count")
        )
        .filter(pl.col("_analysis_row_count") > 0)
        .with_columns(
            pl.col("row_count").cast(pl.Int64),
            (
                pl.col("row_count").cast(pl.Float64)
                / pl.col("_analysis_row_count").cast(pl.Float64)
                * 100.0
            ).alias("pct_rows"),
        )
        .drop("_analysis_row_count")
    )


def _quality_by_line_lazy(quality_rows: pl.LazyFrame) -> pl.LazyFrame:
    return (
        quality_rows.with_columns(_conservative_default_expr().alias("conservative_excluded_default"))
        .group_by("line_ref")
        .agg(
            pl.len().alias("row_count"),
            pl.col("published_line_name").sort_by("recorded_at_utc").first().alias("line_name"),
            pl.col("is_implausible_delay").sum().alias("implausible_delay_rows"),
            pl.col("is_stale_observation").sum().alias("stale_rows"),
            pl.col("is_pre_trip_observation").sum().alias("pre_trip_rows"),
            pl.col("is_post_trip_observation").sum().alias("post_trip_rows"),
            pl.col("is_origin_layover").sum().alias("origin_layover_rows"),
            pl.col("is_no_realtime_trip").sum().alias("no_realtime_trip_rows"),
            pl.col("has_stop_call_disagreement").sum().alias("stop_call_disagreement_rows"),
            pl.col("conservative_excluded_default").sum().alias("conservative_excluded_rows"),
            pl.any_horizontal([pl.col(column) for column in DATA_FAULT_COLUMNS])
            .sum()
            .alias("data_fault_rows"),
        )
        .with_columns(
            (pl.col("conservative_excluded_rows") / pl.col("row_count") * 100.0).alias(
                "conservative_excluded_pct"
            ),
            (pl.col("data_fault_rows") / pl.col("row_count") * 100.0).alias("data_fault_pct"),
        )
    )


def build_quality_by_line(
    quality_rows: pl.DataFrame,
    *,
    min_observations: int,
    limit: int,
) -> pl.DataFrame:
    if quality_rows.is_empty():
        return pl.DataFrame()
    return build_quality_by_line_lazy(
        quality_rows.lazy(),
        min_observations=min_observations,
        limit=limit,
    ).collect()


def build_quality_by_line_lazy(
    quality_rows: pl.LazyFrame,
    *,
    min_observations: int,
    limit: int,
) -> pl.LazyFrame:
    return round_numeric_lazy(
        _quality_by_line_lazy(quality_rows)
        .filter(pl.col("row_count") >= min_observations)
        .sort(
            ["conservative_excluded_pct", "conservative_excluded_rows", "line_ref"],
            descending=[True, True, False],
        )
        .head(limit)
    )


def build_line_quality(
    settings: ReportSettings,
    quality_rows: pl.LazyFrame,
    coverage: pl.DataFrame,
) -> pl.DataFrame:
    """Per-line data-fault share and observed-vs-scheduled trip coverage for every line.

    A line is low quality when more than LOW_QUALITY_FAULT_PCT of its rows are stale,
    implausible, post-trip or from trips without realtime data, or when fewer than
    LOW_QUALITY_TRIP_COVERAGE_PCT of its scheduled trips appear in the data. Rows of a
    bus waiting before departure are excluded from metrics but are not data faults.

    Trip coverage only uses full local days whose collector coverage was at least
    FULL_COVERAGE_PCT, so collector gaps are not mistaken for missing vehicles.
    """
    exclusions = _quality_by_line_lazy(quality_rows).select(
        "line_ref",
        "line_name",
        "row_count",
        "conservative_excluded_rows",
        "conservative_excluded_pct",
        "data_fault_rows",
        "data_fault_pct",
    ).collect(engine="streaming")
    full_days = (
        coverage.filter(
            (pl.col("coverage_pct") >= FULL_COVERAGE_PCT)
            & (pl.col("observed_hours") >= FULL_DAY_HOURS)
        )["local_date"].to_list()
        if not coverage.is_empty()
        else []
    )
    observed = (
        quality_rows.select(
            "line_ref",
            "trip_match_key",
            pl.col("origin_aimed_departure_time_utc")
            .dt.convert_time_zone(settings.timezone)
            .dt.date()
            .alias("service_date"),
        )
        .filter(pl.col("service_date").is_in(pl.lit(pl.Series(full_days, dtype=pl.Date)).implode()))
        .group_by("line_ref")
        .agg(pl.col("trip_match_key").n_unique().alias("observed_trip_count"))
        .collect(engine="streaming")
    )
    calendar = settings.gtfs_calendar()
    scheduled: dict[str, int] = {}
    for day in full_days:
        for line, count in calendar.scheduled_trips(day).items():
            scheduled[line] = scheduled.get(line, 0) + count
    scheduled_df = pl.DataFrame(
        {"line_ref": list(scheduled), "scheduled_trip_count": list(scheduled.values())},
        schema={"line_ref": pl.Utf8, "scheduled_trip_count": pl.Int64},
    )
    result = (
        exclusions.join(observed, on="line_ref", how="left")
        .join(scheduled_df, on="line_ref", how="left")
        .with_columns(
            pl.lit(len(full_days)).alias("coverage_day_count"),
            pl.when(pl.col("scheduled_trip_count") > 0)
            .then(pl.col("observed_trip_count").fill_null(0) / pl.col("scheduled_trip_count") * 100.0)
            .otherwise(None)
            .alias("trip_coverage_pct"),
        )
        .with_columns(
            (
                (pl.col("data_fault_pct") > LOW_QUALITY_FAULT_PCT)
                | (pl.col("trip_coverage_pct") < LOW_QUALITY_TRIP_COVERAGE_PCT).fill_null(False)
            ).alias("low_quality")
        )
        .sort("line_ref")
    )
    return round_numeric(result)


# --- Delay metric tables ------------------------------------------------------------


def _line_metrics_lazy(
    delay_buckets: pl.LazyFrame,
    *,
    min_observations: int,
    min_trips: int,
) -> pl.LazyFrame:
    return summarize_delay_metrics_lazy(
        delay_buckets,
        ["line_ref"],
        min_observations=min_observations,
        min_trips=min_trips,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )


def _trip_rank_codes(trips: pl.Series) -> np.ndarray:
    """Lexicographic dense rank of trip keys; the DuckDB path uses the same ordering."""
    return trips.cast(pl.Utf8).rank("dense").cast(pl.Int64).to_numpy()


def _line_partitions(
    delay_buckets: pl.LazyFrame,
    line_refs: list[str],
    columns: list[str],
) -> dict[str, pl.DataFrame]:
    if not line_refs:
        return {}
    data = (
        delay_buckets.filter(pl.col("line_ref").is_in(pl.lit(pl.Series(line_refs, dtype=pl.Utf8)).implode()))
        .select(["line_ref", *columns])
        .collect()
    )
    return {
        str(key[0]): frame
        for key, frame in data.partition_by("line_ref", as_dict=True, maintain_order=True).items()
    }


def add_line_ranking_intervals(
    table: pl.DataFrame,
    delay_buckets: pl.LazyFrame,
    ranking: str,
    *,
    reps: int = DEFAULT_BOOTSTRAP_REPS,
) -> pl.DataFrame:
    """Trip-cluster bootstrap intervals for the ranked lines only (bounded memory)."""
    if table.is_empty():
        return table
    names = EARLY_RANKING_INTERVAL_COLUMNS if ranking == "early" else LATE_RANKING_INTERVAL_COLUMNS
    partitions = _line_partitions(
        delay_buckets,
        table["line_ref"].to_list(),
        ["trip_match_key", "delay_seconds"],
    )
    rows = []
    for line_ref in table["line_ref"].to_list():
        frame = partitions.get(str(line_ref))
        if frame is None or frame.is_empty():
            rows.append({"line_ref": line_ref, **dict.fromkeys(names)})
            continue
        rows.append(
            {
                "line_ref": line_ref,
                **line_ranking_intervals(
                    ranking,
                    line_ref,
                    _trip_rank_codes(frame["trip_match_key"]),
                    frame["delay_seconds"].to_numpy(),
                    reps=reps,
                ),
            }
        )
    intervals = pl.DataFrame(rows, schema={"line_ref": pl.Utf8, **dict.fromkeys(names, pl.Float64)})
    return table.join(intervals, on="line_ref", how="left", maintain_order="left")


def build_line_rankings_lazy(
    delay_buckets: pl.LazyFrame,
    ranking: str,
    *,
    min_observations: int,
    limit: int,
    min_trips: int = 1,
) -> pl.LazyFrame:
    metrics = _line_metrics_lazy(delay_buckets, min_observations=min_observations, min_trips=min_trips)
    if ranking == "early":
        return metrics.sort(
            ["pct_over_3_min_early", "p90_early_min_abs", "bucket_count", "line_ref"],
            descending=[True, True, True, False],
        ).head(limit)
    return sort_robust_delay_metrics_lazy(metrics, limit=limit)


def build_line_rankings(
    delay_buckets: pl.DataFrame | pl.LazyFrame,
    ranking: str,
    *,
    min_observations: int,
    limit: int,
    min_trips: int = 1,
    reps: int = DEFAULT_BOOTSTRAP_REPS,
) -> pl.DataFrame:
    lazy = delay_buckets.lazy() if isinstance(delay_buckets, pl.DataFrame) else delay_buckets
    table = build_line_rankings_lazy(
        lazy,
        ranking,
        min_observations=min_observations,
        limit=limit,
        min_trips=min_trips,
    ).collect()
    table = add_line_ranking_intervals(table, lazy, ranking, reps=reps)
    columns = EARLY_RANKING_COLUMNS if ranking == "early" else LATE_RANKING_COLUMNS
    return round_numeric(table.select([column for column in columns if column in table.columns]))


def build_context_delay_metrics_lazy(
    delay_buckets: pl.LazyFrame,
    *,
    line_ref: str | None = None,
    direction_ref: str | None = None,
    day_type: str = "all",
    min_observations: int,
    limit: int,
    min_trips: int = 1,
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
        min_trips=min_trips,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )
    return round_numeric_lazy(
        sort_robust_delay_metrics_lazy(metrics, limit=limit)
        .with_columns(
            (pl.col("local_hour").cast(pl.Utf8).str.zfill(2) + pl.lit(":00")).alias("hour_local")
        )
        .select(["line_ref", "line_name", "direction_ref", "hour_local", "day_type", *METRIC_COLUMNS])
    )


def build_context_delay_metrics(
    delay_buckets: pl.DataFrame,
    *,
    line_ref: str | None = None,
    direction_ref: str | None = None,
    day_type: str = "all",
    min_observations: int,
    limit: int,
    min_trips: int = 1,
) -> pl.DataFrame:
    if delay_buckets.is_empty():
        return pl.DataFrame()
    return build_context_delay_metrics_lazy(
        delay_buckets.lazy(),
        line_ref=line_ref,
        direction_ref=direction_ref,
        day_type=day_type,
        min_observations=min_observations,
        limit=limit,
        min_trips=min_trips,
    ).collect()


def build_hourly_delay_profile_lazy(
    delay_buckets: pl.LazyFrame,
    *,
    line_ref: str | None = None,
    min_observations: int,
    limit: int | None = None,
    min_trips: int = 1,
    by_day_type: bool = False,
) -> pl.LazyFrame:
    """Hourly metrics in hour order (the old version kept the 20 worst hours only)."""
    buckets = delay_buckets.filter(pl.col("line_ref") == line_ref) if line_ref else delay_buckets
    keys = ["day_type", "local_hour"] if by_day_type else ["local_hour"]
    metrics = summarize_delay_metrics_lazy(
        buckets,
        keys,
        min_observations=min_observations,
        min_trips=min_trips,
    ).sort(keys)
    if limit is not None:
        metrics = metrics.head(limit)
    return round_numeric_lazy(
        metrics.with_columns(
            (pl.col("local_hour").cast(pl.Utf8).str.zfill(2) + pl.lit(":00")).alias("hour_local")
        ).select([*(["day_type"] if by_day_type else []), "hour_local", *METRIC_COLUMNS])
    )


def build_hourly_delay_profile(
    delay_buckets: pl.DataFrame,
    *,
    line_ref: str | None = None,
    min_observations: int,
    limit: int | None = None,
    min_trips: int = 1,
    by_day_type: bool = False,
) -> pl.DataFrame:
    if delay_buckets.is_empty():
        return pl.DataFrame()
    return build_hourly_delay_profile_lazy(
        delay_buckets.lazy(),
        line_ref=line_ref,
        min_observations=min_observations,
        limit=limit,
        min_trips=min_trips,
        by_day_type=by_day_type,
    ).collect()


def build_rush_impact(
    delay_buckets: pl.DataFrame | pl.LazyFrame,
    *,
    rush_windows: tuple[str, ...],
    include_weekends: bool,
    min_observations: int,
    limit: int,
    offpeak_windows: tuple[str, ...] = DEFAULT_OFFPEAK_WINDOWS,
    min_trips: int = 1,
    reps: int = DEFAULT_BOOTSTRAP_REPS,
) -> pl.DataFrame:
    """Weekday rush windows versus weekday off-peak daytime on the same line.

    The comparison window excludes nights, weekends and reduced-service holidays, and
    lines are ranked by the lift in the share of buckets more than five minutes late so a
    line that merely runs less early during the rush does not rank as rush-affected.
    """
    lazy = delay_buckets.lazy() if isinstance(delay_buckets, pl.DataFrame) else delay_buckets
    marked = _mark_rush_offpeak(lazy, rush_windows, offpeak_windows, include_weekends)
    grouped = summarize_delay_metrics_lazy(
        marked,
        ["line_ref", "is_rush"],
        min_observations=min_observations,
        min_trips=min_trips,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )
    sides = {
        name: grouped.filter(pl.col("is_rush") == flag)
        .select(["line_ref", "line_name", *RUSH_SIDE_COLUMNS])
        .rename({column: f"{column}_{name}" for column in ["line_name", *RUSH_SIDE_COLUMNS]})
        for name, flag in (("offpeak", False), ("rush", True))
    }
    table = (
        sides["offpeak"]
        .join(sides["rush"], on="line_ref", how="inner")
        .with_columns(
            pl.coalesce("line_name_rush", "line_name_offpeak").alias("line_name"),
            (pl.col("median_delay_min_rush") - pl.col("median_delay_min_offpeak")).alias(
                "rush_median_delay_lift_min"
            ),
            (pl.col("p90_delay_min_rush") - pl.col("p90_delay_min_offpeak")).alias(
                "rush_p90_delay_lift_min"
            ),
            (pl.col("pct_over_5_min_late_rush") - pl.col("pct_over_5_min_late_offpeak")).alias(
                "rush_over_5_min_late_pct_point_lift"
            ),
        )
        .sort(
            [
                "rush_over_5_min_late_pct_point_lift",
                "rush_p90_delay_lift_min",
                "bucket_count_rush",
                "line_ref",
            ],
            descending=[True, True, True, False],
        )
        .head(limit)
        .collect()
    )
    if table.is_empty():
        return table
    partitions = _line_partitions(
        marked,
        table["line_ref"].to_list(),
        ["trip_match_key", "delay_seconds", "is_rush"],
    )
    rows = []
    for line_ref in table["line_ref"].to_list():
        frame = partitions.get(str(line_ref))
        if frame is None:
            rows.append(rush_lift_interval(line_ref, [], [], [], reps=reps))
            continue
        rows.append(
            rush_lift_interval(
                line_ref,
                _trip_rank_codes(frame["trip_match_key"]),
                frame["delay_seconds"].to_numpy(),
                frame["is_rush"].to_numpy(),
                reps=reps,
            )
        )
    table = table.with_columns(
        pl.Series(
            "rush_over_5_min_late_lift_ci_lo",
            [row["rush_over_5_min_late_lift_ci_lo"] for row in rows],
            dtype=pl.Float64,
        ),
        pl.Series(
            "rush_over_5_min_late_lift_ci_hi",
            [row["rush_over_5_min_late_lift_ci_hi"] for row in rows],
            dtype=pl.Float64,
        ),
    )
    return round_numeric(table.select(RUSH_COLUMNS))


def _mark_rush_offpeak(
    buckets: pl.LazyFrame,
    rush_windows: tuple[str, ...],
    offpeak_windows: tuple[str, ...],
    include_weekends: bool,
) -> pl.LazyFrame:
    rush = time_window_expr(parse_rush_windows(rush_windows), include_weekends=include_weekends)
    offpeak = time_window_expr(parse_rush_windows(offpeak_windows), include_weekends=include_weekends)
    return (
        buckets.with_columns(rush.alias("is_rush"), (offpeak & ~rush).alias("_is_offpeak"))
        .filter(pl.col("is_rush") | pl.col("_is_offpeak"))
        .drop("_is_offpeak")
    )


def build_daily_network_profile(
    delay_buckets: pl.LazyFrame,
    coverage: pl.DataFrame,
) -> pl.DataFrame:
    """One row per local date: delay metrics next to collector coverage for that day."""
    daily = summarize_delay_metrics_lazy(
        delay_buckets,
        ["local_date"],
        extra_aggs={
            "local_weekday": ("local_weekday", "first"),
            "day_type": ("day_type", "first"),
            "is_holiday": ("is_holiday", "first"),
        },
    ).collect()
    if coverage.is_empty():
        daily = daily.with_columns(
            pl.lit(None, dtype=pl.Float64).alias("collector_coverage_pct"),
            pl.lit(None, dtype=pl.Float64).alias("collector_missing_min"),
        )
    else:
        daily = daily.join(
            coverage.select(
                "local_date",
                pl.col("coverage_pct").alias("collector_coverage_pct"),
                pl.col("missing_min").alias("collector_missing_min"),
            ),
            on="local_date",
            how="left",
        )
    return round_numeric(daily.sort("local_date").select(DAILY_COLUMNS))


# --- Collector tables ---------------------------------------------------------------


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


def _poll_dicts(polls: pl.DataFrame) -> list[dict[str, object]]:
    return [] if polls.is_empty() else polls.to_dicts()


def build_collector_blackouts(polls: pl.DataFrame, limit: int) -> pl.DataFrame:
    rows = blackout_rows(source_summaries(_poll_dicts(polls)))
    return round_numeric(pl.DataFrame(rows).head(limit)) if rows else pl.DataFrame()


def build_missing_spots(
    polls: pl.DataFrame,
    *,
    gap_multiplier: float = 2.0,
    min_missing_minutes: float = 0.0,
) -> pl.DataFrame:
    rows = missing_spots(
        _poll_dicts(polls),
        gap_multiplier=gap_multiplier,
        min_missing_minutes=min_missing_minutes,
    )
    return round_numeric(pl.DataFrame(rows)) if rows else pl.DataFrame()


def summarize_missing_spots(spots: pl.DataFrame, polls: pl.DataFrame) -> pl.DataFrame:
    del spots  # Summaries are recomputed from the polls with the shared gap definition.
    rows = missing_summary_rows(source_summaries(_poll_dicts(polls)))
    return round_numeric(pl.DataFrame(rows)) if rows else pl.DataFrame()


def build_daily_coverage(settings: ReportSettings, polls: pl.DataFrame) -> pl.DataFrame:
    rows = daily_coverage(_poll_dicts(polls), timezone=settings.timezone)
    if not rows:
        return pl.DataFrame(
            schema={
                "local_date": pl.Date,
                "observed_hours": pl.Float64,
                "missing_min": pl.Float64,
                "coverage_pct": pl.Float64,
                "success_count": pl.Int64,
            }
        )
    return pl.DataFrame(rows)


# --- Service alerts -----------------------------------------------------------------

ALERT_ROW_COLUMNS = (
    "alert_type",
    "source_alert_id",
    "line_ref",
    "cause",
    "effect",
    "priority",
    "is_active",
    "departure_time_utc",
    "validity_start_utc",
    "validity_end_utc",
    "repeat_json",
    "affected_routes_json",
    "affected_stops_json",
    "created_at_utc",
    "header",
)


def load_alert_rows(settings: ReportSettings) -> list[dict[str, object]]:
    """All alert snapshots as plain dicts (about 100k small rows)."""
    if not _db_table_exists(settings.db, "service_alerts"):
        return []
    with closing(sqlite3.connect(f"file:{settings.db.as_posix()}?mode=ro", uri=True)) as con:
        con.row_factory = sqlite3.Row
        columns = {row[1] for row in con.execute("PRAGMA table_info(service_alerts)")}
        select = ", ".join(
            column if column in columns else f"NULL AS {column}" for column in ALERT_ROW_COLUMNS
        )
        return [dict(row) for row in con.execute(f"SELECT {select} FROM service_alerts")]


def build_alert_results_lazy(
    settings: ReportSettings,
    observations: pl.LazyFrame,
    *,
    alert_kind: str = "any",
) -> dict[str, pl.DataFrame]:
    """Stratified alert-versus-control comparison plus inventory and cancellation tables."""
    empty = {
        "service_alert_grouped": pl.DataFrame(),
        "service_alert_by_line": pl.DataFrame(),
        "service_alert_inventory": pl.DataFrame(),
        "service_cancellations": pl.DataFrame(),
    }
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
        return empty

    targets, inventory, cancellations = build_alert_targets(
        load_alert_rows(settings),
        make_route_resolver(settings.gtfs_calendar(), settings.timezone),
        start,
        end,
    )
    result = dict(empty)
    if inventory:
        result["service_alert_inventory"] = round_numeric(
            pl.DataFrame(inventory).with_columns(
                pl.col("first_active_utc").dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                pl.col("last_active_utc").dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
        )
    if cancellations:
        result["service_cancellations"] = pl.DataFrame(summarize_cancellations(cancellations))
    targets = [
        row
        for row in targets
        if alert_kind == "any" or row["alert_scope"] == alert_kind
    ]
    if not targets:
        return result

    target_rows = pl.DataFrame(
        targets,
        schema={
            "alert_key": pl.Utf8,
            "cause": pl.Utf8,
            "effect": pl.Utf8,
            "priority": pl.Int64,
            "alert_scope": pl.Utf8,
            "target_ref": pl.Utf8,
            "start_utc": pl.Datetime("us", "UTC"),
            "end_utc": pl.Datetime("us", "UTC"),
        },
    )
    active = _active_alert_buckets(target_rows, observations)
    if active.is_empty():
        return result

    any_active = active.select("bucket_id").unique()
    # Narrow projection keeps per-group frames small: integer stratum and trip codes,
    # with bucket ids and line names only on alert rows (controls never need them).
    analysis_frame = observations.select(
        "bucket_id",
        *MATCH_CONTEXT_COLUMNS,
        pl.struct(MATCH_CONTEXT_COLUMNS).hash(seed=0).alias("stratum"),
        "trip_match_key",
        "delay_seconds",
        "published_line_name",
    )
    output_columns = [
        "bucket_id",
        "stratum",
        "line_ref",
        "trip_match_key",
        "delay_seconds",
        "published_line_name",
        "is_active",
    ]
    grouped_rows: list[dict[str, object]] = []
    line_rows: list[dict[str, object]] = []
    for group_values, group_active in active.partition_by(
        ALERT_GROUP_COLUMNS,
        as_dict=True,
        maintain_order=True,
    ).items():
        group = dict(zip(ALERT_GROUP_COLUMNS, group_values, strict=True))
        active_ids = group_active.select("bucket_id").unique().lazy()
        alert_buckets = analysis_frame.join(active_ids, on="bucket_id", how="semi")
        strata = alert_buckets.select("stratum").unique()
        controls = (
            analysis_frame.join(strata, on="stratum", how="semi")
            .join(any_active.lazy(), on="bucket_id", how="anti")
            .with_columns(
                pl.lit(None, dtype=pl.Utf8).alias("bucket_id"),
                pl.lit(None, dtype=pl.Utf8).alias("published_line_name"),
            )
        )
        combined = (
            pl.concat(
                [
                    alert_buckets.with_columns(pl.lit(True).alias("is_active")),
                    controls.with_columns(pl.lit(False).alias("is_active")),
                ],
                how="vertical_relaxed",
            )
            .with_columns(pl.col("trip_match_key").rank("dense").cast(pl.Int64))
            .select(output_columns)
            .collect()
        )
        grouped_row, group_line_rows = summarize_alert_group(
            group,
            {name: combined[name].to_numpy() for name in combined.columns},
            group_active.select("alert_key", "bucket_id").iter_rows(),
            min_observations=settings.min_observations,
            min_trips=settings.effective_min_trips,
            reps=settings.bootstrap_reps,
        )
        del combined
        if grouped_row is not None:
            grouped_rows.append(grouped_row)
        line_rows.extend(group_line_rows)
    result["service_alert_grouped"] = _alert_frame(grouped_rows, settings.limit, include_line=False)
    result["service_alert_by_line"] = _alert_frame(line_rows, settings.limit, include_line=True)
    return result


def _active_alert_buckets(targets: pl.DataFrame, observations: pl.LazyFrame) -> pl.DataFrame:
    """Alert-active buckets, one alert interval at a time.

    Filtering each interval's time window before joining its route/stop targets keeps the
    join small; joining all targets first would pair network-wide stop notices with every
    bucket at every stop.
    """
    columns = [*ALERT_GROUP_COLUMNS, "alert_key", "bucket_id"]
    frames = []
    interval_keys = ["alert_key", "alert_scope", "start_utc", "end_utc"]
    for (alert_key, scope, start, end), interval in targets.partition_by(
        interval_keys,
        as_dict=True,
        maintain_order=True,
    ).items():
        key = "line_ref" if scope == "route" else "next_stop_point_ref"
        refs = interval["target_ref"].unique()
        group = interval.row(0, named=True)
        found = (
            observations.filter(
                (pl.col("representative_time_utc") >= start)
                & (pl.col("representative_time_utc") <= end)
                & pl.col(key).is_in(pl.lit(refs).implode())
            )
            .select("bucket_id")
            .collect()
        )
        if found.is_empty():
            continue
        frames.append(
            found.with_columns(
                pl.lit(group["cause"]).alias("cause"),
                pl.lit(group["effect"]).alias("effect"),
                pl.lit(group["priority"], dtype=pl.Int64).alias("priority"),
                pl.lit(scope).alias("alert_scope"),
                pl.lit(alert_key).alias("alert_key"),
            ).select(columns)
        )
    if not frames:
        return pl.DataFrame(schema={column: pl.Utf8 for column in columns} | {"priority": pl.Int64})
    return pl.concat(frames, how="vertical_relaxed").unique(maintain_order=True)


def _alert_frame(rows: list[dict[str, object]], limit: int, *, include_line: bool) -> pl.DataFrame:
    finalized = finalize_alert_rows(rows, limit, include_line=include_line)
    return round_numeric(pl.DataFrame(finalized, infer_schema_length=None)) if finalized else pl.DataFrame()


def build_alert_results(
    settings: ReportSettings,
    observations: pl.DataFrame,
    *,
    alert_kind: str = "any",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if observations.is_empty():
        return pl.DataFrame(), pl.DataFrame()
    tables = build_alert_results_lazy(settings.resolved(), observations.lazy(), alert_kind=alert_kind)
    return tables["service_alert_grouped"], tables["service_alert_by_line"]


# --- Explicit-period stop change helpers (used by stop-delay-change.py) -------------


def round_numeric_lazy(df: pl.LazyFrame, digits: int = 2) -> pl.LazyFrame:
    return df.with_columns(cs.numeric().round(digits))


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


# --- Result cache build -------------------------------------------------------------


def _build_result_tables(
    settings: ReportSettings,
    *,
    progress: Callable[[str], None] | None = None,
) -> None:
    quality_rows = _scan_table(settings.cache_dir, QUALITY_ROWS_NAME)
    delay_buckets = _scan_table(settings.cache_dir, DELAY_BUCKETS_NAME)
    polls = load_collector_polls(settings)
    coverage = build_daily_coverage(settings, polls)

    with tempfile.TemporaryDirectory(
        dir=settings.cache_dir,
        prefix=".polars-report-results-",
    ) as temp_name:
        temp_dir = Path(temp_name)

        def write(table_name: str, table: pl.DataFrame | pl.LazyFrame) -> None:
            _progress(progress, f"Writing {table_name}")
            _write_result_table(settings.cache_dir, table_name, table, temp_dir)

        write("quality_summary", round_numeric_lazy(build_quality_summary_lazy(quality_rows)))
        write(
            "quality_by_line",
            build_quality_by_line_lazy(
                quality_rows,
                min_observations=settings.min_observations,
                limit=settings.limit,
            ),
        )
        write("line_quality", build_line_quality(settings, quality_rows, coverage))
        for ranking in ("late", "early"):
            write(
                f"line_{ranking}_rankings",
                build_line_rankings(
                    delay_buckets,
                    ranking,
                    min_observations=settings.min_observations,
                    min_trips=settings.effective_min_trips,
                    limit=settings.limit,
                    reps=settings.bootstrap_reps,
                ),
            )
        write(
            "context_delay_metrics",
            build_context_delay_metrics_lazy(
                delay_buckets,
                min_observations=settings.min_observations,
                min_trips=settings.effective_min_trips,
                limit=settings.limit,
            ),
        )
        write(
            "hourly_delay_profile",
            build_hourly_delay_profile_lazy(
                delay_buckets,
                min_observations=settings.min_observations,
                min_trips=settings.effective_min_trips,
                by_day_type=True,
            ),
        )
        write(
            "rush_impact",
            build_rush_impact(
                delay_buckets,
                rush_windows=settings.rush_windows,
                offpeak_windows=settings.offpeak_windows,
                include_weekends=settings.include_weekends,
                min_observations=settings.min_observations,
                min_trips=settings.effective_min_trips,
                limit=settings.limit,
                reps=settings.bootstrap_reps,
            ),
        )
        write("daily_network_profile", build_daily_network_profile(delay_buckets, coverage))
        write("collector_blackouts", build_collector_blackouts(polls, settings.limit))
        write("collector_missing_summary", summarize_missing_spots(pl.DataFrame(), polls))
        spots = build_missing_spots(polls)
        write("collector_missing_spots", spots.head(settings.limit) if not spots.is_empty() else spots)
        _progress(progress, "Matching service alerts")
        for table_name, table in build_alert_results_lazy(settings, delay_buckets).items():
            write(table_name, table)


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


# --- Markdown report ----------------------------------------------------------------

REPORT_SECTIONS = [
    ("Data Quality Summary", "quality_summary"),
    ("Worst Data Quality By Line", "quality_by_line"),
    ("Most Late Lines", "line_late_rankings"),
    ("Most Early Lines", "line_early_rankings"),
    ("Context Delay Metrics", "context_delay_metrics"),
    ("Hourly Delay Profile", "hourly_delay_profile"),
    ("Rush-Time Impact (Weekday Rush vs Weekday Off-Peak)", "rush_impact"),
    ("Daily Network Profile", "daily_network_profile"),
    ("Service Alert Matched-Control Groups", "service_alert_grouped"),
    ("Service Alert Matched-Control By Line", "service_alert_by_line"),
    ("Service Alert Inventory", "service_alert_inventory"),
    ("Cancelled Departures", "service_cancellations"),
    ("Collector Blackouts", "collector_blackouts"),
    ("Collector Missing-Data Summary", "collector_missing_summary"),
    ("Collector Missing-Data Spots", "collector_missing_spots"),
]

REPORT_CAVEATS = [
    "SIRI VM delay is estimated vehicle-monitoring state, not measured arrival truth.",
    "Raw vehicle-monitoring rows are repeated polls; default results use trip-stop buckets.",
    (
        "Conservative filtering excludes implausible, stale, pre-trip, post-trip, origin-layover "
        "and no-realtime-trip rows; the last two remove placeholder zero delays."
    ),
    (
        "Buckets from one trip are correlated; groups need a minimum number of distinct trips and "
        "intervals resample whole trips."
    ),
    "Reduced-service weekday holidays (from the GTFS calendar) count as weekend days.",
    (
        "Service-alert lift compares alert buckets with non-alert buckets in the same line, "
        "direction, hour and day type; it is an association, not a causal effect."
    ),
]


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
            f"timezone `{settings.timezone}`, minimum observations {settings.min_observations}, "
            f"minimum trips {settings.min_trips}"
        ),
        f"Reduced-service holidays: {', '.join(settings.holiday_dates()) or 'none'}",
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
    for title, table_name in REPORT_SECTIONS:
        lines.extend(_render_table_section(settings.cache_dir, title, table_name))
    lines.extend(["## Caveats", "", *[f"- {item}" for item in REPORT_CAVEATS]])
    lines.extend(["- This report is generated by the Polars secondary analysis path.", ""])
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


# --- Manifest -----------------------------------------------------------------------


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
        "holiday_dates": list(settings.holiday_dates()),
    }
    if not base_only:
        values.update(
            {
                "limit": settings.limit,
                "min_observations": settings.min_observations,
                "min_trips": settings.min_trips,
                "rush_windows": list(settings.rush_windows),
                "offpeak_windows": list(settings.offpeak_windows),
                "include_weekends": settings.include_weekends,
                "bootstrap_reps": settings.bootstrap_reps,
                "gtfs_dir": str(settings.gtfs_dir) if settings.gtfs_dir else None,
                "gtfs_dir_metadata": gtfs_dir_fingerprint(settings.gtfs_dir),
                "gtfs_root": str(settings.gtfs_root),
                "gtfs_metadata": gtfs_metadata_fingerprint(settings.gtfs_root),
                "gtfs_calendar_metadata": calendar_fingerprint(
                    gtfs_root=settings.gtfs_root,
                    gtfs_dir=settings.gtfs_dir,
                ),
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
    with closing(sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)) as con:
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
    if isinstance(value, datetime | date):
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
