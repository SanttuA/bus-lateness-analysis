from __future__ import annotations

from contextlib import closing
import json
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Callable

import duckdb
import numpy as np
import pandas as pd

try:
    from ._shared import (
        BUCKET_MODES,
        DEFAULT_ANALYSIS_CACHE_DIR,
        DEFAULT_BUCKET_MODE,
        DEFAULT_DB_PATH,
        DEFAULT_GTFS_ROOT,
        DEFAULT_OFFPEAK_WINDOWS,
        DEFAULT_QUALITY_MODE,
        DEFAULT_RUSH_WINDOWS,
        DEFAULT_TIMEZONE,
        METRIC_COLUMNS,
        NO_REALTIME_MIN_STOPS,
        QUALITY_FLAG_COLUMNS,
        QUALITY_MODES,
        CONSERVATIVE_EXCLUSION_COLUMNS,
        DATA_FAULT_COLUMNS,
        resolve_project_path,
    )
    from .alert_analysis import (
        ALERT_GROUP_COLUMNS,
        ALERT_RESULT_COLUMNS,
        build_alert_targets,
        finalize_alert_rows,
        make_route_resolver,
        summarize_alert_group,
        summarize_cancellations,
    )
    from .collector_gaps import (
        blackout_rows,
        daily_coverage,
        missing_spots,
        missing_summary_rows,
        source_summaries,
    )
    from .service_calendar import GtfsCalendar, cached_calendar, calendar_fingerprint
    from .uncertainty import (
        DEFAULT_BOOTSTRAP_REPS,
        EARLY_RANKING_INTERVAL_COLUMNS,
        LATE_RANKING_INTERVAL_COLUMNS,
        line_ranking_intervals,
        rush_lift_interval,
    )
except ImportError:  # pragma: no cover - used when called as analysis/*.py script.
    from _shared import (
        BUCKET_MODES,
        DEFAULT_ANALYSIS_CACHE_DIR,
        DEFAULT_BUCKET_MODE,
        DEFAULT_DB_PATH,
        DEFAULT_GTFS_ROOT,
        DEFAULT_OFFPEAK_WINDOWS,
        DEFAULT_QUALITY_MODE,
        DEFAULT_RUSH_WINDOWS,
        DEFAULT_TIMEZONE,
        METRIC_COLUMNS,
        NO_REALTIME_MIN_STOPS,
        QUALITY_FLAG_COLUMNS,
        QUALITY_MODES,
        CONSERVATIVE_EXCLUSION_COLUMNS,
        DATA_FAULT_COLUMNS,
        resolve_project_path,
    )
    from alert_analysis import (
        ALERT_GROUP_COLUMNS,
        ALERT_RESULT_COLUMNS,
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


CACHE_VERSION = 3
DEFAULT_CACHE_DIR = DEFAULT_ANALYSIS_CACHE_DIR
DEFAULT_REPORT_PATH = Path("reports/generated/overall-results.md")
MANIFEST_NAME = "manifest.json"
CACHE_DB_NAME = "report-cache.duckdb"
DEFAULT_MIN_TRIPS = 10
# Keep DuckDB well inside a ~15 GiB workstation; larger intermediates spill to disk.
DUCKDB_MEMORY_LIMIT = "3GB"
DUCKDB_THREADS = 8
FULL_COVERAGE_PCT = 95.0
FULL_DAY_HOURS = 23.0
LOW_QUALITY_FAULT_PCT = 20.0
LOW_QUALITY_TRIP_COVERAGE_PCT = 80.0

BASE_TABLES = [
    "quality_rows",
    "delay_buckets",
    "delay_cache_summary",
]

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
BUCKET_OUTPUT_COLUMNS = [
    "bucket_id",
    "bucket_mode",
    "line_ref",
    "direction_ref",
    "trip_match_key",
    "published_line_name",
    "destination_name",
    "delay_seconds",
    "delay_min",
    "raw_poll_count",
    "next_stop_point_ref",
    "next_stop_point_name",
    "representative_time_utc",
    "recorded_at_utc",
    "first_recorded_at_utc",
    "last_recorded_at_utc",
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
        for window in (*self.rush_windows, *self.offpeak_windows):
            _parse_hhmm_window(window)

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
    cache_db = settings.cache_dir / CACHE_DB_NAME
    current = _read_manifest(manifest_path)

    if (
        not force
        and cache_db.exists()
        and _manifest_file_matches(current, collect_db_file_metadata(settings.db))
        and _manifest_settings_match(current, settings)
        and _cache_has_tables(cache_db, RESULT_TABLES)
    ):
        _progress(progress, "Reusing report cache")
        return CacheResult(
            status="reused",
            cache_db=cache_db,
            manifest=current,
            timings={"cache_build_seconds": time.perf_counter() - started},
        )

    _progress(progress, "Rebuilding report cache")
    base_result = ensure_analysis_cache(settings, force=force, progress=progress)
    db_metadata = _current_or_collected_db_metadata(base_result.manifest, settings.db)
    expected = _expected_manifest(settings, db_metadata)
    _progress(progress, "Building result tables")
    _build_result_cache(settings, cache_db, progress=progress)

    manifest = {
        **expected,
        **_expected_base_manifest(settings, db_metadata),
        "built_at_utc": _utc_now_iso(),
        "cache_db": str(cache_db),
        "base_tables": BASE_TABLES,
        "result_tables": RESULT_TABLES,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return CacheResult(
        status="rebuilt",
        cache_db=cache_db,
        manifest=manifest,
        timings={"cache_build_seconds": time.perf_counter() - started},
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
    cache_db = settings.cache_dir / CACHE_DB_NAME
    current = _read_manifest(manifest_path)
    file_metadata = collect_db_file_metadata(settings.db)
    if (
        not force
        and cache_db.exists()
        and _manifest_file_matches(current, file_metadata)
        and _manifest_base_settings_match(current, settings)
        and _cache_has_tables(cache_db, BASE_TABLES)
    ):
        _progress(progress, "Reusing base cache")
        return CacheResult(
            status="reused",
            cache_db=cache_db,
            manifest=current,
            timings={"base_cache_seconds": time.perf_counter() - started},
        )

    _progress(progress, "Building base cache")
    if cache_db.exists():
        cache_db.unlink()
    db_metadata = collect_db_metadata(settings.db)
    expected = _expected_base_manifest(settings, db_metadata)
    _build_base_cache(settings, cache_db, progress=progress)

    manifest = {
        **expected,
        "built_at_utc": _utc_now_iso(),
        "cache_db": str(cache_db),
        "base_tables": BASE_TABLES,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return CacheResult(
        status="rebuilt",
        cache_db=cache_db,
        manifest=manifest,
        timings={"base_cache_seconds": time.perf_counter() - started},
    )


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
            SELECT
                COUNT(*),
                MAX(id),
                MIN(recorded_at_utc),
                MAX(recorded_at_utc)
            FROM vehicle_observations
            """
        ).fetchone()
        analysis = con.execute(
            """
            SELECT
                COUNT(*),
                MAX(id),
                MIN(recorded_at_utc),
                MAX(recorded_at_utc)
            FROM vehicle_observations
            WHERE
                is_gtfs_matchable = 1
                AND delay_seconds IS NOT NULL
                AND line_ref IS NOT NULL
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


def write_markdown_report(
    settings: ReportSettings,
    cache_result: CacheResult,
    output_path: Path = DEFAULT_REPORT_PATH,
) -> Path:
    settings = settings.resolved()
    output_path = resolve_project_path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines = _render_report_lines(settings, cache_result)
    output_path.write_text("\n".join(lines).rstrip() + "\n")
    return output_path


def _progress(callback: Callable[[str], None] | None, message: str) -> None:
    if callback is not None:
        callback(message)


def read_result_table(cache_db: Path, table_name: str) -> pd.DataFrame:
    with duckdb.connect(str(cache_db), read_only=True) as con:
        if not _duckdb_table_exists(con, table_name):
            return pd.DataFrame()
        return con.execute(f"SELECT * FROM {table_name}").fetchdf()


def connect_cache(cache_db: Path, *, preserve_order: bool = True) -> duckdb.DuckDBPyConnection:
    """Writable cache connection with bounded memory and on-disk spilling.

    Large unordered base tables are built with ``preserve_order=False`` so DuckDB can
    stream and spill them; ranked result tables keep insertion order.
    """
    con = duckdb.connect(str(cache_db))
    spill_dir = cache_db.parent / ".duckdb-spill"
    spill_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET memory_limit = {_sql_literal(DUCKDB_MEMORY_LIMIT)}")
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    con.execute(f"SET temp_directory = {_sql_literal(spill_dir.as_posix())}")
    con.execute(f"SET preserve_insertion_order = {'true' if preserve_order else 'false'}")
    return con


def _attach_source(con: duckdb.DuckDBPyConnection, settings: ReportSettings) -> None:
    con.execute("LOAD sqlite")
    con.execute(
        f"ATTACH {_sql_literal(settings.db.as_posix())} AS source_db "
        "(TYPE SQLITE, READ_ONLY)"
    )


def _build_base_cache(
    settings: ReportSettings,
    cache_db: Path,
    *,
    progress: Callable[[str], None] | None = None,
) -> None:
    with connect_cache(cache_db, preserve_order=False) as con:
        _attach_source(con, settings)
        _progress(progress, "Building quality rows")
        _build_quality_rows(con, settings)
        _progress(progress, "Building delay buckets")
        _build_delay_buckets(con, settings)
        _build_delay_cache_summary(con)


def _build_result_cache(
    settings: ReportSettings,
    cache_db: Path,
    *,
    progress: Callable[[str], None] | None = None,
) -> None:
    with connect_cache(cache_db) as con:
        _attach_source(con, settings)
        _drop_tables(con, ["line_metrics", "midpoint_summary", "stop_midpoint_change", *RESULT_TABLES])
        polls = _load_polls(con)
        coverage = daily_coverage(polls, timezone=settings.timezone)
        _progress(progress, "Building quality tables")
        _build_quality_results(con, settings, coverage)
        _progress(progress, "Building delay metric tables")
        _build_delay_metric_results(con, settings)
        _build_rush_impact(con, settings)
        _build_daily_network_profile(con, coverage)
        _build_collector_results(con, settings, polls)
        _progress(progress, "Matching service alerts")
        for table_name, frame in build_alert_tables(con, settings, "delay_buckets").items():
            _create_table_from_frame(con, table_name, frame)
        _export_result_csvs(con, settings.cache_dir)


# --- Base tables --------------------------------------------------------------------


def _quality_pass_sql(settings: ReportSettings) -> str:
    if settings.quality_mode != "conservative":
        return "TRUE"
    exclusions = CONSERVATIVE_EXCLUSION_COLUMNS.copy()
    if settings.exclude_stop_call_disagreement:
        exclusions.append("has_stop_call_disagreement")
    return "NOT (" + " OR ".join(exclusions) + ")"


def _build_quality_rows(con: duckdb.DuckDBPyConnection, settings: ReportSettings) -> None:
    source_columns = {
        row[1] for row in con.execute("PRAGMA table_info('source_db.vehicle_observations')").fetchall()
    }

    def optional(column: str) -> str:
        return f"v.{column}" if column in source_columns else f"NULL::VARCHAR AS {column}"

    con.execute(
        f"""
        CREATE TEMP TABLE quality_rows_flagged AS
        WITH source_rows AS (
            SELECT
                v.id,
                v.poll_id,
                v.vehicle_id,
                TRY_CAST(v.recorded_at_utc AS TIMESTAMPTZ) AS recorded_at_utc,
                TRY_CAST(v.valid_until_utc AS TIMESTAMPTZ) AS valid_until_utc,
                TRY_CAST(p.collected_at_utc AS TIMESTAMPTZ) AS collected_at_utc,
                v.line_ref,
                v.direction_ref,
                TRY_CAST(v.origin_aimed_departure_time_utc AS TIMESTAMPTZ)
                    AS origin_aimed_departure_time_utc,
                {optional("origin_ref")},
                {optional("destination_name")},
                v.trip_match_key,
                v.published_line_name,
                TRY_CAST(v.delay_seconds AS DOUBLE) AS delay_seconds,
                v.next_stop_point_ref,
                v.next_stop_point_name,
                TRY_CAST(v.next_aimed_arrival_time_utc AS TIMESTAMPTZ)
                    AS next_aimed_arrival_time_utc,
                TRY_CAST(v.next_expected_arrival_time_utc AS TIMESTAMPTZ)
                    AS next_expected_arrival_time_utc,
                TRY_CAST(v.next_aimed_departure_time_utc AS TIMESTAMPTZ)
                    AS next_aimed_departure_time_utc,
                TRY_CAST(v.next_expected_departure_time_utc AS TIMESTAMPTZ)
                    AS next_expected_departure_time_utc,
                TRY_CAST(v.destination_aimed_arrival_time_utc AS TIMESTAMPTZ)
                    AS destination_aimed_arrival_time_utc,
                TRY_CAST(v.created_at_utc AS TIMESTAMPTZ) AS created_at_utc
            FROM source_db.vehicle_observations v
            LEFT JOIN source_db.collector_polls p ON p.id = v.poll_id
            WHERE
                v.is_gtfs_matchable = 1
                AND v.delay_seconds IS NOT NULL
                AND v.line_ref IS NOT NULL
        ),
        measured AS (
            SELECT
                *,
                COALESCE(collected_at_utc, created_at_utc) AS quality_collected_at_utc,
                DATE_DIFF(
                    'second',
                    recorded_at_utc,
                    COALESCE(collected_at_utc, created_at_utc)
                ) AS observation_age_seconds,
                DATE_DIFF(
                    'second',
                    valid_until_utc,
                    COALESCE(collected_at_utc, created_at_utc)
                ) AS validity_lag_seconds,
                COALESCE(
                    DATE_DIFF(
                        'second',
                        next_aimed_arrival_time_utc,
                        next_expected_arrival_time_utc
                    ),
                    DATE_DIFF(
                        'second',
                        next_aimed_departure_time_utc,
                        next_expected_departure_time_utc
                    )
                ) AS stop_call_delay_seconds
            FROM source_rows
        )
        SELECT
            *,
            ABS(stop_call_delay_seconds - delay_seconds) AS stop_call_delay_diff_seconds,
            COALESCE(ABS(delay_seconds) > 7200, FALSE) AS is_implausible_delay,
            COALESCE(observation_age_seconds > 300, FALSE)
                OR COALESCE(valid_until_utc < quality_collected_at_utc, FALSE)
                AS is_stale_observation,
            COALESCE(
                recorded_at_utc < origin_aimed_departure_time_utc - INTERVAL '15 minutes',
                FALSE
            ) AS is_pre_trip_observation,
            COALESCE(
                recorded_at_utc > destination_aimed_arrival_time_utc + INTERVAL '30 minutes',
                FALSE
            ) AS is_post_trip_observation,
            COALESCE(
                CAST(next_stop_point_ref AS VARCHAR) = CAST(origin_ref AS VARCHAR)
                    AND recorded_at_utc < origin_aimed_departure_time_utc,
                FALSE
            ) AS is_origin_layover,
            COALESCE(ABS(stop_call_delay_seconds - delay_seconds) > 600, FALSE)
                AS has_stop_call_disagreement,
            COALESCE(delay_seconds = 0 AND ABS(stop_call_delay_seconds) > 60, FALSE)
                AS is_placeholder_zero
        FROM measured
        """
    )
    flag_sum = " + ".join(f"CAST({column} AS INTEGER)" for column in QUALITY_FLAG_COLUMNS)
    con.execute(
        f"""
        CREATE TABLE quality_rows AS
        WITH trip_stats AS (
            SELECT
                trip_match_key,
                vehicle_id,
                COALESCE(
                    MAX(ABS(delay_seconds)) = 0
                        AND COUNT(DISTINCT next_stop_point_ref) >= {NO_REALTIME_MIN_STOPS},
                    FALSE
                ) AS is_no_realtime_trip
            FROM quality_rows_flagged
            GROUP BY trip_match_key, vehicle_id
        ),
        joined AS (
            SELECT
                flagged.*,
                COALESCE(trip_stats.is_no_realtime_trip, FALSE) AS is_no_realtime_trip
            FROM quality_rows_flagged flagged
            LEFT JOIN trip_stats
                ON flagged.trip_match_key IS NOT DISTINCT FROM trip_stats.trip_match_key
                AND flagged.vehicle_id IS NOT DISTINCT FROM trip_stats.vehicle_id
        )
        SELECT
            *,
            ({flag_sum}) AS quality_issue_count,
            {_quality_pass_sql(settings)} AS quality_pass
        FROM joined
        """
    )
    con.execute("DROP TABLE quality_rows_flagged")


def delay_bucket_sql(
    *,
    bucket: str,
    timezone: str,
    holidays: tuple[str, ...] | list[str],
    source_table: str = "quality_rows",
    extra_where_parts: list[str] | None = None,
) -> str:
    """SELECT producing delay buckets; shared by the cache build and windowed CLI queries."""
    where_parts = [
        "quality_pass",
        "delay_seconds IS NOT NULL",
        "COALESCE(next_aimed_arrival_time_utc, recorded_at_utc) IS NOT NULL",
        "line_ref IS NOT NULL",
        *(extra_where_parts or []),
    ]
    where_sql = " AND ".join(where_parts)
    local_columns = _local_time_select(timezone, holidays)

    if bucket == "poll":
        return f"""
            SELECT
                COALESCE(CAST(id AS VARCHAR), CAST(row_number() OVER () AS VARCHAR))
                    AS bucket_id,
                'poll' AS bucket_mode,
                CAST(line_ref AS VARCHAR) AS line_ref,
                COALESCE(CAST(direction_ref AS VARCHAR), 'Unknown') AS direction_ref,
                CAST(trip_match_key AS VARCHAR) AS trip_match_key,
                COALESCE(CAST(published_line_name AS VARCHAR), CAST(line_ref AS VARCHAR))
                    AS published_line_name,
                CAST(destination_name AS VARCHAR) AS destination_name,
                delay_seconds,
                delay_seconds / 60.0 AS delay_min,
                1::BIGINT AS raw_poll_count,
                CAST(next_stop_point_ref AS VARCHAR) AS next_stop_point_ref,
                CAST(next_stop_point_name AS VARCHAR) AS next_stop_point_name,
                COALESCE(next_aimed_arrival_time_utc, recorded_at_utc)
                    AS representative_time_utc,
                recorded_at_utc,
                recorded_at_utc AS first_recorded_at_utc,
                recorded_at_utc AS last_recorded_at_utc,
                {local_columns}
            FROM {source_table}
            WHERE {where_sql}
        """

    if bucket == "trip-stop":
        group_keys = [
            "trip_match_key",
            "vehicle_id",
            "line_ref",
            "direction_ref",
            "next_stop_point_ref",
        ]
    elif bucket == "vehicle-trip":
        group_keys = ["trip_match_key", "vehicle_id", "line_ref", "direction_ref"]
    else:
        group_keys = ["line_ref", "direction_ref", "local_date", "local_hour", "day_type"]

    base = f"""
        SELECT
            id,
            CAST(trip_match_key AS VARCHAR) AS trip_match_key,
            vehicle_id,
            CAST(line_ref AS VARCHAR) AS line_ref,
            COALESCE(CAST(direction_ref AS VARCHAR), 'Unknown') AS direction_ref,
            CAST(next_stop_point_ref AS VARCHAR) AS next_stop_point_ref,
            COALESCE(CAST(published_line_name AS VARCHAR), CAST(line_ref AS VARCHAR))
                AS published_line_name,
            CAST(next_stop_point_name AS VARCHAR) AS next_stop_point_name,
            CAST(destination_name AS VARCHAR) AS destination_name,
            delay_seconds,
            recorded_at_utc,
            COALESCE(next_aimed_arrival_time_utc, recorded_at_utc)
                AS representative_time_utc
        FROM {source_table}
        WHERE {where_sql}
    """
    if bucket == "line-hour":
        base = f"""
            SELECT *, {local_columns}
            FROM ({base}) base_rows
            WHERE representative_time_utc IS NOT NULL
        """

    group_sql = ", ".join(group_keys)
    bucket_id_sql = " || '|' || ".join(
        f"COALESCE(CAST({column} AS VARCHAR), '<NA>')" for column in group_keys
    )
    trip_column = "trip_match_key" if "trip_match_key" in group_keys else "NULL::VARCHAR"
    stop_column = "next_stop_point_ref" if "next_stop_point_ref" in group_keys else "NULL::VARCHAR"
    return f"""
        WITH base_rows AS (
            {base}
        ),
        grouped AS (
            SELECT
                {group_sql},
                MEDIAN(delay_seconds) AS delay_seconds,
                COUNT(*) AS raw_poll_count,
                FIRST(published_line_name ORDER BY representative_time_utc)
                    AS published_line_name,
                FIRST(next_stop_point_name ORDER BY representative_time_utc)
                    AS next_stop_point_name,
                FIRST(destination_name ORDER BY representative_time_utc)
                    AS destination_name,
                MIN(representative_time_utc) AS representative_time_utc,
                MIN(recorded_at_utc) AS recorded_at_utc,
                MIN(recorded_at_utc) AS first_recorded_at_utc,
                MAX(recorded_at_utc) AS last_recorded_at_utc
            FROM base_rows
            WHERE representative_time_utc IS NOT NULL
            GROUP BY {group_sql}
        )
        SELECT
            {bucket_id_sql} AS bucket_id,
            {_sql_literal(bucket)} AS bucket_mode,
            line_ref,
            direction_ref,
            {trip_column} AS trip_match_key,
            published_line_name,
            destination_name,
            delay_seconds,
            delay_seconds / 60.0 AS delay_min,
            raw_poll_count,
            {stop_column} AS next_stop_point_ref,
            next_stop_point_name,
            representative_time_utc,
            recorded_at_utc,
            first_recorded_at_utc,
            last_recorded_at_utc,
            {local_columns}
        FROM grouped
    """


def _build_delay_buckets(con: duckdb.DuckDBPyConnection, settings: ReportSettings) -> None:
    con.execute(
        "CREATE TABLE delay_buckets AS "
        + delay_bucket_sql(
            bucket=settings.bucket,
            timezone=settings.timezone,
            holidays=settings.holiday_dates(),
        )
    )


def _build_delay_cache_summary(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE delay_cache_summary AS
        SELECT
            COUNT(*) AS bucket_count,
            COALESCE(SUM(raw_poll_count), 0) AS raw_poll_count,
            COUNT(DISTINCT line_ref) AS line_count,
            STRFTIME(
                MAKE_TIMESTAMP_MS(MIN(EPOCH_MS(representative_time_utc))),
                '%Y-%m-%dT%H:%M:%SZ'
            ) AS observation_start_utc,
            STRFTIME(
                MAKE_TIMESTAMP_MS(MAX(EPOCH_MS(representative_time_utc))),
                '%Y-%m-%dT%H:%M:%SZ'
            ) AS observation_end_utc,
            MIN(local_date) AS local_start_date,
            MAX(local_date) AS local_end_date
        FROM delay_buckets
        """
    )


# --- Quality tables -----------------------------------------------------------------


def _conservative_default_sql() -> str:
    return "(" + " OR ".join(CONSERVATIVE_EXCLUSION_COLUMNS) + ")"


def quality_summary_sql() -> str:
    checks = [
        "SELECT 'analysis_rows' AS quality_check, COUNT(*)::DOUBLE AS row_count, 0 AS ordinal "
        "FROM quality_rows"
    ]
    for ordinal, column in enumerate(QUALITY_FLAG_COLUMNS, start=1):
        checks.append(
            f"SELECT '{column}', SUM(CAST({column} AS INTEGER))::DOUBLE, {ordinal} FROM quality_rows"
        )
    conservative = _conservative_default_sql()
    checks.append(
        f"SELECT 'conservative_excluded_default', SUM(CAST({conservative} AS INTEGER))::DOUBLE, "
        f"{len(QUALITY_FLAG_COLUMNS) + 1} FROM quality_rows"
    )
    checks.append(
        "SELECT 'conservative_excluded_with_stop_call_disagreement', "
        f"SUM(CAST({conservative} OR has_stop_call_disagreement AS INTEGER))::DOUBLE, "
        f"{len(QUALITY_FLAG_COLUMNS) + 2} FROM quality_rows"
    )
    union = "\nUNION ALL\n".join(checks)
    return f"""
        WITH checks AS ({union}),
        totals AS (SELECT COUNT(*)::DOUBLE AS total_rows FROM quality_rows)
        SELECT
            quality_check,
            CAST(row_count AS BIGINT) AS row_count,
            ROUND(row_count / total_rows * 100.0, 2) AS pct_rows
        FROM checks, totals
        WHERE total_rows > 0
        ORDER BY ordinal
    """


def quality_by_line_sql() -> str:
    return f"""
        SELECT
            CAST(line_ref AS VARCHAR) AS line_ref,
            COUNT(*) AS row_count,
            FIRST(published_line_name ORDER BY recorded_at_utc) AS line_name,
            CAST(SUM(CAST(is_implausible_delay AS INTEGER)) AS BIGINT) AS implausible_delay_rows,
            CAST(SUM(CAST(is_stale_observation AS INTEGER)) AS BIGINT) AS stale_rows,
            CAST(SUM(CAST(is_pre_trip_observation AS INTEGER)) AS BIGINT) AS pre_trip_rows,
            CAST(SUM(CAST(is_post_trip_observation AS INTEGER)) AS BIGINT) AS post_trip_rows,
            CAST(SUM(CAST(is_origin_layover AS INTEGER)) AS BIGINT) AS origin_layover_rows,
            CAST(SUM(CAST(is_no_realtime_trip AS INTEGER)) AS BIGINT) AS no_realtime_trip_rows,
            CAST(SUM(CAST(has_stop_call_disagreement AS INTEGER)) AS BIGINT) AS stop_call_disagreement_rows,
            CAST(SUM(CAST({_conservative_default_sql()} AS INTEGER)) AS BIGINT)
                AS conservative_excluded_rows,
            ROUND(conservative_excluded_rows / COUNT(*) * 100.0, 2) AS conservative_excluded_pct,
            CAST(SUM(CAST(({" OR ".join(DATA_FAULT_COLUMNS)}) AS INTEGER)) AS BIGINT)
                AS data_fault_rows,
            ROUND(data_fault_rows / COUNT(*) * 100.0, 2) AS data_fault_pct
        FROM quality_rows
        GROUP BY line_ref
    """


def _build_quality_results(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
    coverage: list[dict[str, object]],
) -> None:
    con.execute(f"CREATE TABLE quality_summary AS {quality_summary_sql()}")
    con.execute(
        f"""
        CREATE TABLE quality_by_line AS
        SELECT * FROM ({quality_by_line_sql()})
        WHERE row_count >= {settings.min_observations}
        ORDER BY conservative_excluded_pct DESC, conservative_excluded_rows DESC, line_ref
        LIMIT {settings.limit}
        """
    )
    exclusions = con.execute(
        f"""
        SELECT line_ref, line_name, row_count, conservative_excluded_rows, conservative_excluded_pct,
            data_fault_rows, data_fault_pct
        FROM ({quality_by_line_sql()})
        """
    ).fetchdf()
    full_days = [
        row["local_date"]
        for row in coverage
        if float(row["coverage_pct"]) >= FULL_COVERAGE_PCT and float(row["observed_hours"]) >= FULL_DAY_HOURS
    ]
    observed = pd.DataFrame({"line_ref": pd.Series(dtype="object"), "observed_trip_count": pd.Series(dtype="int64")})
    if full_days:
        day_list = ", ".join(f"DATE '{day.isoformat()}'" for day in full_days)
        observed = con.execute(
            f"""
            SELECT
                CAST(line_ref AS VARCHAR) AS line_ref,
                COUNT(DISTINCT trip_match_key) AS observed_trip_count
            FROM quality_rows
            WHERE CAST(timezone({_sql_literal(settings.timezone)}, origin_aimed_departure_time_utc) AS DATE)
                IN ({day_list})
            GROUP BY line_ref
            """
        ).fetchdf()
    calendar = settings.gtfs_calendar()
    scheduled: dict[str, int] = {}
    for day in full_days:
        for line, count in calendar.scheduled_trips(day).items():
            scheduled[line] = scheduled.get(line, 0) + count
    table = exclusions.merge(observed, on="line_ref", how="left").merge(
        pd.DataFrame({"line_ref": list(scheduled), "scheduled_trip_count": list(scheduled.values())}),
        on="line_ref",
        how="left",
    )
    table["coverage_day_count"] = len(full_days)
    scheduled_count = table["scheduled_trip_count"]
    table["trip_coverage_pct"] = np.where(
        scheduled_count.fillna(0) > 0,
        table["observed_trip_count"].fillna(0) / scheduled_count.where(scheduled_count > 0) * 100.0,
        np.nan,
    )
    table["low_quality"] = (table["data_fault_pct"] > LOW_QUALITY_FAULT_PCT) | (
        table["trip_coverage_pct"] < LOW_QUALITY_TRIP_COVERAGE_PCT
    ).fillna(False)
    table["observed_trip_count"] = table["observed_trip_count"].astype("Int64")
    table["scheduled_trip_count"] = table["scheduled_trip_count"].astype("Int64")
    _create_table_from_frame(con, "line_quality", _round_frame(table.sort_values("line_ref")))


# --- Delay metric tables ------------------------------------------------------------


def _having(settings: ReportSettings) -> str:
    having = f"HAVING COUNT(*) >= {settings.min_observations}"
    if settings.effective_min_trips > 1:
        having += f" AND COUNT(DISTINCT trip_match_key) >= {settings.effective_min_trips}"
    return having


def _line_metrics_sql(settings: ReportSettings, source: str = "delay_buckets") -> str:
    return f"""
        SELECT
            line_ref,
            FIRST(published_line_name ORDER BY representative_time_utc) AS line_name,
            {_metric_select()}
        FROM {source}
        GROUP BY line_ref
        {_having(settings)}
    """


def line_ranking_frame(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
    ranking: str,
    *,
    source: str = "delay_buckets",
) -> pd.DataFrame:
    if ranking == "early":
        order = "pct_over_3_min_early DESC, p90_early_min_abs DESC, bucket_count DESC, line_ref"
        columns = EARLY_RANKING_COLUMNS
        interval_columns = EARLY_RANKING_INTERVAL_COLUMNS
    else:
        order = "p90_delay_min DESC, pct_over_5_min_late DESC, bucket_count DESC, line_ref"
        columns = LATE_RANKING_COLUMNS
        interval_columns = LATE_RANKING_INTERVAL_COLUMNS
    table = con.execute(
        f"SELECT * FROM ({_line_metrics_sql(settings, source)}) ORDER BY {order} LIMIT {settings.limit}"
    ).fetchdf()
    intervals = []
    for line_ref, frame in _line_frames(con, source, table["line_ref"].tolist()).items():
        intervals.append(
            {
                "line_ref": line_ref,
                **line_ranking_intervals(
                    ranking,
                    line_ref,
                    frame["trip_match_key"].to_numpy(dtype=object),
                    frame["delay_seconds"].to_numpy(dtype=np.float64),
                    reps=settings.bootstrap_reps,
                ),
            }
        )
    interval_frame = pd.DataFrame(intervals, columns=["line_ref", *interval_columns])
    table = table.merge(interval_frame, on="line_ref", how="left")
    return _round_frame(table[[column for column in columns if column in table.columns]])


def _line_frames(
    con: duckdb.DuckDBPyConnection,
    source: str,
    line_refs: list[str],
    extra_columns: tuple[str, ...] = (),
) -> dict[str, pd.DataFrame]:
    if not line_refs:
        return {}
    values = ", ".join(_sql_literal(str(line_ref)) for line_ref in line_refs)
    data = con.execute(
        f"""
        SELECT line_ref, trip_match_key, delay_seconds {''.join(', ' + column for column in extra_columns)}
        FROM {source}
        WHERE line_ref IN ({values})
        """
    ).fetchdf()
    return {str(key): frame for key, frame in data.groupby("line_ref", sort=False)}


def context_metrics_sql(settings: ReportSettings, where_sql: str = "", source: str = "delay_buckets") -> str:
    return f"""
        SELECT
            line_ref,
            FIRST(published_line_name ORDER BY representative_time_utc) AS line_name,
            direction_ref,
            LPAD(CAST(local_hour AS VARCHAR), 2, '0') || ':00' AS hour_local,
            day_type,
            {_metric_select()}
        FROM {source}
        {where_sql}
        GROUP BY line_ref, direction_ref, local_hour, day_type
        {_having(settings)}
        ORDER BY p90_delay_min DESC, pct_over_5_min_late DESC, bucket_count DESC,
            line_ref, direction_ref, hour_local, day_type
        LIMIT {settings.limit}
    """


def hourly_profile_sql(
    settings: ReportSettings,
    *,
    by_day_type: bool,
    where_sql: str = "",
    source: str = "delay_buckets",
) -> str:
    keys = "day_type, local_hour" if by_day_type else "local_hour"
    day_column = "day_type, " if by_day_type else ""
    return f"""
        SELECT
            {day_column}LPAD(CAST(local_hour AS VARCHAR), 2, '0') || ':00' AS hour_local,
            {_metric_select()}
        FROM {source}
        {where_sql}
        GROUP BY {keys}
        {_having(settings)}
        ORDER BY {keys}
    """


def _build_delay_metric_results(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
) -> None:
    for ranking in ("late", "early"):
        _create_table_from_frame(con, f"line_{ranking}_rankings", line_ranking_frame(con, settings, ranking))
    _create_table_from_frame(
        con,
        "context_delay_metrics",
        _round_frame(con.execute(context_metrics_sql(settings)).fetchdf()),
    )
    _create_table_from_frame(
        con,
        "hourly_delay_profile",
        _round_frame(con.execute(hourly_profile_sql(settings, by_day_type=True)).fetchdf()),
    )


def rush_impact_frame(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
    *,
    source: str = "delay_buckets",
) -> pd.DataFrame:
    """Weekday rush versus weekday off-peak daytime on the same line (see Polars twin)."""
    rush = _window_condition(settings.rush_windows)
    offpeak = _window_condition(settings.offpeak_windows)
    if not settings.include_weekends:
        rush = f"({rush}) AND is_weekday"
        offpeak = f"({offpeak}) AND is_weekday"
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW rush_marked AS
        SELECT * FROM (
            SELECT *, ({rush}) AS is_rush, ({offpeak}) AND NOT ({rush}) AS is_offpeak
            FROM {source}
        )
        WHERE is_rush OR is_offpeak
        """
    )
    side_columns = ["bucket_count", "raw_poll_count", "trip_count", "median_delay_min", "p90_delay_min", "pct_over_5_min_late"]
    side_select = ",\n".join(
        f"{side}.{column} AS {column}_{side}" for side in ("offpeak", "rush") for column in side_columns
    )
    table = con.execute(
        f"""
        WITH grouped AS (
            SELECT
                line_ref,
                is_rush,
                FIRST(published_line_name ORDER BY representative_time_utc) AS line_name,
                {_metric_select()}
            FROM rush_marked
            GROUP BY line_ref, is_rush
            {_having(settings)}
        ),
        offpeak AS (SELECT * FROM grouped WHERE NOT is_rush),
        rush AS (SELECT * FROM grouped WHERE is_rush)
        SELECT
            rush.line_ref,
            COALESCE(rush.line_name, offpeak.line_name) AS line_name,
            {side_select},
            rush.median_delay_min - offpeak.median_delay_min AS rush_median_delay_lift_min,
            rush.p90_delay_min - offpeak.p90_delay_min AS rush_p90_delay_lift_min,
            rush.pct_over_5_min_late - offpeak.pct_over_5_min_late
                AS rush_over_5_min_late_pct_point_lift
        FROM rush
        JOIN offpeak USING (line_ref)
        ORDER BY rush_over_5_min_late_pct_point_lift DESC,
            rush_p90_delay_lift_min DESC,
            bucket_count_rush DESC,
            line_ref
        LIMIT {settings.limit}
        """
    ).fetchdf()
    intervals = [
        {
            "line_ref": line_ref,
            **rush_lift_interval(
                line_ref,
                frame["trip_match_key"].to_numpy(dtype=object),
                frame["delay_seconds"].to_numpy(dtype=np.float64),
                frame["is_rush"].to_numpy(dtype=bool),
                reps=settings.bootstrap_reps,
            ),
        }
        for line_ref, frame in _line_frames(con, "rush_marked", table["line_ref"].tolist(), ("is_rush",)).items()
    ]
    interval_frame = pd.DataFrame(
        intervals,
        columns=["line_ref", "rush_over_5_min_late_lift_ci_lo", "rush_over_5_min_late_lift_ci_hi"],
    )
    table = table.merge(interval_frame, on="line_ref", how="left")
    return _round_frame(table[RUSH_COLUMNS])


def _build_rush_impact(con: duckdb.DuckDBPyConnection, settings: ReportSettings) -> None:
    _create_table_from_frame(con, "rush_impact", rush_impact_frame(con, settings))


def _build_daily_network_profile(
    con: duckdb.DuckDBPyConnection,
    coverage: list[dict[str, object]],
) -> None:
    daily = con.execute(
        f"""
        SELECT
            local_date,
            FIRST(local_weekday ORDER BY representative_time_utc) AS local_weekday,
            FIRST(day_type ORDER BY representative_time_utc) AS day_type,
            FIRST(is_holiday ORDER BY representative_time_utc) AS is_holiday,
            {_metric_select()}
        FROM delay_buckets
        GROUP BY local_date
        ORDER BY local_date
        """
    ).fetchdf()
    coverage_frame = pd.DataFrame(
        coverage,
        columns=["local_date", "observed_hours", "missing_min", "coverage_pct", "success_count"],
    ).rename(columns={"coverage_pct": "collector_coverage_pct", "missing_min": "collector_missing_min"})
    if not daily.empty:
        daily["local_date"] = pd.to_datetime(daily["local_date"]).dt.date
    daily = daily.merge(
        coverage_frame[["local_date", "collector_coverage_pct", "collector_missing_min"]],
        on="local_date",
        how="left",
    )
    _create_table_from_frame(
        con,
        "daily_network_profile",
        _round_frame(daily[DAILY_COLUMNS]),
        date_columns=("local_date",),
    )


# --- Collector tables ---------------------------------------------------------------


def _load_polls(con: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    if not _sqlite_table_exists(con, "collector_polls"):
        return []
    frame = con.execute(
        """
        SELECT source, attempted_at_utc, collected_at_utc, status, ok, row_count,
               gap_seconds_since_previous_success
        FROM source_db.collector_polls
        ORDER BY source, attempted_at_utc
        """
    ).fetchdf()
    return frame.astype(object).where(frame.notna(), None).to_dict("records")


def _build_collector_results(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
    polls: list[dict[str, object]],
) -> None:
    summaries = source_summaries(polls)
    _create_table_from_frame(
        con,
        "collector_blackouts",
        _round_frame(pd.DataFrame(blackout_rows(summaries)).head(settings.limit)),
    )
    _create_table_from_frame(
        con,
        "collector_missing_summary",
        _round_frame(pd.DataFrame(missing_summary_rows(summaries))),
    )
    _create_table_from_frame(
        con,
        "collector_missing_spots",
        _round_frame(pd.DataFrame(missing_spots(polls)).head(settings.limit)),
    )


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


def load_alert_rows(db_path: Path) -> list[dict[str, object]]:
    with closing(sqlite3.connect(f"file:{Path(db_path).as_posix()}?mode=ro", uri=True)) as con:
        if not _table_exists_sqlite(con, "service_alerts"):
            return []
        con.row_factory = sqlite3.Row
        columns = {row[1] for row in con.execute("PRAGMA table_info(service_alerts)")}
        select = ", ".join(
            column if column in columns else f"NULL AS {column}" for column in ALERT_ROW_COLUMNS
        )
        return [dict(row) for row in con.execute(f"SELECT {select} FROM service_alerts")]


def build_alert_tables(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
    bucket_table: str,
    *,
    alert_kind: str = "any",
) -> dict[str, pd.DataFrame]:
    """Stratified alert-versus-control comparison over any bucket table or view.

    Used by the report cache (``delay_buckets``) and by the alert CLI, which registers its
    windowed buckets as an in-memory DuckDB view. The per-group statistics come from
    ``alert_analysis.summarize_alert_group``, shared with the Polars path.
    """
    result = {
        "service_alert_grouped": pd.DataFrame(),
        "service_alert_by_line": pd.DataFrame(),
        "service_alert_inventory": pd.DataFrame(),
        "service_cancellations": pd.DataFrame(),
    }
    # Epoch milliseconds avoid DuckDB's pytz dependency for TIMESTAMPTZ -> Python values.
    start_ms, end_ms = con.execute(
        f"SELECT EPOCH_MS(MIN(representative_time_utc)), EPOCH_MS(MAX(representative_time_utc)) "
        f"FROM {bucket_table}"
    ).fetchone()
    if start_ms is None or end_ms is None:
        return result
    targets, inventory, cancellations = build_alert_targets(
        load_alert_rows(settings.db),
        make_route_resolver(settings.gtfs_calendar(), settings.timezone),
        datetime.fromtimestamp(start_ms / 1000, UTC),
        datetime.fromtimestamp(end_ms / 1000, UTC),
    )
    if inventory:
        inventory_frame = pd.DataFrame(inventory)
        for column in ("first_active_utc", "last_active_utc"):
            inventory_frame[column] = [
                value.strftime("%Y-%m-%dT%H:%M:%SZ") if pd.notna(value) else None
                for value in inventory_frame[column]
            ]
        result["service_alert_inventory"] = _round_frame(inventory_frame)
    if cancellations:
        result["service_cancellations"] = pd.DataFrame(summarize_cancellations(cancellations))
    targets = [row for row in targets if alert_kind == "any" or row["alert_scope"] == alert_kind]
    if not targets:
        return result

    target_frame = pd.DataFrame(targets)
    con.register("alert_targets_frame", target_frame)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE alert_active AS
        SELECT DISTINCT
            t.cause, t.effect, CAST(t.priority AS BIGINT) AS priority, t.alert_scope,
            t.alert_key, b.bucket_id
        FROM alert_targets_frame t
        JOIN {bucket_table} b
            ON t.alert_scope = 'route'
            AND b.line_ref = t.target_ref
            AND b.representative_time_utc >= t.start_utc
            AND b.representative_time_utc <= t.end_utc
        UNION
        SELECT DISTINCT
            t.cause, t.effect, CAST(t.priority AS BIGINT) AS priority, t.alert_scope,
            t.alert_key, b.bucket_id
        FROM alert_targets_frame t
        JOIN {bucket_table} b
            ON t.alert_scope = 'stop'
            AND b.next_stop_point_ref = t.target_ref
            AND b.representative_time_utc >= t.start_utc
            AND b.representative_time_utc <= t.end_utc
        """
    )
    con.unregister("alert_targets_frame")
    groups = con.execute(
        "SELECT DISTINCT cause, effect, priority, alert_scope FROM alert_active "
        "ORDER BY cause, effect, priority, alert_scope"
    ).fetchall()
    grouped_rows: list[dict[str, object]] = []
    line_rows: list[dict[str, object]] = []
    stratum_sql = (
        "CAST(b.line_ref AS VARCHAR) || '|' || CAST(b.direction_ref AS VARCHAR) || '|' || "
        "CAST(b.local_hour AS VARCHAR) || '|' || CAST(b.day_type AS VARCHAR)"
    )
    for values in groups:
        group = dict(zip(ALERT_GROUP_COLUMNS, values, strict=True))
        filters = "cause = ? AND effect = ? AND priority = ? AND alert_scope = ?"
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE alert_group_ids AS "
            f"SELECT DISTINCT bucket_id FROM alert_active WHERE {filters}",
            list(values),
        )
        combined = con.execute(
            f"""
            WITH active_rows AS (
                SELECT b.* FROM {bucket_table} b SEMI JOIN alert_group_ids g USING (bucket_id)
            ),
            strata AS (
                SELECT DISTINCT line_ref, direction_ref, local_hour, day_type FROM active_rows
            )
            SELECT b.bucket_id, {stratum_sql} AS stratum, b.line_ref, b.trip_match_key,
                b.delay_seconds, b.published_line_name, TRUE AS is_active
            FROM active_rows b
            UNION ALL
            SELECT b.bucket_id, {stratum_sql} AS stratum, b.line_ref, b.trip_match_key,
                b.delay_seconds, b.published_line_name, FALSE AS is_active
            FROM {bucket_table} b
            SEMI JOIN strata s USING (line_ref, direction_ref, local_hour, day_type)
            ANTI JOIN (SELECT DISTINCT bucket_id FROM alert_active) a USING (bucket_id)
            """
        ).fetchdf()
        pairs = con.execute(
            f"SELECT alert_key, bucket_id FROM alert_active WHERE {filters}",
            list(values),
        ).fetchall()
        grouped_row, group_line_rows = summarize_alert_group(
            group,
            {name: combined[name].to_numpy(dtype=object) for name in combined.columns},
            pairs,
            min_observations=settings.min_observations,
            min_trips=settings.effective_min_trips,
            reps=settings.bootstrap_reps,
        )
        if grouped_row is not None:
            grouped_rows.append(grouped_row)
        line_rows.extend(group_line_rows)
    result["service_alert_grouped"] = _alert_frame(grouped_rows, settings.limit, include_line=False)
    result["service_alert_by_line"] = _alert_frame(line_rows, settings.limit, include_line=True)
    return result


def _alert_frame(rows: list[dict[str, object]], limit: int, *, include_line: bool) -> pd.DataFrame:
    finalized = finalize_alert_rows(rows, limit, include_line=include_line)
    if not finalized:
        return pd.DataFrame()
    frame = pd.DataFrame(finalized)
    for column in ("priority", *[name for name in ALERT_RESULT_COLUMNS if name.endswith("count")]):
        frame[column] = frame[column].astype("int64")
    return _round_frame(frame)


# --- Output helpers -----------------------------------------------------------------


def _round_frame(frame: pd.DataFrame, digits: int = 2) -> pd.DataFrame:
    result = frame.copy()
    numeric = result.select_dtypes(include="number").columns
    result[numeric] = result[numeric].round(digits)
    return result


def _create_table_from_frame(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    frame: pd.DataFrame,
    *,
    date_columns: tuple[str, ...] = (),
) -> None:
    con.execute(f"DROP TABLE IF EXISTS {_sql_identifier(table_name)}")
    if frame is None or frame.empty and len(frame.columns) == 0:
        _create_empty_table(con, table_name)
        return
    view_name = f"_{table_name}_frame"
    con.register(view_name, frame)
    replace = ", ".join(f"CAST({column} AS DATE) AS {column}" for column in date_columns)
    select = f"* REPLACE ({replace})" if replace else "*"
    con.execute(f"CREATE TABLE {_sql_identifier(table_name)} AS SELECT {select} FROM {view_name}")
    con.unregister(view_name)


def _export_result_csvs(con: duckdb.DuckDBPyConnection, cache_dir: Path) -> None:
    for table_name in RESULT_TABLES:
        if not _duckdb_table_exists(con, table_name):
            continue
        output_path = cache_dir / f"{table_name}.csv"
        con.execute(
            f"COPY (SELECT * FROM {table_name}) TO "
            f"{_sql_literal(output_path.as_posix())} (HEADER, DELIMITER ',')"
        )


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
    "The source data is from Turku Region Public Transport / Foli API under CC BY 4.0.",
]


def _render_report_lines(
    settings: ReportSettings,
    cache_result: CacheResult,
) -> list[str]:
    render_started = time.perf_counter()
    manifest = cache_result.manifest
    db_meta = manifest["db_metadata"]
    report_generated_at = _utc_now_iso()

    lines = [
        "# Overall Bus Lateness Results",
        "",
        f"Generated at: {report_generated_at}",
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
            f"{_format_int(db_meta.get('vehicle_observation_count', 0))} raw "
            "vehicle observations"
        ),
        (
            "Settings: "
            f"quality `{settings.quality_mode}`, bucket `{settings.bucket}`, "
            f"timezone `{settings.timezone}`, minimum observations "
            f"{settings.min_observations}, minimum trips {settings.min_trips}"
        ),
        f"Reduced-service holidays: {', '.join(settings.holiday_dates()) or 'none'}",
        "",
    ]
    timing_index = len(lines) - 1

    summary = read_result_table(cache_result.cache_db, "delay_cache_summary")
    if not summary.empty:
        row = summary.iloc[0].to_dict()
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
        lines.extend(_render_table_section(cache_result.cache_db, settings.cache_dir, title, table_name))

    lines.extend(["## Caveats", "", *[f"- {item}" for item in REPORT_CAVEATS], ""])
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


def _render_table_section(
    cache_db: Path,
    cache_dir: Path,
    title: str,
    table_name: str,
) -> list[str]:
    df = read_result_table(cache_db, table_name)
    csv_path = cache_dir / f"{table_name}.csv"
    lines = [f"## {title}", ""]
    if csv_path.exists():
        lines.append(f"Cached CSV: `{csv_path}`")
        lines.append("")
    lines.extend(_markdown_table(df))
    lines.append("")
    return lines


def _markdown_table(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return ["_No matching rows._"]

    columns = list(df.columns)
    lines = [
        "| " + " | ".join(_escape_markdown(column) for column in columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for _, row in df.iterrows():
        lines.append(
            "| "
            + " | ".join(_escape_markdown(_format_value(row[column])) for column in columns)
            + " |"
        )
    return lines


# --- SQL fragments shared with cached_queries ---------------------------------------


def _metric_select(prefix: str = "") -> str:
    """Robust delay metrics; column order matches the Polars ``metric_aggs``."""
    return f"""
        COUNT(*) AS {prefix}bucket_count,
        CAST(COALESCE(SUM(raw_poll_count), 0) AS BIGINT) AS {prefix}raw_poll_count,
        CAST(COUNT(DISTINCT trip_match_key) AS BIGINT) AS {prefix}trip_count,
        CAST(COUNT(DISTINCT local_date) AS BIGINT) AS {prefix}day_count,
        AVG(delay_seconds) / 60.0 AS {prefix}signed_mean_delay_min,
        AVG(delay_seconds) / 60.0 AS {prefix}avg_delay_min,
        MEDIAN(delay_seconds) / 60.0 AS {prefix}median_delay_min,
        QUANTILE_CONT(delay_seconds / 60.0, 0.75) AS {prefix}p75_delay_min,
        QUANTILE_CONT(delay_seconds / 60.0, 0.90) AS {prefix}p90_delay_min,
        QUANTILE_CONT(delay_seconds / 60.0, 0.95) AS {prefix}p95_delay_min,
        AVG(CASE WHEN delay_seconds BETWEEN -60 AND 180 THEN 1.0 ELSE 0.0 END) * 100.0
            AS {prefix}pct_on_time,
        AVG(CASE WHEN delay_seconds > 0 THEN 1.0 ELSE 0.0 END) * 100.0
            AS {prefix}pct_late,
        AVG(CASE WHEN delay_seconds > 180 THEN 1.0 ELSE 0.0 END) * 100.0
            AS {prefix}pct_over_3_min_late,
        AVG(CASE WHEN delay_seconds > 300 THEN 1.0 ELSE 0.0 END) * 100.0
            AS {prefix}pct_over_5_min_late,
        AVG(CASE WHEN delay_seconds < 0 THEN 1.0 ELSE 0.0 END) * 100.0
            AS {prefix}pct_early,
        AVG(CASE WHEN delay_seconds < -60 THEN 1.0 ELSE 0.0 END) * 100.0
            AS {prefix}pct_over_1_min_early,
        AVG(CASE WHEN delay_seconds < -180 THEN 1.0 ELSE 0.0 END) * 100.0
            AS {prefix}pct_over_3_min_early,
        COALESCE(
            QUANTILE_CONT(ABS(delay_seconds) / 60.0, 0.50)
                FILTER (WHERE delay_seconds < 0),
            0.0
        ) AS {prefix}median_early_min_abs,
        COALESCE(
            QUANTILE_CONT(ABS(delay_seconds) / 60.0, 0.90)
                FILTER (WHERE delay_seconds < 0),
            0.0
        ) AS {prefix}p90_early_min_abs
    """


def _local_time_select(timezone: str, holidays: tuple[str, ...] | list[str] = ()) -> str:
    """Local calendar columns. Reduced-service holidays count as weekend days."""
    tz = _sql_literal(timezone)
    local = f"timezone({tz}, representative_time_utc)"
    holiday_values = ", ".join(f"DATE '{date.fromisoformat(str(day)).isoformat()}'" for day in holidays)
    is_holiday = f"(CAST({local} AS DATE) IN ({holiday_values}))" if holiday_values else "FALSE"
    is_weekday = f"(EXTRACT(isodow FROM {local}) BETWEEN 1 AND 5 AND NOT {is_holiday})"
    return f"""
        {local} AS local_time,
        CAST({local} AS DATE) AS local_date,
        CAST(EXTRACT(hour FROM {local}) AS INTEGER) AS local_hour,
        CAST(EXTRACT(isodow FROM {local}) - 1 AS INTEGER) AS local_weekday,
        CAST({is_holiday} AS BOOLEAN) AS is_holiday,
        CAST({is_weekday} AS BOOLEAN) AS is_weekday,
        CASE WHEN {is_weekday} THEN 'weekday' ELSE 'weekend' END AS day_type,
        CAST(
            EXTRACT(hour FROM {local}) * 60 + EXTRACT(minute FROM {local})
            AS INTEGER
        ) AS local_minutes
    """


def _window_condition(windows: tuple[str, ...]) -> str:
    parts = []
    for window in windows:
        start, end = _parse_hhmm_window(window)
        if start < end:
            parts.append(f"(local_minutes >= {start} AND local_minutes < {end})")
        else:
            parts.append(f"(local_minutes >= {start} OR local_minutes < {end})")
    if not parts:
        return "FALSE"
    return "(" + " OR ".join(parts) + ")"


_rush_condition = _window_condition


def _parse_hhmm_window(value: str) -> tuple[int, int]:
    start_text, end_text = value.split("-", maxsplit=1)
    return _parse_hhmm(start_text), _parse_hhmm(end_text)


def _parse_hhmm(value: str) -> int:
    hour_text, minute_text = value.strip().split(":", maxsplit=1)
    hour = int(hour_text)
    minute = int(minute_text)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(value)
    return hour * 60 + minute


# --- Manifest -----------------------------------------------------------------------


def _expected_manifest(
    settings: ReportSettings,
    db_metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "cache_version": CACHE_VERSION,
        "db_metadata": db_metadata,
        "settings": _settings_manifest(settings),
    }


def _expected_base_manifest(
    settings: ReportSettings,
    db_metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "cache_version": CACHE_VERSION,
        "db_metadata": db_metadata,
        "base_settings": _base_settings_manifest(settings),
    }


def _settings_manifest(settings: ReportSettings) -> dict[str, Any]:
    return {
        "quality_mode": settings.quality_mode,
        "bucket": settings.bucket,
        "timezone": settings.timezone,
        "limit": settings.limit,
        "min_observations": settings.min_observations,
        "min_trips": settings.min_trips,
        "exclude_stop_call_disagreement": settings.exclude_stop_call_disagreement,
        "rush_windows": list(settings.rush_windows),
        "offpeak_windows": list(settings.offpeak_windows),
        "include_weekends": settings.include_weekends,
        "bootstrap_reps": settings.bootstrap_reps,
        "gtfs_dir": str(settings.gtfs_dir) if settings.gtfs_dir else None,
        "gtfs_root": str(settings.gtfs_root),
        "gtfs_calendar_metadata": calendar_fingerprint(
            gtfs_root=settings.gtfs_root,
            gtfs_dir=settings.gtfs_dir,
        ),
    }


def _base_settings_manifest(settings: ReportSettings) -> dict[str, Any]:
    return {
        "quality_mode": settings.quality_mode,
        "bucket": settings.bucket,
        "timezone": settings.timezone,
        "exclude_stop_call_disagreement": settings.exclude_stop_call_disagreement,
        "holiday_dates": list(settings.holiday_dates()),
    }


def _manifest_file_matches(
    current: dict[str, Any] | None,
    file_metadata: dict[str, Any],
) -> bool:
    if current is None:
        return False
    if current.get("cache_version") != CACHE_VERSION:
        return False
    db_metadata = current.get("db_metadata", {})
    return all(db_metadata.get(key) == value for key, value in file_metadata.items())


def _manifest_settings_match(
    current: dict[str, Any] | None,
    settings: ReportSettings,
) -> bool:
    if current is None:
        return False
    return current.get("settings") == _settings_manifest(settings)


def _manifest_base_settings_match(
    current: dict[str, Any] | None,
    settings: ReportSettings,
) -> bool:
    if current is None:
        return False
    current_base = current.get("base_settings")
    if current_base is None:
        current_settings = current.get("settings", {})
        current_base = {
            key: current_settings.get(key)
            for key in _base_settings_manifest(settings)
        }
    return current_base == _base_settings_manifest(settings)


def _current_or_collected_db_metadata(
    current: dict[str, Any] | None,
    db_path: Path,
) -> dict[str, Any]:
    if current is not None:
        metadata = current.get("db_metadata")
        if isinstance(metadata, dict) and _manifest_file_matches(
            current,
            collect_db_file_metadata(db_path),
        ):
            return metadata
    return collect_db_metadata(db_path)


def _read_manifest(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def _table_count(con: sqlite3.Connection, table_name: str) -> int:
    if not _table_exists_sqlite(con, table_name):
        return 0
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0] or 0)


def _table_exists_sqlite(con: sqlite3.Connection, table_name: str) -> bool:
    return (
        con.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            """,
            (table_name,),
        ).fetchone()
        is not None
    )


def _sqlite_table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM source_db.{_sql_identifier(table_name)} LIMIT 0")
    except duckdb.CatalogException:
        return False
    return True


def _duckdb_table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    return bool(
        con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchone()[0]
    )


def _cache_has_tables(cache_db: Path, table_names: list[str]) -> bool:
    try:
        with duckdb.connect(str(cache_db), read_only=True) as con:
            return all(_duckdb_table_exists(con, table_name) for table_name in table_names)
    except duckdb.Error:
        return False


def _drop_tables(con: duckdb.DuckDBPyConnection, table_names: list[str]) -> None:
    for table_name in table_names:
        con.execute(f"DROP TABLE IF EXISTS {_sql_identifier(table_name)}")


def _create_empty_table(con: duckdb.DuckDBPyConnection, table_name: str) -> None:
    con.execute(f"CREATE TABLE {table_name} AS SELECT NULL::VARCHAR AS note WHERE FALSE")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _format_value(value: object) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(value, float):
        return f"{value:.2f}"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _format_int(value: object) -> str:
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "0"


def _format_seconds(value: object) -> str:
    if value is None:
        return "not recorded"
    try:
        return f"{float(value):.2f}s"
    except (TypeError, ValueError):
        return "not recorded"


def _escape_markdown(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")
