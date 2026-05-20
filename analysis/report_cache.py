from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

try:
    from ._shared import (
        BUCKET_MODES,
        DEFAULT_BUCKET_MODE,
        DEFAULT_DB_PATH,
        DEFAULT_QUALITY_MODE,
        DEFAULT_RUSH_WINDOWS,
        DEFAULT_TIMEZONE,
        QUALITY_MODES,
        latest_gtfs_dir,
        resolve_project_path,
    )
except ImportError:  # pragma: no cover - used when called as analysis/*.py script.
    from _shared import (
        BUCKET_MODES,
        DEFAULT_BUCKET_MODE,
        DEFAULT_DB_PATH,
        DEFAULT_QUALITY_MODE,
        DEFAULT_RUSH_WINDOWS,
        DEFAULT_TIMEZONE,
        QUALITY_MODES,
        latest_gtfs_dir,
        resolve_project_path,
    )


CACHE_VERSION = 1
DEFAULT_CACHE_DIR = Path("outputs/report-cache")
DEFAULT_REPORT_PATH = Path("reports/generated/overall-results.md")
MANIFEST_NAME = "manifest.json"
CACHE_DB_NAME = "report-cache.duckdb"

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
        for window in self.rush_windows:
            _parse_hhmm_window(window)


@dataclass(frozen=True)
class CacheResult:
    status: str
    cache_db: Path
    manifest: dict[str, Any]


def ensure_report_cache(settings: ReportSettings, *, force: bool = False) -> CacheResult:
    settings = settings.resolved()
    settings.validate()
    settings.cache_dir.mkdir(parents=True, exist_ok=True)

    db_metadata = collect_db_metadata(settings.db)
    expected = _expected_manifest(settings, db_metadata)
    manifest_path = settings.cache_dir / MANIFEST_NAME
    cache_db = settings.cache_dir / CACHE_DB_NAME

    current = _read_manifest(manifest_path)
    if not force and cache_db.exists() and _manifest_matches(current, expected):
        return CacheResult(status="reused", cache_db=cache_db, manifest=current)

    if cache_db.exists():
        cache_db.unlink()
    _build_cache(settings, cache_db)

    manifest = {
        **expected,
        "built_at_utc": _utc_now_iso(),
        "cache_db": str(cache_db),
        "result_tables": RESULT_TABLES,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return CacheResult(status="rebuilt", cache_db=cache_db, manifest=manifest)


def collect_db_metadata(db_path: Path) -> dict[str, Any]:
    db_path = resolve_project_path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    stat = db_path.stat()
    with sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True) as con:
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
        "db_path": str(db_path),
        "db_size_bytes": stat.st_size,
        "db_mtime_ns": stat.st_mtime_ns,
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


def read_result_table(cache_db: Path, table_name: str) -> pd.DataFrame:
    with duckdb.connect(str(cache_db), read_only=True) as con:
        if not _duckdb_table_exists(con, table_name):
            return pd.DataFrame()
        return con.execute(f"SELECT * FROM {table_name}").fetchdf()


def _build_cache(settings: ReportSettings, cache_db: Path) -> None:
    with duckdb.connect(str(cache_db)) as con:
        con.execute("LOAD sqlite")
        con.execute(
            f"ATTACH {_sql_literal(settings.db.as_posix())} AS source_db "
            "(TYPE SQLITE, READ_ONLY)"
        )
        _build_quality_rows(con, settings)
        _build_delay_buckets(con, settings)
        _build_delay_cache_summary(con)
        _build_quality_results(con, settings)
        _build_delay_metric_results(con, settings)
        _build_rush_impact(con, settings)
        _build_stop_midpoint_change(con, settings)
        _build_collector_results(con, settings)
        _build_alert_targets(con, settings)
        _build_service_alert_results(con, settings)
        _export_result_csvs(con, settings.cache_dir)


def _build_quality_rows(con: duckdb.DuckDBPyConnection, settings: ReportSettings) -> None:
    quality_pass = "TRUE"
    if settings.quality_mode == "conservative":
        exclusions = [
            "is_implausible_delay",
            "is_stale_observation",
            "is_pre_trip_observation",
            "is_post_trip_observation",
        ]
        if settings.exclude_stop_call_disagreement:
            exclusions.append("has_stop_call_disagreement")
        quality_pass = "NOT (" + " OR ".join(exclusions) + ")"

    con.execute(
        f"""
        CREATE TABLE quality_rows AS
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
        ),
        flagged AS (
            SELECT
                *,
                ABS(stop_call_delay_seconds - delay_seconds)
                    AS stop_call_delay_diff_seconds,
                COALESCE(ABS(delay_seconds) > 7200, FALSE) AS is_implausible_delay,
                COALESCE(observation_age_seconds > 300, FALSE)
                    OR COALESCE(valid_until_utc < quality_collected_at_utc, FALSE)
                    AS is_stale_observation,
                COALESCE(
                    recorded_at_utc
                        < origin_aimed_departure_time_utc - INTERVAL '15 minutes',
                    FALSE
                ) AS is_pre_trip_observation,
                COALESCE(
                    recorded_at_utc
                        > destination_aimed_arrival_time_utc + INTERVAL '30 minutes',
                    FALSE
                ) AS is_post_trip_observation,
                COALESCE(ABS(stop_call_delay_seconds - delay_seconds) > 600, FALSE)
                    AS has_stop_call_disagreement
            FROM measured
        )
        SELECT
            *,
            (
                CAST(is_implausible_delay AS INTEGER)
                + CAST(is_stale_observation AS INTEGER)
                + CAST(is_pre_trip_observation AS INTEGER)
                + CAST(is_post_trip_observation AS INTEGER)
                + CAST(has_stop_call_disagreement AS INTEGER)
            ) AS quality_issue_count,
            {quality_pass} AS quality_pass
        FROM flagged
        """
    )


def _build_delay_buckets(con: duckdb.DuckDBPyConnection, settings: ReportSettings) -> None:
    if settings.bucket == "poll":
        con.execute(
            f"""
            CREATE TABLE delay_buckets AS
            SELECT
                COALESCE(CAST(id AS VARCHAR), CAST(row_number() OVER () AS VARCHAR))
                    AS bucket_id,
                'poll' AS bucket_mode,
                CAST(line_ref AS VARCHAR) AS line_ref,
                COALESCE(CAST(direction_ref AS VARCHAR), 'Unknown') AS direction_ref,
                COALESCE(CAST(published_line_name AS VARCHAR), CAST(line_ref AS VARCHAR))
                    AS published_line_name,
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
                {_local_time_select(settings.timezone)}
            FROM quality_rows
            WHERE quality_pass
                AND delay_seconds IS NOT NULL
                AND recorded_at_utc IS NOT NULL
                AND line_ref IS NOT NULL
            """
        )
        return

    if settings.bucket == "trip-stop":
        group_keys = [
            "trip_match_key",
            "vehicle_id",
            "line_ref",
            "direction_ref",
            "next_stop_point_ref",
        ]
    elif settings.bucket == "vehicle-trip":
        group_keys = ["trip_match_key", "vehicle_id", "line_ref", "direction_ref"]
    else:
        group_keys = ["line_ref", "direction_ref", "local_date", "local_hour", "day_type"]

    base = f"""
        SELECT
            id,
            trip_match_key,
            vehicle_id,
            CAST(line_ref AS VARCHAR) AS line_ref,
            COALESCE(CAST(direction_ref AS VARCHAR), 'Unknown') AS direction_ref,
            CAST(next_stop_point_ref AS VARCHAR) AS next_stop_point_ref,
            COALESCE(CAST(published_line_name AS VARCHAR), CAST(line_ref AS VARCHAR))
                AS published_line_name,
            CAST(next_stop_point_name AS VARCHAR) AS next_stop_point_name,
            delay_seconds,
            recorded_at_utc,
            COALESCE(next_aimed_arrival_time_utc, recorded_at_utc)
                AS representative_time_utc
        FROM quality_rows
        WHERE quality_pass
            AND delay_seconds IS NOT NULL
            AND recorded_at_utc IS NOT NULL
            AND line_ref IS NOT NULL
    """
    if settings.bucket == "line-hour":
        base = f"""
            SELECT *, {_local_time_select(settings.timezone)}
            FROM ({base}) base_rows
            WHERE representative_time_utc IS NOT NULL
        """

    group_sql = ", ".join(group_keys)
    bucket_id_sql = " || '|' || ".join(
        f"COALESCE(CAST({column} AS VARCHAR), '<NA>')" for column in group_keys
    )
    con.execute(
        f"""
        CREATE TABLE delay_buckets AS
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
            {_sql_literal(settings.bucket)} AS bucket_mode,
            line_ref,
            direction_ref,
            published_line_name,
            delay_seconds,
            delay_seconds / 60.0 AS delay_min,
            raw_poll_count,
            {'next_stop_point_ref' if 'next_stop_point_ref' in group_keys else 'NULL::VARCHAR'}
                AS next_stop_point_ref,
            next_stop_point_name,
            representative_time_utc,
            recorded_at_utc,
            first_recorded_at_utc,
            last_recorded_at_utc,
            {_local_time_select(settings.timezone)}
        FROM grouped
        """
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


def _build_quality_results(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
) -> None:
    con.execute(
        """
        CREATE TABLE quality_summary AS
        WITH totals AS (
            SELECT COUNT(*)::DOUBLE AS total_rows
            FROM quality_rows
        ),
        checks AS (
            SELECT 'analysis_rows' AS quality_check, COUNT(*)::DOUBLE AS row_count
            FROM quality_rows
            UNION ALL
            SELECT 'is_implausible_delay', SUM(CAST(is_implausible_delay AS INTEGER))
            FROM quality_rows
            UNION ALL
            SELECT 'is_stale_observation', SUM(CAST(is_stale_observation AS INTEGER))
            FROM quality_rows
            UNION ALL
            SELECT 'is_pre_trip_observation', SUM(CAST(is_pre_trip_observation AS INTEGER))
            FROM quality_rows
            UNION ALL
            SELECT 'is_post_trip_observation', SUM(CAST(is_post_trip_observation AS INTEGER))
            FROM quality_rows
            UNION ALL
            SELECT 'has_stop_call_disagreement',
                SUM(CAST(has_stop_call_disagreement AS INTEGER))
            FROM quality_rows
            UNION ALL
            SELECT 'conservative_excluded_default',
                SUM(CAST(
                    is_implausible_delay
                    OR is_stale_observation
                    OR is_pre_trip_observation
                    OR is_post_trip_observation
                    AS INTEGER
                ))
            FROM quality_rows
            UNION ALL
            SELECT 'conservative_excluded_with_stop_call_disagreement',
                SUM(CAST(
                    is_implausible_delay
                    OR is_stale_observation
                    OR is_pre_trip_observation
                    OR is_post_trip_observation
                    OR has_stop_call_disagreement
                    AS INTEGER
                ))
            FROM quality_rows
        )
        SELECT
            quality_check,
            CAST(row_count AS BIGINT) AS row_count,
            CASE
                WHEN total_rows > 0 THEN row_count / total_rows * 100.0
                ELSE 0.0
            END AS pct_rows
        FROM checks, totals
        """
    )
    con.execute(
        f"""
        CREATE TABLE quality_by_line AS
        SELECT
            CAST(line_ref AS VARCHAR) AS line_ref,
            FIRST(published_line_name ORDER BY recorded_at_utc) AS line_name,
            COUNT(*) AS row_count,
            SUM(CAST(is_implausible_delay AS INTEGER)) AS implausible_delay_rows,
            SUM(CAST(is_stale_observation AS INTEGER)) AS stale_rows,
            SUM(CAST(is_pre_trip_observation AS INTEGER)) AS pre_trip_rows,
            SUM(CAST(is_post_trip_observation AS INTEGER)) AS post_trip_rows,
            SUM(CAST(has_stop_call_disagreement AS INTEGER))
                AS stop_call_disagreement_rows,
            SUM(CAST(
                is_implausible_delay
                OR is_stale_observation
                OR is_pre_trip_observation
                OR is_post_trip_observation
                AS INTEGER
            )) AS conservative_excluded_rows,
            conservative_excluded_rows / COUNT(*) * 100.0
                AS conservative_excluded_pct
        FROM quality_rows
        GROUP BY line_ref
        HAVING COUNT(*) >= {settings.min_observations}
        ORDER BY conservative_excluded_pct DESC, conservative_excluded_rows DESC, line_ref
        LIMIT {settings.limit}
        """
    )


def _build_delay_metric_results(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
) -> None:
    con.execute(
        f"""
        CREATE TABLE line_metrics AS
        SELECT
            line_ref,
            FIRST(published_line_name ORDER BY representative_time_utc) AS line_name,
            {_metric_select()}
        FROM delay_buckets
        GROUP BY line_ref
        HAVING COUNT(*) >= {settings.min_observations}
        """
    )
    con.execute(
        f"""
        CREATE TABLE line_late_rankings AS
        SELECT
            line_ref,
            line_name,
            bucket_count,
            raw_poll_count,
            signed_mean_delay_min,
            median_delay_min,
            p75_delay_min,
            p90_delay_min,
            p95_delay_min,
            pct_over_3_min_late,
            pct_over_5_min_late
        FROM line_metrics
        ORDER BY p90_delay_min DESC, pct_over_5_min_late DESC, bucket_count DESC, line_ref
        LIMIT {settings.limit}
        """
    )
    con.execute(
        f"""
        CREATE TABLE line_early_rankings AS
        SELECT
            line_ref,
            line_name,
            bucket_count,
            raw_poll_count,
            signed_mean_delay_min,
            median_delay_min,
            pct_early,
            pct_over_1_min_early,
            pct_over_3_min_early,
            median_early_min_abs,
            p90_early_min_abs
        FROM line_metrics
        ORDER BY p90_early_min_abs DESC, pct_over_3_min_early DESC, bucket_count DESC, line_ref
        LIMIT {settings.limit}
        """
    )
    con.execute(
        f"""
        CREATE TABLE context_delay_metrics AS
        SELECT
            line_ref,
            FIRST(published_line_name ORDER BY representative_time_utc) AS line_name,
            direction_ref,
            LPAD(CAST(local_hour AS VARCHAR), 2, '0') || ':00' AS hour_local,
            day_type,
            {_metric_select()}
        FROM delay_buckets
        GROUP BY line_ref, direction_ref, local_hour, day_type
        HAVING COUNT(*) >= {settings.min_observations}
        ORDER BY p90_delay_min DESC, pct_over_5_min_late DESC, bucket_count DESC
        LIMIT {settings.limit}
        """
    )
    con.execute(
        f"""
        CREATE TABLE hourly_delay_profile AS
        SELECT
            LPAD(CAST(local_hour AS VARCHAR), 2, '0') || ':00' AS hour_local,
            {_metric_select()}
        FROM delay_buckets
        GROUP BY local_hour
        HAVING COUNT(*) >= {settings.min_observations}
        ORDER BY local_hour
        """
    )


def _build_rush_impact(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
) -> None:
    rush_condition = _rush_condition(settings.rush_windows)
    if not settings.include_weekends:
        rush_condition = f"({rush_condition}) AND is_weekday"
    con.execute(
        f"""
        CREATE TABLE rush_impact AS
        WITH marked AS (
            SELECT *, {rush_condition} AS is_rush
            FROM delay_buckets
        ),
        grouped AS (
            SELECT
                line_ref,
                is_rush,
                FIRST(published_line_name ORDER BY representative_time_utc) AS line_name,
                {_metric_select()}
            FROM marked
            GROUP BY line_ref, is_rush
            HAVING COUNT(*) >= {settings.min_observations}
        ),
        non_rush AS (
            SELECT * FROM grouped WHERE NOT is_rush
        ),
        rush AS (
            SELECT * FROM grouped WHERE is_rush
        )
        SELECT
            rush.line_ref,
            COALESCE(rush.line_name, non_rush.line_name) AS line_name,
            non_rush.bucket_count AS bucket_count_non_rush,
            rush.bucket_count AS bucket_count_rush,
            non_rush.raw_poll_count AS raw_poll_count_non_rush,
            rush.raw_poll_count AS raw_poll_count_rush,
            non_rush.median_delay_min AS median_delay_min_non_rush,
            rush.median_delay_min AS median_delay_min_rush,
            rush.median_delay_min - non_rush.median_delay_min
                AS rush_median_delay_lift_min,
            non_rush.p90_delay_min AS p90_delay_min_non_rush,
            rush.p90_delay_min AS p90_delay_min_rush,
            rush.p90_delay_min - non_rush.p90_delay_min AS rush_p90_delay_lift_min,
            non_rush.pct_over_5_min_late AS pct_over_5_min_late_non_rush,
            rush.pct_over_5_min_late AS pct_over_5_min_late_rush,
            rush.pct_over_5_min_late - non_rush.pct_over_5_min_late
                AS rush_over_5_min_late_pct_point_lift
        FROM rush
        JOIN non_rush ON non_rush.line_ref = rush.line_ref
        ORDER BY rush_p90_delay_lift_min DESC,
            rush_over_5_min_late_pct_point_lift DESC,
            bucket_count_rush DESC
        LIMIT {settings.limit}
        """
    )


def _build_stop_midpoint_change(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
) -> None:
    start_ms, end_ms = con.execute(
        """
        SELECT
            MIN(EPOCH_MS(representative_time_utc)),
            MAX(EPOCH_MS(representative_time_utc))
        FROM delay_buckets
        WHERE representative_time_utc IS NOT NULL
        """
    ).fetchone()
    if start_ms is None or end_ms is None or start_ms == end_ms:
        con.execute(
            """
            CREATE TABLE stop_midpoint_change (
                stop_id VARCHAR,
                stop_name VARCHAR,
                baseline_bucket_count BIGINT,
                comparison_bucket_count BIGINT,
                p90_delay_change_min DOUBLE
            )
            """
        )
        con.execute(
            """
            CREATE TABLE midpoint_summary (
                baseline_start_utc VARCHAR,
                baseline_end_utc VARCHAR,
                comparison_start_utc VARCHAR,
                comparison_end_utc VARCHAR
            )
            """
        )
        return

    midpoint_ms = int(start_ms + ((end_ms - start_ms) / 2))
    con.execute(
        f"""
        CREATE TABLE midpoint_summary AS
        SELECT
            STRFTIME(MAKE_TIMESTAMP_MS({int(start_ms)}), '%Y-%m-%dT%H:%M:%SZ')
                AS baseline_start_utc,
            STRFTIME(MAKE_TIMESTAMP_MS({midpoint_ms}), '%Y-%m-%dT%H:%M:%SZ')
                AS baseline_end_utc,
            STRFTIME(MAKE_TIMESTAMP_MS({midpoint_ms}), '%Y-%m-%dT%H:%M:%SZ')
                AS comparison_start_utc,
            STRFTIME(MAKE_TIMESTAMP_MS({int(end_ms)}), '%Y-%m-%dT%H:%M:%SZ')
                AS comparison_end_utc
        """
    )
    con.execute(
        f"""
        CREATE TABLE stop_midpoint_change AS
        WITH period_rows AS (
            SELECT
                *,
                next_stop_point_ref AS stop_id,
                CASE
                    WHEN EPOCH_MS(representative_time_utc) >= {int(start_ms)}
                        AND EPOCH_MS(representative_time_utc) < {midpoint_ms}
                    THEN 'baseline'
                    WHEN EPOCH_MS(representative_time_utc) >= {midpoint_ms}
                        AND EPOCH_MS(representative_time_utc) <= {int(end_ms)}
                    THEN 'comparison'
                    ELSE NULL
                END AS period
            FROM delay_buckets
            WHERE next_stop_point_ref IS NOT NULL
        ),
        baseline_contexts AS (
            SELECT DISTINCT stop_id, line_ref, direction_ref, local_weekday, local_hour
            FROM period_rows
            WHERE period = 'baseline'
        ),
        comparison_contexts AS (
            SELECT DISTINCT stop_id, line_ref, direction_ref, local_weekday, local_hour
            FROM period_rows
            WHERE period = 'comparison'
        ),
        matched_contexts AS (
            SELECT baseline_contexts.*
            FROM baseline_contexts
            JOIN comparison_contexts USING (
                stop_id,
                line_ref,
                direction_ref,
                local_weekday,
                local_hour
            )
        ),
        matched AS (
            SELECT period_rows.*
            FROM period_rows
            JOIN matched_contexts USING (
                stop_id,
                line_ref,
                direction_ref,
                local_weekday,
                local_hour
            )
            WHERE period IS NOT NULL
        ),
        baseline AS (
            SELECT
                stop_id,
                FIRST(next_stop_point_name ORDER BY representative_time_utc) AS stop_name,
                {_metric_select(prefix="baseline_")}
            FROM matched
            WHERE period = 'baseline'
            GROUP BY stop_id
            HAVING COUNT(*) >= {settings.min_observations}
        ),
        comparison AS (
            SELECT
                stop_id,
                FIRST(next_stop_point_name ORDER BY representative_time_utc) AS stop_name,
                {_metric_select(prefix="comparison_")}
            FROM matched
            WHERE period = 'comparison'
            GROUP BY stop_id
            HAVING COUNT(*) >= {settings.min_observations}
        )
        SELECT
            baseline.stop_id,
            COALESCE(comparison.stop_name, baseline.stop_name) AS stop_name,
            baseline.baseline_bucket_count,
            comparison.comparison_bucket_count,
            baseline.baseline_raw_poll_count,
            comparison.comparison_raw_poll_count,
            baseline.baseline_median_delay_min,
            comparison.comparison_median_delay_min,
            comparison.comparison_median_delay_min - baseline.baseline_median_delay_min
                AS median_delay_change_min,
            baseline.baseline_p90_delay_min,
            comparison.comparison_p90_delay_min,
            comparison.comparison_p90_delay_min - baseline.baseline_p90_delay_min
                AS p90_delay_change_min,
            baseline.baseline_pct_over_5_min_late,
            comparison.comparison_pct_over_5_min_late,
            comparison.comparison_pct_over_5_min_late
                - baseline.baseline_pct_over_5_min_late
                AS over_5_min_late_pct_point_change,
            baseline.baseline_pct_over_3_min_early,
            comparison.comparison_pct_over_3_min_early
        FROM baseline
        JOIN comparison USING (stop_id)
        ORDER BY ABS(p90_delay_change_min) DESC, comparison_bucket_count DESC
        LIMIT {settings.limit}
        """
    )


def _build_collector_results(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
) -> None:
    if not _sqlite_table_exists(con, "collector_polls"):
        _create_empty_table(con, "collector_blackouts")
        _create_empty_table(con, "collector_missing_summary")
        _create_empty_table(con, "collector_missing_spots")
        return

    con.execute(
        f"""
        CREATE TABLE collector_blackouts AS
        WITH successful_gaps AS (
            SELECT
                source,
                TRY_CAST(gap_seconds_since_previous_success AS DOUBLE) AS gap_seconds,
                TRY_CAST(row_count AS DOUBLE) AS row_count
            FROM source_db.collector_polls
            WHERE ok = 1 AND gap_seconds_since_previous_success > 0
        ),
        cadence AS (
            SELECT
                source,
                MEDIAN(gap_seconds) AS expected_cadence_seconds,
                AVG(row_count) AS avg_rows_per_success
            FROM successful_gaps
            GROUP BY source
        ),
        source_counts AS (
            SELECT
                source,
                COUNT(*) AS poll_count,
                SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN ok = 1 THEN 0 ELSE 1 END) AS failed_count
            FROM source_db.collector_polls
            GROUP BY source
        ),
        blackout_rows AS (
            SELECT
                successful_gaps.source,
                GREATEST(0.0, gap_seconds - expected_cadence_seconds)
                    AS blackout_seconds,
                GREATEST(0.0, gap_seconds - expected_cadence_seconds)
                    / expected_cadence_seconds AS estimated_missed_polls,
                GREATEST(0.0, gap_seconds - expected_cadence_seconds)
                    / expected_cadence_seconds * avg_rows_per_success
                    AS estimated_missed_rows
            FROM successful_gaps
            JOIN cadence USING (source)
            WHERE gap_seconds > 2 * expected_cadence_seconds
        )
        SELECT
            source_counts.source,
            source_counts.poll_count,
            source_counts.success_count,
            source_counts.failed_count,
            cadence.expected_cadence_seconds,
            COUNT(blackout_rows.source) AS blackout_count,
            COALESCE(SUM(blackout_seconds), 0.0) / 60.0 AS total_blackout_min,
            COALESCE(MAX(blackout_seconds), 0.0) / 60.0 AS largest_blackout_min,
            COALESCE(SUM(estimated_missed_polls), 0.0) AS estimated_missed_polls,
            COALESCE(SUM(estimated_missed_rows), 0.0) AS estimated_missed_rows
        FROM source_counts
        LEFT JOIN cadence USING (source)
        LEFT JOIN blackout_rows USING (source)
        GROUP BY
            source_counts.source,
            source_counts.poll_count,
            source_counts.success_count,
            source_counts.failed_count,
            cadence.expected_cadence_seconds
        ORDER BY total_blackout_min DESC, blackout_count DESC, source
        LIMIT {settings.limit}
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE collector_success_gaps AS
        WITH successful AS (
            SELECT
                source,
                TRY_CAST(collected_at_utc AS TIMESTAMPTZ) AS collected_at_utc,
                status,
                TRY_CAST(row_count AS DOUBLE) AS row_count,
                LAG(TRY_CAST(collected_at_utc AS TIMESTAMPTZ))
                    OVER (
                        PARTITION BY source
                        ORDER BY TRY_CAST(collected_at_utc AS TIMESTAMPTZ)
                    ) AS previous_success_at
            FROM source_db.collector_polls
            WHERE ok = 1 AND collected_at_utc IS NOT NULL
        ),
        cadence AS (
            SELECT
                source,
                MEDIAN(TRY_CAST(gap_seconds_since_previous_success AS DOUBLE))
                    AS expected_cadence_seconds,
                AVG(TRY_CAST(row_count AS DOUBLE)) AS avg_rows_per_success
            FROM source_db.collector_polls
            WHERE ok = 1 AND gap_seconds_since_previous_success > 0
            GROUP BY source
        )
        SELECT
            successful.source,
            previous_success_at AS gap_start_utc,
            collected_at_utc AS gap_end_utc,
            DATE_DIFF('second', previous_success_at, collected_at_utc)::DOUBLE
                AS gap_seconds,
            expected_cadence_seconds,
            GREATEST(
                0.0,
                DATE_DIFF('second', previous_success_at, collected_at_utc)::DOUBLE
                    - expected_cadence_seconds
            ) AS missing_seconds,
            status AS next_success_status,
            avg_rows_per_success
        FROM successful
        JOIN cadence USING (source)
        WHERE previous_success_at IS NOT NULL
            AND DATE_DIFF('second', previous_success_at, collected_at_utc)::DOUBLE
                > 2 * expected_cadence_seconds
        """
    )
    con.execute(
        f"""
        CREATE TABLE collector_missing_spots AS
        SELECT
            source,
            STRFTIME(
                MAKE_TIMESTAMP_MS(EPOCH_MS(gap_start_utc)),
                '%Y-%m-%dT%H:%M:%SZ'
            ) AS gap_start_utc,
            STRFTIME(
                MAKE_TIMESTAMP_MS(EPOCH_MS(gap_end_utc)),
                '%Y-%m-%dT%H:%M:%SZ'
            ) AS gap_end_utc,
            gap_seconds / 60.0 AS gap_min,
            expected_cadence_seconds,
            missing_seconds / 60.0 AS missing_min,
            missing_seconds / expected_cadence_seconds AS estimated_missed_polls,
            missing_seconds / expected_cadence_seconds * avg_rows_per_success
                AS estimated_missed_rows,
            next_success_status
        FROM collector_success_gaps
        ORDER BY missing_min DESC, source
        LIMIT {settings.limit}
        """
    )
    con.execute(
        """
        CREATE TABLE collector_missing_summary AS
        WITH source_counts AS (
            SELECT
                source,
                COUNT(*) AS poll_count,
                SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END) AS success_count,
                SUM(CASE WHEN ok = 1 THEN 0 ELSE 1 END) AS failed_count
            FROM source_db.collector_polls
            GROUP BY source
        ),
        spot_summary AS (
            SELECT
                source,
                COUNT(*) AS missing_spot_count,
                SUM(missing_seconds) / 60.0 AS total_missing_min,
                MAX(missing_seconds) / 60.0 AS largest_missing_min,
                SUM(missing_seconds / expected_cadence_seconds)
                    AS estimated_missed_polls,
                SUM(missing_seconds / expected_cadence_seconds * avg_rows_per_success)
                    AS estimated_missed_rows
            FROM collector_success_gaps
            GROUP BY source
        )
        SELECT
            source_counts.source,
            source_counts.poll_count,
            source_counts.success_count,
            source_counts.failed_count,
            COALESCE(spot_summary.missing_spot_count, 0) AS missing_spot_count,
            COALESCE(spot_summary.total_missing_min, 0.0) AS total_missing_min,
            COALESCE(spot_summary.largest_missing_min, 0.0) AS largest_missing_min,
            COALESCE(spot_summary.estimated_missed_polls, 0.0)
                AS estimated_missed_polls,
            COALESCE(spot_summary.estimated_missed_rows, 0.0)
                AS estimated_missed_rows
        FROM source_counts
        LEFT JOIN spot_summary USING (source)
        ORDER BY total_missing_min DESC, missing_spot_count DESC, source
        """
    )


def _build_alert_targets(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
) -> None:
    obs_start, obs_end = con.execute(
        """
        SELECT observation_start_utc, observation_end_utc
        FROM delay_cache_summary
        """
    ).fetchone()
    targets = _load_alert_targets(settings, obs_start, obs_end)
    if targets.empty:
        con.execute(
            """
            CREATE TABLE alert_targets (
                cause VARCHAR,
                effect VARCHAR,
                priority INTEGER,
                alert_scope VARCHAR,
                target_ref VARCHAR,
                start_utc TIMESTAMPTZ,
                end_utc TIMESTAMPTZ
            )
            """
        )
        return

    con.register("alert_targets_df", targets)
    con.execute(
        """
        CREATE TABLE alert_targets AS
        SELECT DISTINCT
            CAST(cause AS VARCHAR) AS cause,
            CAST(effect AS VARCHAR) AS effect,
            CAST(priority AS INTEGER) AS priority,
            CAST(alert_scope AS VARCHAR) AS alert_scope,
            CAST(target_ref AS VARCHAR) AS target_ref,
            TRY_CAST(start_utc AS TIMESTAMPTZ) AS start_utc,
            TRY_CAST(end_utc AS TIMESTAMPTZ) AS end_utc
        FROM alert_targets_df
        WHERE target_ref IS NOT NULL
            AND start_utc IS NOT NULL
            AND end_utc IS NOT NULL
        """
    )
    con.unregister("alert_targets_df")


def _build_service_alert_results(
    con: duckdb.DuckDBPyConnection,
    settings: ReportSettings,
) -> None:
    if con.execute("SELECT COUNT(*) FROM alert_targets").fetchone()[0] == 0:
        _create_empty_table(con, "service_alert_grouped")
        _create_empty_table(con, "service_alert_by_line")
        return

    con.execute(
        """
        CREATE TEMP TABLE alert_active_buckets AS
        SELECT DISTINCT
            target.cause,
            target.effect,
            target.priority,
            target.alert_scope,
            bucket.bucket_id
        FROM alert_targets target
        JOIN delay_buckets bucket
            ON bucket.representative_time_utc >= target.start_utc
            AND bucket.representative_time_utc <= target.end_utc
            AND (
                (target.alert_scope = 'route' AND bucket.line_ref = target.target_ref)
                OR (
                    target.alert_scope = 'stop'
                    AND bucket.next_stop_point_ref = target.target_ref
                )
            )
        """
    )
    if con.execute("SELECT COUNT(*) FROM alert_active_buckets").fetchone()[0] == 0:
        _create_empty_table(con, "service_alert_grouped")
        _create_empty_table(con, "service_alert_by_line")
        return

    group_cols = "cause, effect, priority, alert_scope"
    active_group_cols = "active.cause, active.effect, active.priority, active.alert_scope"
    context_group_cols = (
        "contexts.cause, contexts.effect, contexts.priority, contexts.alert_scope"
    )
    con.execute(
        f"""
        CREATE TEMP TABLE alert_active AS
        SELECT {active_group_cols}, bucket.*
        FROM alert_active_buckets active
        JOIN delay_buckets bucket USING (bucket_id)
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE alert_contexts AS
        SELECT DISTINCT
            cause,
            effect,
            priority,
            alert_scope,
            line_ref,
            direction_ref,
            local_hour,
            day_type
        FROM alert_active
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE alert_controls AS
        SELECT
            {context_group_cols},
            bucket.*
        FROM alert_contexts contexts
        JOIN delay_buckets bucket
            ON bucket.line_ref = contexts.line_ref
            AND bucket.direction_ref = contexts.direction_ref
            AND bucket.local_hour = contexts.local_hour
            AND bucket.day_type = contexts.day_type
        LEFT JOIN alert_active_buckets active
            ON active.bucket_id = bucket.bucket_id
            AND active.cause = contexts.cause
            AND active.effect = contexts.effect
            AND active.priority = contexts.priority
            AND active.alert_scope = contexts.alert_scope
        WHERE active.bucket_id IS NULL
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE alert_active_metrics AS
        SELECT {group_cols}, {_metric_select("alert_")}
        FROM alert_active
        GROUP BY {group_cols}
        HAVING COUNT(*) >= {settings.min_observations}
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE alert_control_metrics AS
        SELECT {group_cols}, {_metric_select("control_")}
        FROM alert_controls
        GROUP BY {group_cols}
        HAVING COUNT(*) >= {settings.min_observations}
        """
    )
    con.execute(
        f"""
        CREATE TABLE service_alert_grouped AS
        SELECT
            active.cause,
            active.effect,
            active.priority,
            active.alert_scope,
            control.control_bucket_count AS bucket_count_control,
            active.alert_bucket_count AS bucket_count_alert,
            control.control_raw_poll_count AS raw_poll_count_control,
            active.alert_raw_poll_count AS raw_poll_count_alert,
            control.control_median_delay_min AS median_delay_min_control,
            active.alert_median_delay_min AS median_delay_min_alert,
            active.alert_median_delay_min - control.control_median_delay_min
                AS median_delay_lift_min,
            control.control_p90_delay_min AS p90_delay_min_control,
            active.alert_p90_delay_min AS p90_delay_min_alert,
            active.alert_p90_delay_min - control.control_p90_delay_min
                AS p90_delay_lift_min,
            control.control_pct_over_5_min_late AS pct_over_5_min_late_control,
            active.alert_pct_over_5_min_late AS pct_over_5_min_late_alert,
            active.alert_pct_over_5_min_late - control.control_pct_over_5_min_late
                AS over_5_min_late_pct_point_lift,
            control.control_pct_over_3_min_early AS pct_over_3_min_early_control,
            active.alert_pct_over_3_min_early AS pct_over_3_min_early_alert
        FROM alert_active_metrics active
        JOIN alert_control_metrics control USING ({group_cols})
        ORDER BY p90_delay_lift_min DESC,
            over_5_min_late_pct_point_lift DESC,
            bucket_count_alert DESC
        LIMIT {settings.limit}
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE alert_active_line_metrics AS
        SELECT
            {group_cols},
            line_ref,
            FIRST(published_line_name ORDER BY representative_time_utc) AS line_name,
            {_metric_select("alert_")}
        FROM alert_active
        GROUP BY {group_cols}, line_ref
        HAVING COUNT(*) >= {settings.min_observations}
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE alert_control_line_metrics AS
        SELECT
            {group_cols},
            line_ref,
            FIRST(published_line_name ORDER BY representative_time_utc) AS line_name,
            {_metric_select("control_")}
        FROM alert_controls
        GROUP BY {group_cols}, line_ref
        HAVING COUNT(*) >= {settings.min_observations}
        """
    )
    con.execute(
        f"""
        CREATE TABLE service_alert_by_line AS
        SELECT
            active.cause,
            active.effect,
            active.priority,
            active.alert_scope,
            active.line_ref,
            COALESCE(active.line_name, control.line_name) AS line_name,
            control.control_bucket_count AS bucket_count_control,
            active.alert_bucket_count AS bucket_count_alert,
            control.control_raw_poll_count AS raw_poll_count_control,
            active.alert_raw_poll_count AS raw_poll_count_alert,
            control.control_median_delay_min AS median_delay_min_control,
            active.alert_median_delay_min AS median_delay_min_alert,
            active.alert_median_delay_min - control.control_median_delay_min
                AS median_delay_lift_min,
            control.control_p90_delay_min AS p90_delay_min_control,
            active.alert_p90_delay_min AS p90_delay_min_alert,
            active.alert_p90_delay_min - control.control_p90_delay_min
                AS p90_delay_lift_min,
            control.control_pct_over_5_min_late AS pct_over_5_min_late_control,
            active.alert_pct_over_5_min_late AS pct_over_5_min_late_alert,
            active.alert_pct_over_5_min_late - control.control_pct_over_5_min_late
                AS over_5_min_late_pct_point_lift,
            control.control_pct_over_3_min_early AS pct_over_3_min_early_control,
            active.alert_pct_over_3_min_early AS pct_over_3_min_early_alert
        FROM alert_active_line_metrics active
        JOIN alert_control_line_metrics control
            USING ({group_cols}, line_ref)
        ORDER BY p90_delay_lift_min DESC,
            over_5_min_late_pct_point_lift DESC,
            bucket_count_alert DESC
        LIMIT {settings.limit}
        """
    )


def _export_result_csvs(con: duckdb.DuckDBPyConnection, cache_dir: Path) -> None:
    for table_name in RESULT_TABLES:
        if not _duckdb_table_exists(con, table_name):
            continue
        output_path = cache_dir / f"{table_name}.csv"
        con.execute(
            f"COPY (SELECT * FROM {table_name}) TO "
            f"{_sql_literal(output_path.as_posix())} (HEADER, DELIMITER ',')"
        )


def _render_report_lines(
    settings: ReportSettings,
    cache_result: CacheResult,
) -> list[str]:
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
            f"{settings.min_observations}"
        ),
        "",
    ]

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

    midpoint = read_result_table(cache_result.cache_db, "midpoint_summary")
    if not midpoint.empty:
        row = midpoint.iloc[0].to_dict()
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
        lines.extend(_render_table_section(cache_result.cache_db, settings.cache_dir, title, table_name))

    lines.extend(
        [
            "## Caveats",
            "",
            "- SIRI VM delay is estimated vehicle-monitoring state, not measured arrival truth.",
            "- Raw vehicle-monitoring rows are repeated polls; default results use trip-stop buckets.",
            "- Conservative filtering excludes implausible, stale, pre-trip, and post-trip rows.",
            "- The source data is from Turku Region Public Transport / Foli API under CC BY 4.0.",
            "",
        ]
    )
    return lines


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


def _metric_select(prefix: str = "") -> str:
    return f"""
        COUNT(*) AS {prefix}bucket_count,
        COALESCE(SUM(raw_poll_count), 0) AS {prefix}raw_poll_count,
        AVG(delay_seconds) / 60.0 AS {prefix}signed_mean_delay_min,
        AVG(delay_seconds) / 60.0 AS {prefix}avg_delay_min,
        MEDIAN(delay_seconds) / 60.0 AS {prefix}median_delay_min,
        QUANTILE_CONT(delay_seconds / 60.0, 0.75) AS {prefix}p75_delay_min,
        QUANTILE_CONT(delay_seconds / 60.0, 0.90) AS {prefix}p90_delay_min,
        QUANTILE_CONT(delay_seconds / 60.0, 0.95) AS {prefix}p95_delay_min,
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


def _local_time_select(timezone: str) -> str:
    tz = _sql_literal(timezone)
    local = f"timezone({tz}, representative_time_utc)"
    return f"""
        {local} AS local_time,
        CAST({local} AS DATE) AS local_date,
        CAST(EXTRACT(hour FROM {local}) AS INTEGER) AS local_hour,
        CAST(EXTRACT(isodow FROM {local}) - 1 AS INTEGER) AS local_weekday,
        CAST(EXTRACT(isodow FROM {local}) BETWEEN 1 AND 5 AS BOOLEAN) AS is_weekday,
        CASE
            WHEN EXTRACT(isodow FROM {local}) BETWEEN 1 AND 5 THEN 'weekday'
            ELSE 'weekend'
        END AS day_type,
        CAST(
            EXTRACT(hour FROM {local}) * 60 + EXTRACT(minute FROM {local})
            AS INTEGER
        ) AS local_minutes
    """


def _rush_condition(rush_windows: tuple[str, ...]) -> str:
    parts = []
    for window in rush_windows:
        start, end = _parse_hhmm_window(window)
        if start < end:
            parts.append(f"(local_minutes >= {start} AND local_minutes < {end})")
        else:
            parts.append(f"(local_minutes >= {start} OR local_minutes < {end})")
    if not parts:
        return "FALSE"
    return "(" + " OR ".join(parts) + ")"


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


def _load_alert_targets(
    settings: ReportSettings,
    obs_start: object,
    obs_end: object,
) -> pd.DataFrame:
    with sqlite3.connect(f"file:{settings.db.as_posix()}?mode=ro", uri=True) as con:
        if not _table_exists_sqlite(con, "service_alerts"):
            return _empty_alert_targets()
        alerts = pd.read_sql_query(
            """
            SELECT
                source_alert_id,
                line_ref,
                cause,
                effect,
                priority,
                is_active,
                validity_start_utc,
                validity_end_utc,
                affected_routes_json,
                affected_stops_json,
                created_at_utc
            FROM service_alerts
            WHERE is_active = 1
            """,
            con,
        )

    if alerts.empty:
        return _empty_alert_targets()

    route_map = _load_route_map(settings.gtfs_dir)

    rows: list[dict[str, object]] = []
    for alert in alerts.itertuples(index=False):
        start = pd.to_datetime(alert.validity_start_utc, utc=True, errors="coerce")
        end = pd.to_datetime(alert.validity_end_utc, utc=True, errors="coerce")
        created = pd.to_datetime(alert.created_at_utc, utc=True, errors="coerce")
        if pd.isna(start):
            start = created if not pd.isna(created) else pd.Timestamp(obs_start)
        if pd.isna(end):
            end = pd.Timestamp(obs_end) + pd.Timedelta(microseconds=1)
        if pd.isna(start) or pd.isna(end):
            continue

        base = {
            "cause": _clean_alert_value(alert.cause),
            "effect": _clean_alert_value(alert.effect),
            "priority": int(alert.priority) if not pd.isna(alert.priority) else -1,
            "start_utc": start.isoformat(),
            "end_utc": end.isoformat(),
        }
        route_targets = set()
        if alert.line_ref is not None and not pd.isna(alert.line_ref):
            route_targets.add(str(alert.line_ref))
        for route_ref in _json_list(alert.affected_routes_json):
            route_targets.add(route_map.get(route_ref, route_ref))
        for line_ref in route_targets:
            rows.append({**base, "alert_scope": "route", "target_ref": str(line_ref)})

        for stop_id in set(_json_list(alert.affected_stops_json)):
            rows.append({**base, "alert_scope": "stop", "target_ref": str(stop_id)})

    if not rows:
        return _empty_alert_targets()
    return pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)


def _load_route_map(gtfs_dir_arg: Path | None) -> dict[str, str]:
    gtfs_dir = gtfs_dir_arg if gtfs_dir_arg else latest_gtfs_dir()
    if gtfs_dir is None:
        return {}
    routes_path = gtfs_dir / "routes.txt"
    if not routes_path.exists():
        return {}
    routes = pd.read_csv(routes_path, dtype={"route_id": "string", "route_short_name": "string"})
    if not {"route_id", "route_short_name"}.issubset(routes.columns):
        return {}
    return dict(zip(routes["route_id"].astype(str), routes["route_short_name"].astype(str), strict=False))


def _json_list(value: object) -> list[str]:
    if value is None or pd.isna(value):
        return []
    text = str(value).strip()
    if not text:
        return []
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(decoded, list):
        return []
    return [
        str(item)
        for item in decoded
        if item is not None and not isinstance(item, dict | list)
    ]


def _empty_alert_targets() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "cause",
            "effect",
            "priority",
            "alert_scope",
            "target_ref",
            "start_utc",
            "end_utc",
        ]
    )


def _clean_alert_value(value: object) -> str:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return "Unknown"
    return str(value)


def _expected_manifest(
    settings: ReportSettings,
    db_metadata: dict[str, Any],
) -> dict[str, Any]:
    return {
        "cache_version": CACHE_VERSION,
        "db_metadata": db_metadata,
        "settings": {
            "quality_mode": settings.quality_mode,
            "bucket": settings.bucket,
            "timezone": settings.timezone,
            "limit": settings.limit,
            "min_observations": settings.min_observations,
            "exclude_stop_call_disagreement": settings.exclude_stop_call_disagreement,
            "rush_windows": list(settings.rush_windows),
            "include_weekends": settings.include_weekends,
            "gtfs_dir": str(settings.gtfs_dir) if settings.gtfs_dir else None,
        },
    }


def _manifest_matches(
    current: dict[str, Any] | None,
    expected: dict[str, Any],
) -> bool:
    if current is None:
        return False
    for key in ("cache_version", "db_metadata", "settings"):
        if current.get(key) != expected.get(key):
            return False
    return True


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


def _escape_markdown(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")
