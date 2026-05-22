from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

try:
    from ._shared import DEFAULT_RUSH_WINDOWS, round_numeric, rush_window_values
    from .report_cache import (
        ReportSettings,
        _local_time_select,
        _metric_select,
        _rush_condition,
        _sql_literal,
        ensure_analysis_cache,
    )
except ImportError:  # pragma: no cover - used when called as analysis/*.py script.
    from _shared import DEFAULT_RUSH_WINDOWS, round_numeric, rush_window_values
    from report_cache import (
        ReportSettings,
        _local_time_select,
        _metric_select,
        _rush_condition,
        _sql_literal,
        ensure_analysis_cache,
    )


def settings_from_args(args: object) -> ReportSettings:
    return ReportSettings(
        db=getattr(args, "db"),
        cache_dir=getattr(args, "cache_dir"),
        quality_mode=getattr(args, "quality_mode", "conservative"),
        bucket=getattr(args, "bucket", "trip-stop"),
        timezone=getattr(args, "timezone", "Europe/Helsinki"),
        min_observations=getattr(args, "min_observations", 30),
        limit=getattr(args, "limit", 20),
        exclude_stop_call_disagreement=getattr(
            args,
            "exclude_stop_call_disagreement",
            False,
        ),
        rush_windows=tuple(
            rush_window_values(getattr(args, "rush_window", None))
            if hasattr(args, "rush_window")
            else DEFAULT_RUSH_WINDOWS
        ),
        include_weekends=getattr(args, "include_weekends", False),
        gtfs_dir=getattr(args, "gtfs_dir", None),
    )


def ensure_cache_from_args(args: object) -> Path:
    settings = settings_from_args(args)
    result = ensure_analysis_cache(settings, force=getattr(args, "force_cache", False))
    return result.cache_db


def line_rankings(
    args: object,
    ranking: str,
    *,
    cache_db: Path | None = None,
) -> pd.DataFrame:
    if cache_db is None:
        cache_db = ensure_cache_from_args(args)
    line_metrics = f"""
        WITH line_metrics AS (
            SELECT
                line_ref,
                FIRST(published_line_name ORDER BY representative_time_utc) AS line_name,
                {_metric_select()}
            FROM delay_buckets
            GROUP BY line_ref
            HAVING COUNT(*) >= ?
        )
    """
    if ranking == "late":
        query = (
            line_metrics
            + """
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
            ORDER BY p90_delay_min DESC,
                pct_over_5_min_late DESC,
                bucket_count DESC,
                line_ref
            LIMIT ?
            """
        )
    elif ranking == "early":
        query = (
            line_metrics
            + """
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
            ORDER BY p90_early_min_abs DESC,
                pct_over_3_min_early DESC,
                bucket_count DESC,
                line_ref
            LIMIT ?
            """
        )
    else:
        query = (
            line_metrics
            + """
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
                pct_over_5_min_late,
                pct_early,
                pct_over_1_min_early,
                pct_over_3_min_early
            FROM line_metrics
            ORDER BY p90_delay_min DESC,
                pct_over_5_min_late DESC,
                bucket_count ASC
            LIMIT ?
            """
        )
    return round_numeric(_read_cache(cache_db, query, [args.min_observations, args.limit]))


def context_delay_metrics(args: object) -> pd.DataFrame:
    cache_db = ensure_cache_from_args(args)
    where_parts: list[str] = []
    params: list[object] = []
    if getattr(args, "line_ref", None):
        where_parts.append("line_ref = ?")
        params.append(args.line_ref)
    if getattr(args, "direction_ref", None):
        where_parts.append("direction_ref = ?")
        params.append(args.direction_ref)
    if getattr(args, "day_type", "all") != "all":
        where_parts.append("day_type = ?")
        params.append(args.day_type)
    where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""
    query = f"""
        SELECT
            line_ref,
            FIRST(published_line_name ORDER BY representative_time_utc) AS line_name,
            direction_ref,
            LPAD(CAST(local_hour AS VARCHAR), 2, '0') || ':00' AS hour_local,
            day_type,
            {_metric_select()}
        FROM delay_buckets
        {where_sql}
        GROUP BY line_ref, direction_ref, local_hour, day_type
        HAVING COUNT(*) >= ?
        ORDER BY p90_delay_min DESC, pct_over_5_min_late DESC, bucket_count ASC
        LIMIT ?
    """
    params.extend([args.min_observations, args.limit])
    columns = [
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
    return round_numeric(_read_cache(cache_db, query, params)[columns])


def hourly_delay_profile(args: object) -> pd.DataFrame:
    cache_db = ensure_cache_from_args(args)
    where_sql = ""
    params: list[object] = []
    if getattr(args, "line_ref", None):
        where_sql = "WHERE line_ref = ?"
        params.append(args.line_ref)
    query = f"""
        SELECT
            LPAD(CAST(local_hour AS VARCHAR), 2, '0') || ':00' AS hour_local,
            {_metric_select()}
        FROM delay_buckets
        {where_sql}
        GROUP BY local_hour
        HAVING COUNT(*) >= ?
        ORDER BY p90_delay_min DESC, pct_over_5_min_late DESC, bucket_count ASC
        LIMIT ?
    """
    params.extend([args.min_observations, args.limit])
    columns = [
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
    return round_numeric(_read_cache(cache_db, query, params)[columns])


def rush_impact(args: object) -> pd.DataFrame:
    cache_db = ensure_cache_from_args(args)
    rush_condition = _rush_condition(tuple(rush_window_values(args.rush_window)))
    if not args.include_weekends:
        rush_condition = f"({rush_condition}) AND is_weekday"
    query = f"""
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
            HAVING COUNT(*) >= ?
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
        LIMIT ?
    """
    return round_numeric(_read_cache(cache_db, query, [args.min_observations, args.limit]))


def quality_report(args: object) -> pd.DataFrame:
    cache_db = ensure_cache_from_args(args)
    if args.view == "summary":
        query = """
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
                SELECT 'is_pre_trip_observation',
                    SUM(CAST(is_pre_trip_observation AS INTEGER))
                FROM quality_rows
                UNION ALL
                SELECT 'is_post_trip_observation',
                    SUM(CAST(is_post_trip_observation AS INTEGER))
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
        return round_numeric(_read_cache(cache_db, query))

    if args.view == "line":
        query = """
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
            HAVING COUNT(*) >= ?
            ORDER BY conservative_excluded_pct DESC,
                conservative_excluded_rows DESC,
                line_ref
            LIMIT ?
        """
        return round_numeric(_read_cache(cache_db, query, [args.min_observations, args.limit]))

    query = """
        SELECT
            recorded_at_utc,
            line_ref,
            direction_ref,
            vehicle_id,
            trip_match_key,
            next_stop_point_ref,
            delay_seconds / 60.0 AS delay_min,
            observation_age_seconds,
            stop_call_delay_diff_seconds,
            is_implausible_delay,
            is_stale_observation,
            is_pre_trip_observation,
            is_post_trip_observation,
            has_stop_call_disagreement
        FROM quality_rows
        WHERE is_implausible_delay
            OR is_stale_observation
            OR is_pre_trip_observation
            OR is_post_trip_observation
            OR has_stop_call_disagreement
        ORDER BY quality_issue_count DESC, recorded_at_utc ASC
        LIMIT ?
    """
    return round_numeric(_read_cache(cache_db, query, [args.limit]))


def stop_change_buckets(args: object) -> pd.DataFrame:
    cache_db = ensure_cache_from_args(args)
    start, end = _explicit_period_load_window(args)
    where_parts = ["next_stop_point_ref IS NOT NULL"]
    params: list[object] = []
    if getattr(args, "line_ref", None):
        where_parts.append("line_ref = ?")
        params.append(args.line_ref)
    if getattr(args, "direction_ref", None):
        where_parts.append("direction_ref = ?")
        params.append(args.direction_ref)
    _append_representative_time_filters(
        where_parts,
        params,
        start,
        end,
        column="COALESCE(next_aimed_arrival_time_utc, recorded_at_utc)",
    )
    query = _bucketed_quality_rows_query(args, where_parts)
    return _read_cache(cache_db, query, params)


def alert_observation_buckets(
    args: object,
    window: tuple[pd.Timestamp | None, pd.Timestamp | None],
) -> pd.DataFrame:
    cache_db = ensure_cache_from_args(args)
    where_parts: list[str] = []
    params: list[object] = []
    if getattr(args, "line_ref", None):
        where_parts.append("line_ref = ?")
        params.append(args.line_ref)
    _append_representative_time_filters(
        where_parts,
        params,
        window[0],
        window[1],
        column="COALESCE(next_aimed_arrival_time_utc, recorded_at_utc)",
    )
    query = _bucketed_quality_rows_query(args, where_parts)
    return _read_cache(cache_db, query, params)


def _explicit_period_load_window(
    args: object,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    period_args = [
        getattr(args, "baseline_start", None),
        getattr(args, "baseline_end", None),
        getattr(args, "comparison_start", None),
        getattr(args, "comparison_end", None),
    ]
    if any(period_args):
        if not all(period_args):
            raise SystemExit(
                "Provide all four period arguments: --baseline-start, --baseline-end, "
                "--comparison-start, and --comparison-end."
            )
        baseline_start = _parse_timestamp(args.baseline_start, args.timezone)
        baseline_end = _parse_timestamp(args.baseline_end, args.timezone)
        comparison_start = _parse_timestamp(args.comparison_start, args.timezone)
        comparison_end = _parse_timestamp(args.comparison_end, args.timezone)
        if baseline_start >= baseline_end:
            raise SystemExit("Baseline period start must be before baseline period end.")
        if comparison_start >= comparison_end:
            raise SystemExit("Comparison period start must be before comparison period end.")
        return min(baseline_start, comparison_start), max(baseline_end, comparison_end)

    if getattr(args, "legacy_midpoint", False):
        return None, None
    raise SystemExit(
        "Stop-change analysis now requires explicit matched periods. Provide "
        "--baseline-start, --baseline-end, --comparison-start, and "
        "--comparison-end, or pass --legacy-midpoint for the old automatic split."
    )


def _append_representative_time_filters(
    where_parts: list[str],
    params: list[object],
    start_utc: object | None,
    end_utc: object | None,
    *,
    column: str = "representative_time_utc",
) -> None:
    if start_utc is not None:
        where_parts.append(f"{column} >= ?")
        params.append(start_utc)
    if end_utc is not None:
        where_parts.append(f"{column} < ?")
        params.append(end_utc)


def _parse_timestamp(value: str, timezone: str) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(timezone)
    return timestamp.tz_convert("UTC")


def _bucketed_quality_rows_query(args: object, extra_where_parts: list[str]) -> str:
    settings = settings_from_args(args)
    where_parts = [
        "quality_pass",
        "delay_seconds IS NOT NULL",
        "recorded_at_utc IS NOT NULL",
        "line_ref IS NOT NULL",
        *extra_where_parts,
    ]
    where_sql = " AND ".join(where_parts)

    if settings.bucket == "poll":
        return f"""
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
            WHERE {where_sql}
        """

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
        WHERE {where_sql}
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


def _read_cache(
    cache_db: Path,
    query: str,
    params: list[object] | None = None,
) -> pd.DataFrame:
    with duckdb.connect(str(cache_db), read_only=True) as con:
        return con.execute(query, params or []).fetchdf()
