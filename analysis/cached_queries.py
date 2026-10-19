from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

try:
    from ._shared import DEFAULT_GTFS_ROOT, DEFAULT_RUSH_WINDOWS, round_numeric, rush_window_values
    from .report_cache import (
        CONSERVATIVE_EXCLUSION_COLUMNS,
        DEFAULT_MIN_TRIPS,
        METRIC_COLUMNS,
        QUALITY_FLAG_COLUMNS,
        ReportSettings,
        _having,
        _local_time_select,
        _metric_select,
        _rush_condition,
        _sql_literal,
        context_metrics_sql,
        delay_bucket_sql,
        ensure_analysis_cache,
        hourly_profile_sql,
        line_ranking_frame,
        quality_by_line_sql,
        quality_summary_sql,
        rush_impact_frame,
    )
except ImportError:  # pragma: no cover - used when called as analysis/*.py script.
    from _shared import DEFAULT_GTFS_ROOT, DEFAULT_RUSH_WINDOWS, round_numeric, rush_window_values
    from report_cache import (
        CONSERVATIVE_EXCLUSION_COLUMNS,
        DEFAULT_MIN_TRIPS,
        METRIC_COLUMNS,
        QUALITY_FLAG_COLUMNS,
        ReportSettings,
        _having,
        _local_time_select,
        _metric_select,
        _rush_condition,
        _sql_literal,
        context_metrics_sql,
        delay_bucket_sql,
        ensure_analysis_cache,
        hourly_profile_sql,
        line_ranking_frame,
        quality_by_line_sql,
        quality_summary_sql,
        rush_impact_frame,
    )

__all__ = [
    "_local_time_select",
    "_metric_select",
    "_rush_condition",
    "alert_observation_buckets",
    "context_delay_metrics",
    "hourly_delay_profile",
    "line_rankings",
    "quality_report",
    "rush_impact",
    "settings_from_args",
    "stop_change_buckets",
]


def settings_from_args(args: object) -> ReportSettings:
    return ReportSettings(
        db=getattr(args, "db"),
        cache_dir=getattr(args, "cache_dir"),
        quality_mode=getattr(args, "quality_mode", "conservative"),
        bucket=getattr(args, "bucket", "trip-stop"),
        timezone=getattr(args, "timezone", "Europe/Helsinki"),
        min_observations=getattr(args, "min_observations", 30),
        min_trips=getattr(args, "min_trips", None) or DEFAULT_MIN_TRIPS,
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
        gtfs_root=getattr(args, "gtfs_root", None) or DEFAULT_GTFS_ROOT,
    )


def ensure_cache_from_args(args: object) -> Path:
    settings = settings_from_args(args)
    result = ensure_analysis_cache(settings, force=getattr(args, "force_cache", False))
    return result.cache_db


def _connect(cache_db: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(cache_db), read_only=True)


def line_rankings(
    args: object,
    ranking: str,
    *,
    cache_db: Path | None = None,
) -> pd.DataFrame:
    """Late or early ranking (with trip-cluster intervals) or the combined robust view."""
    if cache_db is None:
        cache_db = ensure_cache_from_args(args)
    settings = settings_from_args(args)
    with _connect(cache_db) as con:
        if ranking in ("late", "early"):
            return line_ranking_frame(con, settings, ranking)
        query = f"""
            WITH line_metrics AS (
                SELECT
                    line_ref,
                    FIRST(published_line_name ORDER BY representative_time_utc) AS line_name,
                    {_metric_select()}
                FROM delay_buckets
                GROUP BY line_ref
                {_having(settings)}
            )
            SELECT
                line_ref,
                line_name,
                bucket_count,
                raw_poll_count,
                trip_count,
                signed_mean_delay_min,
                median_delay_min,
                p75_delay_min,
                p90_delay_min,
                p95_delay_min,
                pct_on_time,
                pct_over_3_min_late,
                pct_over_5_min_late,
                pct_early,
                pct_over_1_min_early,
                pct_over_3_min_early
            FROM line_metrics
            ORDER BY p90_delay_min DESC,
                pct_over_5_min_late DESC,
                bucket_count DESC,
                line_ref
            LIMIT {settings.limit}
        """
        return round_numeric(con.execute(query).fetchdf())


def context_delay_metrics(args: object) -> pd.DataFrame:
    cache_db = ensure_cache_from_args(args)
    settings = settings_from_args(args)
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
    columns = ["line_ref", "line_name", "direction_ref", "hour_local", "day_type", *METRIC_COLUMNS]
    return round_numeric(_read_cache(cache_db, context_metrics_sql(settings, where_sql), params)[columns])


def hourly_delay_profile(args: object) -> pd.DataFrame:
    """Hourly metrics in hour order; --limit keeps the first N hours."""
    cache_db = ensure_cache_from_args(args)
    settings = settings_from_args(args)
    where_sql = ""
    params: list[object] = []
    if getattr(args, "line_ref", None):
        where_sql = "WHERE line_ref = ?"
        params.append(args.line_ref)
    query = hourly_profile_sql(settings, by_day_type=False, where_sql=where_sql)
    result = _read_cache(cache_db, query, params).head(settings.limit)
    return round_numeric(result[["hour_local", *METRIC_COLUMNS]])


def rush_impact(args: object) -> pd.DataFrame:
    cache_db = ensure_cache_from_args(args)
    settings = settings_from_args(args)
    with _connect(cache_db) as con:
        return rush_impact_frame(con, settings)


def quality_report(args: object) -> pd.DataFrame:
    cache_db = ensure_cache_from_args(args)
    if args.view == "summary":
        return round_numeric(_read_cache(cache_db, quality_summary_sql()))

    if args.view == "line":
        query = f"""
            SELECT * FROM ({quality_by_line_sql()})
            WHERE row_count >= ?
            ORDER BY conservative_excluded_pct DESC,
                conservative_excluded_rows DESC,
                line_ref
            LIMIT ?
        """
        return round_numeric(_read_cache(cache_db, query, [args.min_observations, args.limit]))

    flag_list = ",\n            ".join(QUALITY_FLAG_COLUMNS)
    any_flag = " OR ".join([*CONSERVATIVE_EXCLUSION_COLUMNS, "has_stop_call_disagreement"])
    query = f"""
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
            {flag_list}
        FROM quality_rows
        WHERE {any_flag}
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
    """Bucket a filtered slice of quality_rows with the same SQL as the cache build."""
    settings = settings_from_args(args).resolved()
    return delay_bucket_sql(
        bucket=settings.bucket,
        timezone=settings.timezone,
        holidays=settings.holiday_dates(),
        extra_where_parts=extra_where_parts,
    )


def _read_cache(
    cache_db: Path,
    query: str,
    params: list[object] | None = None,
) -> pd.DataFrame:
    with _connect(cache_db) as con:
        return con.execute(query, params or []).fetchdf()

