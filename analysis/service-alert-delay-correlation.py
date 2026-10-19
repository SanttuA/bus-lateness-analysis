from __future__ import annotations

import argparse

import duckdb
import pandas as pd

from _shared import (
    QUALIFIED_DELAY_FILTER_SQL,
    add_bucket_arg,
    add_cache_args,
    add_common_args,
    add_gtfs_args,
    add_quality_args,
    add_timezone_arg,
    append_representative_time_filter,
    aggregate_delay_buckets,
    apply_quality_filter,
    base_quality_query,
    connect_readonly_db,
    print_or_empty,
    read_sql,
    representative_time_sql,
    write_optional_csv,
)
from cached_queries import alert_observation_buckets as cached_alert_observation_buckets
from cached_queries import settings_from_args
from report_cache import build_alert_tables


DEFAULT_ANALYSIS_DAYS = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare alert-period delays with matched non-alert controls."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    add_cache_args(parser)
    add_gtfs_args(parser, file_description="routes.txt")
    parser.add_argument(
        "--view",
        choices=("grouped", "line", "both"),
        default="grouped",
        help="Print alert groups, alert groups by line, or both. Defaults to grouped.",
    )
    parser.add_argument(
        "--alert-kind",
        choices=("any", "route", "stop"),
        default="any",
        help="Which alert target scope to use. Defaults to any.",
    )
    parser.add_argument(
        "--line-ref",
        help="Limit analysis to one line_ref, for example 3 or 10A.",
    )
    parser.add_argument(
        "--start",
        help=(
            "Analysis window start timestamp. Naive timestamps use --timezone. "
            "Defaults to the latest observations minus --analysis-days."
        ),
    )
    parser.add_argument(
        "--end",
        help=(
            "Analysis window end timestamp. Naive timestamps use --timezone. "
            "Defaults to the latest observation timestamp."
        ),
    )
    parser.add_argument(
        "--analysis-days",
        type=int,
        default=DEFAULT_ANALYSIS_DAYS,
        help=(
            "Rolling window size when --start or --end is omitted. Defaults to "
            f"{DEFAULT_ANALYSIS_DAYS} days to keep notebook memory use bounded."
        ),
    )
    parser.add_argument(
        "--full-history",
        action="store_true",
        help="Analyze the full database history. This can use a large amount of memory.",
    )
    parser.set_defaults(limit=20, min_observations=30)
    return parser.parse_args()


def load_observations(
    args: argparse.Namespace,
    window: tuple[pd.Timestamp | None, pd.Timestamp | None] | None = None,
) -> pd.DataFrame:
    where = QUALIFIED_DELAY_FILTER_SQL
    params: list[object] = []
    if args.line_ref:
        where += " AND v.line_ref = ?"
        params.append(args.line_ref)
    if window is None:
        window_start, window_end, _ = resolve_analysis_window(args)
    else:
        window_start, window_end = window
    where = append_representative_time_filter(
        where,
        params,
        start_utc=window_start,
        end_utc=window_end,
    )

    query = base_quality_query(where=where)
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query, params)


def parse_timestamp(value: str, timezone: str) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(timezone)
    return timestamp.tz_convert("UTC")


def _latest_observation_timestamp(args: argparse.Namespace) -> pd.Timestamp | None:
    where = QUALIFIED_DELAY_FILTER_SQL
    params: list[object] = []
    if getattr(args, "line_ref", None):
        where += " AND v.line_ref = ?"
        params.append(args.line_ref)

    query = f"""
    SELECT MAX({representative_time_sql()}) AS latest_time_utc
    FROM vehicle_observations v
    WHERE {where}
    """
    with connect_readonly_db(args.db) as con:
        latest_value = con.execute(query, params).fetchone()[0]
    latest = pd.to_datetime(latest_value, utc=True, errors="coerce")
    return None if pd.isna(latest) else latest


def resolve_analysis_window(
    args: argparse.Namespace,
) -> tuple[pd.Timestamp | None, pd.Timestamp | None, str]:
    if getattr(args, "full_history", False):
        return None, None, "full history"

    analysis_days = getattr(args, "analysis_days", DEFAULT_ANALYSIS_DAYS)
    if analysis_days <= 0:
        raise SystemExit("--analysis-days must be positive.")

    start_arg = getattr(args, "start", None)
    end_arg = getattr(args, "end", None)
    start = parse_timestamp(start_arg, args.timezone) if start_arg else None
    end = parse_timestamp(end_arg, args.timezone) if end_arg else None

    if start is None and end is None:
        latest = _latest_observation_timestamp(args)
        if latest is None:
            return None, None, "no observations"
        end = latest.ceil("s") + pd.Timedelta(seconds=1)
        start = end - pd.Timedelta(days=analysis_days)
    elif start is None:
        start = end - pd.Timedelta(days=analysis_days)
    elif end is None:
        end = start + pd.Timedelta(days=analysis_days)

    if start >= end:
        raise SystemExit("Analysis window start must be before analysis window end.")
    return start, end, f"{start.isoformat()}..{end.isoformat()}"


def build_correlation(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    window_start, window_end, _ = resolve_analysis_window(args)
    observations = load_observations(args, (window_start, window_end))
    if observations.empty:
        return pd.DataFrame(), pd.DataFrame()

    observations = apply_quality_filter(
        observations,
        quality_mode=args.quality_mode,
        exclude_stop_call_disagreement=args.exclude_stop_call_disagreement,
    )
    observations = aggregate_delay_buckets(
        observations,
        bucket=args.bucket,
        timezone=args.timezone,
        holidays=settings_from_args(args).resolved().holiday_dates(),
    )
    return build_correlation_from_buckets(args, observations, window_start, window_end)


def build_correlation_from_buckets(
    args: argparse.Namespace,
    observations: pd.DataFrame,
    window_start: pd.Timestamp | None = None,
    window_end: pd.Timestamp | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run the shared stratified alert comparison over a window of delay buckets.

    The buckets are registered in an in-memory DuckDB database so the report cache and
    this CLI use the same ``build_alert_tables`` implementation.
    """
    del window_start, window_end  # The buckets are already restricted to the window.
    if observations.empty:
        return pd.DataFrame(), pd.DataFrame()
    settings = settings_from_args(args).resolved()
    frame = observations.copy()
    for column in ("line_ref", "direction_ref", "next_stop_point_ref", "trip_match_key", "bucket_id"):
        frame[column] = frame[column].astype("string")
    frame["representative_time_utc"] = pd.to_datetime(frame["representative_time_utc"], utc=True)
    with duckdb.connect() as con:
        con.register("alert_window_buckets", frame)
        tables = build_alert_tables(con, settings, "alert_window_buckets", alert_kind=args.alert_kind)
    return tables["service_alert_grouped"], tables["service_alert_by_line"]


def main() -> None:
    args = parse_args()
    if args.no_cache:
        grouped, line = build_correlation(args)
    else:
        window_start, window_end, _ = resolve_analysis_window(args)
        buckets = cached_alert_observation_buckets(args, (window_start, window_end))
        grouped, line = build_correlation_from_buckets(
            args,
            buckets,
            window_start,
            window_end,
        )

    csv_frames: list[pd.DataFrame] = []
    if args.view in ("both", "grouped"):
        print("Alert matched-control correlation")
        print_or_empty(grouped)
        print()
        if not grouped.empty:
            export = grouped.copy()
            export.insert(0, "view", "grouped")
            csv_frames.append(export)

    if args.view in ("both", "line"):
        print("Alert matched-control correlation by line")
        print_or_empty(line)
        print()
        if not line.empty:
            export = line.copy()
            export.insert(0, "view", "line")
            csv_frames.append(export)

    if args.output_csv:
        combined = pd.concat(csv_frames, ignore_index=True) if csv_frames else pd.DataFrame()
        write_optional_csv(combined, args.output_csv)


if __name__ == "__main__":
    main()
