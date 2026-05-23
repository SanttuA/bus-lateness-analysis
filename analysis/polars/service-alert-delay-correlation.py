from __future__ import annotations

import argparse
from datetime import timedelta

import polars as pl

from _shared import (
    add_bucket_arg,
    add_cache_args,
    add_common_args,
    add_gtfs_args,
    add_quality_args,
    add_timezone_arg,
    parse_timestamp,
    print_or_empty,
    write_optional_csv,
)
from cli_common import load_delay_buckets_for_args
from report_cache import build_alert_results, settings_from_args


DEFAULT_ANALYSIS_DAYS = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare alert-period delays with matched non-alert controls using Polars."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    add_cache_args(parser)
    add_gtfs_args(parser, file_description="routes.txt")
    parser.add_argument("--view", choices=("grouped", "line", "both"), default="grouped")
    parser.add_argument("--alert-kind", choices=("any", "route", "stop"), default="any")
    parser.add_argument("--line-ref")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--analysis-days", type=int, default=DEFAULT_ANALYSIS_DAYS)
    parser.add_argument("--full-history", action="store_true")
    parser.set_defaults(limit=20, min_observations=30)
    return parser.parse_args()


def resolve_analysis_window(
    args: argparse.Namespace,
    buckets: pl.DataFrame,
) -> tuple[object | None, object | None, str]:
    if args.full_history:
        return None, None, "full history"
    if args.analysis_days <= 0:
        raise SystemExit("--analysis-days must be positive.")
    start = parse_timestamp(args.start, args.timezone) if args.start else None
    end = parse_timestamp(args.end, args.timezone) if args.end else None
    if start is None and end is None:
        latest = buckets["representative_time_utc"].max() if not buckets.is_empty() else None
        if latest is None:
            return None, None, "no observations"
        end = latest + timedelta(seconds=1)
        start = end - timedelta(days=args.analysis_days)
    elif start is None:
        start = end - timedelta(days=args.analysis_days)
    elif end is None:
        end = start + timedelta(days=args.analysis_days)
    if start >= end:
        raise SystemExit("Analysis window start must be before analysis window end.")
    return start, end, f"{start.isoformat()}..{end.isoformat()}"


def main() -> None:
    args = parse_args()
    buckets = load_delay_buckets_for_args(args)
    if args.line_ref:
        buckets = buckets.filter(pl.col("line_ref") == args.line_ref)
    window_start, window_end, _ = resolve_analysis_window(args, buckets)
    if window_start is not None:
        buckets = buckets.filter(pl.col("representative_time_utc") >= window_start)
    if window_end is not None:
        buckets = buckets.filter(pl.col("representative_time_utc") < window_end)
    grouped, line = build_alert_results(
        settings_from_args(args),
        buckets,
        alert_kind=args.alert_kind,
    )

    csv_frames: list[pl.DataFrame] = []
    if args.view in ("both", "grouped"):
        print("Alert matched-control correlation")
        print_or_empty(grouped)
        print()
        if not grouped.is_empty():
            csv_frames.append(grouped.with_columns(pl.lit("grouped").alias("view")).select("view", *grouped.columns))
    if args.view in ("both", "line"):
        print("Alert matched-control correlation by line")
        print_or_empty(line)
        print()
        if not line.is_empty():
            csv_frames.append(line.with_columns(pl.lit("line").alias("view")).select("view", *line.columns))
    if args.output_csv:
        combined = pl.concat(csv_frames, how="diagonal_relaxed") if csv_frames else pl.DataFrame()
        write_optional_csv(combined, args.output_csv)


if __name__ == "__main__":
    main()

