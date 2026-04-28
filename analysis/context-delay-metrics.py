from __future__ import annotations

import argparse

import pandas as pd

from _shared import (
    QUALIFIED_DELAY_FILTER_SQL,
    add_bucket_arg,
    add_common_args,
    add_quality_args,
    add_timezone_arg,
    aggregate_delay_buckets,
    apply_quality_filter,
    base_quality_query,
    connect_readonly_db,
    print_or_empty,
    read_sql,
    round_numeric,
    sort_robust_delay_metrics,
    summarize_delay_metrics,
    write_optional_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Report robust delay metrics by line, direction, local hour, and "
            "weekday/weekend context."
        )
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    parser.add_argument(
        "--line-ref",
        help="Limit analysis to one line_ref, for example 3 or 10A.",
    )
    parser.add_argument(
        "--direction-ref",
        help="Limit analysis to one direction_ref.",
    )
    parser.add_argument(
        "--day-type",
        choices=("all", "weekday", "weekend"),
        default="all",
        help="Limit output to weekday or weekend contexts. Defaults to all.",
    )
    parser.set_defaults(limit=50, min_observations=30)
    return parser.parse_args()


def load_observations(args: argparse.Namespace) -> pd.DataFrame:
    where = QUALIFIED_DELAY_FILTER_SQL
    params: list[object] = []
    if args.line_ref:
        where += " AND v.line_ref = ?"
        params.append(args.line_ref)
    if args.direction_ref:
        where += " AND v.direction_ref = ?"
        params.append(args.direction_ref)

    query = base_quality_query(where=where)
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query, params)


def build_context_metrics(args: argparse.Namespace, df: pd.DataFrame) -> pd.DataFrame:
    df = apply_quality_filter(
        df,
        quality_mode=args.quality_mode,
        exclude_stop_call_disagreement=args.exclude_stop_call_disagreement,
    )
    buckets = aggregate_delay_buckets(df, bucket=args.bucket, timezone=args.timezone)
    if args.day_type != "all":
        buckets = buckets[buckets["day_type"] == args.day_type]
    if buckets.empty:
        return pd.DataFrame()

    metrics = summarize_delay_metrics(
        buckets,
        ["line_ref", "direction_ref", "local_hour", "day_type"],
        min_observations=args.min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )
    metrics = sort_robust_delay_metrics(metrics, limit=args.limit)
    if metrics.empty:
        return metrics
    metrics["hour_local"] = metrics["local_hour"].map(lambda hour: f"{hour:02d}:00")
    ordered = [
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
    return round_numeric(metrics[ordered])


def main() -> None:
    args = parse_args()
    df = load_observations(args)
    result = build_context_metrics(args, df)

    print_or_empty(result)
    write_optional_csv(result, args.output_csv)


if __name__ == "__main__":
    main()
