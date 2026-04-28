from __future__ import annotations

import argparse

import pandas as pd

from _shared import (
    QUALIFIED_DELAY_FILTER_SQL,
    add_common_args,
    add_bucket_arg,
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
        description="Show when buses are most late by local hour."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    parser.add_argument(
        "--line-ref",
        help="Limit the hourly profile to one line_ref, for example 3 or 10A.",
    )
    parser.set_defaults(limit=24, min_observations=30)
    return parser.parse_args()


def load_observations(args: argparse.Namespace) -> pd.DataFrame:
    where = QUALIFIED_DELAY_FILTER_SQL
    params: list[object] = []
    if args.line_ref:
        where += " AND v.line_ref = ?"
        params.append(args.line_ref)

    query = base_quality_query(where=where)
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query, params)


def build_profile(args: argparse.Namespace, df: pd.DataFrame) -> pd.DataFrame:
    df = apply_quality_filter(
        df,
        quality_mode=args.quality_mode,
        exclude_stop_call_disagreement=args.exclude_stop_call_disagreement,
    )
    df = aggregate_delay_buckets(df, bucket=args.bucket, timezone=args.timezone)
    if df.empty:
        return pd.DataFrame()

    grouped = summarize_delay_metrics(
        df,
        ["local_hour"],
        min_observations=args.min_observations,
    )
    grouped["hour_local"] = grouped["local_hour"].map(lambda hour: f"{hour:02d}:00")
    grouped = sort_robust_delay_metrics(grouped, limit=args.limit)
    grouped = grouped[
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
    ]
    return round_numeric(grouped)


def main() -> None:
    args = parse_args()
    df = load_observations(args)
    profile = build_profile(args, df)

    print_or_empty(profile)
    write_optional_csv(profile, args.output_csv)


if __name__ == "__main__":
    main()
