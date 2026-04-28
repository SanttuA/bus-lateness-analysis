from __future__ import annotations

import argparse

import pandas as pd

from _shared import (
    QUALIFIED_DELAY_FILTER_SQL,
    add_bucket_arg,
    add_common_args,
    add_quality_args,
    add_rush_window_args,
    add_timezone_arg,
    aggregate_delay_buckets,
    apply_quality_filter,
    base_quality_query,
    connect_readonly_db,
    flag_rush_period,
    parse_rush_windows,
    print_or_empty,
    read_sql,
    round_numeric,
    rush_window_values,
    summarize_delay_metrics,
    write_optional_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rank lines by robust delay lift during rush windows."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    add_rush_window_args(parser)
    parser.set_defaults(limit=10, min_observations=30)
    return parser.parse_args()


def load_observations(args: argparse.Namespace) -> pd.DataFrame:
    query = base_quality_query(where=QUALIFIED_DELAY_FILTER_SQL)
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query)


def build_rush_impact(args: argparse.Namespace, df: pd.DataFrame) -> pd.DataFrame:
    df = apply_quality_filter(
        df,
        quality_mode=args.quality_mode,
        exclude_stop_call_disagreement=args.exclude_stop_call_disagreement,
    )
    df = aggregate_delay_buckets(df, bucket=args.bucket, timezone=args.timezone)
    if df.empty:
        return pd.DataFrame()

    windows = parse_rush_windows(rush_window_values(args.rush_window))
    df["is_rush"] = flag_rush_period(
        df,
        windows,
        include_weekends=args.include_weekends,
    )
    grouped = summarize_delay_metrics(
        df,
        ["line_ref", "is_rush"],
        min_observations=args.min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )
    if grouped.empty or not {True, False}.issubset(set(grouped["is_rush"])):
        return pd.DataFrame()

    rush = grouped[grouped["is_rush"]].drop(columns=["is_rush"])
    non_rush = grouped[~grouped["is_rush"]].drop(columns=["is_rush"])
    result = non_rush.merge(
        rush,
        how="inner",
        on="line_ref",
        suffixes=("_non_rush", "_rush"),
    )
    if result.empty:
        return result

    result["line_name"] = result["line_name_rush"].combine_first(
        result["line_name_non_rush"]
    )
    result["rush_p90_delay_lift_min"] = (
        result["p90_delay_min_rush"] - result["p90_delay_min_non_rush"]
    )
    result["rush_median_delay_lift_min"] = (
        result["median_delay_min_rush"] - result["median_delay_min_non_rush"]
    )
    result["rush_over_5_min_late_pct_point_lift"] = (
        result["pct_over_5_min_late_rush"] - result["pct_over_5_min_late_non_rush"]
    )
    result = result.sort_values(
        [
            "rush_p90_delay_lift_min",
            "rush_over_5_min_late_pct_point_lift",
            "bucket_count_rush",
        ],
        ascending=[False, False, False],
    ).head(args.limit)

    ordered = [
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
    return round_numeric(result[ordered])


def main() -> None:
    args = parse_args()
    df = load_observations(args)
    impact = build_rush_impact(args, df)

    print_or_empty(impact)
    write_optional_csv(impact, args.output_csv)


if __name__ == "__main__":
    main()
