from __future__ import annotations

import argparse

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
        description="Show bus lines with the strongest robust late-delay profile."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    parser.set_defaults(limit=10, min_observations=30)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    query = base_quality_query(where=QUALIFIED_DELAY_FILTER_SQL)
    with connect_readonly_db(args.db) as con:
        df = read_sql(con, query)

    df = apply_quality_filter(
        df,
        quality_mode=args.quality_mode,
        exclude_stop_call_disagreement=args.exclude_stop_call_disagreement,
    )
    buckets = aggregate_delay_buckets(df, bucket=args.bucket, timezone=args.timezone)
    metrics = summarize_delay_metrics(
        buckets,
        ["line_ref"],
        min_observations=args.min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )
    metrics = sort_robust_delay_metrics(metrics, limit=args.limit)
    ordered = [
        "line_ref",
        "line_name",
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
    result = round_numeric(metrics[ordered]) if not metrics.empty else metrics
    print_or_empty(result)
    write_optional_csv(result, args.output_csv)


if __name__ == "__main__":
    main()
