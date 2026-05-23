from __future__ import annotations

import argparse

from _shared import (
    QUALIFIED_DELAY_FILTER_SQL,
    add_cache_args,
    add_common_args,
    add_quality_pass,
    add_timezone_arg,
    base_quality_query,
    print_or_empty,
    read_sql,
    round_numeric,
    write_optional_csv,
)
from report_cache import (
    QUALITY_ROWS_NAME,
    build_quality_by_line,
    build_quality_summary,
    ensure_analysis_cache,
    read_table,
    settings_from_args,
)

import polars as pl


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report data-quality flags before delay metrics are computed using Polars."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_cache_args(parser)
    parser.add_argument("--view", choices=("summary", "line", "examples"), default="summary")
    parser.set_defaults(limit=20, min_observations=1)
    return parser.parse_args()


def load_quality_rows(args: argparse.Namespace) -> pl.DataFrame:
    if args.no_cache:
        return add_quality_pass(read_sql(args.db, base_quality_query(where=QUALIFIED_DELAY_FILTER_SQL)))
    settings = settings_from_args(args)
    ensure_analysis_cache(settings, force=args.force_cache)
    return read_table(settings.cache_dir, QUALITY_ROWS_NAME)


def build_examples(df: pl.DataFrame, limit: int) -> pl.DataFrame:
    if df.is_empty():
        return df
    flagged = df.filter(pl.any_horizontal([pl.col(column) for column in [
        "is_implausible_delay",
        "is_stale_observation",
        "is_pre_trip_observation",
        "is_post_trip_observation",
        "has_stop_call_disagreement",
    ]]))
    if flagged.is_empty():
        return flagged
    return round_numeric(
        flagged.with_columns((pl.col("delay_seconds") / 60.0).alias("delay_min"))
        .sort(["quality_issue_count", "recorded_at_utc"], descending=[True, False])
        .head(limit)
        .select(
            [
                "recorded_at_utc",
                "line_ref",
                "direction_ref",
                "vehicle_id",
                "trip_match_key",
                "next_stop_point_ref",
                "delay_min",
                "observation_age_seconds",
                "stop_call_delay_diff_seconds",
                "is_implausible_delay",
                "is_stale_observation",
                "is_pre_trip_observation",
                "is_post_trip_observation",
                "has_stop_call_disagreement",
            ]
        )
    )


def main() -> None:
    args = parse_args()
    rows = load_quality_rows(args)
    if args.view == "summary":
        result = build_quality_summary(rows)
    elif args.view == "line":
        result = build_quality_by_line(
            rows,
            min_observations=args.min_observations,
            limit=args.limit,
        )
    else:
        result = build_examples(rows, args.limit)
    print_or_empty(result)
    write_optional_csv(result, args.output_csv)


if __name__ == "__main__":
    main()

