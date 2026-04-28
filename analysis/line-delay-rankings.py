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
    summarize_delay_metrics,
    write_optional_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rank lines by robust late and early schedule inaccuracy."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    parser.add_argument(
        "--ranking",
        choices=("both", "late", "early"),
        default="both",
        help="Which ranking to print. Defaults to both.",
    )
    parser.set_defaults(limit=10, min_observations=30)
    return parser.parse_args()


def load_observations(args: argparse.Namespace) -> pd.DataFrame:
    query = base_quality_query(where=QUALIFIED_DELAY_FILTER_SQL)
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query)


def prepare_buckets(args: argparse.Namespace, df: pd.DataFrame) -> pd.DataFrame:
    df = apply_quality_filter(
        df,
        quality_mode=args.quality_mode,
        exclude_stop_call_disagreement=args.exclude_stop_call_disagreement,
    )
    return aggregate_delay_buckets(df, bucket=args.bucket, timezone=args.timezone)


def line_metrics(df: pd.DataFrame, min_observations: int) -> pd.DataFrame:
    return summarize_delay_metrics(
        df,
        ["line_ref"],
        min_observations=min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")},
    )


def rank_late(df: pd.DataFrame, min_observations: int, limit: int) -> pd.DataFrame:
    grouped = line_metrics(df, min_observations)
    if grouped.empty:
        return grouped
    grouped = grouped.sort_values(
        ["p90_delay_min", "pct_over_5_min_late", "bucket_count", "line_ref"],
        ascending=[False, False, False, True],
    ).head(limit)
    return round_numeric(
        grouped[
            [
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
            ]
        ]
    )


def rank_early(df: pd.DataFrame, min_observations: int, limit: int) -> pd.DataFrame:
    grouped = line_metrics(df, min_observations)
    if grouped.empty:
        return grouped
    grouped = grouped.sort_values(
        ["p90_early_min_abs", "pct_over_3_min_early", "bucket_count", "line_ref"],
        ascending=[False, False, False, True],
    ).head(limit)
    return round_numeric(
        grouped[
            [
                "line_ref",
                "line_name",
                "bucket_count",
                "raw_poll_count",
                "signed_mean_delay_min",
                "median_delay_min",
                "pct_early",
                "pct_over_1_min_early",
                "pct_over_3_min_early",
                "median_early_min_abs",
                "p90_early_min_abs",
            ]
        ]
    )


def main() -> None:
    args = parse_args()
    df = load_observations(args)
    buckets = prepare_buckets(args, df)

    outputs: list[tuple[str, pd.DataFrame]] = []
    if args.ranking in ("both", "late"):
        outputs.append(
            ("Most late lines", rank_late(buckets, args.min_observations, args.limit))
        )
    if args.ranking in ("both", "early"):
        outputs.append(
            ("Most early lines", rank_early(buckets, args.min_observations, args.limit))
        )

    csv_frames: list[pd.DataFrame] = []
    for title, table in outputs:
        print(title)
        print_or_empty(table)
        print()
        if not table.empty:
            export = table.copy()
            export.insert(0, "ranking", title)
            csv_frames.append(export)

    if args.output_csv:
        combined = pd.concat(csv_frames, ignore_index=True) if csv_frames else pd.DataFrame()
        write_optional_csv(combined, args.output_csv)


if __name__ == "__main__":
    main()
