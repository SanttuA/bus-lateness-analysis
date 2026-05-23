from __future__ import annotations

import argparse

import polars as pl

from _shared import (
    add_bucket_arg,
    add_cache_args,
    add_common_args,
    add_quality_args,
    add_timezone_arg,
    print_or_empty,
    write_optional_csv,
)
from cli_common import load_delay_buckets_for_args
from report_cache import build_line_rankings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rank lines by robust late and early schedule inaccuracy using Polars."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    add_cache_args(parser)
    parser.add_argument("--ranking", choices=("both", "late", "early"), default="both")
    parser.set_defaults(limit=10, min_observations=30)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    buckets = load_delay_buckets_for_args(args)
    outputs: list[tuple[str, pl.DataFrame]] = []
    if args.ranking in ("both", "late"):
        outputs.append(
            (
                "Most late lines",
                build_line_rankings(
                    buckets,
                    "late",
                    min_observations=args.min_observations,
                    limit=args.limit,
                ),
            )
        )
    if args.ranking in ("both", "early"):
        outputs.append(
            (
                "Most early lines",
                build_line_rankings(
                    buckets,
                    "early",
                    min_observations=args.min_observations,
                    limit=args.limit,
                ),
            )
        )

    csv_frames: list[pl.DataFrame] = []
    for title, table in outputs:
        print(title)
        print_or_empty(table)
        print()
        if not table.is_empty():
            csv_frames.append(table.with_columns(pl.lit(title).alias("ranking")).select("ranking", *table.columns))
    if args.output_csv:
        combined = pl.concat(csv_frames, how="diagonal_relaxed") if csv_frames else pl.DataFrame()
        write_optional_csv(combined, args.output_csv)


if __name__ == "__main__":
    main()

