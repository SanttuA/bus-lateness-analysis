from __future__ import annotations

import argparse

from _shared import (
    add_bucket_arg,
    add_cache_args,
    add_common_args,
    add_quality_args,
    add_rush_window_args,
    add_timezone_arg,
    print_or_empty,
    rush_window_values,
    write_optional_csv,
)
from cli_common import load_delay_buckets_for_args
from report_cache import build_rush_impact


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank lines by robust delay lift during rush windows.")
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    add_rush_window_args(parser)
    add_cache_args(parser)
    parser.set_defaults(limit=10, min_observations=30)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_rush_impact(
        load_delay_buckets_for_args(args),
        rush_windows=tuple(rush_window_values(args.rush_window)),
        include_weekends=args.include_weekends,
        min_observations=args.min_observations,
        limit=args.limit,
    )
    print_or_empty(result)
    write_optional_csv(result, args.output_csv)


if __name__ == "__main__":
    main()

