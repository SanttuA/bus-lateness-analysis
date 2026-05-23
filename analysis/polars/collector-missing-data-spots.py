from __future__ import annotations

import argparse

import polars as pl

from _shared import add_common_args, print_or_empty, write_optional_csv
from report_cache import (
    ReportSettings,
    build_missing_spots,
    load_collector_polls,
    summarize_missing_spots,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List collector gaps and estimate how much data was missed using Polars."
    )
    add_common_args(parser)
    parser.add_argument("--source")
    parser.add_argument("--gap-multiplier", type=float, default=2.0)
    parser.add_argument("--min-missing-minutes", type=float, default=0.0)
    parser.add_argument("--view", choices=("both", "summary", "spots"), default="both")
    parser.set_defaults(limit=20, min_observations=1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = ReportSettings(db=args.db, limit=args.limit, min_observations=args.min_observations).resolved()
    polls = load_collector_polls(settings, source=args.source)
    spots = build_missing_spots(
        polls,
        gap_multiplier=args.gap_multiplier,
        min_missing_minutes=args.min_missing_minutes,
    )
    summary = summarize_missing_spots(spots, polls)
    csv_frames: list[pl.DataFrame] = []
    if args.view in ("both", "summary"):
        print("Collector missing-data summary")
        print_or_empty(summary, "No collector polls found.")
        print()
        if not summary.is_empty():
            csv_frames.append(summary.with_columns(pl.lit("summary").alias("view")).select("view", *summary.columns))
    if args.view in ("both", "spots"):
        print("Collector missing-data spots")
        shown = spots.head(args.limit) if not spots.is_empty() else spots
        print_or_empty(shown, "No missing-data spots found.")
        print()
        if not spots.is_empty():
            csv_frames.append(spots.with_columns(pl.lit("spots").alias("view")).select("view", *spots.columns))
    if args.output_csv:
        combined = pl.concat(csv_frames, how="diagonal_relaxed") if csv_frames else pl.DataFrame()
        write_optional_csv(combined, args.output_csv)


if __name__ == "__main__":
    main()

