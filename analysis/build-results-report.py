from __future__ import annotations

import argparse
from pathlib import Path

from _shared import (
    BUCKET_MODES,
    DEFAULT_BUCKET_MODE,
    DEFAULT_DB_PATH,
    DEFAULT_QUALITY_MODE,
    DEFAULT_RUSH_WINDOWS,
    DEFAULT_TIMEZONE,
    QUALITY_MODES,
)
from report_cache import (
    DEFAULT_CACHE_DIR,
    DEFAULT_REPORT_PATH,
    ReportSettings,
    ensure_report_cache,
    write_markdown_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build cached intermediate analysis results and render one Markdown "
            "report for the full bus lateness dataset."
        )
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to the Foli SQLite database. Defaults to {DEFAULT_DB_PATH}.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help=f"Directory for DuckDB cache and compact CSV outputs. Defaults to {DEFAULT_CACHE_DIR}.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_REPORT_PATH,
        help=f"Markdown report path. Defaults to {DEFAULT_REPORT_PATH}.",
    )
    parser.add_argument(
        "--quality-mode",
        choices=QUALITY_MODES,
        default=DEFAULT_QUALITY_MODE,
        help=f"Quality handling for delay rows. Defaults to {DEFAULT_QUALITY_MODE}.",
    )
    parser.add_argument(
        "--exclude-stop-call-disagreement",
        action="store_true",
        help="In conservative mode, also exclude stop-call delay disagreement rows.",
    )
    parser.add_argument(
        "--bucket",
        choices=BUCKET_MODES,
        default=DEFAULT_BUCKET_MODE,
        help=f"Pre-metric aggregation bucket. Defaults to {DEFAULT_BUCKET_MODE}.",
    )
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help=f"Local timezone for local-hour analysis. Defaults to {DEFAULT_TIMEZONE}.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Rows to include in ranked report tables. Defaults to 20.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=30,
        help="Minimum buckets required for grouped metrics. Defaults to 30.",
    )
    parser.add_argument(
        "--rush-window",
        action="append",
        default=None,
        help=(
            "Rush window in HH:MM-HH:MM local time. May be repeated. "
            f"Defaults to {', '.join(DEFAULT_RUSH_WINDOWS)}."
        ),
    )
    parser.add_argument(
        "--include-weekends",
        action="store_true",
        help="Include weekends in rush-window comparisons.",
    )
    parser.add_argument(
        "--gtfs-dir",
        type=Path,
        help="GTFS directory containing routes.txt for alert route mapping.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild cached intermediate results even if the manifest is fresh.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = ReportSettings(
        db=args.db,
        cache_dir=args.cache_dir,
        quality_mode=args.quality_mode,
        bucket=args.bucket,
        timezone=args.timezone,
        limit=args.limit,
        min_observations=args.min_observations,
        exclude_stop_call_disagreement=args.exclude_stop_call_disagreement,
        rush_windows=tuple(args.rush_window or DEFAULT_RUSH_WINDOWS),
        include_weekends=args.include_weekends,
        gtfs_dir=args.gtfs_dir,
    )
    cache_result = ensure_report_cache(settings, force=args.force)
    output_path = write_markdown_report(settings, cache_result, args.output)
    print(f"Cache {cache_result.status}: {cache_result.cache_db}")
    print(f"Wrote report: {output_path}")


if __name__ == "__main__":
    main()
