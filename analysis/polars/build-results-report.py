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
        description="Build Polars-backed cached analysis results and render a Markdown report."
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument("--quality-mode", choices=QUALITY_MODES, default=DEFAULT_QUALITY_MODE)
    parser.add_argument("--exclude-stop-call-disagreement", action="store_true")
    parser.add_argument("--bucket", choices=BUCKET_MODES, default=DEFAULT_BUCKET_MODE)
    parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--min-observations", type=int, default=30)
    parser.add_argument("--rush-window", action="append", default=None)
    parser.add_argument("--include-weekends", action="store_true")
    parser.add_argument("--gtfs-dir", type=Path)
    parser.add_argument("--gtfs-root", type=Path, default=Path("data/gtfs"))
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def print_progress(message: str) -> None:
    print(f"[report] {message}", flush=True)


def format_elapsed(seconds: float | None) -> str:
    if seconds is None:
        return "not recorded"
    return f"{seconds:.2f}s"


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
        gtfs_root=args.gtfs_root,
    )
    cache_result = ensure_report_cache(
        settings,
        force=args.force,
        progress=print_progress,
    )
    print_progress("Rendering Markdown report")
    output_path = write_markdown_report(settings, cache_result, args.output)
    print_progress(
        f"Finished in {format_elapsed(cache_result.timings.get('total_report_seconds'))}"
    )
    print(f"Cache {cache_result.status}: {cache_result.cache_db}")
    print(f"Wrote report: {output_path}")


if __name__ == "__main__":
    main()
