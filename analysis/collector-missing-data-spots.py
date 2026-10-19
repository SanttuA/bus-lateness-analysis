from __future__ import annotations

import argparse

import pandas as pd

from _shared import (
    add_common_args,
    connect_readonly_db,
    print_or_empty,
    read_sql,
    round_numeric,
    write_optional_csv,
)
from collector_gaps import missing_spots, missing_summary_rows, source_summaries


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List collector gaps and estimate how much data was missed."
    )
    add_common_args(parser)
    parser.add_argument(
        "--source",
        help="Limit to one collector source, for example siri_vm or siri_alerts.",
    )
    parser.add_argument(
        "--gap-multiplier",
        type=float,
        default=2.0,
        help="Treat success gaps larger than this many expected cadences as missing data.",
    )
    parser.add_argument(
        "--min-missing-minutes",
        type=float,
        default=0.0,
        help="Only show gaps with at least this many estimated missing minutes.",
    )
    parser.add_argument(
        "--view",
        choices=("both", "summary", "spots"),
        default="both",
        help="Which table to print. Defaults to both.",
    )
    parser.set_defaults(limit=20, min_observations=1)
    return parser.parse_args()


def load_polls(args: argparse.Namespace) -> pd.DataFrame:
    where = "1 = 1"
    params: list[object] = []
    if args.source:
        where += " AND source = ?"
        params.append(args.source)

    query = f"""
    SELECT
        source,
        attempted_at_utc,
        collected_at_utc,
        status,
        ok,
        row_count,
        gap_seconds_since_previous_success
    FROM collector_polls
    WHERE {where}
    ORDER BY source, attempted_at_utc
    """
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query, params)


def _poll_records(polls: pd.DataFrame) -> list[dict[str, object]]:
    return polls.astype(object).where(polls.notna(), None).to_dict("records")


def build_missing_spots(
    polls: pd.DataFrame,
    gap_multiplier: float = 2.0,
    min_missing_minutes: float = 0.0,
) -> pd.DataFrame:
    """Gaps between consecutive successful polls (shared definition in collector_gaps)."""
    rows = missing_spots(
        _poll_records(polls),
        gap_multiplier=gap_multiplier,
        min_missing_minutes=min_missing_minutes,
    )
    return round_numeric(pd.DataFrame(rows)) if rows else pd.DataFrame()


def summarize_missing_spots(spots: pd.DataFrame, polls: pd.DataFrame) -> pd.DataFrame:
    del spots  # Recomputed from the polls with the shared gap definition.
    if polls.empty:
        return pd.DataFrame()
    return round_numeric(pd.DataFrame(missing_summary_rows(source_summaries(_poll_records(polls)))))


def main() -> None:
    args = parse_args()
    polls = load_polls(args)
    spots = build_missing_spots(
        polls,
        args.gap_multiplier,
        args.min_missing_minutes,
    )
    summary = summarize_missing_spots(spots, polls)

    csv_frames: list[pd.DataFrame] = []
    if args.view in ("both", "summary"):
        print("Collector missing-data summary")
        print_or_empty(summary, "No collector polls found.")
        print()
        if not summary.empty:
            export = summary.copy()
            export.insert(0, "view", "summary")
            csv_frames.append(export)

    if args.view in ("both", "spots"):
        print("Collector missing-data spots")
        print_or_empty(spots.head(args.limit), "No missing-data spots found.")
        print()
        if not spots.empty:
            export = spots.copy()
            export.insert(0, "view", "spots")
            csv_frames.append(export)

    if args.output_csv:
        combined = pd.concat(csv_frames, ignore_index=True) if csv_frames else pd.DataFrame()
        write_optional_csv(combined, args.output_csv)


if __name__ == "__main__":
    main()
