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
from collector_gaps import blackout_rows, source_summaries


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize collector polling blackouts by source."
    )
    add_common_args(parser)
    parser.set_defaults(limit=20, min_observations=1)
    return parser.parse_args()


def load_polls(args: argparse.Namespace) -> pd.DataFrame:
    query = """
    SELECT
        source,
        attempted_at_utc,
        collected_at_utc,
        status,
        ok,
        row_count,
        gap_seconds_since_previous_success
    FROM collector_polls
    ORDER BY source, attempted_at_utc
    """
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query)


def summarize_blackouts(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    """Per-source blackout totals (shared gap definition in collector_gaps)."""
    records = df.astype(object).where(df.notna(), None).to_dict("records")
    rows = blackout_rows(source_summaries(records))
    return round_numeric(pd.DataFrame(rows).head(limit)) if rows else pd.DataFrame()


def main() -> None:
    args = parse_args()
    polls = load_polls(args)
    summary = summarize_blackouts(polls, args.limit)

    print_or_empty(summary, "No collector polls found.")
    write_optional_csv(summary, args.output_csv)


if __name__ == "__main__":
    main()
