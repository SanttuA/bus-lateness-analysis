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
    rows: list[dict[str, object]] = []
    for source, source_df in df.groupby("source"):
        successful = source_df[source_df["ok"] == 1].copy()
        successful_gaps = successful["gap_seconds_since_previous_success"].dropna()
        successful_gaps = successful_gaps[successful_gaps > 0]

        if successful_gaps.empty:
            expected_cadence_seconds = float("nan")
            blackout_df = successful.iloc[0:0]
        else:
            expected_cadence_seconds = float(successful_gaps.median())
            blackout_df = successful[
                successful["gap_seconds_since_previous_success"]
                > (2 * expected_cadence_seconds)
            ].copy()

        avg_rows_per_success = successful["row_count"].mean() if not successful.empty else 0.0
        blackout_duration_seconds = (
            blackout_df["gap_seconds_since_previous_success"] - expected_cadence_seconds
        ).clip(lower=0)
        estimated_missed_polls = blackout_duration_seconds / expected_cadence_seconds
        estimated_missed_rows = estimated_missed_polls * avg_rows_per_success

        rows.append(
            {
                "source": source,
                "poll_count": len(source_df),
                "success_count": int(successful.shape[0]),
                "failed_count": int((source_df["ok"] != 1).sum()),
                "expected_cadence_seconds": expected_cadence_seconds,
                "blackout_count": int(blackout_df.shape[0]),
                "total_blackout_min": blackout_duration_seconds.sum() / 60.0,
                "largest_blackout_min": blackout_duration_seconds.max() / 60.0
                if not blackout_duration_seconds.empty
                else 0.0,
                "estimated_missed_polls": estimated_missed_polls.sum(),
                "estimated_missed_rows": estimated_missed_rows.sum(),
            }
        )

    result = pd.DataFrame(rows)
    result = result.sort_values(
        ["total_blackout_min", "blackout_count", "source"],
        ascending=[False, False, True],
    ).head(limit)
    return round_numeric(result)


def main() -> None:
    args = parse_args()
    polls = load_polls(args)
    summary = summarize_blackouts(polls, args.limit)

    print_or_empty(summary, "No collector polls found.")
    write_optional_csv(summary, args.output_csv)


if __name__ == "__main__":
    main()
