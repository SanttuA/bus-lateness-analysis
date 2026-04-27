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


def build_missing_spots(
    polls: pd.DataFrame,
    gap_multiplier: float,
    min_missing_minutes: float,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if polls.empty:
        return pd.DataFrame()

    polls = polls.copy()
    polls["attempted_at_utc"] = pd.to_datetime(
        polls["attempted_at_utc"],
        utc=True,
        errors="coerce",
    )
    polls["collected_at_utc"] = pd.to_datetime(
        polls["collected_at_utc"],
        utc=True,
        errors="coerce",
    )

    for source, source_df in polls.groupby("source"):
        source_df = source_df.sort_values("attempted_at_utc")
        successful = source_df[source_df["ok"] == 1].copy()
        successful_gaps = successful["gap_seconds_since_previous_success"].dropna()
        successful_gaps = successful_gaps[successful_gaps > 0]
        if successful_gaps.empty:
            continue

        expected_cadence_seconds = float(successful_gaps.median())
        successful = successful.dropna(subset=["collected_at_utc"])
        previous_success_at: pd.Timestamp | None = None

        for success in successful.itertuples(index=False):
            current_success_at = success.collected_at_utc
            if previous_success_at is None:
                previous_success_at = current_success_at
                continue

            gap_seconds = (current_success_at - previous_success_at).total_seconds()
            missing_seconds = max(0.0, gap_seconds - expected_cadence_seconds)
            if gap_seconds <= gap_multiplier * expected_cadence_seconds:
                previous_success_at = current_success_at
                continue
            if missing_seconds / 60.0 < min_missing_minutes:
                previous_success_at = current_success_at
                continue

            failed_attempts = source_df[
                (source_df["attempted_at_utc"] > previous_success_at)
                & (source_df["attempted_at_utc"] < current_success_at)
                & (source_df["ok"] != 1)
            ]
            avg_rows_per_success = successful["row_count"].mean()
            estimated_missed_polls = missing_seconds / expected_cadence_seconds
            estimated_missed_rows = estimated_missed_polls * avg_rows_per_success

            rows.append(
                {
                    "source": source,
                    "gap_start_utc": previous_success_at.isoformat(),
                    "gap_end_utc": current_success_at.isoformat(),
                    "gap_min": gap_seconds / 60.0,
                    "expected_cadence_seconds": expected_cadence_seconds,
                    "missing_min": missing_seconds / 60.0,
                    "estimated_missed_polls": estimated_missed_polls,
                    "estimated_missed_rows": estimated_missed_rows,
                    "failed_attempt_count": int(failed_attempts.shape[0]),
                    "next_success_status": success.status,
                }
            )
            previous_success_at = current_success_at

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result = result.sort_values(["missing_min", "source"], ascending=[False, True])
    return round_numeric(result)


def summarize_missing_spots(spots: pd.DataFrame, polls: pd.DataFrame) -> pd.DataFrame:
    if polls.empty:
        return pd.DataFrame()

    summary_rows: list[dict[str, object]] = []
    spot_groups = spots.groupby("source") if not spots.empty else {}

    for source, source_df in polls.groupby("source"):
        source_spots = (
            spot_groups.get_group(source)
            if not spots.empty and source in spot_groups.groups
            else pd.DataFrame()
        )
        summary_rows.append(
            {
                "source": source,
                "poll_count": int(source_df.shape[0]),
                "success_count": int((source_df["ok"] == 1).sum()),
                "failed_count": int((source_df["ok"] != 1).sum()),
                "missing_spot_count": int(source_spots.shape[0]),
                "total_missing_min": source_spots["missing_min"].sum()
                if not source_spots.empty
                else 0.0,
                "largest_missing_min": source_spots["missing_min"].max()
                if not source_spots.empty
                else 0.0,
                "estimated_missed_polls": source_spots["estimated_missed_polls"].sum()
                if not source_spots.empty
                else 0.0,
                "estimated_missed_rows": source_spots["estimated_missed_rows"].sum()
                if not source_spots.empty
                else 0.0,
            }
        )

    result = pd.DataFrame(summary_rows)
    result = result.sort_values(
        ["total_missing_min", "missing_spot_count", "source"],
        ascending=[False, False, True],
    )
    return round_numeric(result)


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
