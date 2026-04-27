from __future__ import annotations

import argparse

import pandas as pd

from _shared import (
    DELAY_FILTER_SQL,
    add_common_args,
    connect_readonly_db,
    minutes,
    print_or_empty,
    read_sql,
    round_numeric,
    write_optional_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rank lines by late-only and early-only schedule inaccuracy."
    )
    add_common_args(parser)
    parser.add_argument(
        "--ranking",
        choices=("both", "late", "early"),
        default="both",
        help="Which ranking to print. Defaults to both.",
    )
    parser.set_defaults(limit=10, min_observations=1)
    return parser.parse_args()


def load_observations(args: argparse.Namespace) -> pd.DataFrame:
    query = f"""
    SELECT
        line_ref,
        published_line_name,
        delay_seconds
    FROM vehicle_observations
    WHERE {DELAY_FILTER_SQL}
    """
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query)


def rank_late(df: pd.DataFrame, min_observations: int, limit: int) -> pd.DataFrame:
    late = df[df["delay_seconds"] > 0].copy()
    if late.empty:
        return pd.DataFrame()

    grouped = late.groupby("line_ref", as_index=False).agg(
        line_name=("published_line_name", "first"),
        late_obs_count=("delay_seconds", "size"),
        avg_late_min=("delay_seconds", lambda s: minutes(s).mean()),
        median_late_min=("delay_seconds", lambda s: minutes(s).median()),
        max_late_min=("delay_seconds", lambda s: minutes(s).max()),
    )
    grouped = grouped[grouped["late_obs_count"] >= min_observations]
    grouped = grouped.sort_values(
        ["avg_late_min", "late_obs_count", "line_ref"],
        ascending=[False, False, True],
    ).head(limit)
    return round_numeric(grouped)


def rank_early(df: pd.DataFrame, min_observations: int, limit: int) -> pd.DataFrame:
    early = df[df["delay_seconds"] < 0].copy()
    if early.empty:
        return pd.DataFrame()

    early["early_seconds_abs"] = early["delay_seconds"].abs()
    grouped = early.groupby("line_ref", as_index=False).agg(
        line_name=("published_line_name", "first"),
        early_obs_count=("early_seconds_abs", "size"),
        avg_early_min=("early_seconds_abs", lambda s: minutes(s).mean()),
        median_early_min=("early_seconds_abs", lambda s: minutes(s).median()),
        max_early_min=("early_seconds_abs", lambda s: minutes(s).max()),
    )
    grouped = grouped[grouped["early_obs_count"] >= min_observations]
    grouped = grouped.sort_values(
        ["avg_early_min", "early_obs_count", "line_ref"],
        ascending=[False, False, True],
    ).head(limit)
    return round_numeric(grouped)


def main() -> None:
    args = parse_args()
    df = load_observations(args)

    outputs: list[tuple[str, pd.DataFrame]] = []
    if args.ranking in ("both", "late"):
        outputs.append(("Most late lines", rank_late(df, args.min_observations, args.limit)))
    if args.ranking in ("both", "early"):
        outputs.append(("Most early lines", rank_early(df, args.min_observations, args.limit)))

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
