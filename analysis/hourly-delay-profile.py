from __future__ import annotations

import argparse

import pandas as pd

from _shared import (
    DELAY_FILTER_SQL,
    add_common_args,
    add_local_time_columns,
    add_timezone_arg,
    connect_readonly_db,
    minutes,
    print_or_empty,
    read_sql,
    round_numeric,
    write_optional_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show when buses are most late by local hour."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    parser.add_argument(
        "--line-ref",
        help="Limit the hourly profile to one line_ref, for example 3 or 10A.",
    )
    parser.set_defaults(limit=24, min_observations=1)
    return parser.parse_args()


def load_observations(args: argparse.Namespace) -> pd.DataFrame:
    where = DELAY_FILTER_SQL
    params: list[object] = []
    if args.line_ref:
        where += " AND line_ref = ?"
        params.append(args.line_ref)

    query = f"""
    SELECT
        recorded_at_utc,
        line_ref,
        delay_seconds
    FROM vehicle_observations
    WHERE {where}
    """
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query, params)


def build_profile(df: pd.DataFrame, timezone: str, min_observations: int, limit: int) -> pd.DataFrame:
    df = add_local_time_columns(df, "recorded_at_utc", timezone)
    if df.empty:
        return pd.DataFrame()

    grouped = df.groupby("local_hour", as_index=False).agg(
        obs_count=("delay_seconds", "size"),
        avg_delay_min=("delay_seconds", lambda s: minutes(s).mean()),
        median_delay_min=("delay_seconds", lambda s: minutes(s).median()),
        pct_late=("delay_seconds", lambda s: (s > 0).mean() * 100.0),
        pct_over_3_min_late=("delay_seconds", lambda s: (s > 180).mean() * 100.0),
    )
    grouped = grouped[grouped["obs_count"] >= min_observations]
    grouped["hour_local"] = grouped["local_hour"].map(lambda hour: f"{hour:02d}:00")
    grouped = grouped.sort_values(["avg_delay_min", "obs_count"], ascending=[False, False])
    grouped = grouped.head(limit)
    grouped = grouped[
        [
            "hour_local",
            "obs_count",
            "avg_delay_min",
            "median_delay_min",
            "pct_late",
            "pct_over_3_min_late",
        ]
    ]
    return round_numeric(grouped)


def main() -> None:
    args = parse_args()
    df = load_observations(args)
    profile = build_profile(df, args.timezone, args.min_observations, args.limit)

    print_or_empty(profile)
    write_optional_csv(profile, args.output_csv)


if __name__ == "__main__":
    main()
