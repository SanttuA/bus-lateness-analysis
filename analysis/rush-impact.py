from __future__ import annotations

import argparse

import pandas as pd

from _shared import (
    DELAY_FILTER_SQL,
    add_common_args,
    add_local_time_columns,
    add_rush_window_args,
    add_timezone_arg,
    connect_readonly_db,
    flag_rush_period,
    minutes,
    parse_rush_windows,
    print_or_empty,
    read_sql,
    round_numeric,
    rush_window_values,
    write_optional_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rank lines by how much worse they perform during rush windows."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_rush_window_args(parser)
    parser.set_defaults(limit=10, min_observations=1)
    return parser.parse_args()


def load_observations(args: argparse.Namespace) -> pd.DataFrame:
    query = f"""
    SELECT
        recorded_at_utc,
        line_ref,
        published_line_name,
        delay_seconds
    FROM vehicle_observations
    WHERE {DELAY_FILTER_SQL}
    """
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query)


def build_rush_impact(args: argparse.Namespace, df: pd.DataFrame) -> pd.DataFrame:
    df = add_local_time_columns(df, "recorded_at_utc", args.timezone)
    if df.empty:
        return pd.DataFrame()

    windows = parse_rush_windows(rush_window_values(args.rush_window))
    df["is_rush"] = flag_rush_period(
        df,
        windows,
        include_weekends=args.include_weekends,
    )
    df["delay_abs_seconds"] = df["delay_seconds"].abs()

    grouped = df.groupby(["line_ref", "is_rush"], as_index=False).agg(
        line_name=("published_line_name", "first"),
        obs_count=("delay_seconds", "size"),
        avg_delay_min=("delay_seconds", lambda s: minutes(s).mean()),
        avg_abs_delay_min=("delay_abs_seconds", lambda s: minutes(s).mean()),
        pct_late=("delay_seconds", lambda s: (s > 0).mean() * 100.0),
    )
    if not {True, False}.issubset(set(grouped["is_rush"])):
        return pd.DataFrame()

    pivot = grouped.pivot(index="line_ref", columns="is_rush")

    result = pd.DataFrame(index=pivot.index)
    result["line_name"] = pivot[("line_name", True)].combine_first(
        pivot[("line_name", False)]
    )
    result["rush_obs_count"] = pivot[("obs_count", True)]
    result["non_rush_obs_count"] = pivot[("obs_count", False)]
    result["rush_avg_delay_min"] = pivot[("avg_delay_min", True)]
    result["non_rush_avg_delay_min"] = pivot[("avg_delay_min", False)]
    result["rush_avg_abs_delay_min"] = pivot[("avg_abs_delay_min", True)]
    result["non_rush_avg_abs_delay_min"] = pivot[("avg_abs_delay_min", False)]
    result["rush_pct_late"] = pivot[("pct_late", True)]
    result["non_rush_pct_late"] = pivot[("pct_late", False)]
    result = result.reset_index()

    result = result.dropna(subset=["rush_obs_count", "non_rush_obs_count"])
    result = result[
        (result["rush_obs_count"] >= args.min_observations)
        & (result["non_rush_obs_count"] >= args.min_observations)
    ]
    result["rush_delay_lift_min"] = (
        result["rush_avg_delay_min"] - result["non_rush_avg_delay_min"]
    )
    result["rush_abs_delay_lift_min"] = (
        result["rush_avg_abs_delay_min"] - result["non_rush_avg_abs_delay_min"]
    )
    result["rush_late_pct_point_lift"] = (
        result["rush_pct_late"] - result["non_rush_pct_late"]
    )

    result = result.sort_values(
        ["rush_delay_lift_min", "rush_abs_delay_lift_min", "rush_obs_count"],
        ascending=[False, False, False],
    ).head(args.limit)
    return round_numeric(result)


def main() -> None:
    args = parse_args()
    df = load_observations(args)
    impact = build_rush_impact(args, df)

    print_or_empty(impact)
    write_optional_csv(impact, args.output_csv)


if __name__ == "__main__":
    main()
