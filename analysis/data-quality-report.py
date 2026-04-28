from __future__ import annotations

import argparse

import pandas as pd

from _shared import (
    CONSERVATIVE_EXCLUSION_COLUMNS,
    QUALIFIED_DELAY_FILTER_SQL,
    QUALITY_FLAG_COLUMNS,
    add_common_args,
    add_timezone_arg,
    add_quality_flags,
    base_quality_query,
    connect_readonly_db,
    print_or_empty,
    read_sql,
    round_numeric,
    write_optional_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report data-quality flags before delay metrics are computed."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    parser.add_argument(
        "--view",
        choices=("summary", "line", "examples"),
        default="summary",
        help="Which quality report to print. Defaults to summary.",
    )
    parser.set_defaults(limit=20, min_observations=1)
    return parser.parse_args()


def load_observations(args: argparse.Namespace) -> pd.DataFrame:
    query = base_quality_query(where=QUALIFIED_DELAY_FILTER_SQL)
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query)


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    total = len(df)
    rows = [
        {
            "quality_check": "analysis_rows",
            "row_count": total,
            "pct_rows": 100.0,
        }
    ]
    for column in QUALITY_FLAG_COLUMNS:
        count = int(df[column].sum())
        rows.append(
            {
                "quality_check": column,
                "row_count": count,
                "pct_rows": count / total * 100.0,
            }
        )

    conservative_excluded = df[CONSERVATIVE_EXCLUSION_COLUMNS].any(axis=1)
    rows.append(
        {
            "quality_check": "conservative_excluded_default",
            "row_count": int(conservative_excluded.sum()),
            "pct_rows": conservative_excluded.mean() * 100.0,
        }
    )
    with_stop_call = conservative_excluded | df["has_stop_call_disagreement"]
    rows.append(
        {
            "quality_check": "conservative_excluded_with_stop_call_disagreement",
            "row_count": int(with_stop_call.sum()),
            "pct_rows": with_stop_call.mean() * 100.0,
        }
    )
    return round_numeric(pd.DataFrame(rows))


def build_line_report(df: pd.DataFrame, min_observations: int, limit: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    working = df.copy()
    working["conservative_excluded_default"] = working[
        CONSERVATIVE_EXCLUSION_COLUMNS
    ].any(axis=1)
    grouped = working.groupby("line_ref", as_index=False).agg(
        row_count=("delay_seconds", "size"),
        line_name=("published_line_name", "first"),
        implausible_delay_rows=("is_implausible_delay", "sum"),
        stale_rows=("is_stale_observation", "sum"),
        pre_trip_rows=("is_pre_trip_observation", "sum"),
        post_trip_rows=("is_post_trip_observation", "sum"),
        stop_call_disagreement_rows=("has_stop_call_disagreement", "sum"),
        conservative_excluded_rows=("conservative_excluded_default", "sum"),
    )
    grouped = grouped[grouped["row_count"] >= min_observations].copy()
    if grouped.empty:
        return grouped
    grouped["conservative_excluded_pct"] = (
        grouped["conservative_excluded_rows"] / grouped["row_count"] * 100.0
    )
    grouped = grouped.sort_values(
        ["conservative_excluded_pct", "conservative_excluded_rows", "line_ref"],
        ascending=[False, False, True],
    ).head(limit)
    return round_numeric(grouped)


def build_examples(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()

    flagged = df[df[QUALITY_FLAG_COLUMNS].any(axis=1)].copy()
    if flagged.empty:
        return flagged
    flagged["delay_min"] = flagged["delay_seconds"] / 60.0
    columns = [
        "recorded_at_utc",
        "line_ref",
        "direction_ref",
        "vehicle_id",
        "trip_match_key",
        "next_stop_point_ref",
        "delay_min",
        "observation_age_seconds",
        "stop_call_delay_diff_seconds",
        *QUALITY_FLAG_COLUMNS,
    ]
    return round_numeric(
        flagged.sort_values(["quality_issue_count", "recorded_at_utc"], ascending=[False, True])[
            columns
        ].head(limit)
    )


def main() -> None:
    args = parse_args()
    df = add_quality_flags(load_observations(args))

    if args.view == "summary":
        result = build_summary(df)
    elif args.view == "line":
        result = build_line_report(df, args.min_observations, args.limit)
    else:
        result = build_examples(df, args.limit)

    print_or_empty(result)
    write_optional_csv(result, args.output_csv)


if __name__ == "__main__":
    main()
