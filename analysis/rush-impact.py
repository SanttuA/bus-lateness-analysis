from __future__ import annotations

import argparse

import duckdb
import pandas as pd

from _shared import (
    holiday_dates_for_args,
    QUALIFIED_DELAY_FILTER_SQL,
    add_bucket_arg,
    add_cache_args,
    add_common_args,
    add_quality_args,
    add_rush_window_args,
    add_timezone_arg,
    aggregate_delay_buckets,
    apply_quality_filter,
    base_quality_query,
    connect_readonly_db,
    print_or_empty,
    read_sql,
    write_optional_csv,
)
from cached_queries import rush_impact as cached_rush_impact
from cached_queries import settings_from_args
from report_cache import rush_impact_frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rank lines by robust delay lift during rush windows."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    add_rush_window_args(parser)
    add_cache_args(parser)
    parser.set_defaults(limit=10, min_observations=30)
    return parser.parse_args()


def load_observations(args: argparse.Namespace) -> pd.DataFrame:
    query = base_quality_query(where=QUALIFIED_DELAY_FILTER_SQL)
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query)


def build_rush_impact(args: argparse.Namespace, df: pd.DataFrame) -> pd.DataFrame:
    """Legacy pandas path: bucket in pandas, then run the shared rush-vs-off-peak query."""
    df = apply_quality_filter(
        df,
        quality_mode=args.quality_mode,
        exclude_stop_call_disagreement=args.exclude_stop_call_disagreement,
    )
    df = aggregate_delay_buckets(
        df, bucket=args.bucket, timezone=args.timezone, holidays=holiday_dates_for_args(args)
    )
    if df.empty:
        return pd.DataFrame()
    settings = settings_from_args(args).resolved()
    with duckdb.connect() as con:
        con.register("legacy_rush_buckets", df)
        return rush_impact_frame(con, settings, source="legacy_rush_buckets")


def main() -> None:
    args = parse_args()
    if args.no_cache:
        df = load_observations(args)
        impact = build_rush_impact(args, df)
    else:
        impact = cached_rush_impact(args)

    print_or_empty(impact)
    write_optional_csv(impact, args.output_csv)


if __name__ == "__main__":
    main()
