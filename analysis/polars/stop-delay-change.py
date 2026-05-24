from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl

from _shared import (
    DEFAULT_GTFS_ROOT,
    QUALIFIED_DELAY_FILTER_SQL,
    add_bucket_arg,
    add_cache_args,
    add_common_args,
    add_gtfs_args,
    add_quality_args,
    add_timezone_arg,
    load_gtfs_stop_metadata,
    parse_timestamp,
    print_or_empty,
    read_sql,
    representative_time_sql,
    resolve_project_path,
    round_numeric,
    summarize_delay_metrics,
    write_optional_csv,
)
from cli_common import load_delay_buckets_for_args
from report_cache import enrich_stops, matched_context_rows, summarize_period


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare robust delay changes by next stop or city part using Polars."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    add_cache_args(parser)
    add_gtfs_args(parser, file_description="stops.txt")
    parser.add_argument("--city-parts-csv", type=Path)
    parser.add_argument("--group-by", choices=("stop", "city-part"), default="stop")
    parser.add_argument("--sort-by", choices=("increase", "decrease", "absolute"), default="absolute")
    parser.add_argument("--line-ref")
    parser.add_argument("--direction-ref")
    parser.add_argument("--baseline-start")
    parser.add_argument("--baseline-end")
    parser.add_argument("--comparison-start")
    parser.add_argument("--comparison-end")
    parser.add_argument("--legacy-midpoint", action="store_true")
    parser.set_defaults(limit=20, min_observations=30)
    return parser.parse_args()


def load_city_parts(path: Path | None) -> pl.DataFrame:
    if path is None:
        return pl.DataFrame({"stop_id": [], "city_part": []})
    mapping_path = resolve_project_path(path)
    if not mapping_path.exists():
        raise SystemExit(f"City-part mapping CSV not found: {mapping_path}")
    mapping = pl.read_csv(mapping_path, schema_overrides={"stop_id": pl.Utf8, "city_part": pl.Utf8})
    missing = {"stop_id", "city_part"}.difference(mapping.columns)
    if missing:
        raise SystemExit(f"{mapping_path} is missing required column(s): {', '.join(sorted(missing))}")
    return mapping.select("stop_id", "city_part")


def default_recent_periods(
    db_path: Path,
    *,
    timezone: str,
    period_days: int = 1,
) -> tuple[str | None, str | None, str | None, str | None]:
    if period_days <= 0:
        raise ValueError("period_days must be positive")

    query = f"""
    SELECT MAX({representative_time_sql()}) AS latest_time_utc
    FROM vehicle_observations v
    WHERE {QUALIFIED_DELAY_FILTER_SQL}
      AND v.next_stop_point_ref IS NOT NULL
    """
    rows = read_sql(db_path, query)
    latest_value = rows["latest_time_utc"][0] if not rows.is_empty() else None
    if latest_value is None:
        return None, None, None, None

    latest = parse_timestamp(latest_value, "UTC").astimezone(ZoneInfo(timezone))
    if latest.microsecond:
        latest = latest.replace(microsecond=0) + timedelta(seconds=1)
    comparison_end = latest + timedelta(seconds=1)
    comparison_start = comparison_end - timedelta(days=period_days)
    baseline_end = comparison_start
    baseline_start = baseline_end - timedelta(days=period_days)
    return (
        baseline_start.isoformat(),
        baseline_end.isoformat(),
        comparison_start.isoformat(),
        comparison_end.isoformat(),
    )


def add_period_column(df: pl.DataFrame, args: argparse.Namespace) -> tuple[pl.DataFrame, str]:
    periods = [args.baseline_start, args.baseline_end, args.comparison_start, args.comparison_end]
    if any(periods):
        if not all(periods):
            raise SystemExit(
                "Provide all four period arguments: --baseline-start, --baseline-end, "
                "--comparison-start, and --comparison-end."
            )
        baseline_start = parse_timestamp(args.baseline_start, args.timezone)
        baseline_end = parse_timestamp(args.baseline_end, args.timezone)
        comparison_start = parse_timestamp(args.comparison_start, args.timezone)
        comparison_end = parse_timestamp(args.comparison_end, args.timezone)
    elif args.legacy_midpoint:
        baseline_start = df["representative_time_utc"].min()
        comparison_end = df["representative_time_utc"].max()
        if baseline_start is None or comparison_end is None or baseline_start >= comparison_end:
            return pl.DataFrame(schema=df.schema), ""
        baseline_end = baseline_start + ((comparison_end - baseline_start) / 2)
        comparison_start = baseline_end
    else:
        raise SystemExit(
            "Stop-change analysis now requires explicit matched periods. Provide "
            "--baseline-start, --baseline-end, --comparison-start, and --comparison-end, "
            "or pass --legacy-midpoint for the old automatic split."
        )
    if baseline_start >= baseline_end:
        raise SystemExit("Baseline period start must be before baseline period end.")
    if comparison_start >= comparison_end:
        raise SystemExit("Comparison period start must be before comparison period end.")
    result = df.with_columns(
        pl.when(
            (pl.col("representative_time_utc") >= baseline_start)
            & (pl.col("representative_time_utc") < baseline_end)
        )
        .then(pl.lit("baseline"))
        .when(
            (pl.col("representative_time_utc") >= comparison_start)
            & (pl.col("representative_time_utc") < comparison_end)
        )
        .then(pl.lit("comparison"))
        .otherwise(None)
        .alias("period")
    ).filter(pl.col("period").is_not_null())
    description = (
        f"baseline={baseline_start.isoformat()}..{baseline_end.isoformat()}, "
        f"comparison={comparison_start.isoformat()}..{comparison_end.isoformat()}"
    )
    return result, description


def build_stop_change_from_buckets(
    args: argparse.Namespace,
    buckets: pl.DataFrame,
) -> tuple[pl.DataFrame, str]:
    df = buckets.filter(pl.col("next_stop_point_ref").is_not_null())
    if args.line_ref:
        df = df.filter(pl.col("line_ref") == args.line_ref)
    if args.direction_ref:
        df = df.filter(pl.col("direction_ref") == args.direction_ref)
    if df.is_empty():
        return pl.DataFrame(), ""
    stops = load_gtfs_stop_metadata(
        gtfs_dir=resolve_project_path(args.gtfs_dir) if args.gtfs_dir else None,
        gtfs_root=resolve_project_path(getattr(args, "gtfs_root", DEFAULT_GTFS_ROOT)),
    )
    city_parts = load_city_parts(args.city_parts_csv)
    df = enrich_stops(df, stops, city_parts)
    if args.group_by == "city-part":
        if city_parts.is_empty():
            raise SystemExit("--group-by city-part requires --city-parts-csv.")
        df = df.filter(pl.col("city_part").is_not_null())
        keys = ["city_part"]
    else:
        keys = ["stop_id"]
    df, period_description = add_period_column(df, args)
    if df.is_empty():
        return pl.DataFrame(), period_description
    df = matched_context_rows(df, keys)
    if df.is_empty():
        return pl.DataFrame(), period_description
    baseline = summarize_period(df.filter(pl.col("period") == "baseline"), keys, "baseline")
    comparison = summarize_period(df.filter(pl.col("period") == "comparison"), keys, "comparison")
    result = baseline.join(comparison, on=keys, how="inner", suffix="_comparison")
    for column in ("stop_name", "city_part", "stop_lat", "stop_lon"):
        comparison_column = f"{column}_comparison"
        if comparison_column in result.columns:
            if column in result.columns:
                result = result.with_columns(
                    pl.coalesce(pl.col(column), pl.col(comparison_column)).alias(column)
                ).drop(comparison_column)
            else:
                result = result.rename({comparison_column: column})
    result = result.filter(
        (pl.col("baseline_bucket_count") >= args.min_observations)
        & (pl.col("comparison_bucket_count") >= args.min_observations)
    )
    if result.is_empty():
        return result, period_description
    result = result.with_columns(
        (pl.col("comparison_median_delay_min") - pl.col("baseline_median_delay_min")).alias(
            "median_delay_change_min"
        ),
        (pl.col("comparison_p90_delay_min") - pl.col("baseline_p90_delay_min")).alias(
            "p90_delay_change_min"
        ),
        (
            pl.col("comparison_pct_over_5_min_late")
            - pl.col("baseline_pct_over_5_min_late")
        ).alias("over_5_min_late_pct_point_change"),
    )
    if args.sort_by == "increase":
        result = result.sort(["p90_delay_change_min", "comparison_bucket_count"], descending=[True, True])
    elif args.sort_by == "decrease":
        result = result.sort(["p90_delay_change_min", "comparison_bucket_count"], descending=[False, True])
    else:
        result = result.with_columns(pl.col("p90_delay_change_min").abs().alias("_abs_change")).sort(
            ["_abs_change", "comparison_bucket_count"], descending=[True, True]
        ).drop("_abs_change")
    ordered = keys.copy()
    for column in ("stop_name", "city_part", "stop_lat", "stop_lon"):
        if column in result.columns and column not in ordered:
            ordered.append(column)
    ordered.extend(
        [
            "baseline_bucket_count",
            "comparison_bucket_count",
            "baseline_raw_poll_count",
            "comparison_raw_poll_count",
            "baseline_median_delay_min",
            "comparison_median_delay_min",
            "median_delay_change_min",
            "baseline_p90_delay_min",
            "comparison_p90_delay_min",
            "p90_delay_change_min",
            "baseline_pct_over_5_min_late",
            "comparison_pct_over_5_min_late",
            "over_5_min_late_pct_point_change",
            "baseline_pct_over_3_min_early",
            "comparison_pct_over_3_min_early",
        ]
    )
    return round_numeric(result.select([column for column in ordered if column in result.columns]).head(args.limit)), period_description


def main() -> None:
    args = parse_args()
    result, period_description = build_stop_change_from_buckets(args, load_delay_buckets_for_args(args))
    if period_description:
        print(period_description)
    print_or_empty(result)
    write_optional_csv(result, args.output_csv)


if __name__ == "__main__":
    main()
