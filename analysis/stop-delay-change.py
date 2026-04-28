from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _shared import (
    QUALIFIED_DELAY_FILTER_SQL,
    add_bucket_arg,
    add_common_args,
    add_quality_args,
    add_timezone_arg,
    aggregate_delay_buckets,
    apply_quality_filter,
    base_quality_query,
    connect_readonly_db,
    latest_gtfs_dir,
    print_or_empty,
    read_sql,
    resolve_project_path,
    round_numeric,
    summarize_delay_metrics,
    write_optional_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare robust delay changes by next stop, or by city part, using "
            "explicit matched periods."
        )
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    parser.add_argument(
        "--gtfs-dir",
        type=Path,
        help="GTFS directory containing stops.txt. Defaults to the newest data/gtfs/* directory.",
    )
    parser.add_argument(
        "--city-parts-csv",
        type=Path,
        help="Optional CSV with stop_id and city_part columns.",
    )
    parser.add_argument(
        "--group-by",
        choices=("stop", "city-part"),
        default="stop",
        help="Aggregate by stop or by city part. Defaults to stop.",
    )
    parser.add_argument(
        "--sort-by",
        choices=("increase", "decrease", "absolute"),
        default="absolute",
        help="How to rank p90 delay changes. Defaults to absolute.",
    )
    parser.add_argument(
        "--line-ref",
        help="Limit analysis to one line_ref, for example 3 or 10A.",
    )
    parser.add_argument(
        "--direction-ref",
        help="Limit analysis to one direction_ref.",
    )
    parser.add_argument("--baseline-start", help="Baseline period start timestamp.")
    parser.add_argument("--baseline-end", help="Baseline period end timestamp.")
    parser.add_argument("--comparison-start", help="Comparison period start timestamp.")
    parser.add_argument("--comparison-end", help="Comparison period end timestamp.")
    parser.add_argument(
        "--legacy-midpoint",
        action="store_true",
        help="Use the old automatic first-half vs second-half split when explicit periods are absent.",
    )
    parser.set_defaults(limit=20, min_observations=30)
    return parser.parse_args()


def load_observations(args: argparse.Namespace) -> pd.DataFrame:
    where = f"{QUALIFIED_DELAY_FILTER_SQL} AND v.next_stop_point_ref IS NOT NULL"
    params: list[object] = []
    if args.line_ref:
        where += " AND v.line_ref = ?"
        params.append(args.line_ref)
    if args.direction_ref:
        where += " AND v.direction_ref = ?"
        params.append(args.direction_ref)

    query = base_quality_query(where=where)
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query, params)


def load_stop_metadata(gtfs_dir_arg: Path | None) -> pd.DataFrame:
    gtfs_dir = resolve_project_path(gtfs_dir_arg) if gtfs_dir_arg else latest_gtfs_dir()
    if gtfs_dir is None:
        return pd.DataFrame(columns=["stop_id", "gtfs_stop_name", "stop_lat", "stop_lon"])

    stops_path = gtfs_dir / "stops.txt"
    if not stops_path.exists():
        raise SystemExit(f"GTFS stops.txt not found: {stops_path}")

    stops = pd.read_csv(stops_path, dtype={"stop_id": "string"})
    return stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]].rename(
        columns={"stop_name": "gtfs_stop_name"}
    )


def load_city_parts(path: Path | None) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame(columns=["stop_id", "city_part"])

    mapping_path = resolve_project_path(path)
    if not mapping_path.exists():
        raise SystemExit(f"City-part mapping CSV not found: {mapping_path}")

    mapping = pd.read_csv(mapping_path, dtype={"stop_id": "string", "city_part": "string"})
    required = {"stop_id", "city_part"}
    missing = required.difference(mapping.columns)
    if missing:
        raise SystemExit(
            f"{mapping_path} is missing required column(s): {', '.join(sorted(missing))}"
        )
    return mapping[["stop_id", "city_part"]]


def parse_timestamp(value: str, timezone: str) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(timezone)
    return timestamp.tz_convert("UTC")


def add_period_column(df: pd.DataFrame, args: argparse.Namespace) -> tuple[pd.DataFrame, str]:
    period_args = [
        args.baseline_start,
        args.baseline_end,
        args.comparison_start,
        args.comparison_end,
    ]
    result = df.copy()
    result["representative_time_utc"] = pd.to_datetime(
        result["representative_time_utc"],
        utc=True,
        errors="coerce",
    )
    result = result.dropna(subset=["representative_time_utc"])

    if any(period_args):
        if not all(period_args):
            raise SystemExit(
                "Provide all four period arguments: --baseline-start, --baseline-end, "
                "--comparison-start, and --comparison-end."
            )
        baseline_start = parse_timestamp(args.baseline_start, args.timezone)
        baseline_end = parse_timestamp(args.baseline_end, args.timezone)
        comparison_start = parse_timestamp(args.comparison_start, args.timezone)
        comparison_end = parse_timestamp(args.comparison_end, args.timezone)
    elif args.legacy_midpoint:
        start = result["representative_time_utc"].min()
        end = result["representative_time_utc"].max()
        midpoint = start + ((end - start) / 2)
        baseline_start = start
        baseline_end = midpoint
        comparison_start = midpoint
        comparison_end = end + pd.Timedelta(microseconds=1)
    else:
        raise SystemExit(
            "Stop-change analysis now requires explicit matched periods. Provide "
            "--baseline-start, --baseline-end, --comparison-start, and "
            "--comparison-end, or pass --legacy-midpoint for the old automatic split."
        )

    result["period"] = pd.NA
    result.loc[
        (result["representative_time_utc"] >= baseline_start)
        & (result["representative_time_utc"] < baseline_end),
        "period",
    ] = "baseline"
    result.loc[
        (result["representative_time_utc"] >= comparison_start)
        & (result["representative_time_utc"] < comparison_end),
        "period",
    ] = "comparison"
    result = result.dropna(subset=["period"])

    description = (
        f"baseline={baseline_start.isoformat()}..{baseline_end.isoformat()}, "
        f"comparison={comparison_start.isoformat()}..{comparison_end.isoformat()}"
    )
    return result, description


def enrich_stops(
    df: pd.DataFrame,
    stops: pd.DataFrame,
    city_parts: pd.DataFrame,
) -> pd.DataFrame:
    result = df.copy()
    result["stop_id"] = result["next_stop_point_ref"].astype("string")

    if not stops.empty:
        stops = stops.copy()
        stops["stop_id"] = stops["stop_id"].astype("string")
        result = result.merge(stops, how="left", on="stop_id")
    else:
        result["gtfs_stop_name"] = pd.NA
        result["stop_lat"] = pd.NA
        result["stop_lon"] = pd.NA

    result["stop_name"] = result["gtfs_stop_name"].combine_first(
        result["next_stop_point_name"]
    )

    if not city_parts.empty:
        result = result.merge(city_parts, how="left", on="stop_id")
    else:
        result["city_part"] = pd.NA

    return result


def matched_context_rows(df: pd.DataFrame, group_keys: list[str]) -> pd.DataFrame:
    context_keys = group_keys + ["line_ref", "direction_ref", "local_weekday", "local_hour"]
    baseline_contexts = (
        df[df["period"] == "baseline"][context_keys].drop_duplicates().reset_index(drop=True)
    )
    comparison_contexts = (
        df[df["period"] == "comparison"][context_keys].drop_duplicates().reset_index(drop=True)
    )
    matched_contexts = baseline_contexts.merge(
        comparison_contexts,
        how="inner",
        on=context_keys,
    )
    if matched_contexts.empty:
        return pd.DataFrame(columns=df.columns)
    return df.merge(matched_contexts, how="inner", on=context_keys)


def summarize_period(df: pd.DataFrame, keys: list[str], prefix: str) -> pd.DataFrame:
    extra_agg: dict[str, tuple[str, str]] = {}
    for column in ("stop_name", "city_part", "stop_lat", "stop_lon"):
        if column in df.columns and column not in keys:
            extra_agg[column] = (column, "first")

    grouped = summarize_delay_metrics(
        df,
        keys,
        min_observations=1,
        extra_aggs=extra_agg,
    )

    metric_columns = {
        column: f"{prefix}_{column}"
        for column in grouped.columns
        if column not in keys and column not in extra_agg
    }
    return grouped.rename(columns=metric_columns)


def build_stop_change(args: argparse.Namespace, df: pd.DataFrame) -> tuple[pd.DataFrame, str]:
    if df.empty:
        return pd.DataFrame(), ""

    df = apply_quality_filter(
        df,
        quality_mode=args.quality_mode,
        exclude_stop_call_disagreement=args.exclude_stop_call_disagreement,
    )
    df = aggregate_delay_buckets(df, bucket=args.bucket, timezone=args.timezone)
    if df.empty:
        return pd.DataFrame(), ""

    stops = load_stop_metadata(args.gtfs_dir)
    city_parts = load_city_parts(args.city_parts_csv)
    df = enrich_stops(df, stops, city_parts)

    if args.group_by == "city-part":
        if city_parts.empty:
            raise SystemExit("--group-by city-part requires --city-parts-csv.")
        df = df.dropna(subset=["city_part"])
        keys = ["city_part"]
    else:
        keys = ["stop_id"]

    df, period_description = add_period_column(df, args)
    df = matched_context_rows(df, keys)
    if df.empty:
        return pd.DataFrame(), period_description

    baseline = summarize_period(df[df["period"] == "baseline"], keys, "baseline")
    comparison = summarize_period(df[df["period"] == "comparison"], keys, "comparison")
    result = baseline.merge(comparison, how="inner", on=keys, suffixes=("", "_comparison"))

    for column in ("stop_name", "city_part", "stop_lat", "stop_lon"):
        comparison_column = f"{column}_comparison"
        if comparison_column in result.columns:
            if column in result.columns:
                result[column] = result[column].combine_first(result[comparison_column])
            else:
                result[column] = result[comparison_column]
            result = result.drop(columns=[comparison_column])

    result = result[
        (result["baseline_bucket_count"] >= args.min_observations)
        & (result["comparison_bucket_count"] >= args.min_observations)
    ]
    if result.empty:
        return result, period_description

    result["median_delay_change_min"] = (
        result["comparison_median_delay_min"] - result["baseline_median_delay_min"]
    )
    result["p90_delay_change_min"] = (
        result["comparison_p90_delay_min"] - result["baseline_p90_delay_min"]
    )
    result["over_5_min_late_pct_point_change"] = (
        result["comparison_pct_over_5_min_late"]
        - result["baseline_pct_over_5_min_late"]
    )

    if args.sort_by == "increase":
        result = result.sort_values(
            ["p90_delay_change_min", "comparison_bucket_count"],
            ascending=[False, False],
        )
    elif args.sort_by == "decrease":
        result = result.sort_values(
            ["p90_delay_change_min", "comparison_bucket_count"],
            ascending=[True, False],
        )
    else:
        result = result.assign(abs_delay_change=result["p90_delay_change_min"].abs())
        result = result.sort_values(
            ["abs_delay_change", "comparison_bucket_count"],
            ascending=[False, False],
        ).drop(columns=["abs_delay_change"])

    ordered_columns = keys.copy()
    for column in ("stop_name", "city_part", "stop_lat", "stop_lon"):
        if column in result.columns and column not in ordered_columns:
            ordered_columns.append(column)
    ordered_columns.extend(
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
    result = result[ordered_columns].head(args.limit)
    return round_numeric(result), period_description


def main() -> None:
    args = parse_args()
    observations = load_observations(args)
    result, period_description = build_stop_change(args, observations)

    if period_description:
        print(period_description)
    print_or_empty(result)
    write_optional_csv(result, args.output_csv)


if __name__ == "__main__":
    main()
