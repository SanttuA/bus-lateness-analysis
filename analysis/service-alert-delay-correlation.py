from __future__ import annotations

import argparse
import json
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


ALERT_GROUP_COLUMNS = ["cause", "effect", "priority", "alert_scope"]
MATCH_CONTEXT_COLUMNS = ["line_ref", "direction_ref", "local_hour", "day_type"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare alert-period delays with matched non-alert controls."
    )
    add_common_args(parser)
    add_timezone_arg(parser)
    add_quality_args(parser)
    add_bucket_arg(parser)
    parser.add_argument(
        "--gtfs-dir",
        type=Path,
        help="GTFS directory containing routes.txt. Defaults to the newest data/gtfs/* directory.",
    )
    parser.add_argument(
        "--view",
        choices=("grouped", "line", "both"),
        default="grouped",
        help="Print alert groups, alert groups by line, or both. Defaults to grouped.",
    )
    parser.add_argument(
        "--alert-kind",
        choices=("any", "route", "stop"),
        default="any",
        help="Which alert target scope to use. Defaults to any.",
    )
    parser.add_argument(
        "--line-ref",
        help="Limit analysis to one line_ref, for example 3 or 10A.",
    )
    parser.set_defaults(limit=20, min_observations=30)
    return parser.parse_args()


def json_list(value: object) -> list[str]:
    if value is None or pd.isna(value):
        return []
    if isinstance(value, str) and not value.strip():
        return []
    try:
        decoded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    if not isinstance(decoded, list):
        return []
    return [
        str(item)
        for item in decoded
        if item is not None and not isinstance(item, dict | list)
    ]


def load_route_map(gtfs_dir_arg: Path | None) -> dict[str, str]:
    gtfs_dir = resolve_project_path(gtfs_dir_arg) if gtfs_dir_arg else latest_gtfs_dir()
    if gtfs_dir is None:
        return {}

    routes_path = gtfs_dir / "routes.txt"
    if not routes_path.exists():
        raise SystemExit(f"GTFS routes.txt not found: {routes_path}")

    routes = pd.read_csv(routes_path, dtype={"route_id": "string", "route_short_name": "string"})
    return dict(zip(routes["route_id"], routes["route_short_name"], strict=False))


def load_observations(args: argparse.Namespace) -> pd.DataFrame:
    where = QUALIFIED_DELAY_FILTER_SQL
    params: list[object] = []
    if args.line_ref:
        where += " AND v.line_ref = ?"
        params.append(args.line_ref)

    query = base_quality_query(where=where)
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query, params)


def load_alerts(args: argparse.Namespace) -> pd.DataFrame:
    query = """
    SELECT
        source_alert_id,
        line_ref,
        cause,
        effect,
        priority,
        is_active,
        validity_start_utc,
        validity_end_utc,
        affected_routes_json,
        affected_stops_json,
        created_at_utc
    FROM service_alerts
    WHERE is_active = 1
    """
    with connect_readonly_db(args.db) as con:
        return read_sql(con, query)


def build_alert_targets(
    alerts: pd.DataFrame,
    route_map: dict[str, str],
    obs_start: pd.Timestamp,
    obs_end: pd.Timestamp,
    *,
    include_routes: bool,
    include_stops: bool,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if alerts.empty:
        return pd.DataFrame(
            columns=[*ALERT_GROUP_COLUMNS, "target_ref", "start_utc", "end_utc"]
        )

    for alert in alerts.itertuples(index=False):
        start = pd.to_datetime(alert.validity_start_utc, utc=True, errors="coerce")
        end = pd.to_datetime(alert.validity_end_utc, utc=True, errors="coerce")
        if pd.isna(start):
            start = pd.to_datetime(alert.created_at_utc, utc=True, errors="coerce")
        if pd.isna(start):
            start = obs_start
        if pd.isna(end):
            end = obs_end + pd.Timedelta(microseconds=1)

        base = {
            "cause": _clean_alert_value(alert.cause),
            "effect": _clean_alert_value(alert.effect),
            "priority": int(alert.priority) if not pd.isna(alert.priority) else -1,
            "start_utc": start,
            "end_utc": end,
        }

        if include_routes:
            lines = set()
            if alert.line_ref is not None and not pd.isna(alert.line_ref):
                lines.add(str(alert.line_ref))
            for route_ref in json_list(alert.affected_routes_json):
                lines.add(route_map.get(route_ref, route_ref))
            for line_ref in lines:
                rows.append({**base, "alert_scope": "route", "target_ref": line_ref})

        if include_stops:
            for stop_id in set(json_list(alert.affected_stops_json)):
                rows.append({**base, "alert_scope": "stop", "target_ref": stop_id})

    if not rows:
        return pd.DataFrame(
            columns=[*ALERT_GROUP_COLUMNS, "target_ref", "start_utc", "end_utc"]
        )
    return pd.DataFrame(rows).drop_duplicates().reset_index(drop=True)


def mark_active_for_group(observations: pd.DataFrame, intervals: pd.DataFrame) -> pd.Series:
    active = pd.Series(False, index=observations.index)
    if intervals.empty:
        return active

    scope = intervals["alert_scope"].iloc[0]
    if scope == "route":
        observation_key = "line_ref"
    else:
        observation_key = "next_stop_point_ref"

    obs_keys = observations[observation_key].astype("string")
    obs_times = pd.to_datetime(
        observations["representative_time_utc"],
        utc=True,
        errors="coerce",
    )
    intervals = intervals.copy()
    intervals["target_ref"] = intervals["target_ref"].astype("string")

    for target_ref, target_intervals in intervals.groupby("target_ref"):
        key_mask = obs_keys == target_ref
        if not key_mask.any():
            continue
        target_times = obs_times[key_mask]
        target_active = pd.Series(False, index=target_times.index)
        for interval in target_intervals.itertuples(index=False):
            target_active |= (target_times >= interval.start_utc) & (
                target_times <= interval.end_utc
            )
        active.loc[target_active.index] |= target_active

    return active


def matched_control_rows(
    observations: pd.DataFrame,
    active_mask: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    active = observations[active_mask].copy()
    if active.empty:
        return active, pd.DataFrame(columns=observations.columns)

    contexts = active[MATCH_CONTEXT_COLUMNS].drop_duplicates()
    controls = observations[~active_mask].merge(
        contexts,
        how="inner",
        on=MATCH_CONTEXT_COLUMNS,
    )
    return active, controls


def summarize_alert_lift(
    active: pd.DataFrame,
    controls: pd.DataFrame,
    *,
    min_observations: int,
    group_keys: list[str] | None = None,
) -> pd.DataFrame:
    group_keys = group_keys or []
    active_metrics = summarize_delay_metrics(
        active,
        group_keys,
        min_observations=min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")}
        if "line_ref" in group_keys
        else None,
    )
    control_metrics = summarize_delay_metrics(
        controls,
        group_keys,
        min_observations=min_observations,
        extra_aggs={"line_name": ("published_line_name", "first")}
        if "line_ref" in group_keys
        else None,
    )
    if active_metrics.empty or control_metrics.empty:
        return pd.DataFrame()

    if group_keys:
        result = control_metrics.merge(
            active_metrics,
            how="inner",
            on=group_keys,
            suffixes=("_control", "_alert"),
        )
    else:
        result = pd.concat(
            [
                control_metrics.add_suffix("_control"),
                active_metrics.add_suffix("_alert"),
            ],
            axis=1,
        )
    if result.empty:
        return result

    if "line_name_control" in result.columns:
        result["line_name"] = result["line_name_alert"].combine_first(
            result["line_name_control"]
        )
        result = result.drop(columns=["line_name_control", "line_name_alert"])

    result["median_delay_lift_min"] = (
        result["median_delay_min_alert"] - result["median_delay_min_control"]
    )
    result["p90_delay_lift_min"] = (
        result["p90_delay_min_alert"] - result["p90_delay_min_control"]
    )
    result["over_5_min_late_pct_point_lift"] = (
        result["pct_over_5_min_late_alert"] - result["pct_over_5_min_late_control"]
    )
    return result


def build_correlation(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    observations = load_observations(args)
    if observations.empty:
        return pd.DataFrame(), pd.DataFrame()

    observations = apply_quality_filter(
        observations,
        quality_mode=args.quality_mode,
        exclude_stop_call_disagreement=args.exclude_stop_call_disagreement,
    )
    observations = aggregate_delay_buckets(
        observations,
        bucket=args.bucket,
        timezone=args.timezone,
    )
    if observations.empty:
        return pd.DataFrame(), pd.DataFrame()

    observations["line_ref"] = observations["line_ref"].astype("string")
    observations["direction_ref"] = observations["direction_ref"].astype("string")
    observations["next_stop_point_ref"] = observations["next_stop_point_ref"].astype("string")

    alerts = load_alerts(args)
    route_map = load_route_map(args.gtfs_dir)
    include_routes = args.alert_kind in ("route", "any")
    include_stops = args.alert_kind in ("stop", "any")
    targets = build_alert_targets(
        alerts,
        route_map,
        observations["representative_time_utc"].min(),
        observations["representative_time_utc"].max(),
        include_routes=include_routes,
        include_stops=include_stops,
    )
    if targets.empty:
        return pd.DataFrame(), pd.DataFrame()

    grouped_rows: list[pd.DataFrame] = []
    line_rows: list[pd.DataFrame] = []
    for group_values, intervals in targets.groupby(ALERT_GROUP_COLUMNS, dropna=False):
        active_mask = mark_active_for_group(observations, intervals)
        active, controls = matched_control_rows(observations, active_mask)
        if active.empty or controls.empty:
            continue

        group_data = dict(zip(ALERT_GROUP_COLUMNS, group_values, strict=True))
        grouped = summarize_alert_lift(
            active,
            controls,
            min_observations=args.min_observations,
        )
        if not grouped.empty:
            for column, value in group_data.items():
                grouped[column] = value
            grouped_rows.append(grouped)

        by_line = summarize_alert_lift(
            active,
            controls,
            min_observations=args.min_observations,
            group_keys=["line_ref"],
        )
        if not by_line.empty:
            for column, value in group_data.items():
                by_line[column] = value
            line_rows.append(by_line)

    grouped_result = _format_alert_result(
        pd.concat(grouped_rows, ignore_index=True) if grouped_rows else pd.DataFrame(),
        args.limit,
    )
    line_result = _format_alert_result(
        pd.concat(line_rows, ignore_index=True) if line_rows else pd.DataFrame(),
        args.limit,
        include_line=True,
    )
    return grouped_result, line_result


def _format_alert_result(
    df: pd.DataFrame,
    limit: int,
    *,
    include_line: bool = False,
) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.sort_values(
        ["p90_delay_lift_min", "over_5_min_late_pct_point_lift", "bucket_count_alert"],
        ascending=[False, False, False],
    ).head(limit)

    ordered = ALERT_GROUP_COLUMNS.copy()
    if include_line:
        ordered.extend(["line_ref", "line_name"])
    ordered.extend(
        [
            "bucket_count_control",
            "bucket_count_alert",
            "raw_poll_count_control",
            "raw_poll_count_alert",
            "median_delay_min_control",
            "median_delay_min_alert",
            "median_delay_lift_min",
            "p90_delay_min_control",
            "p90_delay_min_alert",
            "p90_delay_lift_min",
            "pct_over_5_min_late_control",
            "pct_over_5_min_late_alert",
            "over_5_min_late_pct_point_lift",
            "pct_over_3_min_early_control",
            "pct_over_3_min_early_alert",
        ]
    )
    return round_numeric(df[ordered])


def _clean_alert_value(value: object) -> str:
    if value is None or pd.isna(value) or str(value).strip() == "":
        return "Unknown"
    return str(value)


def main() -> None:
    args = parse_args()
    grouped, line = build_correlation(args)

    csv_frames: list[pd.DataFrame] = []
    if args.view in ("both", "grouped"):
        print("Alert matched-control correlation")
        print_or_empty(grouped)
        print()
        if not grouped.empty:
            export = grouped.copy()
            export.insert(0, "view", "grouped")
            csv_frames.append(export)

    if args.view in ("both", "line"):
        print("Alert matched-control correlation by line")
        print_or_empty(line)
        print()
        if not line.empty:
            export = line.copy()
            export.insert(0, "view", "line")
            csv_frames.append(export)

    if args.output_csv:
        combined = pd.concat(csv_frames, ignore_index=True) if csv_frames else pd.DataFrame()
        write_optional_csv(combined, args.output_csv)


if __name__ == "__main__":
    main()
