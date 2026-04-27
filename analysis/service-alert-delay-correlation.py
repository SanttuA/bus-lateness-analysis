from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from _shared import (
    DELAY_FILTER_SQL,
    add_common_args,
    connect_readonly_db,
    latest_gtfs_dir,
    minutes,
    print_or_empty,
    read_sql,
    resolve_project_path,
    round_numeric,
    write_optional_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare observed delays when service alerts are active vs inactive."
    )
    add_common_args(parser)
    parser.add_argument(
        "--gtfs-dir",
        type=Path,
        help="GTFS directory containing routes.txt. Defaults to the newest data/gtfs/* directory.",
    )
    parser.add_argument(
        "--scope",
        choices=("both", "overall", "line"),
        default="both",
        help="Which table to print. Defaults to both.",
    )
    parser.add_argument(
        "--alert-kind",
        choices=("any", "route", "stop"),
        default="route",
        help="Which alert match to use for the active-alert flag. Defaults to route.",
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
    where = DELAY_FILTER_SQL
    params: list[object] = []
    if args.line_ref:
        where += " AND line_ref = ?"
        params.append(args.line_ref)

    query = f"""
    SELECT
        recorded_at_utc,
        line_ref,
        published_line_name,
        delay_seconds,
        next_stop_point_ref
    FROM vehicle_observations
    WHERE {where}
    """
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


def build_alert_intervals(
    alerts: pd.DataFrame,
    route_map: dict[str, str],
    obs_start: pd.Timestamp,
    obs_end: pd.Timestamp,
    *,
    include_routes: bool,
    include_stops: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if alerts.empty:
        empty_route = pd.DataFrame(columns=["line_ref", "start_utc", "end_utc"])
        empty_stop = pd.DataFrame(columns=["stop_id", "start_utc", "end_utc"])
        return empty_route, empty_stop

    route_rows: list[dict[str, object]] = []
    stop_rows: list[dict[str, object]] = []

    for alert in alerts.itertuples(index=False):
        start = pd.to_datetime(alert.validity_start_utc, utc=True, errors="coerce")
        end = pd.to_datetime(alert.validity_end_utc, utc=True, errors="coerce")
        if pd.isna(start):
            start = pd.to_datetime(alert.created_at_utc, utc=True, errors="coerce")
        if pd.isna(start):
            start = obs_start
        if pd.isna(end):
            end = obs_end + pd.Timedelta(microseconds=1)

        lines = set()
        if include_routes:
            if alert.line_ref is not None and not pd.isna(alert.line_ref):
                lines.add(str(alert.line_ref))
            for route_ref in json_list(alert.affected_routes_json):
                lines.add(route_map.get(route_ref, route_ref))

        stops = set(json_list(alert.affected_stops_json)) if include_stops else set()

        for line_ref in lines:
            route_rows.append(
                {
                    "line_ref": line_ref,
                    "start_utc": start,
                    "end_utc": end,
                    "source_alert_id": alert.source_alert_id,
                    "cause": alert.cause,
                    "effect": alert.effect,
                    "priority": alert.priority,
                }
            )
        for stop_id in stops:
            stop_rows.append(
                {
                    "stop_id": stop_id,
                    "start_utc": start,
                    "end_utc": end,
                    "source_alert_id": alert.source_alert_id,
                    "cause": alert.cause,
                    "effect": alert.effect,
                    "priority": alert.priority,
                }
            )

    route_intervals = collapse_intervals(pd.DataFrame(route_rows), "line_ref")
    stop_intervals = collapse_intervals(pd.DataFrame(stop_rows), "stop_id")
    return route_intervals, stop_intervals


def collapse_intervals(intervals: pd.DataFrame, key_column: str) -> pd.DataFrame:
    if intervals.empty:
        return pd.DataFrame(columns=[key_column, "start_utc", "end_utc"])

    intervals = intervals[[key_column, "start_utc", "end_utc"]].drop_duplicates()
    intervals = intervals.sort_values([key_column, "start_utc", "end_utc"])
    rows: list[dict[str, object]] = []

    for key, key_intervals in intervals.groupby(key_column):
        current_start: pd.Timestamp | None = None
        current_end: pd.Timestamp | None = None

        for interval in key_intervals.itertuples(index=False):
            start = interval.start_utc
            end = interval.end_utc
            if current_start is None:
                current_start = start
                current_end = end
                continue
            if start <= current_end:
                current_end = max(current_end, end)
                continue

            rows.append(
                {
                    key_column: key,
                    "start_utc": current_start,
                    "end_utc": current_end,
                }
            )
            current_start = start
            current_end = end

        if current_start is not None:
            rows.append(
                {
                    key_column: key,
                    "start_utc": current_start,
                    "end_utc": current_end,
                }
            )

    return pd.DataFrame(rows)


def mark_active_intervals(
    observations: pd.DataFrame,
    intervals: pd.DataFrame,
    observation_key: str,
    interval_key: str,
    output_column: str,
) -> pd.DataFrame:
    result = observations.copy()
    result[output_column] = False
    if intervals.empty:
        return result

    result[observation_key] = result[observation_key].astype("string")
    intervals = intervals.copy()
    intervals[interval_key] = intervals[interval_key].astype("string")

    for key, key_intervals in intervals.groupby(interval_key):
        key_mask = result[observation_key] == key
        if not key_mask.any():
            continue
        times = result.loc[key_mask, "recorded_at_utc"]
        active = pd.Series(False, index=times.index)
        for interval in key_intervals.itertuples(index=False):
            active |= (times >= interval.start_utc) & (times <= interval.end_utc)
        result.loc[active.index, output_column] = active

    return result


def summarize_alert_lift(
    df: pd.DataFrame,
    group_keys: list[str],
    min_observations: int,
    limit: int | None = None,
) -> pd.DataFrame:
    result_group_keys = group_keys
    working = df
    if not group_keys:
        working = df.copy()
        working["_scope"] = "overall"
        group_keys = ["_scope"]

    extra_agg = {}
    if "line_ref" in result_group_keys:
        extra_agg["line_name"] = ("published_line_name", "first")

    grouped = working.groupby(group_keys + ["active_alert"], as_index=False).agg(
        obs_count=("delay_seconds", "size"),
        avg_delay_min=("delay_seconds", lambda s: minutes(s).mean()),
        median_delay_min=("delay_seconds", lambda s: minutes(s).median()),
        pct_late=("delay_seconds", lambda s: (s > 0).mean() * 100.0),
        pct_over_3_min_late=("delay_seconds", lambda s: (s > 180).mean() * 100.0),
        pct_route_alert=("active_route_alert", lambda s: s.mean() * 100.0),
        pct_stop_alert=("active_stop_alert", lambda s: s.mean() * 100.0),
        **extra_agg,
    )
    if grouped.empty or not {True, False}.issubset(set(grouped["active_alert"])):
        return pd.DataFrame()

    active = grouped[grouped["active_alert"]].drop(columns=["active_alert"])
    inactive = grouped[~grouped["active_alert"]].drop(columns=["active_alert"])
    result = inactive.merge(
        active,
        how="inner",
        on=group_keys,
        suffixes=("_no_alert", "_alert"),
    )
    result = result[
        (result["obs_count_no_alert"] >= min_observations)
        & (result["obs_count_alert"] >= min_observations)
    ]
    if result.empty:
        return result

    result["delay_lift_min"] = (
        result["avg_delay_min_alert"] - result["avg_delay_min_no_alert"]
    )
    result["late_pct_point_lift"] = (
        result["pct_late_alert"] - result["pct_late_no_alert"]
    )
    result["over_3_min_late_pct_point_lift"] = (
        result["pct_over_3_min_late_alert"] - result["pct_over_3_min_late_no_alert"]
    )

    if "line_name_no_alert" in result.columns:
        result["line_name"] = result["line_name_alert"].combine_first(
            result["line_name_no_alert"]
        )
        result = result.drop(columns=["line_name_no_alert", "line_name_alert"])

    result = result.sort_values(
        ["delay_lift_min", "obs_count_alert"],
        ascending=[False, False],
    )
    if limit is not None:
        result = result.head(limit)

    if "_scope" in result.columns:
        result = result.drop(columns=["_scope"])

    ordered_columns = result_group_keys.copy()
    if "line_name" in result.columns and "line_name" not in ordered_columns:
        ordered_columns.append("line_name")
    ordered_columns.extend(
        [
            "obs_count_no_alert",
            "obs_count_alert",
            "avg_delay_min_no_alert",
            "avg_delay_min_alert",
            "delay_lift_min",
            "pct_late_no_alert",
            "pct_late_alert",
            "late_pct_point_lift",
            "pct_over_3_min_late_no_alert",
            "pct_over_3_min_late_alert",
            "over_3_min_late_pct_point_lift",
            "pct_route_alert_alert",
            "pct_stop_alert_alert",
        ]
    )
    return round_numeric(result[ordered_columns])


def build_correlation(args: argparse.Namespace) -> tuple[pd.DataFrame, pd.DataFrame]:
    observations = load_observations(args)
    if observations.empty:
        return pd.DataFrame(), pd.DataFrame()

    observations["recorded_at_utc"] = pd.to_datetime(
        observations["recorded_at_utc"],
        utc=True,
        errors="coerce",
    )
    observations = observations.dropna(subset=["recorded_at_utc"])
    observations["line_ref"] = observations["line_ref"].astype("string")
    observations["next_stop_point_ref"] = observations["next_stop_point_ref"].astype("string")

    alerts = load_alerts(args)
    route_map = load_route_map(args.gtfs_dir)
    include_routes = args.alert_kind in ("route", "any")
    include_stops = args.alert_kind in ("stop", "any")
    route_intervals, stop_intervals = build_alert_intervals(
        alerts,
        route_map,
        observations["recorded_at_utc"].min(),
        observations["recorded_at_utc"].max(),
        include_routes=include_routes,
        include_stops=include_stops,
    )

    if include_routes:
        observations = mark_active_intervals(
            observations,
            route_intervals,
            "line_ref",
            "line_ref",
            "active_route_alert",
        )
    else:
        observations["active_route_alert"] = False

    if include_stops:
        observations = mark_active_intervals(
            observations,
            stop_intervals,
            "next_stop_point_ref",
            "stop_id",
            "active_stop_alert",
        )
    else:
        observations["active_stop_alert"] = False

    if args.alert_kind == "route":
        observations["active_alert"] = observations["active_route_alert"]
    elif args.alert_kind == "stop":
        observations["active_alert"] = observations["active_stop_alert"]
    else:
        observations["active_alert"] = (
            observations["active_route_alert"] | observations["active_stop_alert"]
        )

    overall = summarize_alert_lift(observations, [], args.min_observations)
    line = summarize_alert_lift(
        observations,
        ["line_ref"],
        args.min_observations,
        args.limit,
    )
    return overall, line


def main() -> None:
    args = parse_args()
    overall, line = build_correlation(args)

    csv_frames: list[pd.DataFrame] = []
    if args.scope in ("both", "overall"):
        print("Overall alert correlation")
        print_or_empty(overall)
        print()
        if not overall.empty:
            export = overall.copy()
            export.insert(0, "scope", "overall")
            csv_frames.append(export)

    if args.scope in ("both", "line"):
        print("Line alert correlation")
        print_or_empty(line)
        print()
        if not line.empty:
            export = line.copy()
            export.insert(0, "scope", "line")
            csv_frames.append(export)

    if args.output_csv:
        combined = pd.concat(csv_frames, ignore_index=True) if csv_frames else pd.DataFrame()
        write_optional_csv(combined, args.output_csv)


if __name__ == "__main__":
    main()
