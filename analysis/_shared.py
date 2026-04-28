from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "foli.db"
DEFAULT_GTFS_ROOT = PROJECT_ROOT / "data" / "gtfs"
DEFAULT_TIMEZONE = "Europe/Helsinki"
DEFAULT_RUSH_WINDOWS = ("07:00-09:00", "15:00-18:00")
QUALITY_MODES = ("conservative", "diagnostic", "raw")
BUCKET_MODES = ("poll", "trip-stop", "vehicle-trip", "line-hour")
DEFAULT_QUALITY_MODE = "conservative"
DEFAULT_BUCKET_MODE = "trip-stop"
MAX_ABS_DELAY_SECONDS = 120 * 60
STALE_OBSERVATION_SECONDS = 5 * 60
PRE_TRIP_GRACE_SECONDS = 15 * 60
POST_TRIP_GRACE_SECONDS = 30 * 60
STOP_CALL_DISAGREEMENT_SECONDS = 10 * 60

DELAY_FILTER_SQL = """
    is_gtfs_matchable = 1
    AND delay_seconds IS NOT NULL
    AND line_ref IS NOT NULL
"""

QUALIFIED_DELAY_FILTER_SQL = """
    v.is_gtfs_matchable = 1
    AND v.delay_seconds IS NOT NULL
    AND v.line_ref IS NOT NULL
"""

QUALITY_SELECT_SQL = """
    v.id,
    v.poll_id,
    v.vehicle_id,
    v.recorded_at_utc,
    v.valid_until_utc,
    p.collected_at_utc,
    v.line_ref,
    v.direction_ref,
    v.origin_aimed_departure_time_utc,
    v.trip_match_key,
    v.published_line_name,
    v.delay_seconds,
    v.next_stop_point_ref,
    v.next_stop_point_name,
    v.next_aimed_arrival_time_utc,
    v.next_expected_arrival_time_utc,
    v.next_aimed_departure_time_utc,
    v.next_expected_departure_time_utc,
    v.destination_aimed_arrival_time_utc,
    v.created_at_utc
"""

QUALITY_JOIN_SQL = """
    LEFT JOIN collector_polls p ON p.id = v.poll_id
"""

QUALITY_FLAG_COLUMNS = [
    "is_implausible_delay",
    "is_stale_observation",
    "is_pre_trip_observation",
    "is_post_trip_observation",
    "has_stop_call_disagreement",
]

CONSERVATIVE_EXCLUSION_COLUMNS = [
    "is_implausible_delay",
    "is_stale_observation",
    "is_pre_trip_observation",
    "is_post_trip_observation",
]

DELAY_METRIC_COLUMNS = [
    "signed_mean_delay_min",
    "median_delay_min",
    "p75_delay_min",
    "p90_delay_min",
    "p95_delay_min",
]


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to the Foli SQLite database. Defaults to {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of rows to show.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=1,
        help="Only include groups with at least this many observations.",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        help="Optional CSV output path.",
    )


def add_timezone_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help=f"Local timezone for time-of-day analysis. Defaults to {DEFAULT_TIMEZONE}.",
    )


def add_rush_window_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--rush-window",
        action="append",
        default=None,
        help=(
            "Rush window in HH:MM-HH:MM local time. May be repeated. "
            f"Defaults to {', '.join(DEFAULT_RUSH_WINDOWS)}."
        ),
    )
    parser.add_argument(
        "--include-weekends",
        action="store_true",
        help="Include weekends in rush-window comparisons.",
    )


def add_quality_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--quality-mode",
        choices=QUALITY_MODES,
        default=DEFAULT_QUALITY_MODE,
        help=(
            "Quality handling for delay rows. conservative filters clear bad rows, "
            "diagnostic only adds flags, and raw skips filtering. Defaults to "
            f"{DEFAULT_QUALITY_MODE}."
        ),
    )
    parser.add_argument(
        "--exclude-stop-call-disagreement",
        action="store_true",
        help=(
            "In conservative mode, also exclude rows where VM delay differs from "
            "next stop-call expected-vs-aimed delay by more than 10 minutes."
        ),
    )


def add_bucket_arg(
    parser: argparse.ArgumentParser,
    *,
    default: str = DEFAULT_BUCKET_MODE,
) -> None:
    parser.add_argument(
        "--bucket",
        choices=BUCKET_MODES,
        default=default,
        help=(
            "Aggregation bucket before metrics. trip-stop collapses repeated polls "
            "for the same vehicle trip and next stop. Defaults to "
            f"{default}."
        ),
    )


def resolve_db_path(path: Path) -> Path:
    db_path = path.expanduser()
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path
    db_path = db_path.resolve()
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")
    return db_path


def resolve_project_path(path: Path) -> Path:
    resolved = path.expanduser()
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    return resolved.resolve()


def latest_gtfs_dir(root: Path = DEFAULT_GTFS_ROOT) -> Path | None:
    gtfs_root = resolve_project_path(root)
    if not gtfs_root.exists():
        return None

    candidates = [
        path
        for path in gtfs_root.iterdir()
        if path.is_dir() and (path / "stops.txt").exists()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.name)


def connect_readonly_db(path: Path) -> sqlite3.Connection:
    db_path = resolve_db_path(path)
    uri = f"file:{db_path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def read_sql(con: sqlite3.Connection, query: str, params: object | None = None) -> pd.DataFrame:
    return pd.read_sql_query(query, con, params=params)


def base_quality_query(
    *,
    where: str = QUALIFIED_DELAY_FILTER_SQL,
    extra_columns: str = "",
) -> str:
    columns = QUALITY_SELECT_SQL
    if extra_columns:
        columns = f"{columns},\n{extra_columns}"
    return f"""
    SELECT
{columns}
    FROM vehicle_observations v
    {QUALITY_JOIN_SQL}
    WHERE {where}
    """


def add_local_time_columns(
    df: pd.DataFrame,
    utc_column: str,
    timezone: str,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    tz = ZoneInfo(timezone)
    result = df.copy()
    utc_times = pd.to_datetime(result[utc_column], utc=True, errors="coerce")
    local_times = utc_times.dt.tz_convert(tz)

    result["local_time"] = local_times
    result["local_date"] = local_times.dt.date
    result["local_hour"] = local_times.dt.hour
    result["local_weekday"] = local_times.dt.weekday
    result["is_weekday"] = result["local_weekday"] < 5
    result["local_minutes"] = local_times.dt.hour * 60 + local_times.dt.minute
    return result


def add_quality_flags(
    df: pd.DataFrame,
    *,
    max_abs_delay_seconds: int = MAX_ABS_DELAY_SECONDS,
    stale_seconds: int = STALE_OBSERVATION_SECONDS,
    pre_trip_grace_seconds: int = PRE_TRIP_GRACE_SECONDS,
    post_trip_grace_seconds: int = POST_TRIP_GRACE_SECONDS,
    stop_call_disagreement_seconds: int = STOP_CALL_DISAGREEMENT_SECONDS,
) -> pd.DataFrame:
    result = df.copy()
    if result.empty:
        for column in QUALITY_FLAG_COLUMNS:
            result[column] = pd.Series(dtype="bool")
        result["quality_pass"] = pd.Series(dtype="bool")
        return result

    result["delay_seconds"] = pd.to_numeric(result["delay_seconds"], errors="coerce")
    for column in [
        "recorded_at_utc",
        "valid_until_utc",
        "collected_at_utc",
        "created_at_utc",
        "origin_aimed_departure_time_utc",
        "destination_aimed_arrival_time_utc",
        "next_aimed_arrival_time_utc",
        "next_expected_arrival_time_utc",
        "next_aimed_departure_time_utc",
        "next_expected_departure_time_utc",
    ]:
        if column in result.columns:
            result[column] = pd.to_datetime(result[column], utc=True, errors="coerce")
        else:
            result[column] = pd.Series(
                pd.NaT,
                index=result.index,
                dtype="datetime64[ns, UTC]",
            )

    recorded = result["recorded_at_utc"]
    collected = result["collected_at_utc"].combine_first(result["created_at_utc"])
    valid_until = result["valid_until_utc"]
    origin_aimed = result["origin_aimed_departure_time_utc"]
    destination_aimed = result["destination_aimed_arrival_time_utc"]

    observation_age = (collected - recorded).dt.total_seconds()
    result["observation_age_seconds"] = observation_age
    result["validity_lag_seconds"] = (collected - valid_until).dt.total_seconds()

    arrival_delta = (
        result["next_expected_arrival_time_utc"] - result["next_aimed_arrival_time_utc"]
    ).dt.total_seconds()
    departure_delta = (
        result["next_expected_departure_time_utc"]
        - result["next_aimed_departure_time_utc"]
    ).dt.total_seconds()
    result["stop_call_delay_seconds"] = arrival_delta.combine_first(departure_delta)
    result["stop_call_delay_diff_seconds"] = (
        result["stop_call_delay_seconds"] - result["delay_seconds"]
    ).abs()

    result["is_implausible_delay"] = result["delay_seconds"].abs() > max_abs_delay_seconds
    result["is_stale_observation"] = (
        observation_age.gt(stale_seconds).fillna(False)
        | (valid_until.lt(collected).fillna(False))
    )
    result["is_pre_trip_observation"] = (
        recorded.lt(origin_aimed - pd.Timedelta(seconds=pre_trip_grace_seconds)).fillna(False)
    )
    result["is_post_trip_observation"] = (
        recorded.gt(destination_aimed + pd.Timedelta(seconds=post_trip_grace_seconds)).fillna(False)
    )
    result["has_stop_call_disagreement"] = result["stop_call_delay_diff_seconds"].gt(
        stop_call_disagreement_seconds
    ).fillna(False)
    result["quality_issue_count"] = result[QUALITY_FLAG_COLUMNS].sum(axis=1)
    return result


def apply_quality_filter(
    df: pd.DataFrame,
    *,
    quality_mode: str = DEFAULT_QUALITY_MODE,
    exclude_stop_call_disagreement: bool = False,
) -> pd.DataFrame:
    if quality_mode not in QUALITY_MODES:
        raise ValueError(f"quality_mode must be one of: {', '.join(QUALITY_MODES)}")

    result = add_quality_flags(df)
    if quality_mode in ("raw", "diagnostic"):
        result["quality_pass"] = True
        return result

    exclusion_columns = CONSERVATIVE_EXCLUSION_COLUMNS.copy()
    if exclude_stop_call_disagreement:
        exclusion_columns.append("has_stop_call_disagreement")
    result["quality_pass"] = ~result[exclusion_columns].any(axis=1)
    return result[result["quality_pass"]].reset_index(drop=True)


def add_representative_time_columns(
    df: pd.DataFrame,
    *,
    timezone: str = DEFAULT_TIMEZONE,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    result = df.copy()
    for column in ["recorded_at_utc", "next_aimed_arrival_time_utc"]:
        if column in result.columns:
            result[column] = pd.to_datetime(result[column], utc=True, errors="coerce")
        else:
            result[column] = pd.NaT
    result["representative_time_utc"] = result["next_aimed_arrival_time_utc"].combine_first(
        result["recorded_at_utc"]
    )
    result = add_local_time_columns(result, "representative_time_utc", timezone)
    result["day_type"] = result["is_weekday"].map({True: "weekday", False: "weekend"})
    return result


def aggregate_delay_buckets(
    df: pd.DataFrame,
    *,
    bucket: str = DEFAULT_BUCKET_MODE,
    timezone: str = DEFAULT_TIMEZONE,
) -> pd.DataFrame:
    if bucket not in BUCKET_MODES:
        raise ValueError(f"bucket must be one of: {', '.join(BUCKET_MODES)}")
    if df.empty:
        return _empty_bucket_frame()

    working = add_representative_time_columns(df, timezone=timezone)
    working["delay_seconds"] = pd.to_numeric(working["delay_seconds"], errors="coerce")
    working = working.dropna(subset=["delay_seconds", "representative_time_utc", "line_ref"])
    if working.empty:
        return _empty_bucket_frame()

    for column in [
        "id",
        "trip_match_key",
        "vehicle_id",
        "line_ref",
        "direction_ref",
        "next_stop_point_ref",
        "published_line_name",
        "next_stop_point_name",
    ]:
        if column not in working.columns:
            working[column] = pd.NA
    working["direction_ref"] = working["direction_ref"].astype("string").fillna("Unknown")
    working["line_ref"] = working["line_ref"].astype("string")
    working["next_stop_point_ref"] = working["next_stop_point_ref"].astype("string")
    working["published_line_name"] = (
        working["published_line_name"].astype("string").fillna(working["line_ref"])
    )

    if bucket == "poll":
        result = working.copy()
        result["raw_poll_count"] = 1
        result["bucket_id"] = result["id"].astype("string").fillna(
            pd.Series(result.index, index=result.index).astype("string")
        )
    else:
        if bucket == "trip-stop":
            group_keys = [
                "trip_match_key",
                "vehicle_id",
                "line_ref",
                "direction_ref",
                "next_stop_point_ref",
            ]
        elif bucket == "vehicle-trip":
            group_keys = ["trip_match_key", "vehicle_id", "line_ref", "direction_ref"]
        else:
            group_keys = ["line_ref", "direction_ref", "local_date", "local_hour", "day_type"]

        result = working.groupby(group_keys, dropna=False, as_index=False).agg(
            delay_seconds=("delay_seconds", "median"),
            raw_poll_count=("delay_seconds", "size"),
            published_line_name=("published_line_name", "first"),
            next_stop_point_name=("next_stop_point_name", "first"),
            representative_time_utc=("representative_time_utc", "min"),
            recorded_at_utc=("recorded_at_utc", "min"),
            first_recorded_at_utc=("recorded_at_utc", "min"),
            last_recorded_at_utc=("recorded_at_utc", "max"),
        )
        result["bucket_id"] = (
            result[group_keys].astype("string").fillna("<NA>").agg("|".join, axis=1)
        )
        if "local_date" not in result.columns or "local_hour" not in result.columns:
            result = add_local_time_columns(result, "representative_time_utc", timezone)
            result["day_type"] = result["is_weekday"].map({True: "weekday", False: "weekend"})
        else:
            result["local_weekday"] = pd.to_datetime(result["representative_time_utc"]).dt.weekday
            result["is_weekday"] = result["day_type"] == "weekday"
            result["local_minutes"] = result["local_hour"] * 60
            result["local_time"] = pd.to_datetime(
                result["representative_time_utc"], utc=True, errors="coerce"
            ).dt.tz_convert(ZoneInfo(timezone))

    result["delay_min"] = result["delay_seconds"] / 60.0
    result["bucket_mode"] = bucket
    return result.reset_index(drop=True)


def summarize_delay_metrics(
    df: pd.DataFrame,
    group_keys: list[str],
    *,
    min_observations: int = 1,
    extra_aggs: dict[str, tuple[str, str]] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return _empty_metric_frame(group_keys)

    working = df.copy()
    working["delay_seconds"] = pd.to_numeric(working["delay_seconds"], errors="coerce")
    if "raw_poll_count" not in working.columns:
        working["raw_poll_count"] = 1
    if not group_keys:
        working["_scope"] = "overall"
        group_keys = ["_scope"]

    grouped = working.groupby(group_keys, dropna=False, as_index=False).agg(
        bucket_count=("delay_seconds", "size"),
        raw_poll_count=("raw_poll_count", "sum"),
        signed_mean_delay_min=("delay_seconds", lambda s: minutes(s).mean()),
        avg_delay_min=("delay_seconds", lambda s: minutes(s).mean()),
        median_delay_min=("delay_seconds", lambda s: minutes(s).median()),
        p75_delay_min=("delay_seconds", lambda s: minutes(s).quantile(0.75)),
        p90_delay_min=("delay_seconds", lambda s: minutes(s).quantile(0.90)),
        p95_delay_min=("delay_seconds", lambda s: minutes(s).quantile(0.95)),
        pct_late=("delay_seconds", lambda s: (s > 0).mean() * 100.0),
        pct_over_3_min_late=("delay_seconds", lambda s: (s > 180).mean() * 100.0),
        pct_over_5_min_late=("delay_seconds", lambda s: (s > 300).mean() * 100.0),
        pct_early=("delay_seconds", lambda s: (s < 0).mean() * 100.0),
        pct_over_1_min_early=("delay_seconds", lambda s: (s < -60).mean() * 100.0),
        pct_over_3_min_early=("delay_seconds", lambda s: (s < -180).mean() * 100.0),
        median_early_min_abs=("delay_seconds", lambda s: _early_abs_quantile(s, 0.50)),
        p90_early_min_abs=("delay_seconds", lambda s: _early_abs_quantile(s, 0.90)),
        **(extra_aggs or {}),
    )
    grouped = grouped[grouped["bucket_count"] >= min_observations]
    if "_scope" in grouped.columns:
        grouped = grouped.drop(columns=["_scope"])
    return grouped.reset_index(drop=True)


def sort_robust_delay_metrics(
    df: pd.DataFrame,
    *,
    limit: int | None = None,
    ascending: bool = False,
) -> pd.DataFrame:
    if df.empty:
        return df
    result = df.sort_values(
        ["p90_delay_min", "pct_over_5_min_late", "bucket_count"],
        ascending=[ascending, ascending, not ascending],
    )
    if limit is not None:
        result = result.head(limit)
    return result.reset_index(drop=True)


def parse_rush_windows(values: list[str] | tuple[str, ...]) -> list[tuple[int, int]]:
    windows: list[tuple[int, int]] = []
    for value in values:
        try:
            start_text, end_text = value.split("-", maxsplit=1)
            start = parse_hhmm(start_text)
            end = parse_hhmm(end_text)
        except ValueError as exc:
            raise SystemExit(
                f"Invalid rush window {value!r}; expected HH:MM-HH:MM."
            ) from exc
        if start == end:
            raise SystemExit(f"Invalid rush window {value!r}; start and end are equal.")
        windows.append((start, end))
    return windows


def rush_window_values(values: list[str] | None) -> list[str]:
    return values if values is not None else list(DEFAULT_RUSH_WINDOWS)


def parse_hhmm(value: str) -> int:
    hour_text, minute_text = value.strip().split(":", maxsplit=1)
    hour = int(hour_text)
    minute = int(minute_text)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError(value)
    return hour * 60 + minute


def flag_rush_period(
    df: pd.DataFrame,
    windows: list[tuple[int, int]],
    *,
    include_weekends: bool,
) -> pd.Series:
    in_window = pd.Series(False, index=df.index)
    for start, end in windows:
        if start < end:
            in_window |= (df["local_minutes"] >= start) & (df["local_minutes"] < end)
        else:
            in_window |= (df["local_minutes"] >= start) | (df["local_minutes"] < end)

    if include_weekends:
        return in_window
    return in_window & df["is_weekday"]


def minutes(seconds: pd.Series) -> pd.Series:
    return seconds / 60.0


def round_numeric(df: pd.DataFrame, digits: int = 2) -> pd.DataFrame:
    result = df.copy()
    numeric_columns = result.select_dtypes(include="number").columns
    result[numeric_columns] = result[numeric_columns].round(digits)
    return result


def print_or_empty(df: pd.DataFrame, empty_message: str = "No matching observations found.") -> None:
    if df.empty:
        print(empty_message)
        return
    print(df.to_string(index=False))


def write_optional_csv(df: pd.DataFrame, output_csv: Path | None) -> None:
    if output_csv is None:
        return
    output_path = output_csv.expanduser()
    if not output_path.is_absolute():
        output_path = PROJECT_ROOT / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Wrote CSV: {output_path}")


def _early_abs_quantile(seconds: pd.Series, quantile: float) -> float:
    early = seconds[seconds < 0].abs()
    if early.empty:
        return 0.0
    return float(minutes(early).quantile(quantile))


def _empty_bucket_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "bucket_id",
            "bucket_mode",
            "line_ref",
            "direction_ref",
            "published_line_name",
            "delay_seconds",
            "delay_min",
            "raw_poll_count",
            "next_stop_point_ref",
            "next_stop_point_name",
            "representative_time_utc",
            "recorded_at_utc",
            "local_time",
            "local_date",
            "local_hour",
            "local_weekday",
            "is_weekday",
            "day_type",
            "local_minutes",
        ]
    )


def _empty_metric_frame(group_columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            *group_columns,
            "bucket_count",
            "raw_poll_count",
            "signed_mean_delay_min",
            "avg_delay_min",
            "median_delay_min",
            "p75_delay_min",
            "p90_delay_min",
            "p95_delay_min",
            "pct_late",
            "pct_over_3_min_late",
            "pct_over_5_min_late",
            "pct_early",
            "pct_over_1_min_early",
            "pct_over_3_min_early",
            "median_early_min_abs",
            "p90_early_min_abs",
        ]
    )
