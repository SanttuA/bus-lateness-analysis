from __future__ import annotations

import argparse
from bisect import bisect_right
from datetime import UTC, date, datetime
from pathlib import Path
import re
from zoneinfo import ZoneInfo

import polars as pl
import polars.selectors as cs


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "foli.db"
DEFAULT_GTFS_ROOT = PROJECT_ROOT / "data" / "gtfs"
DEFAULT_ANALYSIS_CACHE_DIR = PROJECT_ROOT / "outputs" / "polars-report-cache"
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

GTFS_DIR_PATTERN = re.compile(r"^gtfs_(\d{4}-\d{2}-\d{2})$")


def representative_time_sql(alias: str = "v") -> str:
    return f"COALESCE({alias}.next_aimed_arrival_time_utc, {alias}.recorded_at_utc)"


def add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to the Foli SQLite database. Defaults to {DEFAULT_DB_PATH}",
    )
    parser.add_argument("--limit", type=int, default=10, help="Number of rows to show.")
    parser.add_argument(
        "--min-observations",
        type=int,
        default=1,
        help="Only include groups with at least this many observations.",
    )
    parser.add_argument("--output-csv", type=Path, help="Optional CSV output path.")


def add_timezone_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help=f"Local timezone for time-of-day analysis. Defaults to {DEFAULT_TIMEZONE}.",
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
            f"for the same vehicle trip and next stop. Defaults to {default}."
        ),
    )


def add_cache_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_ANALYSIS_CACHE_DIR,
        help=(
            "Directory for reusable Polars analysis cache. Defaults to "
            f"{DEFAULT_ANALYSIS_CACHE_DIR}."
        ),
    )
    parser.add_argument(
        "--force-cache",
        action="store_true",
        help="Rebuild reusable Polars analysis cache before running.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Read SQLite directly instead of using the reusable Polars cache.",
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


def add_gtfs_args(parser: argparse.ArgumentParser, *, file_description: str) -> None:
    parser.add_argument(
        "--gtfs-dir",
        type=Path,
        help=(
            f"Single GTFS directory containing {file_description}. "
            "Overrides date-aware --gtfs-root behavior."
        ),
    )
    parser.add_argument(
        "--gtfs-root",
        type=Path,
        default=DEFAULT_GTFS_ROOT,
        help=(
            "Root containing extracted gtfs_YYYY-MM-DD directories. Defaults to "
            f"{DEFAULT_GTFS_ROOT}."
        ),
    )


def resolve_project_path(path: Path | str) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    return resolved.resolve()


def resolve_db_path(path: Path | str) -> Path:
    db_path = resolve_project_path(path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")
    return db_path


def sqlite_uri(path: Path | str) -> str:
    db_path = resolve_db_path(path)
    return f"sqlite:///{db_path.as_posix()}"


def read_sql(
    db: Path | str,
    query: str,
    params: list[object] | tuple[object, ...] | None = None,
) -> pl.DataFrame:
    execute_options = {"parameters": tuple(params or ())} if params else None
    return pl.read_database_uri(
        query=query,
        uri=sqlite_uri(db),
        engine="adbc",
        execute_options=execute_options,
    )


def base_quality_query(*, where: str = QUALIFIED_DELAY_FILTER_SQL, extra_columns: str = "") -> str:
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


def base_quality_query_without_collector(
    *,
    where: str = QUALIFIED_DELAY_FILTER_SQL,
    extra_columns: str = "",
) -> str:
    columns = QUALITY_SELECT_SQL.replace(
        "    p.collected_at_utc,",
        "    NULL AS collected_at_utc,",
    )
    if extra_columns:
        columns = f"{columns},\n{extra_columns}"
    return f"""
    SELECT
{columns}
    FROM vehicle_observations v
    WHERE {where}
    """


def utc_sql_timestamp(value: object, *, ceil: bool = False) -> str:
    timestamp = parse_timestamp(value, "UTC")
    if ceil and timestamp.microsecond:
        timestamp = timestamp.replace(microsecond=0) + timedelta_seconds(1)
    else:
        timestamp = timestamp.replace(microsecond=0)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def append_representative_time_filter(
    where: str,
    params: list[object],
    *,
    start_utc: object | None = None,
    end_utc: object | None = None,
    alias: str = "v",
) -> str:
    representative_time = representative_time_sql(alias)
    if start_utc is not None:
        where += f" AND {representative_time} >= ?"
        params.append(utc_sql_timestamp(start_utc))
    if end_utc is not None:
        where += f" AND {representative_time} < ?"
        params.append(utc_sql_timestamp(end_utc, ceil=True))
    return where


def parse_timestamp(value: object, timezone: str = DEFAULT_TIMEZONE) -> datetime:
    if isinstance(value, datetime):
        timestamp = value
    else:
        text = str(value)
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        timestamp = datetime.fromisoformat(text)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=ZoneInfo(timezone))
    return timestamp.astimezone(UTC)


def timedelta_seconds(seconds: int):
    from datetime import timedelta

    return timedelta(seconds=seconds)


def read_quality_rows(args: argparse.Namespace) -> pl.DataFrame:
    query = base_quality_query(where=QUALIFIED_DELAY_FILTER_SQL)
    return read_sql(args.db, query)


def add_quality_flags(
    df: pl.DataFrame,
    *,
    max_abs_delay_seconds: int = MAX_ABS_DELAY_SECONDS,
    stale_seconds: int = STALE_OBSERVATION_SECONDS,
    pre_trip_grace_seconds: int = PRE_TRIP_GRACE_SECONDS,
    post_trip_grace_seconds: int = POST_TRIP_GRACE_SECONDS,
    stop_call_disagreement_seconds: int = STOP_CALL_DISAGREEMENT_SECONDS,
) -> pl.DataFrame:
    result = _ensure_columns(
        df,
        [
            "delay_seconds",
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
        ],
    )
    if result.is_empty():
        return result.with_columns(
            [pl.lit(None, dtype=pl.Boolean).alias(column) for column in QUALITY_FLAG_COLUMNS],
            pl.lit(0, dtype=pl.Int64).alias("quality_issue_count"),
        )

    timestamp_columns = [
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
    ]
    result = result.with_columns(
        pl.col("delay_seconds").cast(pl.Float64, strict=False),
        *[_parse_datetime_expr(column).alias(column) for column in timestamp_columns],
    )
    collected = pl.coalesce(pl.col("collected_at_utc"), pl.col("created_at_utc"))
    arrival_delta = (
        pl.col("next_expected_arrival_time_utc") - pl.col("next_aimed_arrival_time_utc")
    ).dt.total_seconds()
    departure_delta = (
        pl.col("next_expected_departure_time_utc")
        - pl.col("next_aimed_departure_time_utc")
    ).dt.total_seconds()
    result = result.with_columns(
        (collected - pl.col("recorded_at_utc"))
        .dt.total_seconds()
        .alias("observation_age_seconds"),
        (collected - pl.col("valid_until_utc"))
        .dt.total_seconds()
        .alias("validity_lag_seconds"),
        pl.coalesce(arrival_delta, departure_delta).alias("stop_call_delay_seconds"),
    )
    result = result.with_columns(
        (pl.col("stop_call_delay_seconds") - pl.col("delay_seconds"))
        .abs()
        .alias("stop_call_delay_diff_seconds"),
    )
    result = result.with_columns(
        (pl.col("delay_seconds").abs() > max_abs_delay_seconds)
        .fill_null(False)
        .alias("is_implausible_delay"),
        (
            (pl.col("observation_age_seconds") > stale_seconds).fill_null(False)
            | (pl.col("valid_until_utc") < collected).fill_null(False)
        ).alias("is_stale_observation"),
        (
            pl.col("recorded_at_utc")
            < (
                pl.col("origin_aimed_departure_time_utc")
                - pl.duration(seconds=pre_trip_grace_seconds)
            )
        )
        .fill_null(False)
        .alias("is_pre_trip_observation"),
        (
            pl.col("recorded_at_utc")
            > (
                pl.col("destination_aimed_arrival_time_utc")
                + pl.duration(seconds=post_trip_grace_seconds)
            )
        )
        .fill_null(False)
        .alias("is_post_trip_observation"),
        (pl.col("stop_call_delay_diff_seconds") > stop_call_disagreement_seconds)
        .fill_null(False)
        .alias("has_stop_call_disagreement"),
    )
    return result.with_columns(
        pl.sum_horizontal([pl.col(column).cast(pl.Int64) for column in QUALITY_FLAG_COLUMNS]).alias(
            "quality_issue_count"
        )
    )


def add_quality_pass(
    df: pl.DataFrame,
    *,
    quality_mode: str = DEFAULT_QUALITY_MODE,
    exclude_stop_call_disagreement: bool = False,
) -> pl.DataFrame:
    result = add_quality_flags(df)
    if quality_mode in ("raw", "diagnostic"):
        return result.with_columns(pl.lit(True).alias("quality_pass"))

    exclusion_columns = CONSERVATIVE_EXCLUSION_COLUMNS.copy()
    if exclude_stop_call_disagreement:
        exclusion_columns.append("has_stop_call_disagreement")
    return result.with_columns(
        (~pl.any_horizontal([pl.col(column) for column in exclusion_columns])).alias(
            "quality_pass"
        )
    )


def apply_quality_filter(
    df: pl.DataFrame,
    *,
    quality_mode: str = DEFAULT_QUALITY_MODE,
    exclude_stop_call_disagreement: bool = False,
) -> pl.DataFrame:
    if quality_mode not in QUALITY_MODES:
        raise ValueError(f"quality_mode must be one of: {', '.join(QUALITY_MODES)}")
    result = add_quality_pass(
        df,
        quality_mode=quality_mode,
        exclude_stop_call_disagreement=exclude_stop_call_disagreement,
    )
    if quality_mode in ("raw", "diagnostic"):
        return result
    return result.filter(pl.col("quality_pass"))


def add_representative_time_columns(
    df: pl.DataFrame,
    *,
    timezone: str = DEFAULT_TIMEZONE,
) -> pl.DataFrame:
    result = _ensure_columns(df, ["recorded_at_utc", "next_aimed_arrival_time_utc"])
    result = result.with_columns(
        _parse_datetime_expr("recorded_at_utc").alias("recorded_at_utc"),
        _parse_datetime_expr("next_aimed_arrival_time_utc").alias("next_aimed_arrival_time_utc"),
    )
    result = result.with_columns(
        pl.coalesce("next_aimed_arrival_time_utc", "recorded_at_utc").alias(
            "representative_time_utc"
        )
    )
    result = add_local_time_columns(result, "representative_time_utc", timezone)
    return result.with_columns(
        pl.when(pl.col("is_weekday")).then(pl.lit("weekday")).otherwise(pl.lit("weekend")).alias(
            "day_type"
        )
    )


def add_local_time_columns(df: pl.DataFrame, utc_column: str, timezone: str) -> pl.DataFrame:
    local = pl.col(utc_column).dt.convert_time_zone(timezone)
    return df.with_columns(
        local.alias("local_time"),
        local.dt.date().alias("local_date"),
        local.dt.hour().cast(pl.Int64).alias("local_hour"),
        (local.dt.weekday() - 1).cast(pl.Int64).alias("local_weekday"),
        ((local.dt.weekday() - 1) < 5).alias("is_weekday"),
        (
            local.dt.hour().cast(pl.Int64) * 60
            + local.dt.minute().cast(pl.Int64)
        ).alias("local_minutes"),
    )


def aggregate_delay_buckets(
    df: pl.DataFrame,
    *,
    bucket: str = DEFAULT_BUCKET_MODE,
    timezone: str = DEFAULT_TIMEZONE,
) -> pl.DataFrame:
    if bucket not in BUCKET_MODES:
        raise ValueError(f"bucket must be one of: {', '.join(BUCKET_MODES)}")
    if df.is_empty():
        return empty_bucket_frame()

    working = add_representative_time_columns(df, timezone=timezone)
    working = _ensure_columns(
        working,
        [
            "id",
            "trip_match_key",
            "vehicle_id",
            "line_ref",
            "direction_ref",
            "next_stop_point_ref",
            "published_line_name",
            "next_stop_point_name",
        ],
    ).with_columns(
        pl.col("delay_seconds").cast(pl.Float64, strict=False),
        pl.col("line_ref").cast(pl.Utf8),
        pl.col("direction_ref").cast(pl.Utf8).fill_null("Unknown"),
        pl.col("next_stop_point_ref").cast(pl.Utf8),
        pl.coalesce(pl.col("published_line_name").cast(pl.Utf8), pl.col("line_ref").cast(pl.Utf8))
        .alias("published_line_name"),
    )
    working = working.filter(
        pl.col("delay_seconds").is_not_null()
        & pl.col("representative_time_utc").is_not_null()
        & pl.col("line_ref").is_not_null()
    )
    if working.is_empty():
        return empty_bucket_frame()

    if bucket == "poll":
        result = working.with_row_index("_row_nr").with_columns(
            pl.lit(1, dtype=pl.Int64).alias("raw_poll_count"),
            pl.coalesce(pl.col("id").cast(pl.Utf8), pl.col("_row_nr").cast(pl.Utf8)).alias(
                "bucket_id"
            ),
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

        working = working.sort("representative_time_utc")
        result = working.group_by(group_keys, maintain_order=True).agg(
            pl.col("delay_seconds").median().alias("delay_seconds"),
            pl.len().alias("raw_poll_count"),
            pl.col("published_line_name").first(),
            pl.col("next_stop_point_name").first(),
            pl.col("representative_time_utc").min(),
            pl.col("recorded_at_utc").min(),
            pl.col("recorded_at_utc").min().alias("first_recorded_at_utc"),
            pl.col("recorded_at_utc").max().alias("last_recorded_at_utc"),
        )
        result = result.with_columns(
            pl.concat_str(
                [pl.col(column).cast(pl.Utf8).fill_null("<NA>") for column in group_keys],
                separator="|",
            ).alias("bucket_id")
        )
        if "next_stop_point_ref" not in result.columns:
            result = result.with_columns(pl.lit(None, dtype=pl.Utf8).alias("next_stop_point_ref"))

    result = add_local_time_columns(result, "representative_time_utc", timezone)
    result = result.with_columns(
        pl.when(pl.col("is_weekday")).then(pl.lit("weekday")).otherwise(pl.lit("weekend")).alias(
            "day_type"
        ),
        (pl.col("delay_seconds") / 60.0).alias("delay_min"),
        pl.lit(bucket).alias("bucket_mode"),
    )
    columns = [column for column in empty_bucket_frame().columns if column in result.columns]
    return result.select(columns)


def summarize_delay_metrics(
    df: pl.DataFrame,
    group_keys: list[str],
    *,
    min_observations: int = 1,
    extra_aggs: dict[str, tuple[str, str]] | None = None,
) -> pl.DataFrame:
    if df.is_empty():
        return empty_metric_frame(group_keys)

    working = df.with_columns(pl.col("delay_seconds").cast(pl.Float64, strict=False))
    if "raw_poll_count" not in working.columns:
        working = working.with_columns(pl.lit(1, dtype=pl.Int64).alias("raw_poll_count"))
    keys = group_keys.copy()
    if not keys:
        working = working.with_columns(pl.lit("overall").alias("_scope"))
        keys = ["_scope"]

    aggs: list[pl.Expr] = metric_aggs()
    for output, (column, how) in (extra_aggs or {}).items():
        if how != "first":
            raise ValueError("Polars extra_aggs currently supports only first aggregations.")
        aggs.append(pl.col(column).first().alias(output))

    grouped = working.group_by(keys, maintain_order=True).agg(*aggs)
    grouped = grouped.filter(pl.col("bucket_count") >= min_observations)
    if "_scope" in grouped.columns:
        grouped = grouped.drop("_scope")
    return grouped


def metric_aggs(prefix: str = "") -> list[pl.Expr]:
    delay = pl.col("delay_seconds")
    delay_min = delay / 60.0
    early_abs_min = delay.filter(delay < 0).abs() / 60.0
    return [
        pl.len().alias(f"{prefix}bucket_count"),
        pl.col("raw_poll_count").sum().cast(pl.Int64).alias(f"{prefix}raw_poll_count"),
        delay.mean().truediv(60.0).alias(f"{prefix}signed_mean_delay_min"),
        delay.mean().truediv(60.0).alias(f"{prefix}avg_delay_min"),
        delay.median().truediv(60.0).alias(f"{prefix}median_delay_min"),
        delay_min.quantile(0.75, interpolation="linear").alias(f"{prefix}p75_delay_min"),
        delay_min.quantile(0.90, interpolation="linear").alias(f"{prefix}p90_delay_min"),
        delay_min.quantile(0.95, interpolation="linear").alias(f"{prefix}p95_delay_min"),
        (delay > 0).mean().mul(100.0).alias(f"{prefix}pct_late"),
        (delay > 180).mean().mul(100.0).alias(f"{prefix}pct_over_3_min_late"),
        (delay > 300).mean().mul(100.0).alias(f"{prefix}pct_over_5_min_late"),
        (delay < 0).mean().mul(100.0).alias(f"{prefix}pct_early"),
        (delay < -60).mean().mul(100.0).alias(f"{prefix}pct_over_1_min_early"),
        (delay < -180).mean().mul(100.0).alias(f"{prefix}pct_over_3_min_early"),
        early_abs_min.quantile(0.50, interpolation="linear")
        .fill_null(0.0)
        .alias(f"{prefix}median_early_min_abs"),
        early_abs_min.quantile(0.90, interpolation="linear")
        .fill_null(0.0)
        .alias(f"{prefix}p90_early_min_abs"),
    ]


def sort_robust_delay_metrics(
    df: pl.DataFrame,
    *,
    limit: int | None = None,
    ascending: bool = False,
) -> pl.DataFrame:
    if df.is_empty():
        return df
    result = df.sort(
        ["p90_delay_min", "pct_over_5_min_late", "bucket_count"],
        descending=[not ascending, not ascending, ascending],
    )
    return result.head(limit) if limit is not None else result


def parse_rush_windows(values: list[str] | tuple[str, ...]) -> list[tuple[int, int]]:
    windows: list[tuple[int, int]] = []
    for value in values:
        try:
            start_text, end_text = value.split("-", maxsplit=1)
            start = parse_hhmm(start_text)
            end = parse_hhmm(end_text)
        except ValueError as exc:
            raise SystemExit(f"Invalid rush window {value!r}; expected HH:MM-HH:MM.") from exc
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


def rush_period_expr(windows: list[tuple[int, int]], *, include_weekends: bool) -> pl.Expr:
    expr = pl.lit(False)
    for start, end in windows:
        if start < end:
            in_window = (pl.col("local_minutes") >= start) & (pl.col("local_minutes") < end)
        else:
            in_window = (pl.col("local_minutes") >= start) | (pl.col("local_minutes") < end)
        expr = expr | in_window
    if not include_weekends:
        expr = expr & pl.col("is_weekday")
    return expr.alias("is_rush")


def round_numeric(df: pl.DataFrame, digits: int = 2) -> pl.DataFrame:
    if df.is_empty():
        return df
    return df.with_columns(cs.numeric().round(digits))


def print_or_empty(df: pl.DataFrame, empty_message: str = "No matching observations found.") -> None:
    if df.is_empty():
        print(empty_message)
        return
    print(df)


def write_optional_csv(df: pl.DataFrame, output_csv: Path | None) -> None:
    if output_csv is None:
        return
    output_path = resolve_project_path(output_csv)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)
    print(f"Wrote CSV: {output_path}")


def latest_gtfs_dir(root: Path = DEFAULT_GTFS_ROOT) -> Path | None:
    gtfs_root = resolve_project_path(root)
    if not gtfs_root.exists():
        return None
    candidates = [
        path for path in gtfs_root.iterdir() if path.is_dir() and (path / "stops.txt").exists()
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.name)


def parse_gtfs_feed_date(path: Path | str) -> date | None:
    match = GTFS_DIR_PATTERN.match(Path(path).name)
    if match is None:
        return None
    return date.fromisoformat(match.group(1))


def discover_gtfs_feeds(
    root: Path | str = DEFAULT_GTFS_ROOT,
    *,
    required_file: str | None = None,
) -> pl.DataFrame:
    gtfs_root = resolve_project_path(root)
    if not gtfs_root.exists():
        return pl.DataFrame({"gtfs_feed_date": [], "gtfs_dir": []})
    rows: list[dict[str, object]] = []
    for path in gtfs_root.iterdir():
        feed_date = parse_gtfs_feed_date(path)
        if feed_date is None or not path.is_dir():
            continue
        if required_file is not None and not (path / required_file).exists():
            continue
        rows.append({"gtfs_feed_date": feed_date, "gtfs_dir": str(path)})
    if not rows:
        return pl.DataFrame({"gtfs_feed_date": [], "gtfs_dir": []})
    return pl.DataFrame(rows).with_columns(pl.col("gtfs_feed_date").cast(pl.Date)).sort(
        "gtfs_feed_date"
    )


def assign_gtfs_feed_dates(
    df: pl.DataFrame,
    feeds: pl.DataFrame,
    *,
    local_date_column: str = "local_date",
) -> pl.Series:
    if df.is_empty() or feeds.is_empty() or local_date_column not in df.columns:
        return pl.Series("gtfs_feed_date", [None] * df.height, dtype=pl.Date)
    feed_dates = [
        value
        for value in feeds.select(pl.col("gtfs_feed_date").cast(pl.Date)).unique().sort(
            "gtfs_feed_date"
        )["gtfs_feed_date"].to_list()
        if value is not None
    ]
    if not feed_dates:
        return pl.Series("gtfs_feed_date", [None] * df.height, dtype=pl.Date)
    local_dates = df.select(pl.col(local_date_column).cast(pl.Date))[
        local_date_column
    ].to_list()
    assigned: list[date | None] = []
    for value in local_dates:
        if value is None:
            assigned.append(None)
            continue
        position = bisect_right(feed_dates, value) - 1
        assigned.append(feed_dates[position] if position >= 0 else None)
    return pl.Series("gtfs_feed_date", assigned, dtype=pl.Date)


def gtfs_feed_date_for_timestamp(
    timestamp: object,
    feeds: pl.DataFrame,
    *,
    timezone: str = DEFAULT_TIMEZONE,
) -> date | None:
    if feeds.is_empty():
        return None
    local_date = parse_timestamp(timestamp, timezone).astimezone(ZoneInfo(timezone)).date()
    row = pl.DataFrame({"local_date": [local_date]})
    return assign_gtfs_feed_dates(row, feeds)[0]


def load_gtfs_stop_metadata(
    *,
    gtfs_dir: Path | str | None = None,
    gtfs_root: Path | str = DEFAULT_GTFS_ROOT,
) -> pl.DataFrame:
    if gtfs_dir is not None:
        return _load_one_gtfs_stop_metadata(resolve_project_path(gtfs_dir))

    feeds = discover_gtfs_feeds(gtfs_root, required_file="stops.txt")
    frames: list[pl.DataFrame] = []
    for feed in feeds.iter_rows(named=True):
        stops = _load_one_gtfs_stop_metadata(Path(feed["gtfs_dir"]))
        frames.append(stops.with_columns(pl.lit(feed["gtfs_feed_date"]).alias("gtfs_feed_date")))
    if not frames:
        return pl.DataFrame(
            {
                "gtfs_feed_date": pl.Series([], dtype=pl.Date),
                "stop_id": pl.Series([], dtype=pl.Utf8),
                "gtfs_stop_name": pl.Series([], dtype=pl.Utf8),
                "stop_lat": pl.Series([], dtype=pl.Float64),
                "stop_lon": pl.Series([], dtype=pl.Float64),
            }
        )
    return pl.concat(frames, how="vertical").unique(["gtfs_feed_date", "stop_id"], keep="first")


def load_gtfs_route_metadata(
    *,
    gtfs_dir: Path | str | None = None,
    gtfs_root: Path | str = DEFAULT_GTFS_ROOT,
) -> pl.DataFrame:
    if gtfs_dir is not None:
        return _load_one_gtfs_route_metadata(resolve_project_path(gtfs_dir))

    feeds = discover_gtfs_feeds(gtfs_root, required_file="routes.txt")
    frames: list[pl.DataFrame] = []
    for feed in feeds.iter_rows(named=True):
        routes = _load_one_gtfs_route_metadata(Path(feed["gtfs_dir"]))
        frames.append(routes.with_columns(pl.lit(feed["gtfs_feed_date"]).alias("gtfs_feed_date")))
    if not frames:
        return pl.DataFrame(
            {
                "gtfs_feed_date": pl.Series([], dtype=pl.Date),
                "route_id": pl.Series([], dtype=pl.Utf8),
                "route_short_name": pl.Series([], dtype=pl.Utf8),
            }
        )
    return pl.concat(frames, how="vertical").unique(["gtfs_feed_date", "route_id"], keep="first")


def gtfs_metadata_fingerprint(
    root: Path | str = DEFAULT_GTFS_ROOT,
    *,
    filenames: tuple[str, ...] = ("stops.txt", "routes.txt"),
) -> str:
    parts: list[str] = []
    feeds = discover_gtfs_feeds(root)
    for feed in feeds.iter_rows(named=True):
        for filename in filenames:
            path = Path(feed["gtfs_dir"]) / filename
            if not path.exists():
                parts.append(f"{feed['gtfs_feed_date']}|{filename}|missing")
                continue
            stat = path.stat()
            parts.append(
                f"{feed['gtfs_feed_date']}|{filename}|{path.name}|"
                f"{stat.st_mtime_ns}|{stat.st_size}"
            )
    return "||".join(parts)


def gtfs_dir_fingerprint(
    gtfs_dir: Path | str | None,
    *,
    filenames: tuple[str, ...] = ("stops.txt", "routes.txt"),
) -> str | None:
    if gtfs_dir is None:
        return None
    resolved = resolve_project_path(gtfs_dir)
    parts: list[str] = []
    for filename in filenames:
        path = resolved / filename
        if not path.exists():
            parts.append(f"{filename}|missing")
            continue
        stat = path.stat()
        parts.append(f"{filename}|{stat.st_mtime_ns}|{stat.st_size}")
    return "||".join(parts)


def empty_bucket_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "bucket_id": pl.Utf8,
            "bucket_mode": pl.Utf8,
            "line_ref": pl.Utf8,
            "direction_ref": pl.Utf8,
            "published_line_name": pl.Utf8,
            "delay_seconds": pl.Float64,
            "delay_min": pl.Float64,
            "raw_poll_count": pl.Int64,
            "next_stop_point_ref": pl.Utf8,
            "next_stop_point_name": pl.Utf8,
            "representative_time_utc": pl.Datetime(time_zone="UTC"),
            "recorded_at_utc": pl.Datetime(time_zone="UTC"),
            "local_time": pl.Datetime(time_zone=DEFAULT_TIMEZONE),
            "local_date": pl.Date,
            "local_hour": pl.Int64,
            "local_weekday": pl.Int64,
            "is_weekday": pl.Boolean,
            "day_type": pl.Utf8,
            "local_minutes": pl.Int64,
        }
    )


def empty_metric_frame(group_columns: list[str]) -> pl.DataFrame:
    schema = {column: pl.Utf8 for column in group_columns}
    schema.update(
        {
            "bucket_count": pl.Int64,
            "raw_poll_count": pl.Int64,
            "signed_mean_delay_min": pl.Float64,
            "avg_delay_min": pl.Float64,
            "median_delay_min": pl.Float64,
            "p75_delay_min": pl.Float64,
            "p90_delay_min": pl.Float64,
            "p95_delay_min": pl.Float64,
            "pct_late": pl.Float64,
            "pct_over_3_min_late": pl.Float64,
            "pct_over_5_min_late": pl.Float64,
            "pct_early": pl.Float64,
            "pct_over_1_min_early": pl.Float64,
            "pct_over_3_min_early": pl.Float64,
            "median_early_min_abs": pl.Float64,
            "p90_early_min_abs": pl.Float64,
        }
    )
    return pl.DataFrame(schema=schema)


def _parse_datetime_expr(column: str) -> pl.Expr:
    dtype = pl.Datetime(time_zone="UTC")
    return pl.when(pl.col(column).is_null()).then(pl.lit(None, dtype=dtype)).otherwise(
        pl.col(column).cast(pl.Utf8).str.to_datetime(time_zone="UTC", strict=False)
    )


def _ensure_columns(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    missing = [column for column in columns if column not in df.columns]
    if not missing:
        return df
    return df.with_columns([pl.lit(None).alias(column) for column in missing])


def _load_one_gtfs_stop_metadata(gtfs_dir: Path) -> pl.DataFrame:
    stops_path = gtfs_dir / "stops.txt"
    if not stops_path.exists():
        raise FileNotFoundError(f"GTFS stops.txt not found: {stops_path}")
    stops = pl.read_csv(stops_path, schema_overrides={"stop_id": pl.Utf8})
    required = {"stop_id", "stop_name", "stop_lat", "stop_lon"}
    missing = required.difference(stops.columns)
    if missing:
        raise ValueError(f"{stops_path} is missing required column(s): {', '.join(sorted(missing))}")
    return stops.select(
        pl.col("stop_id").cast(pl.Utf8),
        pl.col("stop_name").cast(pl.Utf8).alias("gtfs_stop_name"),
        pl.col("stop_lat").cast(pl.Float64, strict=False),
        pl.col("stop_lon").cast(pl.Float64, strict=False),
    )


def _load_one_gtfs_route_metadata(gtfs_dir: Path) -> pl.DataFrame:
    routes_path = gtfs_dir / "routes.txt"
    if not routes_path.exists():
        raise FileNotFoundError(f"GTFS routes.txt not found: {routes_path}")
    routes = pl.read_csv(
        routes_path,
        schema_overrides={"route_id": pl.Utf8, "route_short_name": pl.Utf8},
    )
    required = {"route_id", "route_short_name"}
    missing = required.difference(routes.columns)
    if missing:
        raise ValueError(
            f"{routes_path} is missing required column(s): {', '.join(sorted(missing))}"
        )
    return routes.select(
        pl.col("route_id").cast(pl.Utf8),
        pl.col("route_short_name").cast(pl.Utf8),
    )
