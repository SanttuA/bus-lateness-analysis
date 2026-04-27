from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "foli.db"
DEFAULT_TIMEZONE = "Europe/Helsinki"
DEFAULT_RUSH_WINDOWS = ("07:00-09:00", "15:00-18:00")

DELAY_FILTER_SQL = """
    is_gtfs_matchable = 1
    AND delay_seconds IS NOT NULL
    AND line_ref IS NOT NULL
"""


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


def resolve_db_path(path: Path) -> Path:
    db_path = path.expanduser()
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path
    db_path = db_path.resolve()
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")
    return db_path


def connect_readonly_db(path: Path) -> sqlite3.Connection:
    db_path = resolve_db_path(path)
    uri = f"file:{db_path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def read_sql(con: sqlite3.Connection, query: str, params: object | None = None) -> pd.DataFrame:
    return pd.read_sql_query(query, con, params=params)


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
