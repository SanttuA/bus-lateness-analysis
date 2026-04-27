from __future__ import annotations

import sqlite3
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "foli.db"
DEFAULT_GTFS_ROOT = PROJECT_ROOT / "data" / "gtfs"
DEFAULT_TIMEZONE = "Europe/Helsinki"

DELAY_FILTER_SQL = """
    is_gtfs_matchable = 1
    AND delay_seconds IS NOT NULL
    AND line_ref IS NOT NULL
"""

METRIC_LABELS = {
    "avg_delay_min": "Average delay (min)",
    "pct_late": "Late observations (%)",
    "pct_over_3_min_late": "Over 3 min late (%)",
    "obs_count": "Observation count",
}

DIVERGING_METRICS = {"avg_delay_min"}


def resolve_project_path(path: Path | str) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    return resolved.resolve()


def latest_gtfs_dir(root: Path | str = DEFAULT_GTFS_ROOT) -> Path | None:
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


def connect_readonly_db(path: Path | str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    db_path = resolve_project_path(path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")
    uri = f"file:{db_path.as_posix()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def load_observations(
    db_path: Path | str = DEFAULT_DB_PATH,
    *,
    limit: int | None = None,
) -> pd.DataFrame:
    query = f"""
    SELECT
        recorded_at_utc,
        line_ref,
        direction_ref,
        published_line_name,
        delay_seconds,
        next_stop_point_ref,
        next_stop_point_name
    FROM vehicle_observations
    WHERE {DELAY_FILTER_SQL}
    """
    params: list[object] = []
    if limit is not None:
        query += "\nLIMIT ?"
        params.append(limit)

    with connect_readonly_db(db_path) as con:
        return pd.read_sql_query(query, con, params=params)


def load_stop_metadata(gtfs_dir: Path | str | None = None) -> pd.DataFrame:
    resolved_gtfs_dir = resolve_project_path(gtfs_dir) if gtfs_dir else latest_gtfs_dir()
    if resolved_gtfs_dir is None:
        raise FileNotFoundError(f"No GTFS stops.txt found below {DEFAULT_GTFS_ROOT}")

    stops_path = resolved_gtfs_dir / "stops.txt"
    if not stops_path.exists():
        raise FileNotFoundError(f"GTFS stops.txt not found: {stops_path}")

    stops = pd.read_csv(stops_path, dtype={"stop_id": "string"})
    required = {"stop_id", "stop_name", "stop_lat", "stop_lon"}
    missing = required.difference(stops.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(f"{stops_path} is missing required column(s): {missing_text}")

    stops = stops[["stop_id", "stop_name", "stop_lat", "stop_lon"]].copy()
    stops["stop_id"] = stops["stop_id"].astype("string")
    stops["stop_lat"] = pd.to_numeric(stops["stop_lat"], errors="coerce")
    stops["stop_lon"] = pd.to_numeric(stops["stop_lon"], errors="coerce")
    return stops.rename(columns={"stop_name": "gtfs_stop_name"})


def prepare_observations(
    observations: pd.DataFrame,
    stops: pd.DataFrame,
    *,
    timezone: str = DEFAULT_TIMEZONE,
) -> pd.DataFrame:
    if observations.empty:
        return _empty_prepared_frame()

    result = observations.copy()
    result["recorded_at_utc"] = pd.to_datetime(
        result["recorded_at_utc"],
        utc=True,
        errors="coerce",
    )
    result["delay_seconds"] = pd.to_numeric(result["delay_seconds"], errors="coerce")
    result = result.dropna(subset=["recorded_at_utc", "delay_seconds", "line_ref"])

    result["delay_min"] = result["delay_seconds"] / 60.0
    result["line_ref"] = result["line_ref"].astype("string")
    result["direction_ref"] = result["direction_ref"].astype("string").fillna("Unknown")
    result["published_line_name"] = (
        result["published_line_name"].astype("string").fillna(result["line_ref"])
    )
    result["stop_id"] = result["next_stop_point_ref"].astype("string")

    tz = ZoneInfo(timezone)
    local_times = result["recorded_at_utc"].dt.tz_convert(tz)
    result["local_time"] = local_times
    result["local_date"] = local_times.dt.date
    result["local_hour"] = local_times.dt.hour
    result["local_weekday"] = local_times.dt.weekday
    result["is_weekday"] = result["local_weekday"] < 5

    stop_columns = ["stop_id", "gtfs_stop_name", "stop_lat", "stop_lon"]
    if stops.empty:
        stop_metadata = pd.DataFrame(columns=stop_columns)
    else:
        stop_metadata = stops[stop_columns].copy()
        stop_metadata["stop_id"] = stop_metadata["stop_id"].astype("string")

    result = result.merge(stop_metadata, how="left", on="stop_id")
    result["next_stop_point_name"] = result["next_stop_point_name"].astype("string")
    result["stop_name"] = result["gtfs_stop_name"].combine_first(
        result["next_stop_point_name"]
    )
    result["stop_lat"] = pd.to_numeric(result["stop_lat"], errors="coerce")
    result["stop_lon"] = pd.to_numeric(result["stop_lon"], errors="coerce")
    return result


def filter_observations(
    df: pd.DataFrame,
    *,
    start_date: object | None = None,
    end_date: object | None = None,
    line_refs: list[str] | tuple[str, ...] | None = None,
    direction_refs: list[str] | tuple[str, ...] | None = None,
    day_filter: str = "All days",
) -> pd.DataFrame:
    result = df
    if start_date is not None:
        result = result[result["local_date"] >= start_date]
    if end_date is not None:
        result = result[result["local_date"] <= end_date]
    if line_refs:
        selected_lines = {str(line_ref) for line_ref in line_refs}
        result = result[result["line_ref"].astype(str).isin(selected_lines)]
    if direction_refs:
        selected_directions = {str(direction_ref) for direction_ref in direction_refs}
        result = result[result["direction_ref"].astype(str).isin(selected_directions)]
    if day_filter == "Weekdays":
        result = result[result["is_weekday"]]
    elif day_filter == "Weekends":
        result = result[~result["is_weekday"]]
    return result


def build_hourly_line_metrics(
    df: pd.DataFrame,
    *,
    min_observations: int = 1,
) -> pd.DataFrame:
    if df.empty:
        return _empty_metric_frame(["line_ref", "local_hour", "line_name"])

    grouped = df.groupby(["line_ref", "local_hour"], as_index=False).agg(
        line_name=("published_line_name", "first"),
        obs_count=("delay_seconds", "size"),
        avg_delay_min=("delay_min", "mean"),
        median_delay_min=("delay_min", "median"),
        pct_late=("delay_seconds", lambda s: (s > 0).mean() * 100.0),
        pct_over_3_min_late=("delay_seconds", lambda s: (s > 180).mean() * 100.0),
    )
    return grouped[grouped["obs_count"] >= min_observations].reset_index(drop=True)


def build_stop_metrics(
    df: pd.DataFrame,
    *,
    min_observations: int = 1,
) -> pd.DataFrame:
    if df.empty:
        return _empty_metric_frame(["stop_id", "stop_name", "stop_lat", "stop_lon"])

    grouped = df.groupby(
        ["stop_id", "stop_name", "stop_lat", "stop_lon"],
        dropna=False,
        as_index=False,
    ).agg(
        obs_count=("delay_seconds", "size"),
        line_count=("line_ref", "nunique"),
        avg_delay_min=("delay_min", "mean"),
        median_delay_min=("delay_min", "median"),
        pct_late=("delay_seconds", lambda s: (s > 0).mean() * 100.0),
        pct_over_3_min_late=("delay_seconds", lambda s: (s > 180).mean() * 100.0),
    )
    return grouped[grouped["obs_count"] >= min_observations].reset_index(drop=True)


def build_stop_heatmap_weights(
    stop_metrics: pd.DataFrame,
    metric_key: str,
    *,
    delay_direction: str = "late",
) -> pd.DataFrame:
    result = stop_metrics.dropna(subset=["stop_lat", "stop_lon"]).copy()
    if result.empty:
        result["heat_weight"] = pd.Series(dtype="float64")
        return result

    if metric_key == "avg_delay_min":
        if delay_direction == "late":
            result["heat_weight"] = result["avg_delay_min"].clip(lower=0) * result[
                "obs_count"
            ]
        elif delay_direction == "early":
            result["heat_weight"] = (-result["avg_delay_min"]).clip(lower=0) * result[
                "obs_count"
            ]
        else:
            raise ValueError("delay_direction must be 'late' or 'early'")
    elif metric_key == "pct_late":
        result["heat_weight"] = result["pct_late"] / 100.0 * result["obs_count"]
    elif metric_key == "pct_over_3_min_late":
        result["heat_weight"] = (
            result["pct_over_3_min_late"] / 100.0 * result["obs_count"]
        )
    elif metric_key == "obs_count":
        result["heat_weight"] = result["obs_count"]
    else:
        raise ValueError(f"Unsupported heatmap metric: {metric_key}")

    result["heat_weight"] = pd.to_numeric(result["heat_weight"], errors="coerce")
    return result[result["heat_weight"] > 0].reset_index(drop=True)


def summarize_observations(df: pd.DataFrame) -> dict[str, float | int]:
    if df.empty:
        return {
            "obs_count": 0,
            "line_count": 0,
            "stop_count": 0,
            "avg_delay_min": 0.0,
            "pct_late": 0.0,
        }

    return {
        "obs_count": int(len(df)),
        "line_count": int(df["line_ref"].nunique(dropna=True)),
        "stop_count": int(df["stop_id"].nunique(dropna=True)),
        "avg_delay_min": float(df["delay_min"].mean()),
        "pct_late": float((df["delay_seconds"] > 0).mean() * 100.0),
    }


def rank_late_stops(stop_metrics: pd.DataFrame, *, limit: int = 20) -> pd.DataFrame:
    return stop_metrics.sort_values(
        ["avg_delay_min", "obs_count"],
        ascending=[False, False],
    ).head(limit)


def rank_early_stops(stop_metrics: pd.DataFrame, *, limit: int = 20) -> pd.DataFrame:
    return stop_metrics.sort_values(
        ["avg_delay_min", "obs_count"],
        ascending=[True, False],
    ).head(limit)


def metric_label(metric_key: str) -> str:
    return METRIC_LABELS.get(metric_key, metric_key)


def _empty_prepared_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "recorded_at_utc",
            "line_ref",
            "direction_ref",
            "published_line_name",
            "delay_seconds",
            "next_stop_point_ref",
            "next_stop_point_name",
            "delay_min",
            "stop_id",
            "local_time",
            "local_date",
            "local_hour",
            "local_weekday",
            "is_weekday",
            "gtfs_stop_name",
            "stop_lat",
            "stop_lon",
            "stop_name",
        ]
    )


def _empty_metric_frame(group_columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            *group_columns,
            "obs_count",
            "line_count",
            "avg_delay_min",
            "median_delay_min",
            "pct_late",
            "pct_over_3_min_late",
        ]
    )
