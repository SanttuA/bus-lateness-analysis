"""Engine-agnostic service-alert targeting and matched-control lift.

Föli alert semantics (see ../foli-data-collector/foli_harvester/parsers.py):

- ``service_alerts`` stores one snapshot per alert per poll (every five minutes).
- Message alerts carry ``affected_routes`` as GTFS route_ids; ``line_ref`` is just the
  single element of that list, so it is never used directly as a public line number.
- Message validity windows live in ``repeat_json``; the validity columns only hold the
  first window. ``isactive`` reports whether the message is in effect at poll time.
- Cancellations name the cancelled departure with the public line number and have no
  validity window. They describe one departure, not a route-wide disruption, so they are
  summarised separately and kept out of the delay-lift comparison.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta
import json
import math
from zoneinfo import ZoneInfo

import numpy as np

try:
    from .uncertainty import DEFAULT_BOOTSTRAP_REPS, DEFAULT_BOOTSTRAP_SEED, cluster_codes
except ImportError:  # pragma: no cover - used when called as analysis/*.py script.
    from uncertainty import DEFAULT_BOOTSTRAP_REPS, DEFAULT_BOOTSTRAP_SEED, cluster_codes

ALERT_GROUP_COLUMNS = ["cause", "effect", "priority", "alert_scope"]
MATCH_CONTEXT_COLUMNS = ["line_ref", "direction_ref", "local_hour", "day_type"]
ALERT_POLL_CADENCE_SECONDS = 300
LONG_RUNNING_SHARE = 0.9
MIN_CONTROLS_PER_STRATUM = 20
LATE_THRESHOLD_SECONDS = 300
MESSAGE_ALERT_TYPES = {"message", "global_message", "emergency_message"}

RouteResolver = Callable[[str, datetime], str]


def parse_utc(value: object) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def json_list(value: object) -> list[str]:
    if value is None:
        return []
    text = str(value).strip()
    if not text:
        return []
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(decoded, list):
        return []
    return [str(item) for item in decoded if item is not None and not isinstance(item, dict | list)]


def repeat_windows(value: object) -> list[tuple[datetime, datetime]]:
    """Parse Föli ``repeat`` windows ([[start_unix, end_unix], ...])."""
    if value is None or str(value).strip() == "":
        return []
    try:
        decoded = json.loads(str(value))
    except json.JSONDecodeError:
        return []
    windows = []
    if not isinstance(decoded, list):
        return windows
    for item in decoded:
        if not isinstance(item, list) or len(item) < 2:
            continue
        try:
            start = datetime.fromtimestamp(float(item[0]), UTC)
            end = datetime.fromtimestamp(float(item[1]), UTC)
        except (TypeError, ValueError, OverflowError, OSError):
            continue
        if end > start:
            windows.append((start, end))
    return windows


def clean_alert_value(value: object) -> str:
    if value is None or str(value).strip() == "":
        return "Unknown"
    return str(value)


def clean_priority(value: object) -> int:
    if value is None:
        return -1
    try:
        if isinstance(value, float) and math.isnan(value):
            return -1
        return int(value)
    except (TypeError, ValueError):
        return -1


def _truthy(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    return bool(int(value)) if isinstance(value, int | float) else str(value).strip() in {"1", "true", "True"}


def _merge(intervals: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    merged: list[tuple[datetime, datetime]] = []
    for start, end in sorted(intervals):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
        else:
            merged.append((start, end))
    return merged


def make_route_resolver(calendar: object | None, timezone: str) -> RouteResolver:
    """Date-aware GTFS route_id -> public line number resolver."""
    tz = ZoneInfo(timezone)

    def resolve(route_id: str, when_utc: datetime) -> str:
        if calendar is None or not calendar:
            return str(route_id)
        return calendar.route_short_name(route_id, when_utc.astimezone(tz).date())

    return resolve


def build_alert_targets(
    alert_rows: Iterable[Mapping[str, object]],
    resolve_route: RouteResolver,
    obs_start: datetime,
    obs_end: datetime,
    *,
    cadence_seconds: int = ALERT_POLL_CADENCE_SECONDS,
    long_running_share: float = LONG_RUNNING_SHARE,
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    """Return (targets, inventory, cancellations) for the observation window.

    targets: one row per alert interval and route/stop target used for the lift.
    inventory: one row per message alert with its active span and inclusion decision.
    cancellations: one row per cancelled departure (public line number, cause).
    """
    obs_start = parse_utc(obs_start)
    obs_end = parse_utc(obs_end)
    if obs_start is None or obs_end is None or obs_end <= obs_start:
        return [], [], []
    window_seconds = (obs_end - obs_start).total_seconds()

    messages: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    cancellations: dict[tuple[str, str], dict[str, object]] = {}
    for row in alert_rows:
        alert_type = str(row.get("alert_type") or "message")
        if alert_type == "cancellation":
            line = row.get("line_ref")
            departure = parse_utc(row.get("departure_time_utc"))
            if line is None or departure is None:
                continue
            key = (str(line), departure.isoformat())
            seen = parse_utc(row.get("created_at_utc"))
            current = cancellations.get(key)
            if current is None:
                cancellations[key] = {
                    "line_ref": str(line),
                    "cause": clean_alert_value(row.get("cause")),
                    "departure_utc": departure,
                    "first_seen_utc": seen,
                    "stop_count": len(json_list(row.get("affected_stops_json"))),
                }
            elif seen is not None and (current["first_seen_utc"] is None or seen < current["first_seen_utc"]):
                current["first_seen_utc"] = seen
            continue
        if alert_type not in MESSAGE_ALERT_TYPES:
            continue
        alert_key = row.get("source_alert_id") or f"{alert_type}:{row.get('header') or ''}"
        messages[str(alert_key)].append(row)

    targets: list[dict[str, object]] = []
    inventory: list[dict[str, object]] = []
    cadence = timedelta(seconds=cadence_seconds)
    for alert_key, snapshots in sorted(messages.items()):
        snapshots = sorted(snapshots, key=lambda row: parse_utc(row.get("created_at_utc")) or obs_start)
        latest = snapshots[-1]
        record: dict[str, object] = {
            "alert_key": alert_key,
            "cause": clean_alert_value(latest.get("cause")),
            "effect": clean_alert_value(latest.get("effect")),
            "priority": clean_priority(latest.get("priority")),
            "first_active_utc": None,
            "last_active_utc": None,
            "active_hours": 0.0,
            "route_target_count": 0,
            "stop_target_count": 0,
            "long_running": False,
            "included": False,
            "exclusion_reason": None,
        }
        active = [row for row in snapshots if _truthy(row.get("is_active"))]
        active_times = [parse_utc(row.get("created_at_utc")) for row in active]
        active_times = [value for value in active_times if value is not None]
        if not active_times:
            record["exclusion_reason"] = "never_active"
            inventory.append(record)
            continue
        span = (min(active_times), max(active_times) + cadence)
        record["first_active_utc"] = span[0]
        record["last_active_utc"] = span[1]

        windows: list[tuple[datetime, datetime]] = []
        for row in active:
            parsed = repeat_windows(row.get("repeat_json"))
            if not parsed:
                start = parse_utc(row.get("validity_start_utc"))
                end = parse_utc(row.get("validity_end_utc"))
                if start is not None and end is not None and end > start:
                    parsed = [(start, end)]
            windows.extend(parsed)
        if not windows:
            windows = [span]
        intervals = []
        for start, end in _merge(windows):
            clipped = (max(start, span[0], obs_start), min(end, span[1], obs_end))
            if clipped[1] > clipped[0]:
                intervals.append(clipped)
        intervals = _merge(intervals)
        active_seconds = sum((end - start).total_seconds() for start, end in intervals)
        record["active_hours"] = active_seconds / 3600.0
        if not intervals:
            record["exclusion_reason"] = "outside_observation_window"
            inventory.append(record)
            continue

        routes: set[str] = set()
        stops: set[str] = set()
        for row in snapshots:
            routes.update(json_list(row.get("affected_routes_json")))
            stops.update(json_list(row.get("affected_stops_json")))
        lines = {resolve_route(route, intervals[0][0]) for route in routes}
        record["route_target_count"] = len(lines)
        record["stop_target_count"] = len(stops)
        if not lines and not stops:
            record["exclusion_reason"] = "no_targets"
            inventory.append(record)
            continue
        if active_seconds >= long_running_share * window_seconds:
            record["long_running"] = True
            record["exclusion_reason"] = "long_running"
            inventory.append(record)
            continue

        record["included"] = True
        inventory.append(record)
        base = {key: record[key] for key in ("alert_key", "cause", "effect", "priority")}
        for start, end in intervals:
            for line in sorted(lines):
                targets.append(
                    {**base, "alert_scope": "route", "target_ref": line, "start_utc": start, "end_utc": end}
                )
            for stop in sorted(stops):
                targets.append(
                    {**base, "alert_scope": "stop", "target_ref": stop, "start_utc": start, "end_utc": end}
                )

    cancellation_rows = sorted(
        cancellations.values(),
        key=lambda row: (row["departure_utc"], row["line_ref"]),
    )
    return targets, inventory, cancellation_rows


def summarize_cancellations(cancellations: list[Mapping[str, object]]) -> list[dict[str, object]]:
    """Cancelled departures per public line number and cause, most frequent first."""
    counts: dict[tuple[str, str], int] = defaultdict(int)
    for row in cancellations:
        counts[(str(row["line_ref"]), str(row["cause"]))] += 1
    rows = [
        {"line_ref": line, "cause": cause, "cancelled_departures": count}
        for (line, cause), count in counts.items()
    ]
    return sorted(rows, key=lambda row: (-int(row["cancelled_departures"]), row["line_ref"], row["cause"]))


def _codes(values: object) -> tuple[np.ndarray, int]:
    return cluster_codes(np.asarray(values))


def _group_medians(group: np.ndarray, values: np.ndarray, n_groups: int) -> np.ndarray:
    medians = np.full(n_groups, np.nan)
    if values.size == 0:
        return medians
    order = np.lexsort((values, group))
    sorted_groups = group[order]
    sorted_values = values[order]
    boundaries = np.flatnonzero(np.diff(sorted_groups)) + 1
    starts = np.concatenate(([0], boundaries))
    ends = np.concatenate((boundaries, [sorted_values.size]))
    for start, end in zip(starts, ends, strict=True):
        medians[sorted_groups[start]] = float(np.median(sorted_values[start:end]))
    return medians


def stratified_lift(
    strata: object,
    trips: object,
    delay_seconds: object,
    is_active: object,
    *,
    min_controls: int = MIN_CONTROLS_PER_STRATUM,
    reps: int = DEFAULT_BOOTSTRAP_REPS,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
    alpha: float = 0.05,
) -> dict[str, float | int | None]:
    """Alert-minus-control difference within matched strata, weighted by alert buckets.

    Each stratum is one line × direction × local hour × day type context. Strata without
    at least ``min_controls`` control buckets are dropped and reported via coverage_pct.
    Intervals come from a Poisson bootstrap over trips.
    """
    delay = np.asarray(delay_seconds, dtype=np.float64)
    active = np.asarray(is_active, dtype=bool)
    stratum_codes, n_strata = _codes(strata)
    trip_codes, n_trips = _codes(trips)
    result: dict[str, float | int | None] = {
        "bucket_count_alert": int(active.sum()),
        "bucket_count_control": int((~active).sum()),
        "active_trip_count": int(np.unique(trip_codes[active]).size) if active.any() else 0,
        "usable_stratum_count": 0,
        "coverage_pct": 0.0,
        "pct_over_5_min_late_alert": None,
        "pct_over_5_min_late_control": None,
        "late5_lift_pp": None,
        "late5_lift_lo": None,
        "late5_lift_hi": None,
        "median_lift_min": None,
    }
    if n_strata == 0 or not active.any():
        return result

    late = (delay > LATE_THRESHOLD_SECONDS).astype(np.float64)
    cell = stratum_codes * 2 + active.astype(np.int64)
    n_cells = n_strata * 2
    sizes = np.bincount(cell, minlength=n_cells).astype(np.float64).reshape(n_strata, 2)
    hits = np.bincount(cell, weights=late, minlength=n_cells).reshape(n_strata, 2)
    usable = (sizes[:, 1] > 0) & (sizes[:, 0] >= min_controls)
    if not usable.any():
        return result
    alert_total = sizes[:, 1].sum()
    usable_alert = sizes[usable, 1].sum()
    weights = sizes[usable, 1] / usable_alert
    alert_share = hits[usable, 1] / sizes[usable, 1]
    control_share = hits[usable, 0] / sizes[usable, 0]
    result["usable_stratum_count"] = int(usable.sum())
    result["coverage_pct"] = float(usable_alert / alert_total * 100.0)
    result["pct_over_5_min_late_alert"] = float(weights @ alert_share * 100.0)
    result["pct_over_5_min_late_control"] = float(weights @ control_share * 100.0)
    result["late5_lift_pp"] = float(weights @ (alert_share - control_share) * 100.0)

    medians = _group_medians(cell, delay, n_cells).reshape(n_strata, 2)
    result["median_lift_min"] = float(weights @ (medians[usable, 1] - medians[usable, 0]) / 60.0)

    if n_trips >= 2:
        row_mask = usable[stratum_codes]
        rows_cell = cell[row_mask]
        rows_trip = trip_codes[row_mask]
        rows_late = late[row_mask]
        rng = np.random.default_rng(seed)
        samples = []
        usable_cells = np.flatnonzero(usable)
        for _ in range(reps):
            trip_weights = rng.poisson(1.0, n_trips).astype(np.float64)
            row_weights = trip_weights[rows_trip]
            rep_sizes = np.bincount(rows_cell, weights=row_weights, minlength=n_cells).reshape(n_strata, 2)
            rep_hits = np.bincount(
                rows_cell,
                weights=row_weights * rows_late,
                minlength=n_cells,
            ).reshape(n_strata, 2)
            alert_sizes = rep_sizes[usable_cells, 1]
            control_sizes = rep_sizes[usable_cells, 0]
            valid = (alert_sizes > 0) & (control_sizes > 0)
            if not valid.any():
                continue
            rep_weights = alert_sizes[valid] / alert_sizes[valid].sum()
            diff = (
                rep_hits[usable_cells, 1][valid] / alert_sizes[valid]
                - rep_hits[usable_cells, 0][valid] / control_sizes[valid]
            )
            samples.append(float(rep_weights @ diff * 100.0))
        if len(samples) >= 2:
            lower, upper = np.quantile(samples, [alpha / 2, 1 - alpha / 2])
            result["late5_lift_lo"] = float(lower)
            result["late5_lift_hi"] = float(upper)
    return result


ALERT_RESULT_COLUMNS = [
    "alert_count",
    "active_trip_count",
    "bucket_count_alert",
    "bucket_count_control",
    "usable_stratum_count",
    "coverage_pct",
    "pct_over_5_min_late_alert",
    "pct_over_5_min_late_control",
    "late5_lift_pp",
    "late5_lift_lo",
    "late5_lift_hi",
    "median_lift_min",
]


def _passes(lift: Mapping[str, object], min_observations: int, min_trips: int) -> bool:
    return (
        lift.get("late5_lift_pp") is not None
        and int(lift.get("bucket_count_alert") or 0) >= min_observations
        and int(lift.get("active_trip_count") or 0) >= min_trips
    )


def summarize_alert_group(
    group: Mapping[str, object],
    combined: Mapping[str, Iterable[object]],
    active_pairs: Iterable[tuple[str, str]],
    *,
    min_observations: int,
    min_trips: int,
    reps: int = DEFAULT_BOOTSTRAP_REPS,
) -> tuple[dict[str, object] | None, list[dict[str, object]]]:
    """Grouped and per-line lift rows for one alert group.

    ``combined`` holds the group's alert buckets and its controls as equal-length columns:
    bucket_id, stratum (line|direction|hour|day type), line_ref, trip_match_key,
    delay_seconds, published_line_name and is_active. ``active_pairs`` lists
    (alert_key, bucket_id) for the group's alert buckets. Both engines build these columns
    natively and share this function, so their results match.
    """
    try:
        from .uncertainty import stable_seed
    except ImportError:  # pragma: no cover - used when called as analysis/*.py script.
        from uncertainty import stable_seed

    columns = {
        name: values if isinstance(values, np.ndarray) else np.asarray(list(values))
        for name, values in combined.items()
    }
    active = columns["is_active"].astype(bool)
    delay = columns["delay_seconds"].astype(np.float64)
    trips = columns["trip_match_key"]
    strata = columns["stratum"]
    line_labels, line_codes = np.unique(np.asarray(columns["line_ref"], dtype=str), return_inverse=True)
    line_codes = line_codes.ravel()
    bucket_ids = columns["bucket_id"].astype(object)
    group_key = tuple(group[name] for name in ALERT_GROUP_COLUMNS)
    alerts_by_bucket: dict[str, set[str]] = defaultdict(set)
    for alert_key, bucket_id in active_pairs:
        alerts_by_bucket[str(bucket_id)].add(str(alert_key))

    grouped_row = None
    lift = stratified_lift(strata, trips, delay, active, reps=reps, seed=stable_seed("grouped", *group_key))
    if _passes(lift, min_observations, min_trips):
        alert_count = len(set().union(*alerts_by_bucket.values())) if alerts_by_bucket else 0
        grouped_row = {**group, "alert_count": alert_count, **lift}

    line_rows = []
    for line_code in np.unique(line_codes[active]):
        line_ref = str(line_labels[line_code])
        mask = line_codes == line_code
        line_active = mask & active
        line_lift = stratified_lift(
            strata[mask],
            trips[mask],
            delay[mask],
            active[mask],
            reps=reps,
            seed=stable_seed("line", *group_key, line_ref),
        )
        if not _passes(line_lift, min_observations, min_trips):
            continue
        active_ids = bucket_ids[line_active]
        first = int(np.argmin(active_ids)) if active_ids.size else 0
        line_alerts: set[str] = set()
        for bucket_id in active_ids:
            line_alerts |= alerts_by_bucket.get(str(bucket_id), set())
        line_rows.append(
            {
                **group,
                "line_ref": line_ref,
                "line_name": columns["published_line_name"][line_active][first],
                "alert_count": len(line_alerts),
                **line_lift,
            }
        )
    return grouped_row, line_rows


def finalize_alert_rows(
    rows: list[Mapping[str, object]],
    limit: int,
    *,
    include_line: bool,
) -> list[dict[str, object]]:
    """Stable column order and ranking (largest lift in >5 min late share first)."""
    columns = [*ALERT_GROUP_COLUMNS, *(["line_ref", "line_name"] if include_line else []), *ALERT_RESULT_COLUMNS]
    ordered = sorted(
        rows,
        key=lambda row: (
            -float(row["late5_lift_pp"]),
            -int(row["bucket_count_alert"]),
            *(str(row[name]) for name in ALERT_GROUP_COLUMNS),
            str(row.get("line_ref", "")),
        ),
    )
    return [{column: row.get(column) for column in columns} for row in ordered[:limit]]
