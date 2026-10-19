"""Engine-agnostic collector gap detection shared by the pandas/DuckDB and Polars paths.

Both report engines used to implement blackout and missing-spot detection separately
and disagreed on the results. This module is now the single implementation: a gap is
two consecutive successful polls of one source further apart than ``gap_multiplier``
times that source's median cadence.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, time, timedelta
from statistics import median
from zoneinfo import ZoneInfo

DEFAULT_GAP_MULTIPLIER = 2.0
VEHICLE_SOURCE = "siri_vm"


def _parse_utc(value: object) -> datetime | None:
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


def _is_ok(row: Mapping[str, object]) -> bool:
    try:
        return int(row.get("ok") or 0) == 1
    except (TypeError, ValueError):
        return False


def _by_source(polls: Iterable[Mapping[str, object]]) -> dict[str, list[Mapping[str, object]]]:
    grouped: dict[str, list[Mapping[str, object]]] = defaultdict(list)
    for row in polls:
        grouped[str(row.get("source"))].append(row)
    return dict(grouped)


def expected_cadence_seconds(rows: list[Mapping[str, object]]) -> float | None:
    gaps = []
    for row in rows:
        if not _is_ok(row):
            continue
        try:
            gap = float(row.get("gap_seconds_since_previous_success") or 0)
        except (TypeError, ValueError):
            continue
        if gap > 0:
            gaps.append(gap)
    return float(median(gaps)) if gaps else None


def _source_spots(
    source: str,
    rows: list[Mapping[str, object]],
    gap_multiplier: float,
) -> tuple[float | None, list[dict[str, object]]]:
    expected = expected_cadence_seconds(rows)
    if expected is None or expected <= 0:
        return expected, []
    successes = sorted(
        (
            (collected, row)
            for row in rows
            if _is_ok(row) and (collected := _parse_utc(row.get("collected_at_utc"))) is not None
        ),
        key=lambda item: item[0],
    )
    failed_attempts = sorted(
        attempted
        for row in rows
        if not _is_ok(row) and (attempted := _parse_utc(row.get("attempted_at_utc"))) is not None
    )
    row_counts = [float(row.get("row_count") or 0) for _, row in successes]
    avg_rows = sum(row_counts) / len(row_counts) if row_counts else 0.0
    spots = []
    for (previous, _), (current, row) in zip(successes, successes[1:], strict=False):
        gap_seconds = (current - previous).total_seconds()
        if gap_seconds <= gap_multiplier * expected:
            continue
        missing_seconds = max(0.0, gap_seconds - expected)
        missed_polls = missing_seconds / expected
        failed = bisect_left(failed_attempts, current) - bisect_right(failed_attempts, previous)
        spots.append(
            {
                "source": source,
                "gap_start_utc": previous,
                "gap_end_utc": current,
                "gap_min": gap_seconds / 60.0,
                "expected_cadence_seconds": expected,
                "missing_min": missing_seconds / 60.0,
                "estimated_missed_polls": missed_polls,
                "estimated_missed_rows": missed_polls * avg_rows,
                "failed_attempt_count": max(failed, 0),
                "next_success_status": row.get("status"),
            }
        )
    return expected, spots


def format_utc(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def missing_spots(
    polls: Iterable[Mapping[str, object]],
    *,
    gap_multiplier: float = DEFAULT_GAP_MULTIPLIER,
    min_missing_minutes: float = 0.0,
) -> list[dict[str, object]]:
    """Detected gaps, largest first, with ISO-8601 UTC timestamps."""
    spots = []
    for source, rows in sorted(_by_source(polls).items()):
        spots.extend(
            spot
            for spot in _source_spots(source, rows, gap_multiplier)[1]
            if float(spot["missing_min"]) >= min_missing_minutes
        )
    spots.sort(key=lambda row: (-float(row["missing_min"]), str(row["source"]), row["gap_start_utc"]))
    return [
        {**spot, "gap_start_utc": format_utc(spot["gap_start_utc"]), "gap_end_utc": format_utc(spot["gap_end_utc"])}
        for spot in spots
    ]


def source_summaries(
    polls: Iterable[Mapping[str, object]],
    *,
    gap_multiplier: float = DEFAULT_GAP_MULTIPLIER,
) -> list[dict[str, object]]:
    """One row per source: poll counts plus totals over the detected gaps."""
    summaries = []
    for source, rows in sorted(_by_source(polls).items()):
        expected, spots = _source_spots(source, rows, gap_multiplier)
        success_count = sum(1 for row in rows if _is_ok(row))
        missing = [float(spot["missing_min"]) for spot in spots]
        summaries.append(
            {
                "source": source,
                "poll_count": len(rows),
                "success_count": success_count,
                "failed_count": len(rows) - success_count,
                "expected_cadence_seconds": expected,
                "gap_count": len(spots),
                "total_missing_min": sum(missing),
                "largest_missing_min": max(missing, default=0.0),
                "estimated_missed_polls": sum(float(spot["estimated_missed_polls"]) for spot in spots),
                "estimated_missed_rows": sum(float(spot["estimated_missed_rows"]) for spot in spots),
            }
        )
    return sorted(summaries, key=lambda row: (-float(row["total_missing_min"]), -int(row["gap_count"]), row["source"]))


def blackout_rows(summaries: list[Mapping[str, object]]) -> list[dict[str, object]]:
    """Legacy ``collector_blackouts`` column names over the shared gap definition."""
    return [
        {
            "source": row["source"],
            "poll_count": row["poll_count"],
            "success_count": row["success_count"],
            "failed_count": row["failed_count"],
            "expected_cadence_seconds": row["expected_cadence_seconds"],
            "blackout_count": row["gap_count"],
            "total_blackout_min": row["total_missing_min"],
            "largest_blackout_min": row["largest_missing_min"],
            "estimated_missed_polls": row["estimated_missed_polls"],
            "estimated_missed_rows": row["estimated_missed_rows"],
        }
        for row in summaries
    ]


def missing_summary_rows(summaries: list[Mapping[str, object]]) -> list[dict[str, object]]:
    """Legacy ``collector_missing_summary`` column names over the shared gap definition."""
    return [
        {
            "source": row["source"],
            "poll_count": row["poll_count"],
            "success_count": row["success_count"],
            "failed_count": row["failed_count"],
            "missing_spot_count": row["gap_count"],
            "total_missing_min": row["total_missing_min"],
            "largest_missing_min": row["largest_missing_min"],
            "estimated_missed_polls": row["estimated_missed_polls"],
            "estimated_missed_rows": row["estimated_missed_rows"],
        }
        for row in summaries
    ]


def daily_coverage(
    polls: Iterable[Mapping[str, object]],
    *,
    timezone: str,
    source: str = VEHICLE_SOURCE,
    gap_multiplier: float = DEFAULT_GAP_MULTIPLIER,
) -> list[dict[str, object]]:
    """Share of each local day (within the collection span) not lost to collector gaps."""
    rows = [row for row in polls if str(row.get("source")) == source]
    expected, spots = _source_spots(source, rows, gap_multiplier)
    successes = sorted(
        collected
        for row in rows
        if _is_ok(row) and (collected := _parse_utc(row.get("collected_at_utc"))) is not None
    )
    if not successes or expected is None:
        return []
    tz = ZoneInfo(timezone)
    span_start, span_end = successes[0], successes[-1]
    missing_intervals = [
        (spot["gap_start_utc"] + timedelta(seconds=expected), spot["gap_end_utc"])
        for spot in spots
    ]
    success_by_day: dict[date, int] = defaultdict(int)
    for collected in successes:
        success_by_day[collected.astimezone(tz).date()] += 1

    result = []
    day = span_start.astimezone(tz).date()
    last_day = span_end.astimezone(tz).date()
    while day <= last_day:
        day_start = datetime.combine(day, time.min, tz).astimezone(UTC)
        day_end = datetime.combine(day + timedelta(days=1), time.min, tz).astimezone(UTC)
        window_start, window_end = max(day_start, span_start), min(day_end, span_end)
        window_seconds = max((window_end - window_start).total_seconds(), 0.0)
        missing_seconds = sum(
            max((min(end, window_end) - max(start, window_start)).total_seconds(), 0.0)
            for start, end in missing_intervals
        )
        result.append(
            {
                "local_date": day,
                "observed_hours": window_seconds / 3600.0,
                "missing_min": missing_seconds / 60.0,
                "coverage_pct": (
                    (1.0 - missing_seconds / window_seconds) * 100.0 if window_seconds > 0 else 0.0
                ),
                "success_count": success_by_day.get(day, 0),
            }
        )
        day += timedelta(days=1)
    return result
