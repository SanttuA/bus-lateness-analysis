"""Engine-agnostic GTFS calendar helpers shared by the pandas/DuckDB and Polars paths.

Only the standard library is used so both pipelines (and their CLI scripts) can import
this module without pulling in a DataFrame engine.
"""

from __future__ import annotations

import csv
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from functools import lru_cache
from datetime import date, datetime, timedelta
from pathlib import Path
import re
from statistics import median

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GTFS_ROOT = PROJECT_ROOT / "data" / "gtfs"
GTFS_DIR_PATTERN = re.compile(r"^gtfs_(\d{4}-\d{2}-\d{2})$")

# A Monday-Friday date whose active service count falls below this share of the
# surrounding weekdays' median runs a reduced (Sunday-like) timetable, e.g. 1.5. and
# Ascension Day. Föli ran ~130 services on those days versus ~296 on normal weekdays.
HOLIDAY_SERVICE_SHARE = 0.6
HOLIDAY_WINDOW_DAYS = 14
WEEKDAY_NAMES = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)


def _resolve(path: Path | str) -> Path:
    resolved = Path(path).expanduser()
    if not resolved.is_absolute():
        resolved = PROJECT_ROOT / resolved
    return resolved.resolve()


def _parse_gtfs_date(value: str) -> date:
    return datetime.strptime(value.strip(), "%Y%m%d").date()


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


@dataclass
class GtfsFeed:
    """One extracted GTFS snapshot with lazily parsed calendar, route and trip data."""

    path: Path
    feed_date: date | None
    _active: dict[date, set[str]] | None = field(default=None, repr=False)
    _routes: dict[str, str] | None = field(default=None, repr=False)
    _service_lines: dict[str, Counter[str]] | None = field(default=None, repr=False)

    def active_services(self) -> dict[date, set[str]]:
        if self._active is None:
            active: dict[date, set[str]] = defaultdict(set)
            for row in _read_csv(self.path / "calendar.txt"):
                flags = [row.get(name, "0").strip() == "1" for name in WEEKDAY_NAMES]
                if not any(flags):
                    continue
                current = _parse_gtfs_date(row["start_date"])
                end = _parse_gtfs_date(row["end_date"])
                while current <= end:
                    if flags[current.weekday()]:
                        active[current].add(row["service_id"])
                    current += timedelta(days=1)
            for row in _read_csv(self.path / "calendar_dates.txt"):
                day = _parse_gtfs_date(row["date"])
                if row.get("exception_type", "").strip() == "1":
                    active[day].add(row["service_id"])
                elif row.get("exception_type", "").strip() == "2":
                    active[day].discard(row["service_id"])
            self._active = dict(active)
        return self._active

    def route_short_names(self) -> dict[str, str]:
        if self._routes is None:
            self._routes = {
                row["route_id"]: (row.get("route_short_name") or row["route_id"])
                for row in _read_csv(self.path / "routes.txt")
                if row.get("route_id")
            }
        return self._routes

    def lines_by_service(self) -> dict[str, Counter[str]]:
        if self._service_lines is None:
            routes = self.route_short_names()
            by_service: dict[str, Counter[str]] = defaultdict(Counter)
            for row in _read_csv(self.path / "trips.txt"):
                line = routes.get(row.get("route_id", ""), row.get("route_id", ""))
                by_service[row.get("service_id", "")][line] += 1
            self._service_lines = dict(by_service)
        return self._service_lines


class GtfsCalendar:
    """Date-aware access to one or more GTFS snapshots.

    Each local date uses the latest snapshot whose folder date is on or before it. Dates
    before the first snapshot fall back to the earliest snapshot, because Föli feeds carry
    calendar_dates for the whole timetable season. Folder dates are collector download
    dates, not schedule start dates, so this choice is an approximation.
    """

    def __init__(self, feeds: list[GtfsFeed]):
        self.feeds = sorted(feeds, key=lambda feed: feed.feed_date or date.min)
        self._feed_dates = [feed.feed_date or date.min for feed in self.feeds]

    @classmethod
    def from_paths(
        cls,
        *,
        gtfs_root: Path | str | None = DEFAULT_GTFS_ROOT,
        gtfs_dir: Path | str | None = None,
    ) -> GtfsCalendar:
        if gtfs_dir is not None:
            path = _resolve(gtfs_dir)
            return cls([GtfsFeed(path, None)] if path.exists() else [])
        if gtfs_root is None:
            return cls([])
        root = _resolve(gtfs_root)
        if not root.exists():
            return cls([])
        feeds = []
        for path in root.iterdir():
            match = GTFS_DIR_PATTERN.match(path.name)
            if match and path.is_dir():
                feeds.append(GtfsFeed(path, date.fromisoformat(match.group(1))))
        return cls(feeds)

    def __bool__(self) -> bool:
        return bool(self.feeds)

    def feed_for(self, day: date) -> GtfsFeed | None:
        if not self.feeds:
            return None
        position = bisect_right(self._feed_dates, day) - 1
        return self.feeds[max(position, 0)]

    def covered_dates(self) -> list[date]:
        dates: set[date] = set()
        for feed in self.feeds:
            dates.update(feed.active_services())
        return sorted(dates)

    def service_count(self, day: date) -> int:
        feed = self.feed_for(day)
        if feed is None:
            return 0
        return len(feed.active_services().get(day, ()))

    def holiday_dates(
        self,
        *,
        threshold: float = HOLIDAY_SERVICE_SHARE,
        window_days: int = HOLIDAY_WINDOW_DAYS,
    ) -> tuple[date, ...]:
        """Monday-Friday dates that run a reduced timetable compared with nearby weekdays."""
        counts = {
            day: self.service_count(day)
            for day in self.covered_dates()
            if day.weekday() < 5
        }
        holidays = []
        for day, count in counts.items():
            neighbours = [
                other_count
                for other, other_count in counts.items()
                if other != day and abs((other - day).days) <= window_days and other_count > 0
            ]
            if len(neighbours) < 3:
                continue
            if count < threshold * median(neighbours):
                holidays.append(day)
        return tuple(sorted(holidays))

    def route_short_name(self, route_id: object, day: date | None) -> str:
        """Map a GTFS route_id to its public line number for the snapshot valid on day."""
        route_text = str(route_id)
        feed = self.feed_for(day) if day is not None else (self.feeds[-1] if self.feeds else None)
        if feed is None:
            return route_text
        return feed.route_short_names().get(route_text, route_text)

    def scheduled_trips(self, day: date) -> Counter[str]:
        """Scheduled trips per public line number on one service date."""
        feed = self.feed_for(day)
        if feed is None:
            return Counter()
        by_service = feed.lines_by_service()
        totals: Counter[str] = Counter()
        for service_id in feed.active_services().get(day, ()):
            totals.update(by_service.get(service_id, Counter()))
        return totals


@lru_cache(maxsize=8)
def _cached_calendar(gtfs_root: str | None, gtfs_dir: str | None, _fingerprint: str) -> GtfsCalendar:
    return GtfsCalendar.from_paths(gtfs_root=gtfs_root, gtfs_dir=gtfs_dir)


def cached_calendar(
    *,
    gtfs_root: Path | str | None = DEFAULT_GTFS_ROOT,
    gtfs_dir: Path | str | None = None,
) -> GtfsCalendar:
    """Process-wide cached calendar, invalidated when the GTFS files change."""
    return _cached_calendar(
        str(gtfs_root) if gtfs_root is not None else None,
        str(gtfs_dir) if gtfs_dir is not None else None,
        calendar_fingerprint(gtfs_root=gtfs_root, gtfs_dir=gtfs_dir),
    )


def holiday_dates(
    *,
    gtfs_root: Path | str | None = DEFAULT_GTFS_ROOT,
    gtfs_dir: Path | str | None = None,
) -> tuple[date, ...]:
    return cached_calendar(gtfs_root=gtfs_root, gtfs_dir=gtfs_dir).holiday_dates()


def holiday_date_strings(
    *,
    gtfs_root: Path | str | None = DEFAULT_GTFS_ROOT,
    gtfs_dir: Path | str | None = None,
) -> tuple[str, ...]:
    return tuple(day.isoformat() for day in holiday_dates(gtfs_root=gtfs_root, gtfs_dir=gtfs_dir))


def calendar_fingerprint(
    *,
    gtfs_root: Path | str | None = DEFAULT_GTFS_ROOT,
    gtfs_dir: Path | str | None = None,
) -> str:
    """Cache key part covering the GTFS files the calendar helpers read."""
    calendar = GtfsCalendar.from_paths(gtfs_root=gtfs_root, gtfs_dir=gtfs_dir)
    parts = []
    for feed in calendar.feeds:
        for filename in ("calendar.txt", "calendar_dates.txt", "routes.txt", "trips.txt"):
            path = feed.path / filename
            if path.exists():
                stat = path.stat()
                parts.append(f"{feed.path.name}|{filename}|{stat.st_mtime_ns}|{stat.st_size}")
            else:
                parts.append(f"{feed.path.name}|{filename}|missing")
    return "||".join(parts)
