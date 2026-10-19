from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np

from analysis.alert_analysis import (
    build_alert_targets,
    stratified_lift,
    summarize_cancellations,
)
from analysis.collector_gaps import daily_coverage, missing_spots, source_summaries
from analysis.service_calendar import GtfsCalendar
from analysis.uncertainty import (
    bootstrap_quantile_ci,
    bootstrap_share_ci,
    bootstrap_share_difference_ci,
)


def write_feed(root: Path, name: str, *, services_by_date: dict[str, int], routes: dict[str, str]) -> Path:
    feed = root / name
    feed.mkdir(parents=True)
    (feed / "calendar.txt").write_text(
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
    )
    lines = ["service_id,date,exception_type"]
    for day, count in services_by_date.items():
        for index in range(count):
            lines.append(f"S{index},{day},1")
    (feed / "calendar_dates.txt").write_text("\n".join(lines) + "\n")
    (feed / "routes.txt").write_text(
        "route_id,route_short_name\n"
        + "".join(f"{route_id},{short}\n" for route_id, short in routes.items())
    )
    trips = ["route_id,trip_id,service_id"]
    for index in range(max(services_by_date.values())):
        trips.append(f"{next(iter(routes))},T{index},S{index}")
    (feed / "trips.txt").write_text("\n".join(trips) + "\n")
    return feed


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


class ServiceCalendarTests(unittest.TestCase):
    def test_reduced_weekday_service_is_a_holiday(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            services = {}
            day = date(2026, 4, 20)
            while day <= date(2026, 5, 22):
                services[day.strftime("%Y%m%d")] = 30 if day.weekday() < 5 else 12
                day += timedelta(days=1)
            services["20260501"] = 13
            write_feed(root, "gtfs_2026-04-20", services_by_date=services, routes={"42": "43"})

            calendar = GtfsCalendar.from_paths(gtfs_root=root)

            self.assertEqual(calendar.holiday_dates(), (date(2026, 5, 1),))
            self.assertEqual(calendar.scheduled_trips(date(2026, 5, 4))["43"], 30)

    def test_route_names_follow_the_snapshot_valid_on_each_date(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            services = {"20260501": 1}
            write_feed(root, "gtfs_2026-04-01", services_by_date=services, routes={"9": "4"})
            write_feed(root, "gtfs_2026-05-01", services_by_date=services, routes={"9": "4T"})

            calendar = GtfsCalendar.from_paths(gtfs_root=root)

            self.assertEqual(calendar.route_short_name("9", date(2026, 4, 15)), "4")
            self.assertEqual(calendar.route_short_name("9", date(2026, 5, 2)), "4T")
            self.assertEqual(calendar.route_short_name("unknown", date(2026, 5, 2)), "unknown")


class UncertaintyTests(unittest.TestCase):
    def test_share_interval_is_deterministic_and_covers_the_estimate(self) -> None:
        rng = np.random.default_rng(1)
        trips = np.repeat(np.arange(200), 20)
        late = rng.random(trips.size) < 0.2

        first = bootstrap_share_ci(late, trips, seed=7)
        second = bootstrap_share_ci(late, trips, seed=7)

        self.assertEqual(first, second)
        self.assertLess(first[0], late.mean() * 100)
        self.assertGreater(first[1], late.mean() * 100)

    def test_clustered_trips_widen_the_interval(self) -> None:
        rng = np.random.default_rng(2)
        trips = np.repeat(np.arange(40), 50)
        trip_is_late = rng.random(40) < 0.3
        late = trip_is_late[trips]

        clustered = bootstrap_share_ci(late, trips, seed=3)
        independent = bootstrap_share_ci(late, np.arange(late.size), seed=3)

        self.assertGreater(clustered[1] - clustered[0], 3 * (independent[1] - independent[0]))

    def test_quantile_and_difference_intervals(self) -> None:
        values = np.concatenate([np.zeros(500), np.full(500, 600.0)])
        trips = np.arange(values.size)
        group = np.arange(values.size) >= 500

        low, high = bootstrap_quantile_ci(values, trips, 0.9, seed=5)
        diff_low, diff_high = bootstrap_share_difference_ci(values > 300, trips, group, seed=5)

        self.assertEqual((low, high), (600.0, 600.0))
        self.assertAlmostEqual(diff_low, 100.0)
        self.assertAlmostEqual(diff_high, 100.0)

    def test_single_cluster_has_no_interval(self) -> None:
        self.assertEqual(bootstrap_share_ci([True, False], ["a", "a"]), (None, None))


def message_snapshot(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "alert_type": "message",
        "source_alert_id": "A1",
        "line_ref": "42",
        "cause": "OTHER_CAUSE",
        "effect": "DETOUR",
        "priority": 900,
        "is_active": 1,
        "validity_start_utc": "2026-05-04T06:00:00Z",
        "validity_end_utc": "2026-05-04T10:00:00Z",
        "repeat_json": None,
        "affected_routes_json": json.dumps(["42"]),
        "affected_stops_json": "[]",
        "created_at_utc": "2026-05-04T06:05:00Z",
    }
    row.update(overrides)
    return row


class AlertTargetTests(unittest.TestCase):
    obs_start = utc(2026, 5, 1)
    obs_end = utc(2026, 5, 20)

    @staticmethod
    def resolver(route_id: str, _when: datetime) -> str:
        return {"42": "43", "6": "3"}.get(route_id, route_id)

    def test_message_routes_are_mapped_and_raw_line_ref_is_not_used(self) -> None:
        snapshots = [
            message_snapshot(created_at_utc="2026-05-04T06:05:00Z"),
            message_snapshot(created_at_utc="2026-05-04T09:55:00Z"),
        ]
        targets, inventory, _ = build_alert_targets(snapshots, self.resolver, self.obs_start, self.obs_end)

        self.assertEqual({row["target_ref"] for row in targets}, {"43"})
        self.assertTrue(inventory[0]["included"])
        self.assertEqual(targets[0]["start_utc"], utc(2026, 5, 4, 6, 5))
        self.assertEqual(targets[0]["end_utc"], utc(2026, 5, 4, 10, 0))

    def test_every_repeat_window_is_used(self) -> None:
        repeat = json.dumps(
            [
                [utc(2026, 5, 4, 6).timestamp(), utc(2026, 5, 4, 8).timestamp()],
                [utc(2026, 5, 5, 6).timestamp(), utc(2026, 5, 5, 8).timestamp()],
            ]
        )
        snapshots = [
            message_snapshot(repeat_json=repeat, created_at_utc="2026-05-04T06:00:00Z"),
            message_snapshot(repeat_json=repeat, created_at_utc="2026-05-05T07:55:00Z"),
        ]
        targets, _, _ = build_alert_targets(snapshots, self.resolver, self.obs_start, self.obs_end)

        self.assertEqual(
            [(row["start_utc"], row["end_utc"]) for row in targets],
            [(utc(2026, 5, 4, 6), utc(2026, 5, 4, 8)), (utc(2026, 5, 5, 6), utc(2026, 5, 5, 8))],
        )

    def test_cancellations_keep_line_numbers_and_stay_out_of_targets(self) -> None:
        cancellation = {
            "alert_type": "cancellation",
            "source_alert_id": None,
            "line_ref": "6",
            "cause": "TECHNICAL_PROBLEM",
            "departure_time_utc": "2026-05-04T04:10:00Z",
            "affected_routes_json": json.dumps(["6"]),
            "affected_stops_json": json.dumps(["1812", "1326"]),
            "created_at_utc": "2026-05-04T03:35:00Z",
        }
        targets, inventory, cancellations = build_alert_targets(
            [cancellation, {**cancellation, "created_at_utc": "2026-05-04T03:40:00Z"}],
            self.resolver,
            self.obs_start,
            self.obs_end,
        )

        self.assertEqual(targets, [])
        self.assertEqual(inventory, [])
        self.assertEqual(len(cancellations), 1)
        self.assertEqual(cancellations[0]["line_ref"], "6")
        self.assertEqual(
            summarize_cancellations(cancellations),
            [{"line_ref": "6", "cause": "TECHNICAL_PROBLEM", "cancelled_departures": 1}],
        )

    def test_long_running_and_never_active_alerts_are_excluded(self) -> None:
        long_running = [
            message_snapshot(
                source_alert_id="L",
                validity_start_utc="2026-03-01T00:00:00Z",
                validity_end_utc="2026-08-31T00:00:00Z",
                created_at_utc=value,
            )
            for value in ("2026-05-01T00:00:00Z", "2026-05-19T23:55:00Z")
        ]
        never_active = [message_snapshot(source_alert_id="N", is_active=0)]
        targets, inventory, _ = build_alert_targets(
            long_running + never_active,
            self.resolver,
            self.obs_start,
            self.obs_end,
        )

        self.assertEqual(targets, [])
        reasons = {row["alert_key"]: row["exclusion_reason"] for row in inventory}
        self.assertEqual(reasons, {"L": "long_running", "N": "never_active"})


class StratifiedLiftTests(unittest.TestCase):
    def test_matched_strata_remove_line_mix_bias(self) -> None:
        rows = []
        # Stratum "quiet": 10% late; stratum "busy": 50% late. Alerts sit mostly in the
        # busy stratum and controls mostly in the quiet one, but within each stratum the
        # alert buckets are exactly as late as their controls.
        for stratum, late_share, active_n, control_n in (
            ("quiet", 0.1, 50, 2000),
            ("busy", 0.5, 950, 300),
        ):
            every = round(1 / late_share)
            for active, count in ((True, active_n), (False, control_n)):
                for index in range(count):
                    late = index % every == 0
                    rows.append((stratum, f"{stratum}-{active}-{index // 5}", 600.0 if late else 0.0, active))
        strata, trips, delay, active = (np.array(column) for column in zip(*rows, strict=True))

        result = stratified_lift(strata, trips, delay, active, reps=200, seed=4)
        pooled = ((delay[active] > 300).mean() - (delay[~active] > 300).mean()) * 100

        self.assertGreater(pooled, 25)
        self.assertAlmostEqual(result["late5_lift_pp"], 0.0, delta=0.5)
        self.assertLess(result["late5_lift_lo"], 0)
        self.assertGreater(result["late5_lift_hi"], 0)
        self.assertEqual(result["coverage_pct"], 100.0)

    def test_strata_without_enough_controls_reduce_coverage(self) -> None:
        strata = np.array(["a"] * 30 + ["b"] * 12)
        active = np.array([True] * 10 + [False] * 20 + [True] * 10 + [False] * 2)
        trips = np.arange(strata.size)
        delay = np.where(active, 400.0, 0.0)

        result = stratified_lift(strata, trips, delay, active, reps=50)

        self.assertEqual(result["usable_stratum_count"], 1)
        self.assertEqual(result["coverage_pct"], 50.0)
        self.assertAlmostEqual(result["late5_lift_pp"], 100.0)
        self.assertAlmostEqual(result["median_lift_min"], 400.0 / 60.0)


class CollectorGapTests(unittest.TestCase):
    def polls(self) -> list[dict[str, object]]:
        rows = []
        start = utc(2026, 5, 1, 21)  # 00:00 Helsinki on 2.5.
        times = [start + timedelta(seconds=30 * index) for index in range(240)]
        times += [times[-1] + timedelta(hours=1) + timedelta(seconds=30 * index) for index in range(1, 121)]
        for when in times:
            rows.append(
                {
                    "source": "siri_vm",
                    "attempted_at_utc": when.isoformat(),
                    "collected_at_utc": when.isoformat(),
                    "status": "OK",
                    "ok": 1,
                    "row_count": 100,
                    "gap_seconds_since_previous_success": 30,
                }
            )
        rows.append(
            {
                "source": "siri_vm",
                "attempted_at_utc": (times[239] + timedelta(minutes=10)).isoformat(),
                "collected_at_utc": None,
                "status": "ERROR",
                "ok": 0,
                "row_count": 0,
                "gap_seconds_since_previous_success": None,
            }
        )
        return rows

    def test_gap_is_detected_with_failed_attempts(self) -> None:
        spots = missing_spots(self.polls())

        self.assertEqual(len(spots), 1)
        self.assertAlmostEqual(spots[0]["missing_min"], 60.0)
        self.assertEqual(spots[0]["failed_attempt_count"], 1)
        summary = source_summaries(self.polls())[0]
        self.assertEqual((summary["gap_count"], summary["failed_count"]), (1, 1))

    def test_daily_coverage_subtracts_gap_time(self) -> None:
        coverage = daily_coverage(self.polls(), timezone="Europe/Helsinki")

        self.assertEqual([row["local_date"] for row in coverage], [date(2026, 5, 2)])
        observed_min = coverage[0]["observed_hours"] * 60
        self.assertAlmostEqual(coverage[0]["missing_min"], 60.0)
        self.assertAlmostEqual(coverage[0]["coverage_pct"], (1 - 60.0 / observed_min) * 100)


if __name__ == "__main__":
    unittest.main()
