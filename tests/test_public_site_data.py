from __future__ import annotations

import importlib.util
import json
import re
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

import polars as pl


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_generator():
    path = PROJECT_ROOT / "analysis" / "build-public-site-data.py"
    spec = importlib.util.spec_from_file_location("build_public_site_data", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class PublicSiteDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.generator = load_generator()

    def test_canonical_stops_choose_latest_feed_and_one_row_per_id(self) -> None:
        stops = pl.DataFrame(
            {
                "gtfs_feed_date": [date(2026, 4, 23), date(2026, 5, 21)],
                "stop_id": ["10", "10"],
                "gtfs_stop_name": ["Old name", "Current name"],
                "stop_lat": [60.45, 60.46],
                "stop_lon": [22.27, 22.28],
            }
        )
        buckets = pl.DataFrame(
            {
                "next_stop_point_ref": ["10", "10"],
                "next_stop_point_name": ["Fallback", "Fallback"],
                "line_ref": ["3", "4"],
                "representative_time_utc": [
                    "2026-04-23T08:00:00Z",
                    "2026-05-21T08:00:00Z",
                ],
            }
        ).lazy()

        result = self.generator.build_canonical_stops(stops, buckets)

        self.assertEqual(result.height, 1)
        self.assertEqual(result.row(0, named=True)["stop_name"], "Current name")
        self.assertEqual(result.row(0, named=True)["line_count"], 2)

    def test_validate_payload_rejects_raw_identifiers(self) -> None:
        with self.assertRaisesRegex(ValueError, "forbidden public keys"):
            self.generator.validate_public_payload(
                "bad",
                {"schema_version": 2, "rows": [{"vehicle_id": "secret"}]},
            )

    def test_validate_payload_rejects_local_database_paths(self) -> None:
        with self.assertRaisesRegex(ValueError, "local paths"):
            self.generator.validate_public_payload(
                "bad",
                {"schema_version": 2, "source": "data/foli.db"},
            )

    def test_validate_payload_rejects_missing_schema_and_non_finite_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "schema version"):
            self.generator.validate_public_payload("bad", {"rows": []})
        with self.assertRaises(ValueError):
            self.generator.validate_public_payload(
                "bad", {"schema_version": 2, "value": float("nan")}
            )

    def test_json_output_is_stable_and_utf8(self) -> None:
        payload = {"schema_version": 2, "name": "Föli", "rows": [{"b": 2, "a": 1}]}
        with tempfile.TemporaryDirectory() as temp_dir:
            first = Path(temp_dir) / "first.json"
            second = Path(temp_dir) / "second.json"
            self.generator.write_json(first, payload)
            self.generator.write_json(second, payload)

            self.assertEqual(first.read_bytes(), second.read_bytes())
            self.assertEqual(json.loads(first.read_text())["name"], "Föli")

    def test_stop_payload_enforces_minimum_bucket_count(self) -> None:
        buckets = pl.DataFrame(
            {
                "next_stop_point_ref": ["10", "10", "20"],
                "next_stop_point_name": ["A", "A", "B"],
                "line_ref": ["3", "3", "4"],
                "direction_ref": ["1", "1", "1"],
                "published_line_name": ["3", "3", "4"],
                "day_type": ["weekday", "weekday", "weekday"],
                "delay_seconds": [60.0, 120.0, 180.0],
                "raw_poll_count": [1, 1, 1],
                "representative_time_utc": [
                    "2026-04-23T08:00:00Z",
                    "2026-04-23T08:01:00Z",
                    "2026-04-23T08:02:00Z",
                ],
            }
        ).lazy()
        stops = pl.DataFrame(
            {
                "stop_id": ["10", "20"],
                "gtfs_stop_name": ["A", "B"],
                "stop_lat": [60.45, 60.46],
                "stop_lon": [22.27, 22.28],
            }
        )

        payload = self.generator.build_stop_payload(buckets, stops, min_buckets=2)

        self.assertEqual([row["stop_id"] for row in payload["stops"]], ["10"])
        self.assertTrue(all(row["bucket_count"] >= 2 for row in payload["metrics"]))

    def test_requested_minimum_bucket_count_configures_report_cache(self) -> None:
        class CacheProbe(Exception):
            pass

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            with mock.patch.object(
                self.generator,
                "ensure_report_cache",
                side_effect=CacheProbe,
            ) as ensure_cache:
                with self.assertRaises(CacheProbe):
                    self.generator.build_public_data(
                        db=root / "foli.db",
                        cache_dir=root / "cache",
                        gtfs_root=root / "gtfs",
                        output_dir=root / "output",
                        min_buckets=47,
                    )

        settings = ensure_cache.call_args.args[0]
        self.assertEqual(settings.min_observations, 47)

    def test_bilingual_caveats_cover_snapshot_estimate_filter_trips_and_gaps(self) -> None:
        caveats = self.generator._overview_caveats(
            start_date=date(2026, 4, 23),
            end_date=date(2026, 5, 23),
            excluded_pct=17.72,
            placeholder_rows=1_590_276,
        )

        self.assertEqual(
            {row["id"] for row in caveats},
            {"snapshot", "estimated-state", "quality-filter", "trips", "collector-gaps"},
        )
        self.assertTrue(all(row["fi"] and row["en"] for row in caveats))
        quality = next(row for row in caveats if row["id"] == "quality-filter")
        self.assertIn("17.72%", quality["en"])
        self.assertIn("17,72 %", quality["fi"])
        self.assertIn("1,590,276", quality["en"])

    def test_headlines_and_takeaways_follow_the_data(self) -> None:
        lines = [
            {"line_ref": "1", "line_name": "1", "trips_per_day": 120, "low_quality": False,
             "pct_over_5_min_late": 5.0, "p90_delay_min": 3.0, "pct_over_3_min_early": 4.0},
            {"line_ref": "2", "line_name": "2", "trips_per_day": 80, "low_quality": False,
             "pct_over_5_min_late": 9.0, "p90_delay_min": 4.0, "pct_over_3_min_early": 2.0,
             "pct_over_5_min_late_ci_lo": 7.0, "pct_over_5_min_late_ci_hi": 11.0},
            {"line_ref": "612", "line_name": "612", "trips_per_day": 2, "low_quality": False,
             "pct_over_5_min_late": 40.0, "p90_delay_min": 14.0, "pct_over_3_min_early": 30.0},
            {"line_ref": "P6", "line_name": "P6", "trips_per_day": 4, "low_quality": True,
             "pct_over_5_min_late": 1.0, "p90_delay_min": 1.0, "pct_over_3_min_early": 60.0},
        ]
        hourly = [
            {"day_type": "weekday", "local_hour": 8, "bucket_count": 5000, "pct_over_5_min_late": 4.0},
            {"day_type": "weekday", "local_hour": 16, "bucket_count": 5000, "pct_over_5_min_late": 8.0},
        ]
        rush = [{"line_name": "2", "rush_over_5_min_late_pct_point_lift": 6.0,
                 "rush_over_5_min_late_lift_ci_lo": 2.0}]

        headlines = self.generator._overview_headlines(lines, hourly, rush, [], [])
        takeaways = self.generator._overview_takeaways(lines, {"pct_on_time": 66.0}, hourly, rush)

        self.assertTrue(headlines["en"]["ranking"].startswith("Lines 2 and 1"))
        self.assertIn("16:00", headlines["en"]["hourly"])
        self.assertIn("line 2", headlines["en"]["rush"])
        late = next(row for row in takeaways if row["id"] == "late-lines")
        self.assertIn("line 2 is most often", late["en"])
        self.assertIn("line 612", late["en"])
        early = next(row for row in takeaways if row["id"] == "early-lines")
        self.assertIn("Among frequent lines, line 1 runs ahead", early["en"])
        self.assertIn("the most extreme is 612", early["en"])
        self.assertNotIn("P6", early["en"])
        self.assertIn("1 lines with incomplete data", early["en"])
        self.assertIn("are 1 and 2.", headlines["en"]["recommendations"][1])

    def test_checked_in_snapshot_reconciles_authoritative_findings(self) -> None:
        overview = json.loads(
            (PROJECT_ROOT / "site" / "public" / "data" / "overview.json").read_text()
        )
        findings = (PROJECT_ROOT / "FINDINGS.md").read_text()

        self.assertEqual(overview["schema_version"], 2)
        self.assertEqual(overview["summary"]["bucket_count"], 3_729_540)
        self.assertEqual(overview["summary"]["raw_poll_count"], 8_582_662)
        self.assertEqual(overview["summary"]["line_count"], 140)
        self.assertEqual(overview["meta"]["conservative_excluded_pct"], 17.72)
        self.assertEqual(overview["meta"]["holiday_dates"], ["2026-05-01", "2026-05-14"])
        for expected in ("3,729,540", "8,582,662", "140", "17.72%"):
            self.assertIn(expected, findings)

    def test_checked_in_lines_meet_trip_minimum_and_carry_intervals(self) -> None:
        payload = json.loads(
            (PROJECT_ROOT / "site" / "public" / "data" / "lines.json").read_text()
        )
        self.assertTrue(all(row["trip_count"] >= 10 for row in payload["contexts"]))
        self.assertTrue(all(row["trip_count"] >= 10 for row in payload["lines"]))
        self.assertTrue(all("pct_over_5_min_late_ci_lo" in row for row in payload["lines"]))

    def test_checked_in_context_replaces_stop_changes_with_daily_profile(self) -> None:
        payload = json.loads(
            (PROJECT_ROOT / "site" / "public" / "data" / "context.json").read_text()
        )
        self.assertNotIn("stop_changes", payload)
        days = {row["local_date"]: row for row in payload["daily"]}
        self.assertEqual(days["2026-05-01"]["day_type"], "weekend")
        self.assertTrue(days["2026-05-01"]["is_holiday"])
        self.assertTrue(all(row["bucket_count_alert"] < 1_000_000 for row in payload["alerts"]))

    def test_checked_in_stop_markers_are_canonical_and_ordered(self) -> None:
        payload = json.loads(
            (PROJECT_ROOT / "site" / "public" / "data" / "stops.json").read_text()
        )
        stop_ids = [row["stop_id"] for row in payload["stops"]]

        self.assertEqual(stop_ids, sorted(stop_ids))
        self.assertEqual(len(stop_ids), len(set(stop_ids)))
        self.assertTrue(all(row["bucket_count"] >= 30 for row in payload["metrics"]))

    def test_checked_in_payloads_do_not_publish_second_level_timestamps(self) -> None:
        for path in sorted((PROJECT_ROOT / "site" / "public" / "data").glob("*.json")):
            self.assertIsNone(re.search(r"T\d{2}:\d{2}:\d{2}", path.read_text()))


if __name__ == "__main__":
    unittest.main()
