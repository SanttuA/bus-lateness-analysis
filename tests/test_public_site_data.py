from __future__ import annotations

import importlib.util
import json
import re
import tempfile
import unittest
from datetime import date
from pathlib import Path

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
                {"schema_version": 1, "rows": [{"vehicle_id": "secret"}]},
            )

    def test_validate_payload_rejects_local_database_paths(self) -> None:
        with self.assertRaisesRegex(ValueError, "local paths"):
            self.generator.validate_public_payload(
                "bad",
                {"schema_version": 1, "source": "data/foli.db"},
            )

    def test_validate_payload_rejects_missing_schema_and_non_finite_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "schema version"):
            self.generator.validate_public_payload("bad", {"rows": []})
        with self.assertRaises(ValueError):
            self.generator.validate_public_payload(
                "bad", {"schema_version": 1, "value": float("nan")}
            )

    def test_json_output_is_stable_and_utf8(self) -> None:
        payload = {"schema_version": 1, "name": "Föli", "rows": [{"b": 2, "a": 1}]}
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

    def test_bilingual_caveats_cover_snapshot_estimate_filter_and_gaps(self) -> None:
        caveats = self.generator._overview_caveats(
            start_date=date(2026, 4, 23),
            end_date=date(2026, 5, 23),
            excluded_pct=5.69,
        )

        self.assertEqual(
            {row["id"] for row in caveats},
            {"snapshot", "estimated-state", "quality-filter", "collector-gaps"},
        )
        self.assertTrue(all(row["fi"] and row["en"] for row in caveats))
        self.assertIn("5.69%", next(row["en"] for row in caveats if row["id"] == "quality-filter"))

    def test_checked_in_snapshot_reconciles_authoritative_findings(self) -> None:
        overview = json.loads(
            (PROJECT_ROOT / "site" / "public" / "data" / "overview.json").read_text()
        )
        findings = (PROJECT_ROOT / "FINDINGS.md").read_text()

        self.assertEqual(overview["summary"]["bucket_count"], 3_746_770)
        self.assertEqual(overview["summary"]["raw_poll_count"], 9_837_244)
        self.assertEqual(overview["summary"]["line_count"], 140)
        self.assertEqual(overview["meta"]["conservative_excluded_pct"], 5.69)
        for expected in ("3,746,770", "9,837,244", "140", "5.69%"):
            self.assertIn(expected, findings)

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
