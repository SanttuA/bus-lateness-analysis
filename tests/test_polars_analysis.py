from __future__ import annotations

from datetime import date
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import polars as pl

from analysis.polars._shared import (
    add_quality_flags,
    aggregate_delay_buckets,
    apply_quality_filter,
    summarize_delay_metrics,
)
from analysis.polars.report_cache import (
    ReportSettings as PolarsReportSettings,
    build_hourly_delay_profile,
    build_line_rankings,
    ensure_report_cache as ensure_polars_report_cache,
    enrich_stops,
    matched_control_rows,
    read_result_table as read_polars_result_table,
)
from analysis.report_cache import (
    ensure_report_cache as ensure_duckdb_report_cache,
    read_result_table as read_duckdb_result_table,
)
from tests.test_results_report import create_report_db, report_settings


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def quality_sample() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "id": [1, 2, 3, 4, 5, 6],
            "poll_id": [1, 1, 1, 1, 1, 1],
            "vehicle_id": ["v1", "v1", "v1", "v1", "v1", "v1"],
            "recorded_at_utc": [
                "2026-04-23T08:00:00Z",
                "2026-04-23T08:00:00Z",
                "2026-04-23T08:00:00Z",
                "2026-04-23T07:30:00Z",
                "2026-04-23T11:00:00Z",
                "2026-04-23T08:00:00Z",
            ],
            "valid_until_utc": [
                "2026-04-23T08:01:00Z",
                "2026-04-23T08:01:00Z",
                "2026-04-23T08:01:00Z",
                "2026-04-23T08:01:00Z",
                "2026-04-23T11:01:00Z",
                "2026-04-23T08:01:00Z",
            ],
            "collected_at_utc": [
                "2026-04-23T08:00:30Z",
                "2026-04-23T08:00:30Z",
                "2026-04-23T08:10:30Z",
                "2026-04-23T07:30:30Z",
                "2026-04-23T11:00:30Z",
                "2026-04-23T08:00:30Z",
            ],
            "line_ref": ["3"] * 6,
            "direction_ref": ["1"] * 6,
            "origin_aimed_departure_time_utc": [
                "2026-04-23T07:50:00Z",
                "2026-04-23T07:50:00Z",
                "2026-04-23T07:50:00Z",
                "2026-04-23T08:00:00Z",
                "2026-04-23T07:50:00Z",
                "2026-04-23T07:50:00Z",
            ],
            "destination_aimed_arrival_time_utc": [
                "2026-04-23T09:00:00Z",
                "2026-04-23T09:00:00Z",
                "2026-04-23T09:00:00Z",
                "2026-04-23T09:00:00Z",
                "2026-04-23T09:00:00Z",
                "2026-04-23T09:00:00Z",
            ],
            "trip_match_key": ["3|1|a"] * 6,
            "published_line_name": ["3"] * 6,
            "delay_seconds": [60, 8_000, 30, 20, 40, 60],
            "next_stop_point_ref": ["10"] * 6,
            "next_stop_point_name": ["Stop"] * 6,
            "next_aimed_arrival_time_utc": ["2026-04-23T08:05:00Z"] * 6,
            "next_expected_arrival_time_utc": [
                "2026-04-23T08:06:00Z",
                "2026-04-23T08:06:00Z",
                "2026-04-23T08:05:30Z",
                "2026-04-23T08:05:20Z",
                "2026-04-23T08:05:40Z",
                "2026-04-23T08:20:00Z",
            ],
            "next_aimed_departure_time_utc": [None] * 6,
            "next_expected_departure_time_utc": [None] * 6,
            "created_at_utc": [
                "2026-04-23T08:00:30Z",
                "2026-04-23T08:00:30Z",
                "2026-04-23T08:10:30Z",
                "2026-04-23T07:30:30Z",
                "2026-04-23T11:00:30Z",
                "2026-04-23T08:00:30Z",
            ],
        }
    )


class PolarsSharedAnalyticsTests(unittest.TestCase):
    def test_quality_flags_and_conservative_filter_match_expected_rows(self) -> None:
        flagged = add_quality_flags(quality_sample())

        self.assertFalse(flagged["is_implausible_delay"][0])
        self.assertTrue(flagged["is_implausible_delay"][1])
        self.assertTrue(flagged["is_stale_observation"][2])
        self.assertTrue(flagged["is_pre_trip_observation"][3])
        self.assertTrue(flagged["is_post_trip_observation"][4])
        self.assertTrue(flagged["has_stop_call_disagreement"][5])

        filtered = apply_quality_filter(quality_sample(), quality_mode="conservative")
        self.assertEqual(filtered["id"].to_list(), [1, 6])

    def test_trip_stop_bucket_and_metrics_use_linear_quantiles(self) -> None:
        rows = quality_sample().head(3).with_columns(
            pl.Series("delay_seconds", [60, 120, 180])
        )
        buckets = aggregate_delay_buckets(rows, bucket="trip-stop")

        self.assertEqual(buckets.height, 1)
        self.assertEqual(buckets["raw_poll_count"][0], 3)
        self.assertEqual(buckets["delay_seconds"][0], 120)
        self.assertEqual(buckets["local_hour"][0], 11)

        metrics = summarize_delay_metrics(
            pl.DataFrame(
                {
                    "line_ref": ["3"] * 5,
                    "delay_seconds": [-240, -120, 0, 240, 600],
                    "raw_poll_count": [1, 2, 1, 3, 4],
                }
            ),
            ["line_ref"],
        )
        row = metrics.row(0, named=True)
        self.assertAlmostEqual(row["p90_delay_min"], 7.6)
        self.assertAlmostEqual(row["median_early_min_abs"], 3.0)

    def test_enrich_stops_uses_gtfs_feed_date(self) -> None:
        rows = pl.DataFrame(
            {
                "local_date": [date(2026, 4, 22), date(2026, 4, 23), date(2026, 4, 30)],
                "next_stop_point_ref": ["10", "10", "10"],
                "next_stop_point_name": ["Fallback before", "Fallback first", "Fallback second"],
            }
        )
        stops = pl.DataFrame(
            {
                "gtfs_feed_date": [date(2026, 4, 23), date(2026, 4, 30)],
                "stop_id": ["10", "10"],
                "gtfs_stop_name": ["First feed", "Second feed"],
                "stop_lat": [60.45, 60.46],
                "stop_lon": [22.27, 22.28],
            }
        )

        enriched = enrich_stops(rows, stops, pl.DataFrame({"stop_id": [], "city_part": []}))

        self.assertIsNone(enriched["gtfs_feed_date"][0])
        self.assertEqual(enriched["stop_name"].to_list(), ["Fallback before", "First feed", "Second feed"])

    def test_alert_controls_match_active_context(self) -> None:
        rows = pl.DataFrame(
            {
                "line_ref": ["3", "3", "3"],
                "direction_ref": ["1", "1", "2"],
                "local_hour": [8, 8, 8],
                "day_type": ["weekday", "weekday", "weekday"],
                "delay_seconds": [60, 120, 180],
            }
        )
        active, controls = matched_control_rows(rows, pl.Series([True, False, False]))

        self.assertEqual(active["delay_seconds"].to_list(), [60])
        self.assertEqual(controls["delay_seconds"].to_list(), [120])


class PolarsReportParityTests(unittest.TestCase):
    def test_polars_report_tables_match_duckdb_fixture_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            db_path = temp / "foli.db"
            create_report_db(db_path)

            duckdb_cache = ensure_duckdb_report_cache(
                report_settings(db_path, temp / "duckdb-cache"),
                force=True,
            )
            polars_settings = PolarsReportSettings(
                db=db_path,
                cache_dir=temp / "polars-cache",
                min_observations=1,
                limit=5,
            )
            ensure_polars_report_cache(polars_settings, force=True)

            duck_quality = read_duckdb_result_table(
                duckdb_cache.cache_db,
                "quality_summary",
            )
            polars_quality = read_polars_result_table(
                polars_settings.cache_dir,
                "quality_summary",
            )
            self.assertEqual(
                dict(zip(duck_quality["quality_check"], duck_quality["row_count"], strict=True)),
                dict(zip(polars_quality["quality_check"], polars_quality["row_count"], strict=True)),
            )

            duck_lines = read_duckdb_result_table(duckdb_cache.cache_db, "line_late_rankings")
            polars_lines = read_polars_result_table(polars_settings.cache_dir, "line_late_rankings")
            self.assertEqual(duck_lines["line_ref"].tolist(), polars_lines["line_ref"].to_list())
            self.assertAlmostEqual(duck_lines.loc[0, "p90_delay_min"], polars_lines["p90_delay_min"][0])

            polars_buckets = read_polars_result_table(polars_settings.cache_dir, "delay_buckets")
            hourly = build_hourly_delay_profile(polars_buckets, min_observations=1, limit=5)
            self.assertFalse(hourly.is_empty())

            stop_change = read_polars_result_table(polars_settings.cache_dir, "stop_midpoint_change")
            stop_10 = stop_change.filter(pl.col("stop_id") == "10").row(0, named=True)
            self.assertAlmostEqual(stop_10["p90_delay_change_min"], 8.0)

    def test_polars_report_and_line_ranking_cli_smoke(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            db_path = temp / "foli.db"
            cache_dir = temp / "cache"
            report_path = temp / "overall-results-polars.md"
            output_csv = temp / "line-rankings.csv"
            create_report_db(db_path)

            completed = subprocess.run(
                [
                    sys.executable,
                    "analysis/polars/build-results-report.py",
                    "--db",
                    str(db_path),
                    "--cache-dir",
                    str(cache_dir),
                    "--output",
                    str(report_path),
                    "--min-observations",
                    "1",
                    "--limit",
                    "5",
                ],
                cwd=PROJECT_ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("Wrote report:", completed.stdout)
            self.assertTrue(report_path.exists())
            self.assertIn("# Overall Bus Lateness Results (Polars)", report_path.read_text())
            self.assertTrue((cache_dir / "manifest.json").exists())
            self.assertTrue((cache_dir / "quality_rows.parquet").exists())
            self.assertTrue((cache_dir / "line_late_rankings.csv").exists())

            completed = subprocess.run(
                [
                    sys.executable,
                    "analysis/polars/line-delay-rankings.py",
                    "--db",
                    str(db_path),
                    "--cache-dir",
                    str(cache_dir),
                    "--output-csv",
                    str(output_csv),
                    "--min-observations",
                    "1",
                    "--limit",
                    "5",
                    "--ranking",
                    "late",
                ],
                cwd=PROJECT_ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("Most late lines", completed.stdout)
            self.assertTrue(output_csv.read_text().splitlines()[0].startswith("ranking,line_ref,line_name"))

    def test_polars_report_handles_missing_optional_ingestion_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            db_path = temp / "foli.db"
            create_report_db(db_path)
            with sqlite3.connect(db_path) as con:
                con.execute("DROP TABLE collector_polls")
                con.execute("DROP TABLE service_alerts")

            settings = PolarsReportSettings(
                db=db_path,
                cache_dir=temp / "polars-cache",
                min_observations=1,
                limit=5,
            )
            result = ensure_polars_report_cache(settings, force=True)

            self.assertEqual(result.status, "rebuilt")
            self.assertTrue((settings.cache_dir / "quality_rows.parquet").exists())
            self.assertTrue(read_polars_result_table(settings.cache_dir, "collector_blackouts").is_empty())
            self.assertTrue(read_polars_result_table(settings.cache_dir, "service_alert_grouped").is_empty())

    def test_polars_report_cache_invalidates_when_explicit_gtfs_dir_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            db_path = temp / "foli.db"
            gtfs_dir = temp / "gtfs"
            gtfs_dir.mkdir()
            (gtfs_dir / "stops.txt").write_text(
                "stop_id,stop_name,stop_lat,stop_lon\n"
                "10,Market,60.45,22.27\n",
                encoding="utf-8",
            )
            (gtfs_dir / "routes.txt").write_text(
                "route_id,route_short_name\n"
                "route-a,3\n",
                encoding="utf-8",
            )
            create_report_db(db_path)
            settings = PolarsReportSettings(
                db=db_path,
                cache_dir=temp / "polars-cache",
                gtfs_dir=gtfs_dir,
                min_observations=1,
                limit=5,
            )

            first = ensure_polars_report_cache(settings)
            time.sleep(0.01)
            (gtfs_dir / "stops.txt").write_text(
                "stop_id,stop_name,stop_lat,stop_lon\n"
                "10,Corrected Market,60.45,22.27\n",
                encoding="utf-8",
            )
            second = ensure_polars_report_cache(settings)

            self.assertEqual(first.status, "rebuilt")
            self.assertEqual(second.status, "rebuilt")


if __name__ == "__main__":
    unittest.main()
