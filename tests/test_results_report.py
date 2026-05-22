from __future__ import annotations

import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

from analysis.cached_queries import line_rankings as cached_line_rankings
from analysis.report_cache import (
    ReportSettings,
    ensure_analysis_cache,
    ensure_report_cache,
    write_markdown_report,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class CachedArgs:
    def __init__(
        self,
        db: Path,
        cache_dir: Path,
        *,
        limit: int,
        min_observations: int,
    ) -> None:
        self.db = db
        self.cache_dir = cache_dir
        self.limit = limit
        self.min_observations = min_observations
        self.quality_mode = "conservative"
        self.bucket = "trip-stop"
        self.timezone = "Europe/Helsinki"
        self.exclude_stop_call_disagreement = False
        self.force_cache = False
        self.rush_window = None
        self.include_weekends = False
        self.gtfs_dir = None


class ResultsReportCacheTests(unittest.TestCase):
    def test_cache_builds_trip_stop_buckets_and_quality_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "foli.db"
            create_report_db(db_path)
            settings = report_settings(db_path, Path(temp_dir) / "cache")

            result = ensure_report_cache(settings, force=True)

            with duckdb.connect(str(result.cache_db), read_only=True) as con:
                quality = con.execute(
                    """
                    SELECT row_count
                    FROM quality_summary
                    WHERE quality_check = 'is_implausible_delay'
                    """
                ).fetchone()[0]
                bucket = con.execute(
                    """
                    SELECT delay_seconds, raw_poll_count
                    FROM delay_buckets
                    WHERE line_ref = '3'
                        AND next_stop_point_ref = '10'
                        AND representative_time_utc < '2026-04-26T00:00:00Z'
                    """
                ).fetchone()
                excluded = con.execute(
                    "SELECT COUNT(*) FROM delay_buckets WHERE delay_seconds > 7200"
                ).fetchone()[0]

            self.assertEqual(quality, 1)
            self.assertEqual(bucket, (120.0, 2))
            self.assertEqual(excluded, 0)

    def test_midpoint_change_uses_matched_stop_contexts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "foli.db"
            create_report_db(db_path)
            settings = report_settings(db_path, Path(temp_dir) / "cache")

            result = ensure_report_cache(settings, force=True)

            with duckdb.connect(str(result.cache_db), read_only=True) as con:
                row = con.execute(
                    """
                    SELECT
                        stop_id,
                        baseline_p90_delay_min,
                        comparison_p90_delay_min,
                        p90_delay_change_min
                    FROM stop_midpoint_change
                    WHERE stop_id = '10'
                    """
                ).fetchone()

            self.assertEqual(row[0], "10")
            self.assertAlmostEqual(row[1], 2.0)
            self.assertAlmostEqual(row[2], 10.0)
            self.assertAlmostEqual(row[3], 8.0)

    def test_cache_builds_collector_coverage_tables(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "foli.db"
            cache_dir = Path(temp_dir) / "cache"
            create_report_db(db_path)
            settings = report_settings(db_path, cache_dir)

            result = ensure_report_cache(settings, force=True)

            with duckdb.connect(str(result.cache_db), read_only=True) as con:
                blackout = con.execute(
                    """
                    SELECT source, blackout_count
                    FROM collector_blackouts
                    WHERE source = 'siri_vm'
                    """
                ).fetchone()
                missing_spots = con.execute(
                    """
                    SELECT missing_spot_count
                    FROM collector_missing_summary
                    WHERE source = 'siri_vm'
                    """
                ).fetchone()[0]

            csv_header = (cache_dir / "collector_blackouts.csv").read_text().splitlines()[0]

            self.assertEqual(blackout, ("siri_vm", 1))
            self.assertEqual(missing_spots, 1)
            self.assertIn("source", csv_header)
            self.assertIn("blackout_count", csv_header)
            self.assertNotEqual(csv_header, "note")

    def test_cache_reuses_matching_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "foli.db"
            create_report_db(db_path)
            settings = report_settings(db_path, Path(temp_dir) / "cache")

            first = ensure_report_cache(settings)
            second = ensure_report_cache(settings)

            self.assertEqual(first.status, "rebuilt")
            self.assertEqual(second.status, "reused")

    def test_base_cache_reuse_ignores_result_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "foli.db"
            cache_dir = Path(temp_dir) / "cache"
            create_report_db(db_path)

            first = ensure_analysis_cache(
                ReportSettings(
                    db=db_path,
                    cache_dir=cache_dir,
                    limit=5,
                    min_observations=1,
                ),
                force=True,
            )
            second = ensure_analysis_cache(
                ReportSettings(
                    db=db_path,
                    cache_dir=cache_dir,
                    limit=2,
                    min_observations=1,
                )
            )

            self.assertEqual(first.status, "rebuilt")
            self.assertEqual(second.status, "reused")

    def test_cached_line_rankings_match_report_fixture(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "foli.db"
            cache_dir = Path(temp_dir) / "cache"
            create_report_db(db_path)

            result = cached_line_rankings(
                CachedArgs(db_path, cache_dir, limit=5, min_observations=1),
                "late",
            )

            self.assertEqual(result["line_ref"].to_list(), ["3", "4"])
            self.assertEqual(result.loc[0, "bucket_count"], 2)
            self.assertEqual(result.loc[0, "raw_poll_count"], 3)
            self.assertAlmostEqual(result.loc[0, "median_delay_min"], 6.0)
            self.assertAlmostEqual(result.loc[0, "p90_delay_min"], 9.2)

    def test_cli_smoke_writes_report_and_compact_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "foli.db"
            cache_dir = Path(temp_dir) / "cache"
            report_path = Path(temp_dir) / "overall-results.md"
            create_report_db(db_path)

            completed = subprocess.run(
                [
                    sys.executable,
                    "analysis/build-results-report.py",
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
            self.assertIn("# Overall Bus Lateness Results", report_path.read_text())
            self.assertTrue((cache_dir / "manifest.json").exists())
            self.assertTrue((cache_dir / "line_late_rankings.csv").exists())

    def test_line_ranking_cli_uses_cache_and_writes_csv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "foli.db"
            cache_dir = Path(temp_dir) / "cache"
            output_csv = Path(temp_dir) / "line-rankings.csv"
            create_report_db(db_path)

            completed = subprocess.run(
                [
                    sys.executable,
                    "analysis/line-delay-rankings.py",
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
            self.assertTrue((cache_dir / "manifest.json").exists())
            csv_header = output_csv.read_text().splitlines()[0]
            self.assertTrue(csv_header.startswith("ranking,line_ref,line_name"))

    def test_report_renderer_includes_cache_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "foli.db"
            cache_dir = Path(temp_dir) / "cache"
            report_path = Path(temp_dir) / "report.md"
            create_report_db(db_path)
            settings = report_settings(db_path, cache_dir)
            result = ensure_report_cache(settings, force=True)

            written = write_markdown_report(settings, result, report_path)
            text = written.read_text()

            self.assertIn("Cache status: rebuilt", text)
            self.assertIn("## Stop-Level Midpoint Changes", text)
            self.assertIn("Cached CSV:", text)


def report_settings(db_path: Path, cache_dir: Path) -> ReportSettings:
    return ReportSettings(
        db=db_path,
        cache_dir=cache_dir,
        limit=5,
        min_observations=1,
    )


def create_report_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as con:
        con.executescript(
            """
            CREATE TABLE collector_polls (
                id INTEGER PRIMARY KEY,
                source TEXT,
                attempted_at_utc TEXT,
                collected_at_utc TEXT,
                status TEXT,
                ok INTEGER,
                row_count INTEGER,
                gap_seconds_since_previous_success INTEGER
            );

            CREATE TABLE vehicle_observations (
                id INTEGER PRIMARY KEY,
                poll_id INTEGER,
                vehicle_id TEXT,
                recorded_at_utc TEXT,
                valid_until_utc TEXT,
                line_ref TEXT,
                direction_ref TEXT,
                origin_aimed_departure_time_utc TEXT,
                trip_match_key TEXT,
                is_gtfs_matchable INTEGER,
                published_line_name TEXT,
                delay_seconds INTEGER,
                next_stop_point_ref TEXT,
                next_stop_point_name TEXT,
                next_aimed_arrival_time_utc TEXT,
                next_expected_arrival_time_utc TEXT,
                next_aimed_departure_time_utc TEXT,
                next_expected_departure_time_utc TEXT,
                destination_aimed_arrival_time_utc TEXT,
                created_at_utc TEXT
            );

            CREATE TABLE service_alerts (
                source_alert_id TEXT,
                line_ref TEXT,
                cause TEXT,
                effect TEXT,
                priority INTEGER,
                is_active INTEGER,
                validity_start_utc TEXT,
                validity_end_utc TEXT,
                affected_routes_json TEXT,
                affected_stops_json TEXT,
                created_at_utc TEXT
            );
            """
        )
        polls = [
            (1, "siri_vm", "2026-04-23T08:00:00Z", "2026-04-23T08:00:30Z", "ok", 1, 2, None),
            (2, "siri_vm", "2026-04-23T08:00:30Z", "2026-04-23T08:01:00Z", "ok", 1, 2, 30),
            (3, "siri_vm", "2026-04-30T08:00:00Z", "2026-04-30T08:00:30Z", "ok", 1, 1, 604770),
            (4, "siri_vm", "2026-04-30T08:10:00Z", "2026-04-30T08:10:30Z", "ok", 1, 1, 600),
        ]
        con.executemany(
            """
            INSERT INTO collector_polls (
                id,
                source,
                attempted_at_utc,
                collected_at_utc,
                status,
                ok,
                row_count,
                gap_seconds_since_previous_success
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            polls,
        )

        rows = [
            observation_row(
                1,
                1,
                "v1",
                "2026-04-23T08:00:00Z",
                "trip-baseline",
                60,
                "10",
                "Market",
            ),
            observation_row(
                2,
                2,
                "v1",
                "2026-04-23T08:00:30Z",
                "trip-baseline",
                180,
                "10",
                "Market",
            ),
            observation_row(
                3,
                3,
                "v2",
                "2026-04-30T08:00:00Z",
                "trip-comparison",
                600,
                "10",
                "Market",
            ),
            observation_row(
                4,
                4,
                "v3",
                "2026-04-30T08:10:00Z",
                "trip-early",
                -240,
                "20",
                "Harbor",
                line_ref="4",
                direction_ref="2",
            ),
            observation_row(
                5,
                4,
                "v4",
                "2026-04-30T08:15:00Z",
                "trip-implausible",
                8000,
                "30",
                "Airport",
            ),
        ]
        con.executemany(
            """
            INSERT INTO vehicle_observations (
                id,
                poll_id,
                vehicle_id,
                recorded_at_utc,
                valid_until_utc,
                line_ref,
                direction_ref,
                origin_aimed_departure_time_utc,
                trip_match_key,
                is_gtfs_matchable,
                published_line_name,
                delay_seconds,
                next_stop_point_ref,
                next_stop_point_name,
                next_aimed_arrival_time_utc,
                next_expected_arrival_time_utc,
                next_aimed_departure_time_utc,
                next_expected_departure_time_utc,
                destination_aimed_arrival_time_utc,
                created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        con.execute(
            """
            INSERT INTO service_alerts (
                source_alert_id,
                line_ref,
                cause,
                effect,
                priority,
                is_active,
                validity_start_utc,
                validity_end_utc,
                affected_routes_json,
                affected_stops_json,
                created_at_utc
            )
            VALUES (
                'alert-1',
                '3',
                'construction',
                'delay',
                1,
                1,
                '2026-04-30T07:00:00Z',
                '2026-04-30T09:00:00Z',
                '[]',
                '[]',
                '2026-04-30T07:00:00Z'
            )
            """
        )


def observation_row(
    row_id: int,
    poll_id: int,
    vehicle_id: str,
    recorded_at_utc: str,
    trip_match_key: str,
    delay_seconds: int,
    stop_id: str,
    stop_name: str,
    *,
    line_ref: str = "3",
    direction_ref: str = "1",
) -> tuple[object, ...]:
    aimed = recorded_at_utc
    expected = timestamp_plus_seconds(recorded_at_utc, delay_seconds)
    return (
        row_id,
        poll_id,
        vehicle_id,
        recorded_at_utc,
        timestamp_plus_seconds(recorded_at_utc, 90),
        line_ref,
        direction_ref,
        timestamp_plus_seconds(recorded_at_utc, -300),
        trip_match_key,
        1,
        line_ref,
        delay_seconds,
        stop_id,
        stop_name,
        aimed,
        expected,
        None,
        None,
        timestamp_plus_seconds(recorded_at_utc, 3600),
        timestamp_plus_seconds(recorded_at_utc, 30),
    )


def timestamp_plus_seconds(value: str, seconds: int) -> str:
    return (
        pd_timestamp(value)
        + seconds_to_timedelta(seconds)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")


def pd_timestamp(value: str):
    import pandas as pd

    return pd.Timestamp(value).tz_convert("UTC")


def seconds_to_timedelta(seconds: int):
    import pandas as pd

    return pd.Timedelta(seconds=seconds)


if __name__ == "__main__":
    unittest.main()
