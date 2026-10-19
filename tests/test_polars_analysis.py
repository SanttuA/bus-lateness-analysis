from __future__ import annotations

from datetime import date
import importlib.util
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import polars as pl
import nbformat

from analysis.polars._shared import (
    QUALITY_FLAG_COLUMNS,
    add_quality_flags,
    aggregate_delay_buckets,
    apply_quality_filter,
    sort_robust_delay_metrics,
    summarize_delay_metrics,
)
from analysis.polars.report_cache import (
    RESULT_TABLES,
    ReportSettings as PolarsReportSettings,
    build_hourly_delay_profile,
    build_line_rankings,
    build_quality_summary_lazy,
    ensure_report_cache as ensure_polars_report_cache,
    enrich_stops,
    read_result_table as read_polars_result_table,
)
from analysis.report_cache import (
    ensure_report_cache as ensure_duckdb_report_cache,
    read_result_table as read_duckdb_result_table,
)
from tests.test_results_report import create_report_db, report_settings


PROJECT_ROOT = Path(__file__).resolve().parents[1]
POLARS_NOTEBOOK_NAMES = [
    "01_hourly_delay_profile.ipynb",
    "02_line_delay_rankings.ipynb",
    "03_rush_impact.ipynb",
    "04_collector_blackouts.ipynb",
    "05_stop_delay_change.ipynb",
    "06_service_alert_delay_correlation.ipynb",
    "07_collector_missing_data_spots.ipynb",
    "08_data_quality_report.ipynb",
    "09_context_delay_metrics.ipynb",
]


def load_polars_script_module(name: str, relative_path: str):
    analysis_path = str(PROJECT_ROOT / "analysis" / "polars")
    previous_path = sys.path.copy()
    top_level_modules = ("_shared", "report_cache", "cli_common")
    missing = object()
    previous_modules = {
        module_name: sys.modules.get(module_name, missing)
        for module_name in top_level_modules
    }
    try:
        sys.path = [path for path in sys.path if path != analysis_path]
        sys.path.insert(0, analysis_path)
        for module_name in top_level_modules:
            sys.modules.pop(module_name, None)
        spec = importlib.util.spec_from_file_location(name, PROJECT_ROOT / relative_path)
        module = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(module)
        return module
    finally:
        for module_name in top_level_modules:
            sys.modules.pop(module_name, None)
            previous = previous_modules[module_name]
            if previous is not missing:
                sys.modules[module_name] = previous
        sys.path = previous_path


def normalized_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    """Engine-neutral comparison form: rounded floats, ISO dates, None for missing."""
    import math

    result = []
    for row in rows:
        normalized = {}
        for key, value in row.items():
            if key == "note":
                continue
            if hasattr(value, "item") and not isinstance(value, str):
                value = value.item()
            if isinstance(value, float):
                value = None if math.isnan(value) else round(value, 2)
            elif hasattr(value, "isoformat"):
                value = value.isoformat()[:10] if key == "local_date" else value.isoformat()
            elif value is not None and type(value).__name__ in {"NAType", "NaTType"}:
                value = None
            if isinstance(value, bool):
                value = bool(value)
            elif isinstance(value, int):
                value = float(value)
            normalized[key] = value
        if normalized:
            result.append(normalized)
    return result


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

    def test_origin_layover_and_no_realtime_trips_are_excluded(self) -> None:
        base = quality_sample().head(1).with_columns(pl.lit("1").alias("origin_ref"))
        layover = base.with_columns(
            pl.lit(0).alias("delay_seconds"),
            pl.lit("10").alias("origin_ref"),
            pl.lit("2026-04-23T07:45:00Z").alias("recorded_at_utc"),
            pl.lit("2026-04-23T07:45:30Z").alias("collected_at_utc"),
            pl.lit("2026-04-23T07:46:00Z").alias("valid_until_utc"),
        )
        silent_trip = pl.concat(
            [
                base.with_columns(
                    pl.lit(100 + index).alias("id"),
                    pl.lit(0).alias("delay_seconds"),
                    pl.lit("silent").alias("trip_match_key"),
                    pl.lit(str(index)).alias("next_stop_point_ref"),
                    pl.lit("1").alias("origin_ref"),
                )
                for index in range(10)
            ]
        )
        rows = pl.concat([base, layover, silent_trip], how="vertical_relaxed")

        flagged = add_quality_flags(rows)
        kept = apply_quality_filter(rows)

        self.assertEqual(flagged["is_origin_layover"].to_list()[:2], [False, True])
        self.assertTrue(all(flagged["is_no_realtime_trip"].to_list()[2:]))
        self.assertEqual(kept["id"].to_list(), [1])

    def test_reduced_service_holiday_counts_as_weekend(self) -> None:
        rows = quality_sample().head(1).with_columns(
            pl.lit("2026-05-01T08:05:00Z").alias("next_aimed_arrival_time_utc")
        )
        normal = aggregate_delay_buckets(rows, bucket="trip-stop")
        holiday = aggregate_delay_buckets(rows, bucket="trip-stop", holidays=("2026-05-01",))

        self.assertEqual(normal["day_type"][0], "weekday")
        self.assertEqual(holiday["day_type"][0], "weekend")
        self.assertTrue(holiday["is_holiday"][0])
        self.assertEqual(holiday["local_weekday"][0], 4)

    def test_hourly_profile_keeps_every_hour_in_order(self) -> None:
        buckets = pl.DataFrame(
            {
                "local_hour": list(range(24)),
                "delay_seconds": [float(hour * 10) for hour in range(24)],
                "raw_poll_count": [1] * 24,
                "trip_match_key": [f"t{hour}" for hour in range(24)],
                "line_ref": ["3"] * 24,
            }
        )
        profile = build_hourly_delay_profile(buckets, min_observations=1)

        self.assertEqual(profile["hour_local"].to_list(), [f"{hour:02d}:00" for hour in range(24)])

    def test_ranking_ties_favour_larger_samples(self) -> None:
        metrics = pl.DataFrame(
            {
                "line_ref": ["small", "large"],
                "p90_delay_min": [1.0, 1.0],
                "pct_over_5_min_late": [0.0, 0.0],
                "bucket_count": [1, 5],
            }
        )
        self.assertEqual(sort_robust_delay_metrics(metrics)["line_ref"].to_list(), ["large", "small"])

    def test_lazy_quality_summary_is_empty_for_no_analysis_rows(self) -> None:
        summary = build_quality_summary_lazy(
            pl.DataFrame(
                schema={
                    "line_ref": pl.Utf8,
                    "published_line_name": pl.Utf8,
                    "is_implausible_delay": pl.Boolean,
                    "is_stale_observation": pl.Boolean,
                    "is_pre_trip_observation": pl.Boolean,
                    "is_post_trip_observation": pl.Boolean,
                    **{column: pl.Boolean for column in QUALITY_FLAG_COLUMNS},
                }
            ).lazy()
        ).collect()

        self.assertTrue(summary.is_empty())
        self.assertEqual(summary.columns, ["quality_check", "row_count", "pct_rows"])


class PolarsReportParityTests(unittest.TestCase):
    def test_polars_notebooks_are_present_valid_and_clear(self) -> None:
        notebook_dir = PROJECT_ROOT / "notebooks" / "polars"
        notebooks = sorted(notebook_dir.glob("*.ipynb"))

        self.assertEqual([path.name for path in notebooks], POLARS_NOTEBOOK_NAMES)

        for path in notebooks:
            with self.subTest(notebook=path.name):
                notebook = nbformat.read(path, as_version=4)
                nbformat.validate(notebook)
                source = "\n".join(
                    "".join(cell.get("source", ""))
                    for cell in notebook.cells
                ).lower()
                self.assertIn("analysis\" / \"polars", source)
                self.assertIn("plotly.express", source)
                self.assertNotIn("import pandas", source)
                for cell in notebook.cells:
                    if cell.cell_type != "code":
                        continue
                    self.assertIsNone(cell.execution_count)
                    self.assertEqual(cell.outputs, [])

    def test_stop_change_default_recent_periods_use_latest_qualifying_stop_observation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "foli.db"
            create_report_db(db_path)
            stop_delay_change = load_polars_script_module(
                "polars_stop_delay_change_test",
                "analysis/polars/stop-delay-change.py",
            )

            periods = stop_delay_change.default_recent_periods(
                db_path,
                timezone="Europe/Helsinki",
                period_days=1,
            )

            self.assertEqual(
                periods,
                (
                    "2026-04-28T11:15:01+03:00",
                    "2026-04-29T11:15:01+03:00",
                    "2026-04-29T11:15:01+03:00",
                    "2026-04-30T11:15:01+03:00",
                ),
            )

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
                min_trips=1,
                limit=5,
            )
            ensure_polars_report_cache(polars_settings, force=True)

            for table in RESULT_TABLES:
                with self.subTest(table=table):
                    duck = normalized_rows(
                        read_duckdb_result_table(duckdb_cache.cache_db, table).to_dict("records")
                    )
                    polars_rows = normalized_rows(
                        read_polars_result_table(polars_settings.cache_dir, table).to_dicts()
                    )
                    self.assertEqual(duck, polars_rows)

            polars_buckets = read_polars_result_table(polars_settings.cache_dir, "delay_buckets")
            hourly = build_hourly_delay_profile(polars_buckets, min_observations=1, limit=5)
            self.assertFalse(hourly.is_empty())
            cancellations = read_polars_result_table(polars_settings.cache_dir, "service_cancellations")
            self.assertEqual(cancellations["line_ref"].to_list(), ["4"])

    def test_polars_report_cache_reuses_matching_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            db_path = temp / "foli.db"
            create_report_db(db_path)
            settings = PolarsReportSettings(
                db=db_path,
                cache_dir=temp / "polars-cache",
                min_observations=1,
                limit=5,
            )

            first = ensure_polars_report_cache(settings)
            second = ensure_polars_report_cache(settings)

            self.assertEqual(first.status, "rebuilt")
            self.assertEqual(second.status, "reused")
            self.assertIn("cache_build_seconds", second.timings)

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
                    "--min-trips",
                    "1",
                    "--limit",
                    "5",
                ],
                cwd=PROJECT_ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("[report] Checking report cache", completed.stdout)
            self.assertIn("[report] Building result tables", completed.stdout)
            self.assertIn("[report] Rendering Markdown report", completed.stdout)
            self.assertRegex(completed.stdout, r"\[report\] Finished in \d+\.\d{2}s")
            self.assertIn("Wrote report:", completed.stdout)
            self.assertTrue(report_path.exists())
            report_text = report_path.read_text()
            self.assertIn("# Overall Bus Lateness Results (Polars)", report_text)
            self.assertIn("## Run Timing", report_text)
            self.assertRegex(report_text, r"- Cache/build: \d+\.\d{2}s")
            self.assertRegex(report_text, r"- Report render: \d+\.\d{2}s")
            self.assertRegex(report_text, r"- Total report run: \d+\.\d{2}s")
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
                    "--min-trips",
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
