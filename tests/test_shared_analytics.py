from __future__ import annotations

from datetime import date
import importlib.util
from pathlib import Path
import sys
import unittest

import pandas as pd

from analysis._shared import (
    aggregate_delay_buckets,
    add_quality_flags,
    apply_quality_filter,
    summarize_delay_metrics,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_script_module(name: str, relative_path: str):
    analysis_path = str(PROJECT_ROOT / "analysis")
    if analysis_path not in sys.path:
        sys.path.insert(0, analysis_path)
    spec = importlib.util.spec_from_file_location(name, PROJECT_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def quality_sample() -> pd.DataFrame:
    return pd.DataFrame(
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
            "next_aimed_departure_time_utc": [pd.NA] * 6,
            "next_expected_departure_time_utc": [pd.NA] * 6,
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


class QualityLayerTests(unittest.TestCase):
    def test_quality_flags_identify_each_issue_type(self) -> None:
        flagged = add_quality_flags(quality_sample())

        self.assertFalse(flagged.loc[0, "is_implausible_delay"])
        self.assertTrue(flagged.loc[1, "is_implausible_delay"])
        self.assertTrue(flagged.loc[2, "is_stale_observation"])
        self.assertTrue(flagged.loc[3, "is_pre_trip_observation"])
        self.assertTrue(flagged.loc[4, "is_post_trip_observation"])
        self.assertTrue(flagged.loc[5, "has_stop_call_disagreement"])

    def test_conservative_filter_keeps_stop_call_disagreement_by_default(self) -> None:
        filtered = apply_quality_filter(quality_sample(), quality_mode="conservative")

        self.assertEqual(filtered["id"].to_list(), [1, 6])

        strict = apply_quality_filter(
            quality_sample(),
            quality_mode="conservative",
            exclude_stop_call_disagreement=True,
        )
        self.assertEqual(strict["id"].to_list(), [1])

    def test_missing_timestamps_do_not_create_quality_failures(self) -> None:
        row = quality_sample().head(1).copy()
        row[[
            "valid_until_utc",
            "collected_at_utc",
            "origin_aimed_departure_time_utc",
            "destination_aimed_arrival_time_utc",
            "next_aimed_arrival_time_utc",
            "next_expected_arrival_time_utc",
        ]] = pd.NA

        flagged = add_quality_flags(row)

        self.assertFalse(flagged.loc[0, "is_stale_observation"])
        self.assertFalse(flagged.loc[0, "is_pre_trip_observation"])
        self.assertFalse(flagged.loc[0, "is_post_trip_observation"])
        self.assertFalse(flagged.loc[0, "has_stop_call_disagreement"])


class BucketAndMetricTests(unittest.TestCase):
    def test_trip_stop_bucket_collapses_repeated_polls(self) -> None:
        rows = quality_sample().head(3).copy()
        rows["delay_seconds"] = [60, 120, 180]
        rows["collected_at_utc"] = rows["created_at_utc"]

        buckets = aggregate_delay_buckets(rows, bucket="trip-stop")

        self.assertEqual(len(buckets), 1)
        self.assertEqual(buckets.loc[0, "raw_poll_count"], 3)
        self.assertEqual(buckets.loc[0, "delay_seconds"], 120)
        self.assertEqual(buckets.loc[0, "local_hour"], 11)

    def test_robust_metrics_include_late_and_early_thresholds(self) -> None:
        df = pd.DataFrame(
            {
                "line_ref": ["3"] * 5,
                "delay_seconds": [-240, -120, 0, 240, 600],
                "raw_poll_count": [1, 2, 1, 3, 4],
            }
        )

        metrics = summarize_delay_metrics(df, ["line_ref"])
        row = metrics.iloc[0]

        self.assertEqual(row["bucket_count"], 5)
        self.assertEqual(row["raw_poll_count"], 11)
        self.assertAlmostEqual(row["median_delay_min"], 0.0)
        self.assertAlmostEqual(row["p90_delay_min"], 7.6)
        self.assertAlmostEqual(row["pct_over_3_min_late"], 40.0)
        self.assertAlmostEqual(row["pct_over_5_min_late"], 20.0)
        self.assertAlmostEqual(row["pct_over_3_min_early"], 20.0)
        self.assertAlmostEqual(row["median_early_min_abs"], 3.0)


class MatchedAnalysisTests(unittest.TestCase):
    def test_stop_change_requires_periods_before_database_load(self) -> None:
        stop_change = load_script_module(
            "stop_delay_change",
            "analysis/stop-delay-change.py",
        )

        class Args:
            db = PROJECT_ROOT / "does-not-exist.db"
            timezone = "Europe/Helsinki"
            line_ref = None
            direction_ref = None
            baseline_start = None
            baseline_end = None
            comparison_start = None
            comparison_end = None
            legacy_midpoint = False

        with self.assertRaises(SystemExit) as raised:
            stop_change.load_observations(Args)

        self.assertIn("requires explicit matched periods", str(raised.exception))

    def test_stop_change_matches_same_line_direction_weekday_and_hour(self) -> None:
        stop_change = load_script_module(
            "stop_delay_change",
            "analysis/stop-delay-change.py",
        )
        rows = pd.DataFrame(
            {
                "period": ["baseline", "comparison", "comparison"],
                "stop_id": ["10", "10", "10"],
                "line_ref": ["3", "3", "3"],
                "direction_ref": ["1", "1", "1"],
                "local_weekday": [0, 0, 1],
                "local_hour": [8, 8, 8],
                "delay_seconds": [60, 120, 180],
            }
        )

        matched = stop_change.matched_context_rows(rows, ["stop_id"])

        self.assertEqual(matched["delay_seconds"].to_list(), [60, 120])

    def test_stop_change_enriches_stops_by_gtfs_feed_date(self) -> None:
        stop_change = load_script_module(
            "stop_delay_change",
            "analysis/stop-delay-change.py",
        )
        rows = pd.DataFrame(
            {
                "local_date": [
                    date(2026, 4, 22),
                    date(2026, 4, 23),
                    date(2026, 4, 30),
                ],
                "next_stop_point_ref": ["10", "10", "10"],
                "next_stop_point_name": ["Fallback before", "Fallback first", "Fallback second"],
            }
        )
        stops = pd.DataFrame(
            {
                "gtfs_feed_date": [date(2026, 4, 23), date(2026, 4, 30)],
                "stop_id": pd.Series(["10", "10"], dtype="string"),
                "gtfs_stop_name": ["First feed", "Second feed"],
                "stop_lat": [60.45, 60.46],
                "stop_lon": [22.27, 22.28],
            }
        )

        enriched = stop_change.enrich_stops(
            rows,
            stops,
            pd.DataFrame(columns=["stop_id", "city_part"]),
        )

        self.assertTrue(pd.isna(enriched.loc[0, "gtfs_feed_date"]))
        self.assertEqual(enriched.loc[0, "stop_name"], "Fallback before")
        self.assertFalse(enriched.loc[0, "has_gtfs_stop_metadata"])
        self.assertEqual(enriched.loc[1, "gtfs_feed_date"], date(2026, 4, 23))
        self.assertEqual(enriched.loc[1, "stop_name"], "First feed")
        self.assertEqual(enriched.loc[2, "gtfs_feed_date"], date(2026, 4, 30))
        self.assertEqual(enriched.loc[2, "stop_name"], "Second feed")

    def test_alert_cli_uses_shared_alert_tables_and_skips_cancellations(self) -> None:
        import tempfile

        from tests.test_results_report import create_report_db

        alerts = load_script_module(
            "service_alert_delay_correlation",
            "analysis/service-alert-delay-correlation.py",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "foli.db"
            create_report_db(db_path)

            class Args:
                db = db_path
                cache_dir = Path(temp_dir) / "cache"
                quality_mode = "conservative"
                bucket = "trip-stop"
                timezone = "Europe/Helsinki"
                min_observations = 1
                min_trips = 1
                limit = 5
                exclude_stop_call_disagreement = False
                force_cache = False
                alert_kind = "any"
                line_ref = None
                gtfs_dir = None
                gtfs_root = None

            buckets = alerts.cached_alert_observation_buckets(Args, (None, None))
            grouped, by_line = alerts.build_correlation_from_buckets(Args, buckets)

        self.assertEqual(len(buckets), 3)
        # One alert bucket and a single same-stratum control is below the 20-control
        # minimum, so no lift is reported rather than a misleading one.
        self.assertTrue(grouped.empty)
        self.assertTrue(by_line.empty)

    def test_alert_window_uses_explicit_start_and_end_without_database_lookup(self) -> None:
        alerts = load_script_module(
            "service_alert_delay_correlation",
            "analysis/service-alert-delay-correlation.py",
        )

        class Args:
            db = PROJECT_ROOT / "does-not-exist.db"
            timezone = "Europe/Helsinki"
            start = "2026-05-06"
            end = "2026-05-08"
            analysis_days = 2
            full_history = False

        start, end, description = alerts.resolve_analysis_window(Args)

        self.assertEqual(start.isoformat(), "2026-05-05T21:00:00+00:00")
        self.assertEqual(end.isoformat(), "2026-05-07T21:00:00+00:00")
        self.assertIn("2026-05-05T21:00:00+00:00", description)

if __name__ == "__main__":
    unittest.main()
