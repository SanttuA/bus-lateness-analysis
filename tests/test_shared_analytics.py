from __future__ import annotations

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

    def test_alert_controls_match_active_line_direction_hour_and_day_type(self) -> None:
        alerts = load_script_module(
            "service_alert_delay_correlation",
            "analysis/service-alert-delay-correlation.py",
        )
        rows = pd.DataFrame(
            {
                "line_ref": ["3", "3", "3"],
                "direction_ref": ["1", "1", "2"],
                "local_hour": [8, 8, 8],
                "day_type": ["weekday", "weekday", "weekday"],
                "delay_seconds": [60, 120, 180],
            }
        )
        active_mask = pd.Series([True, False, False])

        active, controls = alerts.matched_control_rows(rows, active_mask)

        self.assertEqual(active["delay_seconds"].to_list(), [60])
        self.assertEqual(controls["delay_seconds"].to_list(), [120])


if __name__ == "__main__":
    unittest.main()
