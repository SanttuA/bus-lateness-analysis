from __future__ import annotations

import unittest

import pandas as pd

from dashboard_data import (
    DEFAULT_DB_PATH,
    build_hourly_line_metrics,
    build_stop_heatmap_weights,
    build_stop_metrics,
    load_observations,
    load_stop_metadata,
    prepare_observations,
)


def sample_observations() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "recorded_at_utc": [
                "2026-04-23T08:05:22Z",
                "2026-04-23T08:10:22Z",
                "2026-04-23T08:15:22Z",
                "2026-04-23T08:20:22Z",
            ],
            "line_ref": ["3", "3", "3", "4"],
            "direction_ref": ["1", "1", "1", "2"],
            "published_line_name": ["3", "3", "3", "4"],
            "delay_seconds": [60, -120, 0, 240],
            "next_stop_point_ref": ["10", "10", "10", "20"],
            "next_stop_point_name": ["Fallback 10", "Fallback 10", "Fallback 10", "Fallback 20"],
        }
    )


def sample_stops() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stop_id": pd.Series(["10", "20"], dtype="string"),
            "gtfs_stop_name": ["Keskusta", "Satama"],
            "stop_lat": [60.45, 60.43],
            "stop_lon": [22.27, 22.22],
        }
    )


def sample_stop_metrics() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "stop_id": ["late", "early", "zero", "missing"],
            "stop_name": ["Late stop", "Early stop", "Zero stop", "Missing stop"],
            "stop_lat": [60.45, 60.43, 60.41, None],
            "stop_lon": [22.27, 22.22, 22.20, 22.25],
            "obs_count": [10, 5, 4, 8],
            "line_count": [2, 1, 1, 3],
            "avg_delay_min": [2.0, -3.0, 0.0, 4.0],
            "median_delay_min": [1.5, -2.0, 0.0, 3.0],
            "pct_late": [40.0, 50.0, 0.0, 75.0],
            "pct_over_3_min_late": [20.0, 0.0, 0.0, 50.0],
        }
    )


class DashboardDataTests(unittest.TestCase):
    def test_prepare_observations_adds_local_time_and_stop_metadata(self) -> None:
        prepared = prepare_observations(sample_observations(), sample_stops())

        self.assertEqual(prepared.loc[0, "local_hour"], 11)
        self.assertEqual(str(prepared.loc[0, "local_date"]), "2026-04-23")
        self.assertEqual(prepared.loc[0, "stop_name"], "Keskusta")
        self.assertEqual(prepared.loc[0, "stop_lat"], 60.45)
        self.assertEqual(prepared.loc[0, "stop_lon"], 22.27)

    def test_stop_metrics_preserve_signed_delay_and_late_rates(self) -> None:
        prepared = prepare_observations(sample_observations(), sample_stops())
        metrics = build_stop_metrics(prepared, min_observations=1)
        stop_10 = metrics[metrics["stop_id"].astype(str) == "10"].iloc[0]

        self.assertAlmostEqual(stop_10["avg_delay_min"], -1 / 3)
        self.assertAlmostEqual(stop_10["median_delay_min"], 0.0)
        self.assertAlmostEqual(stop_10["pct_late"], 100 / 3)
        self.assertAlmostEqual(stop_10["pct_over_3_min_late"], 0.0)

    def test_min_observations_filters_hourly_groups(self) -> None:
        prepared = prepare_observations(sample_observations(), sample_stops())
        metrics = build_hourly_line_metrics(prepared, min_observations=2)

        self.assertEqual(metrics["line_ref"].astype(str).to_list(), ["3"])
        self.assertEqual(metrics.loc[0, "obs_count"], 3)

    def test_stop_heatmap_weights_split_late_and_early_average_delay(self) -> None:
        metrics = sample_stop_metrics()

        late = build_stop_heatmap_weights(
            metrics,
            "avg_delay_min",
            delay_direction="late",
        )
        early = build_stop_heatmap_weights(
            metrics,
            "avg_delay_min",
            delay_direction="early",
        )

        self.assertEqual(late["stop_id"].to_list(), ["late"])
        self.assertEqual(early["stop_id"].to_list(), ["early"])
        self.assertAlmostEqual(late.loc[0, "heat_weight"], 20.0)
        self.assertAlmostEqual(early.loc[0, "heat_weight"], 15.0)

    def test_stop_heatmap_weights_convert_late_rate_to_observation_count(self) -> None:
        heat = build_stop_heatmap_weights(sample_stop_metrics(), "pct_late")

        weights = dict(zip(heat["stop_id"], heat["heat_weight"], strict=True))

        self.assertEqual(set(weights), {"late", "early"})
        self.assertAlmostEqual(weights["late"], 4.0)
        self.assertAlmostEqual(weights["early"], 2.5)

    def test_stop_heatmap_weights_remove_missing_coordinates_and_zero_heat(self) -> None:
        heat = build_stop_heatmap_weights(sample_stop_metrics(), "pct_over_3_min_late")

        self.assertEqual(heat["stop_id"].to_list(), ["late"])
        self.assertGreater(heat.loc[0, "heat_weight"], 0)
        self.assertTrue(heat["stop_lat"].notna().all())
        self.assertTrue(heat["stop_lon"].notna().all())

    def test_real_data_smoke_loads_and_joins_stops(self) -> None:
        if not DEFAULT_DB_PATH.exists():
            self.skipTest("data/foli.db is not available")

        observations = load_observations(limit=10_000)
        stops = load_stop_metadata()
        prepared = prepare_observations(observations, stops)

        self.assertGreater(len(prepared), 0)
        self.assertGreater(prepared["stop_lat"].notna().sum(), 0)
        self.assertGreater(prepared["stop_lon"].notna().sum(), 0)


if __name__ == "__main__":
    unittest.main()
