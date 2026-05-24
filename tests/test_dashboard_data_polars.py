from __future__ import annotations

from datetime import date, time
from pathlib import Path
import tempfile
import unittest

import polars as pl

from dashboard_data_polars import (
    DEFAULT_DB_PATH,
    build_hourly_line_metrics,
    build_stop_heatmap_weights,
    build_stop_metrics,
    filter_observations,
    load_observations,
    load_stop_metadata,
    prepare_observations,
)


def sample_observations() -> pl.DataFrame:
    return pl.DataFrame(
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


def sample_stops() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "stop_id": ["10", "20"],
            "gtfs_stop_name": ["Keskusta", "Satama"],
            "stop_lat": [60.45, 60.43],
            "stop_lon": [22.27, 22.22],
        },
        schema_overrides={"stop_id": pl.Utf8},
    )


def sample_stop_metrics() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "stop_id": ["late", "early", "zero", "missing"],
            "stop_name": ["Late stop", "Early stop", "Zero stop", "Missing stop"],
            "stop_lat": [60.45, 60.43, 60.41, None],
            "stop_lon": [22.27, 22.22, 22.20, 22.25],
            "bucket_count": [10, 5, 4, 8],
            "raw_poll_count": [25, 15, 4, 20],
            "line_count": [2, 1, 1, 3],
            "signed_mean_delay_min": [2.0, -3.0, 0.0, 4.0],
            "median_delay_min": [1.5, -2.0, 0.0, 3.0],
            "p75_delay_min": [1.8, -1.0, 0.0, 3.5],
            "p90_delay_min": [2.0, -3.0, 0.0, 4.0],
            "p95_delay_min": [2.2, -3.5, 0.0, 4.5],
            "pct_over_3_min_late": [20.0, 0.0, 0.0, 50.0],
            "pct_over_5_min_late": [10.0, 0.0, 0.0, 25.0],
            "pct_early": [0.0, 80.0, 0.0, 0.0],
            "pct_over_1_min_early": [0.0, 60.0, 0.0, 0.0],
            "pct_over_3_min_early": [0.0, 40.0, 0.0, 0.0],
            "median_early_min_abs": [0.0, 2.0, 0.0, 0.0],
            "p90_early_min_abs": [0.0, 3.0, 0.0, 0.0],
        }
    )


def write_gtfs_stops(root: Path, feed_date: str, name: str, lat: float, lon: float) -> None:
    feed_dir = root / f"gtfs_{feed_date}"
    feed_dir.mkdir()
    (feed_dir / "stops.txt").write_text(
        "stop_id,stop_name,stop_lat,stop_lon\n"
        f"10,{name},{lat},{lon}\n",
        encoding="utf-8",
    )


class PolarsDashboardDataTests(unittest.TestCase):
    def test_prepare_observations_adds_local_time_and_stop_metadata(self) -> None:
        prepared = prepare_observations(sample_observations(), sample_stops())
        row = prepared.row(0, named=True)

        self.assertEqual(row["local_hour"], 11)
        self.assertEqual(row["local_minute_of_day"], 11 * 60 + 5)
        self.assertEqual(str(row["local_date"]), "2026-04-23")
        self.assertEqual(row["stop_name"], "Keskusta")
        self.assertEqual(row["stop_lat"], 60.45)
        self.assertEqual(row["stop_lon"], 22.27)
        self.assertEqual(row["raw_poll_count"], 3)

    def test_filter_observations_limits_inclusive_local_time_range(self) -> None:
        prepared = prepare_observations(sample_observations(), sample_stops())

        filtered = filter_observations(
            prepared,
            start_time=time(11, 0),
            end_time=time(11, 10),
        )

        self.assertEqual(filtered["delay_seconds"].to_list(), [0.0])

    def test_filter_observations_full_day_time_range_preserves_rows(self) -> None:
        prepared = prepare_observations(sample_observations(), sample_stops())

        filtered = filter_observations(
            prepared,
            start_time=time(0, 0),
            end_time=time(23, 59),
        )

        self.assertEqual(filtered.height, prepared.height)

    def test_filter_observations_combines_time_with_existing_filters(self) -> None:
        prepared = prepare_observations(sample_observations(), sample_stops())

        filtered = filter_observations(
            prepared,
            line_refs=["3"],
            direction_refs=["1"],
            day_filter="Weekdays",
            start_time=time(11, 0),
            end_time=time(11, 12),
        )

        self.assertEqual(filtered["delay_seconds"].to_list(), [0.0])

    def test_stop_metrics_preserve_signed_delay_and_late_rates(self) -> None:
        prepared = prepare_observations(sample_observations(), sample_stops())
        metrics = build_stop_metrics(prepared, min_observations=1)
        stop_10 = metrics.filter(pl.col("stop_id") == "10").row(0, named=True)

        self.assertEqual(stop_10["bucket_count"], 1)
        self.assertEqual(stop_10["raw_poll_count"], 3)
        self.assertAlmostEqual(stop_10["median_delay_min"], 0.0)
        self.assertAlmostEqual(stop_10["p90_delay_min"], 0.0)
        self.assertAlmostEqual(stop_10["pct_over_3_min_late"], 0.0)

    def test_min_observations_filters_hourly_groups(self) -> None:
        prepared = prepare_observations(sample_observations(), sample_stops())
        metrics = build_hourly_line_metrics(prepared, min_observations=1)

        self.assertEqual(set(metrics["line_ref"].to_list()), {"3", "4"})
        line_3 = metrics.filter(pl.col("line_ref") == "3").row(0, named=True)
        self.assertEqual(line_3["bucket_count"], 1)
        self.assertEqual(line_3["raw_poll_count"], 3)

    def test_stop_heatmap_weights_split_late_and_early_average_delay(self) -> None:
        metrics = sample_stop_metrics()

        late = build_stop_heatmap_weights(
            metrics,
            "p90_delay_min",
            delay_direction="late",
        )
        early = build_stop_heatmap_weights(
            metrics,
            "p90_delay_min",
            delay_direction="early",
        )

        self.assertEqual(late["stop_id"].to_list(), ["late"])
        self.assertEqual(early["stop_id"].to_list(), ["early"])
        self.assertAlmostEqual(late.row(0, named=True)["heat_weight"], 20.0)
        self.assertAlmostEqual(early.row(0, named=True)["heat_weight"], 15.0)

    def test_stop_heatmap_weights_convert_late_rate_to_observation_count(self) -> None:
        heat = build_stop_heatmap_weights(sample_stop_metrics(), "pct_over_5_min_late")

        weights = dict(zip(heat["stop_id"], heat["heat_weight"], strict=True))

        self.assertEqual(set(weights), {"late"})
        self.assertAlmostEqual(weights["late"], 1.0)

    def test_stop_heatmap_weights_remove_missing_coordinates_and_zero_heat(self) -> None:
        heat = build_stop_heatmap_weights(sample_stop_metrics(), "pct_over_3_min_late")

        self.assertEqual(heat["stop_id"].to_list(), ["late"])
        self.assertGreater(heat.row(0, named=True)["heat_weight"], 0)
        self.assertTrue(heat["stop_lat"].is_not_null().all())
        self.assertTrue(heat["stop_lon"].is_not_null().all())

    def test_load_stop_metadata_reads_date_named_gtfs_feeds(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            gtfs_root = Path(temp_dir)
            write_gtfs_stops(gtfs_root, "2026-04-23", "First feed", 60.45, 22.27)
            write_gtfs_stops(gtfs_root, "2026-04-30", "Second feed", 60.46, 22.28)

            stops = load_stop_metadata(gtfs_root=gtfs_root)

        self.assertEqual(
            stops["gtfs_feed_date"].to_list(),
            [date(2026, 4, 23), date(2026, 4, 30)],
        )
        self.assertEqual(stops["gtfs_stop_name"].to_list(), ["First feed", "Second feed"])

    def test_prepare_observations_uses_stop_metadata_by_feed_date(self) -> None:
        observations = pl.DataFrame(
            {
                "recorded_at_utc": [
                    "2026-04-22T08:00:00Z",
                    "2026-04-23T08:00:00Z",
                    "2026-04-30T08:00:00Z",
                ],
                "vehicle_id": ["v-before", "v-first", "v-second"],
                "trip_match_key": ["before", "first", "second"],
                "line_ref": ["3", "3", "3"],
                "direction_ref": ["1", "1", "1"],
                "published_line_name": ["3", "3", "3"],
                "delay_seconds": [60, 120, 180],
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
            },
            schema_overrides={"stop_id": pl.Utf8},
        )

        prepared = prepare_observations(observations, stops)
        by_date = {str(row["local_date"]): row for row in prepared.iter_rows(named=True)}

        self.assertIsNone(by_date["2026-04-22"]["gtfs_feed_date"])
        self.assertFalse(by_date["2026-04-22"]["has_gtfs_stop_metadata"])
        self.assertEqual(by_date["2026-04-22"]["stop_name"], "Fallback before")
        self.assertIsNone(by_date["2026-04-22"]["stop_lat"])

        self.assertEqual(by_date["2026-04-23"]["gtfs_feed_date"], date(2026, 4, 23))
        self.assertTrue(by_date["2026-04-23"]["has_gtfs_stop_metadata"])
        self.assertEqual(by_date["2026-04-23"]["stop_name"], "First feed")
        self.assertEqual(by_date["2026-04-23"]["stop_lat"], 60.45)

        self.assertEqual(by_date["2026-04-30"]["gtfs_feed_date"], date(2026, 4, 30))
        self.assertTrue(by_date["2026-04-30"]["has_gtfs_stop_metadata"])
        self.assertEqual(by_date["2026-04-30"]["stop_name"], "Second feed")
        self.assertEqual(by_date["2026-04-30"]["stop_lat"], 60.46)

    def test_real_data_smoke_loads_and_joins_stops(self) -> None:
        if not DEFAULT_DB_PATH.exists():
            self.skipTest("data/foli.db is not available")

        try:
            observations = load_observations(limit=10_000)
        except Exception as exc:
            self.skipTest(f"data/foli.db is not readable: {exc}")
        stops = load_stop_metadata()
        prepared = prepare_observations(observations, stops)

        self.assertGreater(prepared.height, 0)
        self.assertGreater(prepared["stop_lat"].is_not_null().sum(), 0)
        self.assertGreater(prepared["stop_lon"].is_not_null().sum(), 0)


if __name__ == "__main__":
    unittest.main()
