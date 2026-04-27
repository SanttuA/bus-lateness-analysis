from __future__ import annotations

import unittest

import pandas as pd

from streamlit_app import (
    DELAY_SCALE_AUTO,
    DELAY_SCALE_MANUAL,
    HEATMAP_SCALE_AUTO,
    HEATMAP_SCALE_MANUAL,
    STOP_MARKER_MAX_SIZE,
    STOP_MARKER_MIN_SIZE,
    delay_color_range_extent,
    heatmap_intensity_max,
    make_hourly_heatmap,
    make_stop_map,
    make_stop_heatmap,
    scale_stop_marker_sizes,
    stop_marker_caption,
)
from tests.test_dashboard_data import sample_stop_metrics


class StopMarkerMapTests(unittest.TestCase):
    def test_delay_color_range_extent_auto_ignores_extreme_outlier(self) -> None:
        values = pd.Series([1.0] * 95 + [90.0] * 5)

        extent = delay_color_range_extent(values, DELAY_SCALE_AUTO)

        self.assertGreater(extent, 1.0)
        self.assertLess(extent, values.abs().max())

    def test_delay_color_range_extent_manual_returns_positive_range(self) -> None:
        extent = delay_color_range_extent(
            pd.Series([-1.0, 2.0, 90.0]),
            DELAY_SCALE_MANUAL,
            4.5,
        )

        self.assertEqual(extent, 4.5)

    def test_stop_marker_caption_mentions_time_filter(self) -> None:
        caption = stop_marker_caption(sample_stop_metrics(), "avg_delay_min")

        self.assertIn("date, line, direction, day, and time filters", caption)

    def test_scale_stop_marker_sizes_empty_input_returns_empty(self) -> None:
        self.assertEqual(scale_stop_marker_sizes([]), [])

    def test_scale_stop_marker_sizes_equal_counts_are_equal_and_visible(self) -> None:
        sizes = scale_stop_marker_sizes(pd.Series([10, 10, 10]))

        self.assertEqual(len(sizes), 3)
        self.assertEqual(len(set(sizes)), 1)
        self.assertGreaterEqual(sizes[0], STOP_MARKER_MIN_SIZE)
        self.assertLessEqual(sizes[0], STOP_MARKER_MAX_SIZE)

    def test_scale_stop_marker_sizes_varied_counts_preserve_order_and_bounds(
        self,
    ) -> None:
        sizes = scale_stop_marker_sizes(pd.Series([1, 4, 100]))

        self.assertAlmostEqual(min(sizes), STOP_MARKER_MIN_SIZE)
        self.assertAlmostEqual(max(sizes), STOP_MARKER_MAX_SIZE)
        self.assertLess(sizes[0], sizes[1])
        self.assertLess(sizes[1], sizes[2])

    def test_make_stop_map_uses_visible_dark_marker_layer(self) -> None:
        fig = make_stop_map(sample_stop_metrics(), "avg_delay_min", delay_extent=5.0)

        self.assertEqual(fig.layout.mapbox.style, "carto-darkmatter")
        self.assertEqual(len(fig.data), 2)

        halo_trace, marker_trace = fig.data
        self.assertEqual(halo_trace.hoverinfo, "skip")
        self.assertEqual(halo_trace.marker.color, "rgba(6, 9, 15, 0.95)")
        self.assertEqual(marker_trace.marker.colorbar.title.text, "Delay (min)")
        self.assertEqual(marker_trace.marker.cmin, -5.0)
        self.assertEqual(marker_trace.marker.cmax, 5.0)
        self.assertNotEqual(
            marker_trace.marker.colorbar.title.text,
            "Average delay (min)",
        )

        self.assertEqual(len(marker_trace.lat), 3)
        self.assertEqual(len(halo_trace.lat), len(marker_trace.lat))
        self.assertIn("Stop ID", marker_trace.hovertemplate)
        self.assertIn("Observations", marker_trace.hovertemplate)
        self.assertIn("Average delay (min)", marker_trace.hovertemplate)
        self.assertIn("Median delay (min)", marker_trace.hovertemplate)
        self.assertIn("Late observations (%)", marker_trace.hovertemplate)
        self.assertIn("Over 3 min late (%)", marker_trace.hovertemplate)
        self.assertIn("Lines", marker_trace.hovertemplate)

    def test_make_hourly_heatmap_applies_delay_extent(self) -> None:
        hourly = pd.DataFrame(
            {
                "line_ref": ["1", "1"],
                "local_hour": [8, 9],
                "line_name": ["1", "1"],
                "obs_count": [30, 40],
                "avg_delay_min": [-0.5, 0.75],
            }
        )

        fig = make_hourly_heatmap(hourly, "avg_delay_min", delay_extent=2.0)

        heatmap = fig.data[0]
        self.assertEqual(heatmap.zmin, -2.0)
        self.assertEqual(heatmap.zmax, 2.0)
        self.assertEqual(heatmap.zmid, 0)


class StopHeatmapScaleTests(unittest.TestCase):
    def test_heatmap_intensity_max_empty_input_returns_none(self) -> None:
        self.assertIsNone(heatmap_intensity_max([], HEATMAP_SCALE_AUTO))

    def test_heatmap_intensity_max_auto_uses_95th_percentile(self) -> None:
        weights = pd.Series([10.0] * 95 + [10_000.0] * 5)

        intensity_max = heatmap_intensity_max(weights, HEATMAP_SCALE_AUTO)

        self.assertIsNotNone(intensity_max)
        self.assertGreater(intensity_max, 10.0)
        self.assertLess(intensity_max, weights.max())

    def test_heatmap_intensity_max_auto_handles_constant_data(self) -> None:
        intensity_max = heatmap_intensity_max(
            pd.Series([42.0, 42.0]),
            HEATMAP_SCALE_AUTO,
        )

        self.assertEqual(intensity_max, 42.0)

    def test_heatmap_intensity_max_manual_returns_positive_manual_value(self) -> None:
        intensity_max = heatmap_intensity_max(
            pd.Series([1.0, 2.0, 100.0]),
            HEATMAP_SCALE_MANUAL,
            25.0,
        )

        self.assertEqual(intensity_max, 25.0)

    def test_make_stop_heatmap_applies_color_axis_max(self) -> None:
        fig = make_stop_heatmap(
            sample_stop_metrics(),
            "avg_delay_min",
            delay_direction="late",
            max_intensity=20.0,
        )

        self.assertEqual(fig.layout.coloraxis.cmin, 0)
        self.assertEqual(fig.layout.coloraxis.cmax, 20.0)
        self.assertEqual(
            fig.layout.coloraxis.colorbar.title.text,
            "Late delay intensity",
        )


if __name__ == "__main__":
    unittest.main()
