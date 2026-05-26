from __future__ import annotations

import unittest

import streamlit_app
import streamlit_app_polars


class DefaultStreamlitAppTests(unittest.TestCase):
    def test_default_dashboard_delegates_to_polars_dashboard(self) -> None:
        self.assertIs(streamlit_app.main, streamlit_app_polars.main)
        self.assertIs(
            streamlit_app.make_stop_map,
            streamlit_app_polars.make_stop_map,
        )
        self.assertIs(
            streamlit_app.cached_summary,
            streamlit_app_polars.cached_summary,
        )


if __name__ == "__main__":
    unittest.main()
