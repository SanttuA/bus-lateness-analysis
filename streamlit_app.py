from __future__ import annotations

import re
from datetime import date, time, timedelta
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard_data import (
    DEFAULT_DB_PATH,
    DEFAULT_TIMEZONE,
    DIVERGING_METRICS,
    METRIC_LABELS,
    build_hourly_line_metrics,
    build_stop_heatmap_weights,
    build_stop_metrics,
    filter_observations,
    latest_gtfs_dir,
    load_observations,
    load_stop_metadata,
    metric_label,
    prepare_observations,
    rank_early_stops,
    rank_late_stops,
    summarize_observations,
)


st.set_page_config(
    page_title="Föli Bus Lateness",
    layout="wide",
)


LATE_EARLY_SCALE = [
    [0.0, "#2166ac"],
    [0.5, "#f7f7f7"],
    [1.0, "#b2182b"],
]

SEQUENTIAL_SCALE = "YlOrRd"
EARLY_HEAT_SCALE = "Blues"
STOP_HEATMAP_RADIUS = 25


@st.cache_data(show_spinner="Loading Föli observations")
def cached_dataset(db_path: str, gtfs_dir: str, timezone: str):
    observations = load_observations(Path(db_path))
    stops = load_stop_metadata(Path(gtfs_dir))
    return prepare_observations(observations, stops, timezone=timezone)


def route_sort_key(value: object) -> list[object]:
    parts = re.split(r"(\d+)", str(value))
    return [int(part) if part.isdigit() else part.lower() for part in parts]


def selected_date_range(value: object, fallback_start: date, fallback_end: date):
    if isinstance(value, tuple):
        if len(value) == 2:
            return value[0], value[1]
        if len(value) == 1:
            return value[0], value[0]
        return fallback_start, fallback_end
    return value, value


def make_hourly_heatmap(hourly: object, metric_key: str) -> go.Figure:
    order = hourly.groupby("line_ref").agg(
        sort_metric=(metric_key, "mean"),
        total_obs=("obs_count", "sum"),
    )
    if metric_key == "obs_count":
        ordered_lines = order.sort_values("total_obs", ascending=False).index
    else:
        ordered_lines = order.sort_values(
            ["sort_metric", "total_obs"],
            ascending=[False, False],
        ).index

    values = (
        hourly.pivot(index="line_ref", columns="local_hour", values=metric_key)
        .reindex(index=ordered_lines, columns=range(24))
    )
    counts = (
        hourly.pivot(index="line_ref", columns="local_hour", values="obs_count")
        .reindex(index=ordered_lines, columns=range(24))
    )

    colorbar_title = metric_label(metric_key)
    heatmap_kwargs = {
        "z": values.to_numpy(),
        "x": [f"{hour:02d}:00" for hour in values.columns],
        "y": values.index.astype(str),
        "customdata": counts.to_numpy(),
        "colorscale": LATE_EARLY_SCALE
        if metric_key in DIVERGING_METRICS
        else SEQUENTIAL_SCALE,
        "colorbar": {"title": colorbar_title},
        "hovertemplate": (
            "Line %{y}<br>"
            "Hour %{x}<br>"
            f"{colorbar_title}: %{{z:.2f}}<br>"
            "Observations: %{customdata:.0f}"
            "<extra></extra>"
        ),
    }
    if metric_key in DIVERGING_METRICS:
        heatmap_kwargs["zmid"] = 0

    fig = go.Figure(data=go.Heatmap(**heatmap_kwargs))
    fig.update_layout(
        height=min(1000, max(430, 18 * len(values.index) + 150)),
        margin={"l": 80, "r": 20, "t": 30, "b": 45},
        xaxis_title="Local hour",
        yaxis_title="Line",
        template="plotly_white",
    )
    return fig


def make_stop_map(stop_metrics: object, metric_key: str) -> go.Figure:
    map_df = stop_metrics.dropna(subset=["stop_lat", "stop_lon"]).copy()
    center = {
        "lat": float(map_df["stop_lat"].mean()),
        "lon": float(map_df["stop_lon"].mean()),
    }
    color_kwargs = {
        "color_continuous_scale": LATE_EARLY_SCALE
        if metric_key in DIVERGING_METRICS
        else SEQUENTIAL_SCALE,
    }
    if metric_key in DIVERGING_METRICS:
        color_kwargs["color_continuous_midpoint"] = 0

    fig = px.scatter_mapbox(
        map_df,
        lat="stop_lat",
        lon="stop_lon",
        color=metric_key,
        size="obs_count",
        size_max=24,
        hover_name="stop_name",
        hover_data={
            "stop_id": True,
            "avg_delay_min": ":.2f",
            "median_delay_min": ":.2f",
            "pct_late": ":.1f",
            "pct_over_3_min_late": ":.1f",
            "obs_count": True,
            "line_count": True,
            "stop_lat": False,
            "stop_lon": False,
        },
        center=center,
        zoom=9,
        height=650,
        labels={key: label for key, label in METRIC_LABELS.items()},
        **color_kwargs,
    )
    fig.update_layout(
        mapbox_style="carto-positron",
        margin={"l": 0, "r": 0, "t": 20, "b": 0},
        coloraxis_colorbar_title=metric_label(metric_key),
    )
    return fig


def heatmap_weight_label(metric_key: str, delay_direction: str = "late") -> str:
    if metric_key == "avg_delay_min":
        if delay_direction == "early":
            return "Early-running intensity"
        return "Late delay intensity"
    if metric_key == "pct_late":
        return "Estimated late observations"
    if metric_key == "pct_over_3_min_late":
        return "Estimated >3 min late observations"
    return metric_label(metric_key)


def make_stop_heatmap(
    stop_metrics: object,
    metric_key: str,
    *,
    delay_direction: str = "late",
) -> go.Figure:
    heat_df = build_stop_heatmap_weights(
        stop_metrics,
        metric_key,
        delay_direction=delay_direction,
    )
    center = {
        "lat": float(heat_df["stop_lat"].mean()),
        "lon": float(heat_df["stop_lon"].mean()),
    }
    weight_label = heatmap_weight_label(metric_key, delay_direction)
    fig = px.density_mapbox(
        heat_df,
        lat="stop_lat",
        lon="stop_lon",
        z="heat_weight",
        radius=STOP_HEATMAP_RADIUS,
        hover_name="stop_name",
        hover_data={
            "stop_id": True,
            "avg_delay_min": ":.2f",
            "median_delay_min": ":.2f",
            "pct_late": ":.1f",
            "pct_over_3_min_late": ":.1f",
            "obs_count": True,
            "line_count": True,
            "heat_weight": ":.2f",
            "stop_lat": False,
            "stop_lon": False,
        },
        center=center,
        zoom=9,
        height=650,
        labels={
            **{key: label for key, label in METRIC_LABELS.items()},
            "heat_weight": weight_label,
        },
        color_continuous_scale=EARLY_HEAT_SCALE
        if delay_direction == "early"
        else SEQUENTIAL_SCALE,
    )
    fig.update_layout(
        mapbox_style="carto-positron",
        margin={"l": 0, "r": 0, "t": 20, "b": 0},
        coloraxis_colorbar_title=weight_label,
    )
    return fig


def table_columns(df):
    columns = [
        "stop_id",
        "stop_name",
        "obs_count",
        "line_count",
        "avg_delay_min",
        "median_delay_min",
        "pct_late",
        "pct_over_3_min_late",
    ]
    return df[columns].rename(
        columns={
            "stop_id": "Stop ID",
            "stop_name": "Stop",
            "obs_count": "Observations",
            "line_count": "Lines",
            "avg_delay_min": "Avg delay (min)",
            "median_delay_min": "Median delay (min)",
            "pct_late": "Late (%)",
            "pct_over_3_min_late": "Over 3 min late (%)",
        }
    )


def main() -> None:
    st.title("Föli Bus Lateness")

    gtfs_dir = latest_gtfs_dir()
    if gtfs_dir is None:
        st.error("No GTFS stops.txt found below data/gtfs.")
        st.stop()
    if not DEFAULT_DB_PATH.exists():
        st.error("Database not found at data/foli.db.")
        st.stop()

    df = cached_dataset(
        str(DEFAULT_DB_PATH),
        str(gtfs_dir),
        DEFAULT_TIMEZONE,
    )
    if df.empty:
        st.warning("No analysis-ready observations found.")
        st.stop()

    min_date = min(df["local_date"])
    max_date = max(df["local_date"])
    line_options = sorted(df["line_ref"].dropna().astype(str).unique(), key=route_sort_key)
    direction_options = sorted(
        df["direction_ref"].dropna().astype(str).unique(),
        key=route_sort_key,
    )
    metric_options = list(METRIC_LABELS.keys())
    label_to_metric = {label: key for key, label in METRIC_LABELS.items()}

    with st.sidebar:
        st.header("Filters")
        date_value = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        start_date, end_date = selected_date_range(date_value, min_date, max_date)
        selected_lines = st.multiselect("Lines", line_options)
        selected_directions = st.multiselect("Directions", direction_options)
        day_filter = st.selectbox("Days", ["All days", "Weekdays", "Weekends"])
        selected_metric_label = st.selectbox(
            "Metric",
            [METRIC_LABELS[key] for key in metric_options],
        )
        metric_key = label_to_metric[selected_metric_label]
        min_observations = st.number_input(
            "Minimum observations per group",
            min_value=1,
            max_value=10000,
            value=30,
            step=10,
        )
        heatmap_start_time, heatmap_end_time = st.slider(
            "Heatmap time range",
            min_value=time(0, 0),
            max_value=time(23, 59),
            value=(time(0, 0), time(23, 59)),
            step=timedelta(minutes=30),
            format="HH:mm",
        )

    filtered = filter_observations(
        df,
        start_date=start_date,
        end_date=end_date,
        line_refs=selected_lines,
        direction_refs=selected_directions,
        day_filter=day_filter,
    )
    if filtered.empty:
        st.warning("No observations match the selected filters.")
        st.stop()

    heatmap_filtered = filter_observations(
        filtered,
        start_time=heatmap_start_time,
        end_time=heatmap_end_time,
    )

    summary = summarize_observations(filtered)
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Observations", f"{summary['obs_count']:,}")
    col2.metric("Lines", f"{summary['line_count']:,}")
    col3.metric("Stops", f"{summary['stop_count']:,}")
    col4.metric("Avg delay", f"{summary['avg_delay_min']:.2f} min")
    col5.metric("Late", f"{summary['pct_late']:.1f}%")

    hourly = build_hourly_line_metrics(
        heatmap_filtered,
        min_observations=int(min_observations),
    )
    stop_metrics = build_stop_metrics(
        filtered,
        min_observations=int(min_observations),
    )
    heatmap_stop_metrics = build_stop_metrics(
        heatmap_filtered,
        min_observations=int(min_observations),
    )

    st.subheader("Line By Hour")
    if heatmap_filtered.empty:
        st.info("No observations match the selected heatmap time range.")
    elif hourly.empty:
        st.info("No line-hour groups meet the minimum observation threshold.")
    else:
        st.plotly_chart(
            make_hourly_heatmap(hourly, metric_key),
            use_container_width=True,
        )

    st.subheader("Stops")
    if stop_metrics.empty:
        st.info("No stops meet the minimum observation threshold.")
    else:
        map_df = stop_metrics.dropna(subset=["stop_lat", "stop_lon"])
        if map_df.empty:
            st.info("No mapped stops meet the minimum observation threshold.")
        else:
            marker_tab, heatmap_tab = st.tabs(["Markers", "Heatmap"])
            with marker_tab:
                st.plotly_chart(
                    make_stop_map(map_df, metric_key),
                    use_container_width=True,
                )
            with heatmap_tab:
                if heatmap_filtered.empty:
                    st.info("No observations match the selected heatmap time range.")
                elif heatmap_stop_metrics.empty:
                    st.info(
                        "No stops in the selected heatmap time range meet the "
                        "minimum observation threshold."
                    )
                else:
                    heatmap_map_df = heatmap_stop_metrics.dropna(
                        subset=["stop_lat", "stop_lon"]
                    )
                    if heatmap_map_df.empty:
                        st.info(
                            "No mapped stops in the selected heatmap time range "
                            "meet the minimum observation threshold."
                        )
                    elif metric_key == "avg_delay_min":
                        late_heatmap_tab, early_heatmap_tab = st.tabs(
                            ["Late heatmap", "Early heatmap"]
                        )
                        with late_heatmap_tab:
                            late_heat = build_stop_heatmap_weights(
                                heatmap_map_df,
                                metric_key,
                                delay_direction="late",
                            )
                            if late_heat.empty:
                                st.info(
                                    "No late delay hotspots meet the current heatmap "
                                    "filters."
                                )
                            else:
                                st.plotly_chart(
                                    make_stop_heatmap(
                                        late_heat,
                                        metric_key,
                                        delay_direction="late",
                                    ),
                                    use_container_width=True,
                                )
                        with early_heatmap_tab:
                            early_heat = build_stop_heatmap_weights(
                                heatmap_map_df,
                                metric_key,
                                delay_direction="early",
                            )
                            if early_heat.empty:
                                st.info(
                                    "No early-running hotspots meet the current "
                                    "heatmap filters."
                                )
                            else:
                                st.plotly_chart(
                                    make_stop_heatmap(
                                        early_heat,
                                        metric_key,
                                        delay_direction="early",
                                    ),
                                    use_container_width=True,
                                )
                    else:
                        heat = build_stop_heatmap_weights(heatmap_map_df, metric_key)
                        if heat.empty:
                            st.info("No heatmap hotspots meet the current filters.")
                        else:
                            st.plotly_chart(
                                make_stop_heatmap(heat, metric_key),
                                use_container_width=True,
                            )

        late_tab, early_tab = st.tabs(["Most late stops", "Most early stops"])
        with late_tab:
            st.dataframe(
                table_columns(rank_late_stops(stop_metrics)),
                use_container_width=True,
                hide_index=True,
            )
        with early_tab:
            st.dataframe(
                table_columns(rank_early_stops(stop_metrics)),
                use_container_width=True,
                hide_index=True,
            )

    st.caption(
        "Source: Turku Region Public Transport operating and schedule data, "
        "downloaded from data.foli.fi under CC BY 4.0."
    )


if __name__ == "__main__":
    main()
