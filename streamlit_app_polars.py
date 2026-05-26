from __future__ import annotations

import re
from datetime import date, time, timedelta
from pathlib import Path

import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st

from dashboard_data_polars import (
    DEFAULT_DB_PATH,
    DEFAULT_GTFS_ROOT,
    DEFAULT_TIMEZONE,
    DIVERGING_METRICS,
    METRIC_LABELS,
    build_hourly_line_metrics_lazy,
    build_stop_heatmap_weights,
    build_stop_metrics_lazy,
    collect_filter_options,
    dashboard_cache_fingerprint,
    ensure_dashboard_cache,
    filter_observations_lazy,
    gtfs_stop_metadata_fingerprint,
    latest_gtfs_dir,
    load_stop_metadata,
    metric_label,
    rank_early_stops,
    rank_late_stops,
    scan_cached_observations,
    summarize_observations_lazy,
    summarize_stop_metadata_coverage_lazy,
)


st.set_page_config(
    page_title="Föli Bus Lateness (Polars)",
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
STOP_MARKER_MIN_SIZE = 9.0
STOP_MARKER_MAX_SIZE = 30.0
STOP_MARKER_HALO_PADDING = 5.0
STOP_MARKER_COLORBAR_TITLES = {
    "p90_delay_min": "P90 delay (min)",
    "median_delay_min": "Median delay (min)",
    "p75_delay_min": "P75 delay (min)",
    "p95_delay_min": "P95 delay (min)",
    "signed_mean_delay_min": "Signed mean (min)",
    "pct_over_3_min_late": ">3 min late (%)",
    "pct_over_5_min_late": ">5 min late (%)",
    "pct_early": "Early (%)",
    "pct_over_1_min_early": ">1 min early (%)",
    "pct_over_3_min_early": ">3 min early (%)",
    "bucket_count": "Buckets",
    "raw_poll_count": "Raw polls",
}
HEATMAP_SCALE_AUTO = "Auto"
HEATMAP_SCALE_MANUAL = "Manual maximum"
HEATMAP_AUTO_QUANTILE = 0.95
DELAY_SCALE_AUTO = "Auto"
DELAY_SCALE_MANUAL = "Manual range"
DELAY_AUTO_QUANTILE = 0.95


@st.cache_data(show_spinner="Loading filter options")
def cached_filter_options(cache_dir: str, cache_token: str) -> dict[str, object]:
    return collect_filter_options(scan_cached_observations(Path(cache_dir)))


@st.cache_data(show_spinner="Checking GTFS stop coverage")
def cached_gtfs_coverage(
    cache_dir: str,
    cache_token: str,
    gtfs_root: str,
    gtfs_fingerprint: str,
) -> dict[str, int]:
    stops = load_stop_metadata(gtfs_root=Path(gtfs_root))
    return summarize_stop_metadata_coverage_lazy(
        scan_cached_observations(Path(cache_dir)),
        stops,
    )


@st.cache_data(show_spinner="Summarizing selected observations")
def cached_summary(
    cache_dir: str,
    cache_token: str,
    start_date: date,
    end_date: date,
    line_refs: tuple[str, ...],
    direction_refs: tuple[str, ...],
    day_filter: str,
    start_time: time | None = None,
    end_time: time | None = None,
) -> dict[str, float | int]:
    filtered = filter_observations_lazy(
        scan_cached_observations(Path(cache_dir)),
        start_date=start_date,
        end_date=end_date,
        line_refs=line_refs,
        direction_refs=direction_refs,
        day_filter=day_filter,
        start_time=start_time,
        end_time=end_time,
    )
    return summarize_observations_lazy(filtered)


@st.cache_data(show_spinner="Building line-hour metrics")
def cached_hourly_metrics(
    cache_dir: str,
    cache_token: str,
    start_date: date,
    end_date: date,
    line_refs: tuple[str, ...],
    direction_refs: tuple[str, ...],
    day_filter: str,
    start_time: time,
    end_time: time,
    min_observations: int,
) -> pl.DataFrame:
    filtered = filter_observations_lazy(
        scan_cached_observations(Path(cache_dir)),
        start_date=start_date,
        end_date=end_date,
        line_refs=line_refs,
        direction_refs=direction_refs,
        day_filter=day_filter,
        start_time=start_time,
        end_time=end_time,
    )
    return build_hourly_line_metrics_lazy(
        filtered,
        min_observations=min_observations,
    )


@st.cache_data(show_spinner="Building stop metrics")
def cached_stop_metrics(
    cache_dir: str,
    cache_token: str,
    gtfs_root: str,
    gtfs_fingerprint: str,
    start_date: date,
    end_date: date,
    line_refs: tuple[str, ...],
    direction_refs: tuple[str, ...],
    day_filter: str,
    min_observations: int,
    start_time: time | None = None,
    end_time: time | None = None,
) -> pl.DataFrame:
    stops = load_stop_metadata(gtfs_root=Path(gtfs_root))
    filtered = filter_observations_lazy(
        scan_cached_observations(Path(cache_dir)),
        start_date=start_date,
        end_date=end_date,
        line_refs=line_refs,
        direction_refs=direction_refs,
        day_filter=day_filter,
        start_time=start_time,
        end_time=end_time,
    )
    return build_stop_metrics_lazy(
        filtered,
        stops,
        min_observations=min_observations,
    )


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


def _numeric_series(values: object) -> pl.Series:
    if isinstance(values, pl.Series):
        series = values
    elif hasattr(values, "ravel"):
        series = pl.Series("value", values.ravel().tolist())
    elif hasattr(values, "to_list"):
        series = pl.Series("value", values.to_list())
    else:
        try:
            series = pl.Series("value", list(values))
        except TypeError:
            series = pl.Series("value", [values])
    return series.cast(pl.Float64, strict=False)


def delay_color_range_extent(
    delay_values: object,
    scale_mode: str,
    manual_extent: float | None = None,
) -> float:
    values = _numeric_series(delay_values).abs().drop_nulls()
    values = values.filter(values > 0)

    if scale_mode == DELAY_SCALE_MANUAL:
        if manual_extent is not None and manual_extent > 0:
            return float(manual_extent)
        return 1.0

    if values.is_empty():
        return 1.0

    extent = values.quantile(DELAY_AUTO_QUANTILE, interpolation="linear")
    if extent is None or extent <= 0:
        extent = values.max()
    if extent is None or extent <= 0:
        return 1.0
    return float(extent)


def delay_color_scale_caption(scale_mode: str, extent: float) -> str:
    if scale_mode == DELAY_SCALE_MANUAL:
        return f"Delay color scale: manual range of +/- {extent:,.2f} min."
    return (
        "Delay color scale: auto range of "
        f"+/- {extent:,.2f} min from the 95th percentile."
    )


def make_hourly_heatmap(
    hourly: pl.DataFrame,
    metric_key: str,
    *,
    delay_extent: float | None = None,
) -> go.Figure:
    count_key = "bucket_count" if "bucket_count" in hourly.columns else "obs_count"
    order = hourly.group_by("line_ref").agg(
        pl.col(metric_key).mean().alias("sort_metric"),
        pl.col(count_key).sum().alias("total_buckets"),
    )
    if metric_key in ("bucket_count", "raw_poll_count", "obs_count"):
        order = order.sort("total_buckets", descending=True)
    else:
        order = order.sort(["sort_metric", "total_buckets"], descending=[True, True])
    ordered_lines = order["line_ref"].cast(pl.Utf8).to_list()

    value_lookup: dict[tuple[str, int], object] = {}
    count_lookup: dict[tuple[str, int], object] = {}
    for row in hourly.select("line_ref", "local_hour", metric_key, count_key).iter_rows(named=True):
        key = (str(row["line_ref"]), int(row["local_hour"]))
        value_lookup[key] = row[metric_key]
        count_lookup[key] = row[count_key]

    hours = list(range(24))
    values = [[value_lookup.get((line, hour)) for hour in hours] for line in ordered_lines]
    counts = [[count_lookup.get((line, hour)) for hour in hours] for line in ordered_lines]

    colorbar_title = metric_label(metric_key)
    heatmap_kwargs = {
        "z": values,
        "x": [f"{hour:02d}:00" for hour in hours],
        "y": ordered_lines,
        "customdata": counts,
        "colorscale": LATE_EARLY_SCALE
        if metric_key in DIVERGING_METRICS
        else SEQUENTIAL_SCALE,
        "colorbar": {"title": colorbar_title},
        "hovertemplate": (
            "Line %{y}<br>"
            "Hour %{x}<br>"
            f"{colorbar_title}: %{{z:.2f}}<br>"
            "Buckets: %{customdata:.0f}"
            "<extra></extra>"
        ),
    }
    if metric_key in DIVERGING_METRICS:
        extent = (
            delay_extent
            if delay_extent is not None
            else delay_color_range_extent([value for row in values for value in row], DELAY_SCALE_AUTO)
        )
        heatmap_kwargs["zmid"] = 0
        heatmap_kwargs["zmin"] = -extent
        heatmap_kwargs["zmax"] = extent

    fig = go.Figure(data=go.Heatmap(**heatmap_kwargs))
    fig.update_layout(
        height=min(1000, max(430, 18 * len(ordered_lines) + 150)),
        margin={"l": 80, "r": 20, "t": 30, "b": 45},
        xaxis_title="Local hour",
        yaxis_title="Line",
        template="plotly_white",
    )
    return fig


def scale_stop_marker_sizes(
    obs_counts: object,
    *,
    min_size: float = STOP_MARKER_MIN_SIZE,
    max_size: float = STOP_MARKER_MAX_SIZE,
) -> list[float]:
    counts = _numeric_series(obs_counts).fill_null(0).clip(lower_bound=0)
    if counts.is_empty():
        return []

    roots = counts.sqrt()
    root_min = roots.min()
    root_max = roots.max()
    if root_min == root_max:
        return [float((min_size + max_size) / 2)] * len(roots)

    scaled = min_size + (roots - root_min) / (root_max - root_min) * (max_size - min_size)
    return [float(value) for value in scaled.to_list()]


def stop_marker_colorbar_title(metric_key: str) -> str:
    return STOP_MARKER_COLORBAR_TITLES.get(metric_key, metric_label(metric_key))


def stop_marker_caption(map_df: pl.DataFrame, metric_key: str) -> str:
    buckets = int(map_df["bucket_count"].sum() or 0)
    raw_polls = int(map_df["raw_poll_count"].sum() or 0)
    if metric_key in DIVERGING_METRICS:
        color_text = "Color shows signed delay (blue = early, red = late)."
    else:
        color_text = f"Color shows {metric_label(metric_key).lower()}."
    return (
        "Showing one aggregated marker per mapped GTFS stop "
        f"({map_df.height:,} stops, {buckets:,} buckets from {raw_polls:,} raw polls). "
        f"{color_text} Size shows bucket count. "
        "Stop markers use the selected date, line, direction, day, and time filters."
    )


def make_stop_map(
    stop_metrics: pl.DataFrame,
    metric_key: str,
    *,
    delay_extent: float | None = None,
) -> go.Figure:
    map_df = stop_metrics.drop_nulls(subset=["stop_lat", "stop_lon"])
    center = {
        "lat": float(map_df["stop_lat"].mean()),
        "lon": float(map_df["stop_lon"].mean()),
    }
    marker_sizes = scale_stop_marker_sizes(map_df["bucket_count"])
    halo_sizes = [size + STOP_MARKER_HALO_PADDING for size in marker_sizes]
    color_values = _numeric_series(map_df[metric_key]).to_list()
    colorbar_title = stop_marker_colorbar_title(metric_key)
    marker_style = {
        "size": marker_sizes,
        "color": color_values,
        "colorscale": LATE_EARLY_SCALE
        if metric_key in DIVERGING_METRICS
        else SEQUENTIAL_SCALE,
        "opacity": 0.95,
        "colorbar": {
            "title": {"text": colorbar_title},
            "ticks": "outside",
        },
    }
    if metric_key in DIVERGING_METRICS:
        color_extent = (
            delay_extent
            if delay_extent is not None
            else delay_color_range_extent(color_values, DELAY_SCALE_AUTO)
        )
        marker_style.update(
            {
                "cmin": -color_extent,
                "cmax": color_extent,
                "cmid": 0,
            }
        )

    customdata = map_df.select(
        [
            "stop_id",
            "bucket_count",
            "raw_poll_count",
            "line_count",
            "median_delay_min",
            "p90_delay_min",
            "pct_over_3_min_late",
            "pct_over_5_min_late",
            "pct_over_3_min_early",
        ]
    ).to_numpy()
    fig = go.Figure()
    fig.add_trace(
        go.Scattermapbox(
            lat=map_df["stop_lat"].to_list(),
            lon=map_df["stop_lon"].to_list(),
            mode="markers",
            marker={
                "size": halo_sizes,
                "color": "rgba(6, 9, 15, 0.95)",
                "opacity": 1.0,
            },
            hoverinfo="skip",
            showlegend=False,
        )
    )
    fig.add_trace(
        go.Scattermapbox(
            lat=map_df["stop_lat"].to_list(),
            lon=map_df["stop_lon"].to_list(),
            mode="markers",
            text=map_df["stop_name"].to_list(),
            customdata=customdata,
            marker=marker_style,
            hovertemplate=(
                "<b>%{text}</b><br>"
                "Stop ID: %{customdata[0]}<br>"
                "Buckets: %{customdata[1]:,.0f}<br>"
                "Raw polls: %{customdata[2]:,.0f}<br>"
                "Lines: %{customdata[3]:,.0f}<br>"
                "Median delay (min): %{customdata[4]:.2f}<br>"
                "P90 delay (min): %{customdata[5]:.2f}<br>"
                "Over 3 min late (%): %{customdata[6]:.1f}<br>"
                "Over 5 min late (%): %{customdata[7]:.1f}<br>"
                "Over 3 min early (%): %{customdata[8]:.1f}"
                "<extra></extra>"
            ),
            showlegend=False,
        )
    )
    fig.update_layout(
        height=650,
        template="plotly_dark",
        paper_bgcolor="rgba(0, 0, 0, 0)",
        plot_bgcolor="rgba(0, 0, 0, 0)",
        font={"color": "#f4f4f5"},
        mapbox={
            "style": "carto-darkmatter",
            "center": center,
            "zoom": 9,
        },
        margin={"l": 0, "r": 70, "t": 20, "b": 0},
    )
    return fig


def heatmap_weight_label(metric_key: str, delay_direction: str = "late") -> str:
    if metric_key in DIVERGING_METRICS:
        if delay_direction == "early":
            return "Early-running intensity"
        return "Late delay intensity"
    if metric_key == "pct_over_3_min_late":
        return "Estimated >3 min late observations"
    if metric_key == "pct_over_5_min_late":
        return "Estimated >5 min late buckets"
    if metric_key.startswith("pct_"):
        return f"Estimated {metric_label(metric_key).lower()}"
    return metric_label(metric_key)


def heatmap_intensity_max(
    heat_weights: object,
    scale_mode: str,
    manual_max: float | None = None,
) -> float | None:
    weights = _numeric_series(heat_weights).drop_nulls()
    weights = weights.filter(weights > 0)
    if weights.is_empty():
        return None

    if scale_mode == HEATMAP_SCALE_MANUAL:
        if manual_max is None or manual_max <= 0:
            return None
        return float(manual_max)

    auto_max = weights.quantile(HEATMAP_AUTO_QUANTILE, interpolation="linear")
    if auto_max is None or auto_max <= 0:
        auto_max = weights.max()
    if auto_max is None or auto_max <= 0:
        return None
    return float(auto_max)


def heatmap_scale_caption(
    heat_weights: object,
    scale_mode: str,
    intensity_max: float | None,
) -> str:
    weights = _numeric_series(heat_weights).drop_nulls()
    weights = weights.filter(weights > 0)
    if weights.is_empty() or intensity_max is None:
        return "Heatmap scale: no positive heat values in the current filters."

    actual_max = float(weights.max())
    if scale_mode == HEATMAP_SCALE_MANUAL:
        return (
            f"Manual scale: capped at {intensity_max:,.2f}. "
            f"Current maximum is {actual_max:,.2f}."
        )
    return (
        f"Auto scale: capped at 95th percentile ({intensity_max:,.2f}). "
        f"Current maximum is {actual_max:,.2f}."
    )


def make_stop_heatmap(
    stop_metrics: pl.DataFrame,
    metric_key: str,
    *,
    delay_direction: str = "late",
    max_intensity: float | None = None,
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
            "median_delay_min": ":.2f",
            "p90_delay_min": ":.2f",
            "pct_over_3_min_late": ":.1f",
            "pct_over_5_min_late": ":.1f",
            "pct_over_3_min_early": ":.1f",
            "bucket_count": True,
            "raw_poll_count": True,
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
        range_color=[0, max_intensity] if max_intensity is not None else None,
    )
    fig.update_layout(
        mapbox_style="carto-positron",
        margin={"l": 0, "r": 0, "t": 20, "b": 0},
        coloraxis_colorbar_title=weight_label,
    )
    return fig


def table_columns(df: pl.DataFrame) -> pl.DataFrame:
    columns = [
        "stop_id",
        "stop_name",
        "bucket_count",
        "raw_poll_count",
        "line_count",
        "median_delay_min",
        "p90_delay_min",
        "pct_over_3_min_late",
        "pct_over_5_min_late",
        "pct_over_3_min_early",
    ]
    return df.select(columns).rename(
        {
            "stop_id": "Stop ID",
            "stop_name": "Stop",
            "bucket_count": "Buckets",
            "raw_poll_count": "Raw polls",
            "line_count": "Lines",
            "median_delay_min": "Median delay (min)",
            "p90_delay_min": "P90 delay (min)",
            "pct_over_3_min_late": "Over 3 min late (%)",
            "pct_over_5_min_late": "Over 5 min late (%)",
            "pct_over_3_min_early": "Over 3 min early (%)",
        }
    )


def main() -> None:
    st.title("Föli Bus Lateness (Polars)")
    st.info(
        "Data caveats: SIRI VM delay is estimated vehicle state, not actual "
        "arrival truth. The dashboard uses conservative quality filtering and "
        "trip-stop buckets by default so repeated 30-second polls do not dominate "
        "the metrics. Treat extreme, stale, pre-trip, and post-trip values as "
        "diagnostic data before drawing operational conclusions."
    )

    gtfs_dir = latest_gtfs_dir()
    if gtfs_dir is None:
        st.error("No GTFS stops.txt found below data/gtfs.")
        st.stop()
    if not DEFAULT_DB_PATH.exists():
        st.error("Database not found at data/foli.db.")
        st.stop()

    try:
        gtfs_fingerprint = gtfs_stop_metadata_fingerprint(DEFAULT_GTFS_ROOT)
        progress_box = st.empty()
        cache_result = ensure_dashboard_cache(
            DEFAULT_DB_PATH,
            timezone=DEFAULT_TIMEZONE,
            progress=lambda message: progress_box.info(message),
        )
        progress_box.empty()
        cache_dir = str(cache_result.cache_db)
        cache_token = dashboard_cache_fingerprint(cache_result)
        options = cached_filter_options(cache_dir, cache_token)
        coverage = cached_gtfs_coverage(
            cache_dir,
            cache_token,
            str(DEFAULT_GTFS_ROOT),
            gtfs_fingerprint,
        )
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.stop()
    min_date = options["min_date"]
    max_date = options["max_date"]
    if min_date is None or max_date is None:
        st.warning("No analysis-ready observations found.")
        st.stop()

    unmatched_gtfs_count = coverage["unmatched_gtfs_count"]
    if unmatched_gtfs_count:
        st.warning(
            f"{unmatched_gtfs_count:,} of {coverage['bucket_count']:,} buckets do not have "
            "date-matched GTFS stop metadata. Those rows keep SIRI stop names and "
            "are omitted from coordinate-based maps."
        )

    line_options = sorted(
        options["line_options"],
        key=route_sort_key,
    )
    direction_options = sorted(
        options["direction_options"],
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
        delay_scale_mode = DELAY_SCALE_AUTO
        manual_delay_extent = None
        if metric_key in DIVERGING_METRICS:
            delay_scale_mode = st.selectbox(
                "Delay color scale",
                [DELAY_SCALE_AUTO, DELAY_SCALE_MANUAL],
            )
            if delay_scale_mode == DELAY_SCALE_MANUAL:
                manual_delay_extent = st.number_input(
                    "Delay color range (+/- min)",
                    min_value=0.01,
                    value=5.0,
                    step=0.5,
                    format="%.2f",
                )
        min_observations = st.number_input(
            "Minimum observations per group",
            min_value=1,
            max_value=10000,
            value=30,
            step=10,
        )
        selected_start_time, selected_end_time = st.slider(
            "Map and heatmap time range",
            min_value=time(0, 0),
            max_value=time(23, 59),
            value=(time(0, 0), time(23, 59)),
            step=timedelta(minutes=30),
            format="HH:mm",
        )
        heatmap_scale_mode = st.selectbox(
            "Heatmap scale",
            [HEATMAP_SCALE_AUTO, HEATMAP_SCALE_MANUAL],
        )
        manual_heatmap_max = None
        if heatmap_scale_mode == HEATMAP_SCALE_MANUAL:
            manual_heatmap_max = st.number_input(
                "Heatmap max intensity",
                min_value=0.01,
                value=1000.0,
                step=100.0,
                format="%.2f",
            )

    selected_lines_tuple = tuple(str(line_ref) for line_ref in selected_lines)
    selected_directions_tuple = tuple(
        str(direction_ref) for direction_ref in selected_directions
    )
    summary = cached_summary(
        cache_dir,
        cache_token,
        start_date=start_date,
        end_date=end_date,
        line_refs=selected_lines_tuple,
        direction_refs=selected_directions_tuple,
        day_filter=day_filter,
    )
    if summary["bucket_count"] == 0:
        st.warning("No observations match the selected filters.")
        st.stop()

    heatmap_summary = cached_summary(
        cache_dir,
        cache_token,
        start_date=start_date,
        end_date=end_date,
        line_refs=selected_lines_tuple,
        direction_refs=selected_directions_tuple,
        day_filter=day_filter,
        start_time=selected_start_time,
        end_time=selected_end_time,
    )

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Buckets", f"{summary['bucket_count']:,}")
    col2.metric("Lines", f"{summary['line_count']:,}")
    col3.metric("Stops", f"{summary['stop_count']:,}")
    col4.metric("Median delay", f"{summary['median_delay_min']:.2f} min")
    col5.metric(">5 min late", f"{summary['pct_over_5_min_late']:.1f}%")

    hourly = cached_hourly_metrics(
        cache_dir,
        cache_token,
        start_date=start_date,
        end_date=end_date,
        line_refs=selected_lines_tuple,
        direction_refs=selected_directions_tuple,
        day_filter=day_filter,
        start_time=selected_start_time,
        end_time=selected_end_time,
        min_observations=int(min_observations),
    )
    stop_metrics = cached_stop_metrics(
        cache_dir,
        cache_token,
        str(DEFAULT_GTFS_ROOT),
        gtfs_fingerprint,
        start_date=start_date,
        end_date=end_date,
        line_refs=selected_lines_tuple,
        direction_refs=selected_directions_tuple,
        day_filter=day_filter,
        min_observations=int(min_observations),
    )
    heatmap_stop_metrics = cached_stop_metrics(
        cache_dir,
        cache_token,
        str(DEFAULT_GTFS_ROOT),
        gtfs_fingerprint,
        start_date=start_date,
        end_date=end_date,
        line_refs=selected_lines_tuple,
        direction_refs=selected_directions_tuple,
        day_filter=day_filter,
        min_observations=int(min_observations),
        start_time=selected_start_time,
        end_time=selected_end_time,
    )

    st.subheader("Line By Hour")
    if heatmap_summary["bucket_count"] == 0:
        st.info("No observations match the selected map and heatmap time range.")
    elif hourly.is_empty():
        st.info("No line-hour groups meet the minimum observation threshold.")
    else:
        hourly_delay_extent = None
        if metric_key in DIVERGING_METRICS:
            hourly_delay_extent = delay_color_range_extent(
                hourly[metric_key],
                delay_scale_mode,
                manual_delay_extent,
            )
            st.caption(delay_color_scale_caption(delay_scale_mode, hourly_delay_extent))
        st.plotly_chart(
            make_hourly_heatmap(
                hourly,
                metric_key,
                delay_extent=hourly_delay_extent,
            ),
            use_container_width=True,
        )

    st.subheader("Stops")
    if stop_metrics.is_empty():
        st.info("No stops meet the minimum observation threshold.")
    else:
        marker_tab, heatmap_tab = st.tabs(["Stop markers", "Delay heatmap"])
        with marker_tab:
            if heatmap_summary["bucket_count"] == 0:
                st.info("No observations match the selected map and heatmap time range.")
            elif heatmap_stop_metrics.is_empty():
                st.info(
                    "No stops in the selected time range meet the minimum "
                    "observation threshold."
                )
            else:
                map_df = heatmap_stop_metrics.drop_nulls(subset=["stop_lat", "stop_lon"])
                if map_df.is_empty():
                    st.info(
                        "No mapped stops in the selected time range meet the "
                        "minimum observation threshold."
                    )
                else:
                    st.caption(stop_marker_caption(map_df, metric_key))
                    stop_delay_extent = None
                    if metric_key in DIVERGING_METRICS:
                        stop_delay_extent = delay_color_range_extent(
                            map_df[metric_key],
                            delay_scale_mode,
                            manual_delay_extent,
                        )
                        st.caption(
                            delay_color_scale_caption(
                                delay_scale_mode,
                                stop_delay_extent,
                            )
                        )
                    st.plotly_chart(
                        make_stop_map(
                            map_df,
                            metric_key,
                            delay_extent=stop_delay_extent,
                        ),
                        use_container_width=True,
                    )
        with heatmap_tab:
            if heatmap_summary["bucket_count"] == 0:
                st.info("No observations match the selected map and heatmap time range.")
            elif heatmap_stop_metrics.is_empty():
                st.info(
                    "No stops in the selected time range meet the "
                    "minimum observation threshold."
                )
            else:
                heatmap_map_df = heatmap_stop_metrics.drop_nulls(
                    subset=["stop_lat", "stop_lon"]
                )
                if heatmap_map_df.is_empty():
                    st.info(
                        "No mapped stops in the selected time range meet the "
                        "minimum observation threshold."
                    )
                elif metric_key in DIVERGING_METRICS:
                    late_heatmap_tab, early_heatmap_tab = st.tabs(
                        ["Late heatmap", "Early heatmap"]
                    )
                    with late_heatmap_tab:
                        late_heat = build_stop_heatmap_weights(
                            heatmap_map_df,
                            metric_key,
                            delay_direction="late",
                        )
                        if late_heat.is_empty():
                            st.info(
                                "No late delay hotspots meet the current heatmap "
                                "filters."
                            )
                        else:
                            max_intensity = heatmap_intensity_max(
                                late_heat["heat_weight"],
                                heatmap_scale_mode,
                                manual_heatmap_max,
                            )
                            st.caption(
                                heatmap_scale_caption(
                                    late_heat["heat_weight"],
                                    heatmap_scale_mode,
                                    max_intensity,
                                )
                            )
                            st.plotly_chart(
                                make_stop_heatmap(
                                    late_heat,
                                    metric_key,
                                    delay_direction="late",
                                    max_intensity=max_intensity,
                                ),
                                use_container_width=True,
                            )
                    with early_heatmap_tab:
                        early_heat = build_stop_heatmap_weights(
                            heatmap_map_df,
                            metric_key,
                            delay_direction="early",
                        )
                        if early_heat.is_empty():
                            st.info(
                                "No early-running hotspots meet the current "
                                "heatmap filters."
                            )
                        else:
                            max_intensity = heatmap_intensity_max(
                                early_heat["heat_weight"],
                                heatmap_scale_mode,
                                manual_heatmap_max,
                            )
                            st.caption(
                                heatmap_scale_caption(
                                    early_heat["heat_weight"],
                                    heatmap_scale_mode,
                                    max_intensity,
                                )
                            )
                            st.plotly_chart(
                                make_stop_heatmap(
                                    early_heat,
                                    metric_key,
                                    delay_direction="early",
                                    max_intensity=max_intensity,
                                ),
                                use_container_width=True,
                            )
                else:
                    heat = build_stop_heatmap_weights(heatmap_map_df, metric_key)
                    if heat.is_empty():
                        st.info("No heatmap hotspots meet the current filters.")
                    else:
                        max_intensity = heatmap_intensity_max(
                            heat["heat_weight"],
                            heatmap_scale_mode,
                            manual_heatmap_max,
                        )
                        st.caption(
                            heatmap_scale_caption(
                                heat["heat_weight"],
                                heatmap_scale_mode,
                                max_intensity,
                            )
                        )
                        st.plotly_chart(
                            make_stop_heatmap(
                                heat,
                                metric_key,
                                max_intensity=max_intensity,
                            ),
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
