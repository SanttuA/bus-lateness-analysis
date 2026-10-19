from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections.abc import Iterable, Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path = [
    entry
    for entry in sys.path
    if not entry or Path(entry).resolve() != SCRIPT_DIR
]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Polars allocates through jemalloc, which keeps freed pages for a while. Returning them
# quickly keeps the multi-stage cache build near its per-stage peak (~2 GB instead of
# ~5 GB on the full database). Must be set before polars is first imported.
os.environ.setdefault(
    "_RJEM_MALLOC_CONF",
    "background_thread:true,dirty_decay_ms:500,muzzy_decay_ms:500",
)

import polars as pl  # noqa: E402

from analysis.polars.report_cache import (  # noqa: E402
    DEFAULT_CACHE_DIR,
    ReportSettings,
    ensure_report_cache,
    read_result_table,
    round_numeric_lazy,
    summarize_delay_metrics_lazy,
)
from analysis.uncertainty import line_ranking_intervals  # noqa: E402
from dashboard_data_polars import load_stop_metadata, scan_cached_observations  # noqa: E402


DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "site" / "public" / "data"
MIN_BUCKETS = 30
MIN_TRIPS = 10
FREQUENT_TRIPS_PER_DAY = 20
FULL_COVERAGE_PCT = 95.0
ALL_VALUE = "all"
SCHEMA_VERSION = 2
FORBIDDEN_PUBLIC_KEYS = {
    "vehicle_id",
    "trip_id",
    "trip_match_key",
    "bucket_id",
    "poll_id",
    "source_alert_id",
    "alert_key",
    "recorded_at_utc",
    "representative_time_utc",
}
LINE_COLUMNS = [
    "line_ref",
    "line_name",
    "bucket_count",
    "trip_count",
    "day_count",
    "trips_per_day",
    "median_delay_min",
    "p90_delay_min",
    "p90_delay_ci_lo",
    "p90_delay_ci_hi",
    "pct_on_time",
    "pct_over_3_min_late",
    "pct_over_5_min_late",
    "pct_over_5_min_late_ci_lo",
    "pct_over_5_min_late_ci_hi",
    "pct_over_1_min_early",
    "pct_over_3_min_early",
    "pct_over_3_min_early_ci_lo",
    "pct_over_3_min_early_ci_hi",
    "excluded_pct",
    "data_fault_pct",
    "trip_coverage_pct",
    "low_quality",
]
CONTEXT_COLUMNS = [
    "line_ref",
    "direction_ref",
    "day_type",
    "local_hour",
    "bucket_count",
    "trip_count",
    "median_delay_min",
    "p90_delay_min",
    "pct_on_time",
    "pct_over_5_min_late",
    "pct_over_3_min_early",
]
HOURLY_COLUMNS = [
    "day_type",
    "local_hour",
    "bucket_count",
    "trip_count",
    "median_delay_min",
    "p75_delay_min",
    "p90_delay_min",
    "pct_on_time",
    "pct_over_5_min_late",
    "pct_over_3_min_early",
]
STOP_METRIC_COLUMNS = [
    "stop_id",
    "line_ref",
    "day_type",
    "bucket_count",
    "median_delay_min",
    "p90_delay_min",
    "pct_on_time",
    "pct_over_5_min_late",
    "pct_over_3_min_early",
]
SUMMARY_COLUMNS = [
    "bucket_count",
    "raw_poll_count",
    "trip_count",
    "day_count",
    "median_delay_min",
    "p75_delay_min",
    "p90_delay_min",
    "p95_delay_min",
    "pct_on_time",
    "pct_late",
    "pct_over_3_min_late",
    "pct_over_5_min_late",
    "pct_early",
    "pct_over_1_min_early",
    "pct_over_3_min_early",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic aggregate JSON for the public GitHub Pages report."
    )
    parser.add_argument("--db", type=Path, default=Path("data/foli.db"))
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--gtfs-root", type=Path, default=Path("data/gtfs"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--min-buckets", type=int, default=MIN_BUCKETS)
    parser.add_argument(
        "--min-trips",
        type=int,
        default=MIN_TRIPS,
        help="Minimum distinct trips behind any published line or line-hour group.",
    )
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def _print_progress(message: str) -> None:
    print(f"[public-data] {message}", flush=True)


def _json_value(value: object) -> object:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float):
        return None if math.isnan(value) else round(value, 2)
    return value


def _minute_timestamp(value: object) -> object:
    if value is None:
        return value
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    return parsed.replace(second=0, microsecond=0).isoformat(timespec="minutes")


def frame_records(frame: pl.DataFrame) -> list[dict[str, object]]:
    return [
        {key: _json_value(value) for key, value in row.items()}
        for row in frame.to_dicts()
    ]


def write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _read_table(cache_dir: Path, name: str) -> pl.DataFrame:
    return read_result_table(cache_dir, name)


def _metric_rows(
    buckets: pl.LazyFrame,
    keys: list[str],
    *,
    min_buckets: int,
    min_trips: int = 1,
    line_name: bool = False,
) -> pl.LazyFrame:
    extra = {"line_name": ("published_line_name", "first")} if line_name else None
    return round_numeric_lazy(
        summarize_delay_metrics_lazy(
            buckets,
            keys,
            min_observations=min_buckets,
            min_trips=min_trips,
            extra_aggs=extra,
        )
    )


# --- Text helpers ---------------------------------------------------------------------


def _number(value: float, language: str, digits: int = 1) -> str:
    text = f"{value:.{digits}f}"
    return text.replace(".", ",") if language == "fi" else text


def _percent(value: float, language: str, digits: int = 1) -> str:
    return f"{_number(value, language, digits)} %" if language == "fi" else f"{_number(value, language, digits)}%"


def _interval(low: object, high: object, language: str, *, percent: bool) -> str:
    if low is None or high is None:
        return ""
    fmt = _percent if percent else _number
    label = "95 %:n luottamusväli" if language == "fi" else "95% interval"
    return f" ({label} {fmt(float(low), language)}–{fmt(float(high), language)})"


def _join(items: list[str], language: str) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    word = " ja " if language == "fi" else " and "
    return ", ".join(items[:-1]) + word + items[-1]


# --- Lines ----------------------------------------------------------------------------


def _direction_labels(buckets: pl.LazyFrame) -> dict[str, dict[str, str]]:
    labels = (
        buckets.filter(pl.col("destination_name").is_not_null())
        .group_by(["line_ref", "direction_ref", "destination_name"])
        .agg(pl.len().alias("count"))
        .sort(["line_ref", "direction_ref", "count", "destination_name"], descending=[False, False, True, False])
        .group_by(["line_ref", "direction_ref"], maintain_order=True)
        .agg(pl.col("destination_name").first())
        .collect()
    )
    result: dict[str, dict[str, str]] = {}
    for row in labels.iter_rows(named=True):
        result.setdefault(str(row["line_ref"]), {})[str(row["direction_ref"])] = row["destination_name"]
    return result


def _line_intervals(buckets: pl.LazyFrame, line_refs: list[str]) -> pl.DataFrame:
    """Trip-cluster intervals for every published line, one partition at a time."""
    data = (
        buckets.filter(pl.col("line_ref").is_in(pl.lit(pl.Series(line_refs, dtype=pl.Utf8)).implode()))
        .select("line_ref", "trip_match_key", "delay_seconds")
        .collect()
    )
    rows = []
    for (line_ref,), frame in data.partition_by("line_ref", as_dict=True).items():
        trips = frame["trip_match_key"].rank("dense").cast(pl.Int64).to_numpy()
        delay = frame["delay_seconds"].to_numpy()
        rows.append(
            {
                "line_ref": line_ref,
                **line_ranking_intervals("late", line_ref, trips, delay),
                **line_ranking_intervals("early", line_ref, trips, delay),
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame({"line_ref": []})


def build_line_payload(
    buckets: pl.LazyFrame,
    *,
    min_buckets: int,
    min_trips: int = 1,
    line_quality: pl.DataFrame | None = None,
) -> dict[str, object]:
    lines = (
        _metric_rows(buckets, ["line_ref"], min_buckets=min_buckets, min_trips=min_trips, line_name=True)
        .with_columns((pl.col("trip_count") / pl.col("day_count")).round(1).alias("trips_per_day"))
        .sort("line_ref")
        .collect()
    )
    if not lines.is_empty():
        lines = lines.join(
            _line_intervals(buckets, lines["line_ref"].to_list()),
            on="line_ref",
            how="left",
        )
    quality = (
        line_quality.select(
            "line_ref",
            pl.col("conservative_excluded_pct").alias("excluded_pct"),
            "data_fault_pct",
            "trip_coverage_pct",
            "low_quality",
        )
        if line_quality is not None and not line_quality.is_empty()
        else pl.DataFrame(
            schema={
                "line_ref": pl.Utf8,
                "excluded_pct": pl.Float64,
                "data_fault_pct": pl.Float64,
                "trip_coverage_pct": pl.Float64,
                "low_quality": pl.Boolean,
            }
        )
    )
    lines = lines.join(quality, on="line_ref", how="left").with_columns(
        pl.col("low_quality").fill_null(False)
    )
    lines = lines.select([column for column in LINE_COLUMNS if column in lines.columns])
    labels = _direction_labels(buckets)
    line_records = frame_records(lines)
    for record in line_records:
        record["direction_labels"] = labels.get(str(record["line_ref"]), {})
    contexts = (
        _metric_rows(
            buckets,
            ["line_ref", "direction_ref", "day_type", "local_hour"],
            min_buckets=min_buckets,
            min_trips=min_trips,
        )
        .select(CONTEXT_COLUMNS)
        .sort(["line_ref", "direction_ref", "day_type", "local_hour"])
        .collect()
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "lines": line_records,
        "contexts": frame_records(contexts),
    }


# --- Stops ----------------------------------------------------------------------------


def build_canonical_stops(
    stops: pl.DataFrame,
    buckets: pl.LazyFrame,
) -> pl.DataFrame:
    stop_columns = ["stop_id", "gtfs_stop_name", "stop_lat", "stop_lon"]
    if stops.is_empty():
        metadata = pl.DataFrame(
            schema={
                "stop_id": pl.Utf8,
                "stop_name": pl.Utf8,
                "stop_lat": pl.Float64,
                "stop_lon": pl.Float64,
            }
        )
    else:
        metadata = stops.select(
            *(["gtfs_feed_date"] if "gtfs_feed_date" in stops.columns else []),
            *stop_columns,
        )
        if "gtfs_feed_date" in metadata.columns:
            metadata = metadata.sort(
                ["stop_id", "gtfs_feed_date"], descending=[False, True]
            )
        metadata = (
            metadata.unique("stop_id", keep="first", maintain_order=True)
            .rename({"gtfs_stop_name": "stop_name"})
            .select("stop_id", "stop_name", "stop_lat", "stop_lon")
        )

    fallbacks = (
        buckets.filter(pl.col("next_stop_point_ref").is_not_null())
        .group_by(pl.col("next_stop_point_ref").cast(pl.Utf8).alias("stop_id"))
        .agg(
            pl.col("next_stop_point_name")
            .drop_nulls()
            .sort_by("representative_time_utc")
            .last()
            .alias("fallback_name"),
            pl.col("line_ref").n_unique().alias("line_count"),
        )
        .sort("stop_id")
        .collect()
    )
    return (
        fallbacks.join(metadata, on="stop_id", how="left")
        .with_columns(
            pl.coalesce("stop_name", "fallback_name", "stop_id").alias("stop_name"),
            pl.col("stop_lat").cast(pl.Float64, strict=False),
            pl.col("stop_lon").cast(pl.Float64, strict=False),
        )
        .drop("fallback_name")
        .sort("stop_id")
    )


def _stop_metric_variant(
    buckets: pl.LazyFrame,
    keys: list[str],
    *,
    min_buckets: int,
    line_value: str | None = None,
    day_value: str | None = None,
) -> pl.LazyFrame:
    metrics = _metric_rows(buckets, keys, min_buckets=min_buckets)
    if line_value is not None:
        metrics = metrics.with_columns(pl.lit(line_value).alias("line_ref"))
    if day_value is not None:
        metrics = metrics.with_columns(pl.lit(day_value).alias("day_type"))
    return metrics.select(STOP_METRIC_COLUMNS)


def build_stop_payload(
    buckets: pl.LazyFrame,
    stops: pl.DataFrame,
    *,
    min_buckets: int,
) -> dict[str, object]:
    """Stop metrics; every bucket at one stop is a different trip, so buckets = trips."""
    stop_buckets = buckets.filter(pl.col("next_stop_point_ref").is_not_null()).with_columns(
        pl.col("next_stop_point_ref").cast(pl.Utf8).alias("stop_id")
    )
    variants = [
        _stop_metric_variant(
            stop_buckets,
            ["stop_id"],
            min_buckets=min_buckets,
            line_value=ALL_VALUE,
            day_value=ALL_VALUE,
        ),
        _stop_metric_variant(
            stop_buckets,
            ["stop_id", "day_type"],
            min_buckets=min_buckets,
            line_value=ALL_VALUE,
        ),
        _stop_metric_variant(
            stop_buckets,
            ["stop_id", "line_ref"],
            min_buckets=min_buckets,
            day_value=ALL_VALUE,
        ),
        _stop_metric_variant(
            stop_buckets,
            ["stop_id", "line_ref", "day_type"],
            min_buckets=min_buckets,
        ),
    ]
    metrics = pl.concat(variants).sort(["stop_id", "line_ref", "day_type"]).collect()
    canonical = build_canonical_stops(stops, buckets)
    valid_stop_ids = metrics.select("stop_id").unique()
    canonical = canonical.join(valid_stop_ids, on="stop_id", how="semi")
    if canonical["stop_id"].n_unique() != canonical.height:
        raise ValueError("Canonical public stop metadata must contain one row per stop_id")
    return {
        "schema_version": SCHEMA_VERSION,
        "stops": frame_records(canonical),
        "metrics": frame_records(metrics),
    }


# --- Overview text --------------------------------------------------------------------


def _eligible_lines(lines: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [
        line
        for line in lines
        if not line.get("low_quality") and float(line.get("trips_per_day") or 0) >= FREQUENT_TRIPS_PER_DAY
    ]


def _top(lines: list[Mapping[str, Any]], metric: str) -> list[Mapping[str, Any]]:
    return sorted(lines, key=lambda line: (-float(line.get(metric) or 0), str(line["line_name"])))


def _overview_takeaways(
    lines: list[Mapping[str, Any]],
    summary: Mapping[str, Any],
    hourly: list[Mapping[str, Any]],
    rush: list[Mapping[str, Any]],
) -> list[dict[str, str]]:
    frequent = _eligible_lines(lines)
    takeaways: list[dict[str, str]] = []
    if frequent:
        late = _top(frequent, "pct_over_5_min_late")[0]
        by_p90 = _top([line for line in lines if not line.get("low_quality")], "p90_delay_min")
        texts = {}
        for language in ("fi", "en"):
            share = _percent(float(late["pct_over_5_min_late"]), language)
            interval = _interval(late.get("pct_over_5_min_late_ci_lo"), late.get("pct_over_5_min_late_ci_hi"), language, percent=True)
            if language == "fi":
                text = (
                    f"Säännöllisistä linjoista (vähintään {FREQUENT_TRIPS_PER_DAY} vuoroa päivässä) "
                    f"linja {late['line_name']} on useimmin yli 5 minuuttia myöhässä: {share} "
                    f"pysäkkiohituksista{interval}."
                )
            else:
                text = (
                    f"Among frequent lines (at least {FREQUENT_TRIPS_PER_DAY} trips a day), line "
                    f"{late['line_name']} is most often more than 5 minutes late: {share} of stop "
                    f"passes{interval}."
                )
            if by_p90 and by_p90[0]["line_ref"] != late["line_ref"]:
                worst = by_p90[0]
                trips = _number(float(worst["trips_per_day"]), language)
                text += (
                    f" Korkein p90-viive on linjalla {worst['line_name']}, mutta se ajaa vain noin {trips} vuoroa päivässä."
                    if language == "fi"
                    else f" The highest p90 delay is on line {worst['line_name']}, which runs only about {trips} trips a day."
                )
            texts[language] = text
        takeaways.append({"id": "late-lines", **texts})

    reliable = [line for line in lines if not line.get("low_quality")]
    flagged = len(lines) - len(reliable)
    if frequent:
        early = _top(frequent, "pct_over_3_min_early")[0]
        extreme = _top(reliable, "pct_over_3_min_early")[0]
        texts = {}
        for language in ("fi", "en"):
            share = _percent(float(early["pct_over_3_min_early"]), language)
            interval = _interval(early.get("pct_over_3_min_early_ci_lo"), early.get("pct_over_3_min_early_ci_hi"), language, percent=True)
            text = (
                f"Säännöllisistä linjoista linja {early['line_name']} kulkee useimmin etuajassa: {share} "
                f"pysäkkiohituksista on yli 3 minuuttia aikataulua edellä{interval}."
                if language == "fi"
                else f"Among frequent lines, line {early['line_name']} runs ahead of schedule most often: "
                f"{share} of stop passes are more than 3 minutes early{interval}."
            )
            if extreme["line_ref"] != early["line_ref"]:
                extreme_share = _percent(float(extreme["pct_over_3_min_early"]), language)
                trips = _number(float(extreme["trips_per_day"]), language)
                text += (
                    f" Kaikista linjoista äärimmäisin on {extreme['line_name']} ({extreme_share}), "
                    f"mutta se ajaa vain noin {trips} vuoroa päivässä."
                    if language == "fi"
                    else f" Across all lines the most extreme is {extreme['line_name']} ({extreme_share}), "
                    f"which runs only about {trips} trips a day."
                )
            text += (
                f" {flagged} linjaa, joiden data on puutteellista, on jätetty vertailun ulkopuolelle."
                if language == "fi"
                else f" {flagged} lines with incomplete data are left out of this comparison."
            )
            texts[language] = text
        takeaways.append({"id": "early-lines", **texts})

    weekday = [row for row in hourly if row["day_type"] == "weekday" and int(row["bucket_count"]) >= 1000]
    if weekday:
        peak = max(weekday, key=lambda row: (float(row["pct_over_5_min_late"]), -int(row["local_hour"])))
        hour = f"{int(peak['local_hour']):02d}"
        texts = {}
        for language in ("fi", "en"):
            on_time = _percent(float(summary["pct_on_time"]), language)
            peak_share = _percent(float(peak["pct_over_5_min_late"]), language)
            text = (
                f"Koko verkossa {on_time} pysäkkiohituksista on aikataulussa (1 min etuajassa – 3 min "
                f"myöhässä). Arkisin myöhästymiset kasautuvat klo {hour}, jolloin {peak_share} on yli 5 min myöhässä."
                if language == "fi"
                else f"Across the network, {on_time} of stop passes are on time (1 min early to 3 min "
                f"late). On weekdays late running peaks at {hour}:00, when {peak_share} are more than 5 min late."
            )
            if rush:
                leader = rush[0]
                lift = _number(float(leader["rush_over_5_min_late_pct_point_lift"]), language)
                text += (
                    f" Ruuhka lisää yli 5 min myöhästymisiä eniten linjalla {leader['line_name']} (+{lift} %-yks.)."
                    if language == "fi"
                    else f" The rush adds the most late running on line {leader['line_name']} (+{lift} pp)."
                )
            texts[language] = text
        takeaways.append({"id": "time-pressure", **texts})
    return takeaways


def _overview_headlines(
    lines: list[Mapping[str, Any]],
    hourly: list[Mapping[str, Any]],
    rush: list[Mapping[str, Any]],
    alerts: list[Mapping[str, Any]],
    daily: list[Mapping[str, Any]],
) -> dict[str, dict[str, object]]:
    frequent = _eligible_lines(lines)
    late_lines = [line["line_name"] for line in _top(frequent, "pct_over_5_min_late")[:3]]
    early_lines = [line["line_name"] for line in _top(frequent, "pct_over_3_min_early")[:3]]
    weekday = [row for row in hourly if row["day_type"] == "weekday" and int(row["bucket_count"]) >= 1000]
    peak_hour = (
        f"{int(max(weekday, key=lambda row: float(row['pct_over_5_min_late']))['local_hour']):02d}"
        if weekday
        else None
    )
    rush_leader = rush[0] if rush else None
    rush_clear = bool(rush_leader and (rush_leader.get("rush_over_5_min_late_lift_ci_lo") or 0) > 0)
    clear_alerts = [row for row in alerts if (row.get("late5_lift_lo") or 0) > 0]
    full_days = [row for row in daily if (row.get("collector_coverage_pct") or 0) >= FULL_COVERAGE_PCT]
    gap_days = [row for row in daily if (row.get("collector_coverage_pct") or 0) < FULL_COVERAGE_PCT]
    on_time = [float(row["pct_on_time"]) for row in full_days if row.get("pct_on_time") is not None]

    result: dict[str, dict[str, object]] = {}
    for language in ("fi", "en"):
        fi = language == "fi"
        result[language] = {
            "ranking": (
                f"Linjat {_join(late_lines, language)} ovat useimmin myöhässä — etuajassa kulkeminen on eri ongelma"
                if fi
                else f"Lines {_join(late_lines, language)} run late most often — early running is a separate problem"
            )
            if late_lines
            else ("Linjojen täsmällisyys" if fi else "Punctuality by line"),
            "hourly": (
                f"Arkisin myöhästymiset kasautuvat klo {peak_hour}"
                if fi
                else f"On weekdays, late running peaks at {peak_hour}:00"
            )
            if peak_hour
            else ("Täsmällisyys vuorokauden mukaan" if fi else "Punctuality by hour"),
            "rush": (
                (
                    f"Ruuhka lisää myöhästymisiä eniten linjalla {rush_leader['line_name']}"
                    if fi
                    else f"The rush adds the most late running on line {rush_leader['line_name']}"
                )
                if rush_clear
                else ("Ruuhkan vaikutus linjoihin on epävarma" if fi else "The rush effect on individual lines is uncertain")
            ),
            "alerts": (
                (
                    (
                        "Osa häiriöistä näkyy selvästi myöhästymisinä"
                        if fi
                        else "Some disruptions show a clear rise in late running"
                    )
                    if max(float(row["late5_lift_pp"]) for row in clear_alerts) >= 5
                    else (
                        "Osa häiriöistä lisää myöhästymisiä hieman mutta mitattavasti"
                        if fi
                        else "Some disruptions add a small but measurable amount of late running"
                    )
                )
                if clear_alerts
                else (
                    "Häiriötiedotteiden yhteys myöhästymisiin on pieni ja epävarma"
                    if fi
                    else "Disruption notices show only a small, uncertain link to lateness"
                )
            ),
            "daily": (
                (
                    f"Täyden keräyksen päivinä aikataulussa oli {_percent(min(on_time), language, 0)}–{_percent(max(on_time), language, 0)} pysäkkiohituksista"
                    if fi
                    else f"On fully collected days, {_percent(min(on_time), language, 0)}–{_percent(max(on_time), language, 0)} of stop passes were on time"
                )
                if on_time
                else ("Päiväkohtainen täsmällisyys" if fi else "Daily punctuality")
            ),
            "recommendations": [
                item
                for item in (
                    (
                        f"Tarkastele linjoja {_join(late_lines, language)} ensin suunnan ja tunnin tasolla."
                        if fi
                        else f"Examine lines {_join(late_lines, language)} first at direction and hour level."
                    )
                    if late_lines
                    else None,
                    (
                        f"Käsittele etuajassa kulkemista omana mittarinaan; säännöllisistä linjoista selvimmät tapaukset ovat {_join(early_lines, language)}."
                        if fi
                        else f"Treat early running as its own measure; among frequent lines the clearest cases are {_join(early_lines, language)}."
                    )
                    if early_lines
                    else None,
                    (
                        f"Paikkaa keräyskatkot ennen trendien vertailua: {len(gap_days)} päivää jäi alle {int(FULL_COVERAGE_PCT)} %:n kattavuuden."
                        if fi
                        else f"Close collection gaps before comparing trends: {len(gap_days)} days fell below {int(FULL_COVERAGE_PCT)}% coverage."
                    )
                    if gap_days
                    else None,
                )
                if item
            ],
        }
    return result


def _overview_caveats(
    *,
    start_date: object,
    end_date: object,
    excluded_pct: float,
    placeholder_rows: int = 0,
) -> list[dict[str, str]]:
    start = str(_json_value(start_date))
    end = str(_json_value(end_date))
    placeholder_fi = f"{placeholder_rows:,}".replace(",", " ")
    placeholder_en = f"{placeholder_rows:,}"
    return [
        {
            "id": "snapshot",
            "fi": (
                f"Tulokset kuvaavat rajattua otosta ajalta {start}–{end}, "
                "eivät reaaliaikaista palvelua."
            ),
            "en": f"The results describe a bounded {start}–{end} snapshot, not a live service.",
        },
        {
            "id": "estimated-state",
            "fi": (
                "SIRI-viive on ajoneuvon ilmoittama arvioitu tila seuraavaa pysäkkiä lähestyttäessä, "
                "ei mitattu lähtöaika. Ajantasauspysäkillä etuajassa saapuva bussi voi odottaa."
            ),
            "en": (
                "SIRI delay is the vehicle's reported state while approaching the next stop, not a "
                "measured departure time. A bus that arrives early at a timing stop may wait."
            ),
        },
        {
            "id": "quality-filter",
            "fi": (
                "Konservatiivinen laatufiltteri sulki pois "
                f"{_percent(excluded_pct, 'fi', 2)} analyysiriveistä, mukaan lukien {placeholder_fi} "
                "riviä, joiden nollaviive ei ollut mittaus (lähtöpysäkillä odottaminen tai vuoro ilman reaaliaikadataa)."
            ),
            "en": (
                "The conservative quality filter excluded "
                f"{_percent(excluded_pct, 'en', 2)} of analysis rows, including {placeholder_en} rows whose "
                "zero delay was not a measurement (waiting at the first stop or a trip without realtime data)."
            ),
        },
        {
            "id": "trips",
            "fi": (
                "Saman vuoron peräkkäiset pysäkit eivät ole toisistaan riippumattomia, joten ryhmiltä "
                "vaaditaan riittävästi eri vuoroja ja luottamusvälit lasketaan vuorotasolla."
            ),
            "en": (
                "Consecutive stops of one trip are not independent, so groups need enough distinct trips "
                "and intervals are computed at trip level."
            ),
        },
        {
            "id": "collector-gaps",
            "fi": (
                "Keräyksessä on aukkoja, joten kattavuus ei ole täysin tasainen "
                "koko ajanjaksolla."
            ),
            "en": "Collector gaps mean coverage is not perfectly even throughout the period.",
        },
    ]


def build_overview_payload(
    buckets: pl.LazyFrame,
    cache_dir: Path,
    manifest: Mapping[str, Any],
    *,
    min_buckets: int,
    min_trips: int = MIN_TRIPS,
    line_records: list[Mapping[str, Any]] | None = None,
) -> dict[str, object]:
    summary_row = (
        _metric_rows(buckets, [], min_buckets=min_buckets)
        .select(SUMMARY_COLUMNS)
        .collect()
        .row(0, named=True)
    )
    dimensions = buckets.select(
        pl.col("line_ref").n_unique().alias("line_count"),
        pl.col("next_stop_point_ref").drop_nulls().n_unique().alias("stop_count"),
        pl.col("local_date").min().alias("start_date"),
        pl.col("local_date").max().alias("end_date"),
    ).collect().row(0, named=True)
    summary = {
        **{key: _json_value(value) for key, value in summary_row.items()},
        **{key: _json_value(value) for key, value in dimensions.items()},
    }
    hourly = frame_records(
        _metric_rows(buckets, ["day_type", "local_hour"], min_buckets=min_buckets)
        .select(HOURLY_COLUMNS)
        .sort(["day_type", "local_hour"])
        .collect()
    )
    quality = _read_table(cache_dir, "quality_summary")
    quality_counts = {
        row["quality_check"]: row for row in quality.to_dicts()
    } if not quality.is_empty() else {}
    excluded_pct = float(quality_counts.get("conservative_excluded_default", {}).get("pct_rows", 0.0))
    placeholder_rows = int(quality_counts.get("is_origin_layover", {}).get("row_count", 0)) + int(
        quality_counts.get("is_no_realtime_trip", {}).get("row_count", 0)
    )
    rush = frame_records(_read_table(cache_dir, "rush_impact"))
    alerts = frame_records(_read_table(cache_dir, "service_alert_grouped"))
    daily = frame_records(_read_table(cache_dir, "daily_network_profile"))
    lines = list(line_records or [])
    settings = manifest.get("settings", {})
    holidays = [
        day
        for day in settings.get("holiday_dates", [])
        if str(summary["start_date"]) <= day <= str(summary["end_date"])
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "meta": {
            "title_fi": "Fölin bussit: täsmällisyys datassa",
            "title_en": "Föli buses: punctuality in data",
            "generated_at_utc": _minute_timestamp(manifest.get("built_at_utc")),
            "analysis_start_utc": _minute_timestamp(manifest.get("db_metadata", {}).get("analysis_start_utc")),
            "analysis_end_utc": _minute_timestamp(manifest.get("db_metadata", {}).get("analysis_end_utc")),
            "timezone": settings.get("timezone", "Europe/Helsinki"),
            "bucket_mode": settings.get("bucket", "trip-stop"),
            "quality_mode": settings.get("quality_mode", "conservative"),
            "minimum_bucket_count": min_buckets,
            "minimum_trip_count": min_trips,
            "frequent_trips_per_day": FREQUENT_TRIPS_PER_DAY,
            "on_time_band_min": [-1, 3],
            "holiday_dates": holidays,
            "conservative_excluded_pct": round(excluded_pct, 2),
            "placeholder_rows_excluded": placeholder_rows,
            "license": "CC BY 4.0",
        },
        "summary": summary,
        "headlines": _overview_headlines(lines, hourly, rush, alerts, daily),
        "takeaways": _overview_takeaways(lines, summary, hourly, rush),
        "caveats": _overview_caveats(
            start_date=dimensions["start_date"],
            end_date=dimensions["end_date"],
            excluded_pct=excluded_pct,
            placeholder_rows=placeholder_rows,
        ),
        "hourly_profile": hourly,
    }


# --- Context --------------------------------------------------------------------------


def build_context_payload(cache_dir: Path) -> dict[str, object]:
    collector_gaps = frame_records(_read_table(cache_dir, "collector_missing_spots"))
    for gap in collector_gaps:
        gap["gap_start_utc"] = _minute_timestamp(gap.get("gap_start_utc"))
        gap["gap_end_utc"] = _minute_timestamp(gap.get("gap_end_utc"))
    inventory = _read_table(cache_dir, "service_alert_inventory")
    inventory_summary = {"included": 0, "long_running": 0, "no_targets": 0, "never_active": 0}
    if not inventory.is_empty():
        for row in inventory.to_dicts():
            reason = row.get("exclusion_reason") or "included"
            if reason in inventory_summary:
                inventory_summary[reason] += 1
    cancellations = _read_table(cache_dir, "service_cancellations")
    cancellation_payload: dict[str, object] = {"total_departures": 0, "by_line": [], "by_cause": []}
    if not cancellations.is_empty():
        cancellation_payload = {
            "total_departures": int(cancellations["cancelled_departures"].sum()),
            "by_line": frame_records(
                cancellations.group_by("line_ref")
                .agg(pl.col("cancelled_departures").sum())
                .sort(["cancelled_departures", "line_ref"], descending=[True, False])
                .head(10)
            ),
            "by_cause": frame_records(
                cancellations.group_by("cause")
                .agg(pl.col("cancelled_departures").sum())
                .sort(["cancelled_departures", "cause"], descending=[True, False])
            ),
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "rush_impact": frame_records(_read_table(cache_dir, "rush_impact")),
        "alerts": frame_records(_read_table(cache_dir, "service_alert_grouped")),
        "alert_inventory": inventory_summary,
        "cancellations": cancellation_payload,
        "quality": frame_records(_read_table(cache_dir, "quality_summary")),
        "collector_gaps": collector_gaps,
        "daily": frame_records(_read_table(cache_dir, "daily_network_profile")),
    }


# --- Validation and build -------------------------------------------------------------


def iter_keys(value: object) -> Iterable[str]:
    if isinstance(value, Mapping):
        for key, child in value.items():
            yield str(key)
            yield from iter_keys(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_keys(child)


def validate_public_payload(name: str, payload: Mapping[str, object]) -> None:
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise ValueError(f"{name}: unsupported schema version")
    forbidden = FORBIDDEN_PUBLIC_KEYS.intersection(iter_keys(payload))
    if forbidden:
        raise ValueError(f"{name}: forbidden public keys: {', '.join(sorted(forbidden))}")
    serialized = json.dumps(payload, ensure_ascii=False, allow_nan=False)
    if str(PROJECT_ROOT) in serialized or "data/foli.db" in serialized:
        raise ValueError(f"{name}: local paths must not be published")


def build_public_data(
    *,
    db: Path,
    cache_dir: Path,
    gtfs_root: Path,
    output_dir: Path,
    min_buckets: int = MIN_BUCKETS,
    min_trips: int = MIN_TRIPS,
    force: bool = False,
) -> dict[str, Path]:
    if min_buckets < 1:
        raise ValueError("min_buckets must be at least 1")
    if min_trips < 1:
        raise ValueError("min_trips must be at least 1")
    settings = ReportSettings(
        db=db,
        cache_dir=cache_dir,
        gtfs_root=gtfs_root,
        min_observations=min_buckets,
        min_trips=min_trips,
        limit=20,
    )
    _print_progress("Verifying Polars report cache")
    cache_result = ensure_report_cache(settings, force=force, progress=_print_progress)
    resolved_cache = settings.resolved().cache_dir
    buckets = scan_cached_observations(resolved_cache)
    stops = load_stop_metadata(gtfs_root=gtfs_root)

    _print_progress("Building lines.json")
    lines_payload = build_line_payload(
        buckets,
        min_buckets=min_buckets,
        min_trips=min_trips,
        line_quality=_read_table(resolved_cache, "line_quality"),
    )
    builders = {
        "overview": lambda: build_overview_payload(
            buckets,
            resolved_cache,
            cache_result.manifest,
            min_buckets=min_buckets,
            min_trips=min_trips,
            line_records=lines_payload["lines"],
        ),
        "lines": lambda: lines_payload,
        "stops": lambda: build_stop_payload(buckets, stops, min_buckets=min_buckets),
        "context": lambda: build_context_payload(resolved_cache),
    }
    written: dict[str, Path] = {}
    for name, builder in builders.items():
        _print_progress(f"Building {name}.json")
        payload = builder()
        validate_public_payload(name, payload)
        path = output_dir / f"{name}.json"
        write_json(path, payload)
        written[name] = path
    return written


def main() -> None:
    args = parse_args()
    written = build_public_data(
        db=args.db,
        cache_dir=args.cache_dir,
        gtfs_root=args.gtfs_root,
        output_dir=args.output_dir,
        min_buckets=args.min_buckets,
        min_trips=args.min_trips,
        force=args.force,
    )
    for name, path in written.items():
        print(f"Wrote {name}: {path}")


if __name__ == "__main__":
    main()
