from __future__ import annotations

import argparse
import json
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

import polars as pl

from analysis.polars.report_cache import (  # noqa: E402
    DEFAULT_CACHE_DIR,
    ReportSettings,
    ensure_report_cache,
    read_result_table,
    round_numeric_lazy,
    summarize_delay_metrics_lazy,
)
from dashboard_data_polars import load_stop_metadata, scan_cached_observations  # noqa: E402


DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "site" / "public" / "data"
MIN_BUCKETS = 30
ALL_VALUE = "all"
SCHEMA_VERSION = 1
FORBIDDEN_PUBLIC_KEYS = {
    "vehicle_id",
    "trip_id",
    "trip_match_key",
    "bucket_id",
    "poll_id",
    "source_alert_id",
    "recorded_at_utc",
    "representative_time_utc",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build deterministic aggregate JSON for the public GitHub Pages report."
    )
    parser.add_argument("--db", type=Path, default=Path("data/foli.db"))
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--gtfs-root", type=Path, default=Path("data/gtfs"))
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--min-buckets", type=int, default=MIN_BUCKETS)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def _print_progress(message: str) -> None:
    print(f"[public-data] {message}", flush=True)


def _json_value(value: object) -> object:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, float):
        return round(value, 2)
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
    line_name: bool = False,
) -> pl.LazyFrame:
    extra = {"line_name": ("published_line_name", "first")} if line_name else None
    return round_numeric_lazy(
        summarize_delay_metrics_lazy(
            buckets,
            keys,
            min_observations=min_buckets,
            extra_aggs=extra,
        )
    )


def build_line_payload(
    buckets: pl.LazyFrame,
    *,
    min_buckets: int,
) -> dict[str, object]:
    lines = (
        _metric_rows(buckets, ["line_ref"], min_buckets=min_buckets, line_name=True)
        .sort("line_ref")
        .collect()
    )
    contexts = (
        _metric_rows(
            buckets,
            ["line_ref", "direction_ref", "day_type", "local_hour"],
            min_buckets=min_buckets,
            line_name=True,
        )
        .sort(["line_ref", "direction_ref", "day_type", "local_hour"])
        .collect()
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "lines": frame_records(lines),
        "contexts": frame_records(contexts),
    }


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
    return metrics.select(
        "stop_id",
        "line_ref",
        "day_type",
        "bucket_count",
        "raw_poll_count",
        "signed_mean_delay_min",
        "median_delay_min",
        "p90_delay_min",
        "pct_over_5_min_late",
        "pct_over_3_min_early",
        "p90_early_min_abs",
    )


def build_stop_payload(
    buckets: pl.LazyFrame,
    stops: pl.DataFrame,
    *,
    min_buckets: int,
) -> dict[str, object]:
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


def _overview_takeaways(
    late: pl.DataFrame,
    early: pl.DataFrame,
    hourly: pl.DataFrame,
    rush: pl.DataFrame,
) -> list[dict[str, str]]:
    late_row = late.row(0, named=True)
    early_row = early.row(0, named=True)
    peak_row = hourly.sort(
        ["p90_delay_min", "pct_over_5_min_late"], descending=True
    ).row(0, named=True)
    rush_row = rush.row(0, named=True)
    return [
        {
            "id": "late-lines",
            "fi": (
                f"Linja {late_row['line_name']} erottuu myöhästymisissä: "
                f"p90-viive on {late_row['p90_delay_min']:.2f} minuuttia."
            ),
            "en": (
                f"Line {late_row['line_name']} stands out for lateness: "
                f"its p90 delay is {late_row['p90_delay_min']:.2f} minutes."
            ),
        },
        {
            "id": "early-lines",
            "fi": (
                f"Linja {early_row['line_name']} on selvin etuajassa kulkeva poikkeama: "
                f"{early_row['pct_over_3_min_early']:.2f} % luokista on yli kolme "
                "minuuttia etuajassa."
            ),
            "en": (
                f"Line {early_row['line_name']} is the clearest early-running outlier: "
                f"{early_row['pct_over_3_min_early']:.2f}% of buckets are more than "
                "three minutes early."
            ),
        },
        {
            "id": "time-pressure",
            "fi": (
                "Verkon korkein tuntikohtainen p90-viive osuu klo "
                f"{int(peak_row['local_hour']):02d}:00. "
                f"Ruuhkavaikutus on suurin linjalla {rush_row['line_name']}."
            ),
            "en": (
                "The network's highest hourly p90 delay occurs at "
                f"{int(peak_row['local_hour']):02d}:00. "
                f"The strongest rush effect is on line {rush_row['line_name']}."
            ),
        },
    ]


def _overview_caveats(
    *,
    start_date: object,
    end_date: object,
    excluded_pct: float,
) -> list[dict[str, str]]:
    start = str(_json_value(start_date))
    end = str(_json_value(end_date))
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
                "SIRI-viive on ajoneuvon ilmoittama arvioitu tila, ei havaittu "
                "saapumisen totuus."
            ),
            "en": "SIRI delay is a reported estimated state, not observed arrival truth.",
        },
        {
            "id": "quality-filter",
            "fi": (
                "Konservatiivinen laatufiltteri sulki pois "
                f"{excluded_pct:.2f} % analyysiriveistä."
            ),
            "en": (
                "The conservative quality filter excluded "
                f"{excluded_pct:.2f}% of analysis rows."
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
) -> dict[str, object]:
    summary_row = (
        _metric_rows(buckets, [], min_buckets=min_buckets)
        .with_columns(
            pl.col("bucket_count"),
            pl.col("raw_poll_count"),
        )
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
    hourly = (
        _metric_rows(buckets, ["local_hour"], min_buckets=min_buckets)
        .sort("local_hour")
        .collect()
    )
    late = _read_table(cache_dir, "line_late_rankings")
    early = _read_table(cache_dir, "line_early_rankings")
    rush = _read_table(cache_dir, "rush_impact")
    db_metadata = manifest.get("db_metadata", {})
    quality = _read_table(cache_dir, "quality_summary")
    excluded = quality.filter(pl.col("quality_check") == "conservative_excluded_default")
    excluded_pct = float(excluded["pct_rows"][0]) if excluded.height else 0.0
    return {
        "schema_version": SCHEMA_VERSION,
        "meta": {
            "title_fi": "Fölin bussit: täsmällisyys datassa",
            "title_en": "Föli buses: punctuality in data",
            "generated_at_utc": _minute_timestamp(manifest.get("built_at_utc")),
            "analysis_start_utc": _minute_timestamp(db_metadata.get("analysis_start_utc")),
            "analysis_end_utc": _minute_timestamp(db_metadata.get("analysis_end_utc")),
            "timezone": manifest.get("settings", {}).get("timezone", "Europe/Helsinki"),
            "bucket_mode": manifest.get("settings", {}).get("bucket", "trip-stop"),
            "quality_mode": manifest.get("settings", {}).get("quality_mode", "conservative"),
            "minimum_bucket_count": min_buckets,
            "conservative_excluded_pct": round(excluded_pct, 2),
            "license": "CC BY 4.0",
        },
        "summary": summary,
        "takeaways": _overview_takeaways(late, early, hourly, rush),
        "caveats": _overview_caveats(
            start_date=dimensions["start_date"],
            end_date=dimensions["end_date"],
            excluded_pct=excluded_pct,
        ),
        "hourly_profile": frame_records(hourly),
    }


def build_context_payload(cache_dir: Path) -> dict[str, object]:
    collector_gaps = frame_records(_read_table(cache_dir, "collector_missing_spots"))
    for gap in collector_gaps:
        gap["gap_start_utc"] = _minute_timestamp(gap.get("gap_start_utc"))
        gap["gap_end_utc"] = _minute_timestamp(gap.get("gap_end_utc"))
    return {
        "schema_version": SCHEMA_VERSION,
        "rush_impact": frame_records(_read_table(cache_dir, "rush_impact")),
        "alerts": frame_records(_read_table(cache_dir, "service_alert_grouped")),
        "quality": frame_records(_read_table(cache_dir, "quality_summary")),
        "collector_gaps": collector_gaps,
        "stop_changes": frame_records(_read_table(cache_dir, "stop_midpoint_change")),
    }


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
    force: bool = False,
) -> dict[str, Path]:
    if min_buckets < 1:
        raise ValueError("min_buckets must be at least 1")
    settings = ReportSettings(
        db=db,
        cache_dir=cache_dir,
        gtfs_root=gtfs_root,
        min_observations=MIN_BUCKETS,
        limit=20,
    )
    _print_progress("Verifying Polars report cache")
    cache_result = ensure_report_cache(settings, force=force, progress=_print_progress)
    resolved_cache = settings.resolved().cache_dir
    buckets = scan_cached_observations(resolved_cache)
    stops = load_stop_metadata(gtfs_root=gtfs_root)

    builders = {
        "overview": lambda: build_overview_payload(
            buckets,
            resolved_cache,
            cache_result.manifest,
            min_buckets=min_buckets,
        ),
        "lines": lambda: build_line_payload(buckets, min_buckets=min_buckets),
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
        force=args.force,
    )
    for name, path in written.items():
        print(f"Wrote {name}: {path}")


if __name__ == "__main__":
    main()
