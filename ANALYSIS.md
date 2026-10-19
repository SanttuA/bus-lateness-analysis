# Analysis Guide

Commands and metric notes for the bus lateness analysis scripts and notebooks.

## Defaults

Delay analytics now use conservative data quality filtering and trip-stop
buckets by default.

- Quality filtering starts from GTFS-matchable rows with non-null delay and
  line, then excludes implausible absolute delays over 120 minutes, stale or
  expired VM observations, pre-trip rows earlier than origin minus 15 minutes,
  and post-trip rows later than destination plus 30 minutes.
- Two more conservative exclusions remove placeholder zero delays that Föli
  reports when it has no real estimate:
  - `is_origin_layover`: the next stop is the trip origin and the row was
    recorded before the aimed origin departure (a bus waiting to start).
  - `is_no_realtime_trip`: every delay in the trip is exactly 0 across at least
    10 distinct stops.
- `is_placeholder_zero` (delay exactly 0 while the next stop-call delay is more
  than 60 seconds away from zero) is a diagnostic flag only and is not
  excluded.
- Stop-call disagreement over 10 minutes is flagged by default. Add
  `--exclude-stop-call-disagreement` to exclude those rows too.
- Trip-stop buckets from one trip are correlated, so grouped tables require
  both `--min-observations` buckets and `--min-trips` distinct trips (default
  10). The trip gate is skipped when a bucket mode has no trip key
  (`line-hour`).
- Weekday holidays with reduced service are detected from the GTFS calendar: a
  Monday-Friday date with fewer than 60% of the median active services of the
  surrounding weekdays (±14 days). They count as `weekend` in every
  weekday/weekend split, and buckets carry an `is_holiday` flag. Use
  `--gtfs-root` to point at the snapshot root used for this.
- `--quality-mode conservative|diagnostic|raw` controls filtering. Diagnostic
  mode adds flags without filtering; raw mode skips quality filtering.
- `--bucket trip-stop|vehicle-trip|line-hour|poll` controls pre-metric
  aggregation. `trip-stop` is the default and collapses repeated polls for the
  same vehicle trip and next stop.
- Most delay-analysis scripts reuse the DuckDB cache under
  `outputs/report-cache/` by default. Use `--no-cache` for the legacy pandas
  path, `--force-cache` to rebuild reusable base tables, or `--cache-dir` to
  point at a different cache directory.
- A secondary Polars CLI path lives under `analysis/polars/`. It keeps separate
  Parquet and CSV outputs under `outputs/polars-report-cache/` and does not
  replace the default analysis scripts.
- Stop and route metadata use extracted GTFS directories named
  `data/gtfs/gtfs_YYYY-MM-DD` by default. Each snapshot applies from that local
  date until the next snapshot date. Use `--gtfs-dir` on GTFS-aware scripts to
  force one directory for all rows, or `--gtfs-root` to point at another
  date-aware snapshot root. Folder dates are download dates, not schedule
  start dates.
- Cache builds are sized for a machine with about 15 GiB of RAM. DuckDB runs
  with `memory_limit=3GB`, 8 threads and a spill directory inside the cache
  directory. Polars builds the buckets in 16 partitions and sets
  `_RJEM_MALLOC_CONF` (jemalloc background decay) before importing Polars, so
  freed memory goes back to the OS between partitions.

## Scripts

Generate the overall Markdown results report:

```sh
uv run python analysis/build-results-report.py
```

The report is written to `reports/generated/overall-results.md`. The command
builds a disk-backed DuckDB cache in `outputs/report-cache/`, including compact
CSV outputs for each report table. It reuses the cache when the database
metadata and report settings match, and rebuilds it when they change. Use
`--force` to rebuild manually. The command prints progress updates and writes
cache/build, render, and total run timings into the generated report:

```sh
uv run python analysis/build-results-report.py --force
```

Generate the separate Polars report and cache:

```sh
uv run python analysis/polars/build-results-report.py
```

The report is written to `reports/generated/overall-results-polars.md`, with
Parquet intermediates and matching CSVs under `outputs/polars-report-cache/`.
It prints the same progress updates and timing section for side-by-side
comparison with the DuckDB/pandas report. Use `--force` to rebuild manually:

```sh
uv run python analysis/polars/build-results-report.py --force
```

Data-quality summary before delay metrics:

```sh
uv run python analysis/data-quality-report.py --view summary
uv run python analysis/polars/data-quality-report.py --view summary
```

Show quality issues by line or example flagged rows:

```sh
uv run python analysis/data-quality-report.py --view line --limit 20
uv run python analysis/data-quality-report.py --view examples --limit 20
```

Robust line delay ranking:

```sh
uv run python analysis/avg-line-delay.py --limit 20 --min-observations 50
uv run python analysis/polars/avg-line-delay.py --limit 20 --min-observations 50
```

Context-aware robust metrics by `line_ref + direction_ref + local hour +
weekday/weekend`:

```sh
uv run python analysis/context-delay-metrics.py --limit 50 --min-observations 50
```

Hourly delay profile:

```sh
uv run python analysis/hourly-delay-profile.py --limit 24
uv run python analysis/hourly-delay-profile.py --line-ref 3 --limit 24
uv run python analysis/polars/hourly-delay-profile.py --line-ref 3 --limit 24
```

Late and early line rankings:

```sh
uv run python analysis/line-delay-rankings.py --ranking both --limit 10
uv run python analysis/polars/line-delay-rankings.py --ranking both --limit 10
```

Rush-time impact compares the default weekday rush windows of 07:00-09:00 and
15:00-18:00 with weekday daytime off-peak service (09:00-15:00) on the same
line, in Europe/Helsinki local time. Nights, weekends and holidays are in
neither group. Lines are ranked by `rush_over_5_min_late_pct_point_lift`, the
rush minus off-peak share of buckets more than 5 minutes late, with a 95%
trip-bootstrap interval (`rush_over_5_min_late_lift_ci_lo/hi`). Both sides
must pass the bucket and trip minimums:

```sh
uv run python analysis/rush-impact.py --limit 10 --min-observations 50
uv run python analysis/rush-impact.py --rush-window 06:30-09:30 --rush-window 15:00-18:30
```

Collector quality reports:

```sh
uv run python analysis/collector-blackouts.py
uv run python analysis/collector-missing-data-spots.py --limit 20
```

Stop-level delay changes now require explicit periods unless the legacy
midpoint split is requested. Naive timestamps are interpreted in the configured
local timezone and converted to UTC:

```sh
uv run python analysis/stop-delay-change.py \
  --baseline-start 2026-04-20 \
  --baseline-end 2026-04-24 \
  --comparison-start 2026-04-27 \
  --comparison-end 2026-05-01 \
  --limit 20 \
  --min-observations 50
```

The comparison only uses contexts present in both periods for the same group,
line, direction, local weekday, and local hour. The old automatic first-half vs
second-half split scans the full observation history and is available only as:

```sh
uv run python analysis/stop-delay-change.py --legacy-midpoint
```

The overall results report no longer uses the midpoint split. Its halves were
uneven (collector gaps, and holidays falling in only one half) and the listed
stops had only 31-130 buckets per half. The report has a
`daily_network_profile` table instead: one row per local date with day type,
holiday flag, buckets, trips, the delay metrics and the siri_vm collector
coverage for that day.

Aggregate stop changes by city part with a mapping CSV:

```sh
uv run python analysis/stop-delay-change.py \
  --group-by city-part \
  --city-parts-csv data/stop-city-parts.csv \
  --baseline-start 2026-04-20 \
  --baseline-end 2026-04-24 \
  --comparison-start 2026-04-27 \
  --comparison-end 2026-05-01
```

The city-part CSV must contain:

```text
stop_id,city_part
1,Satama
10,Keskusta
```

Service alert matched-control analysis groups by cause, effect, priority, and
route/stop scope. The method:

- **Targets.** A message's `line_ref` is a GTFS `route_id`. It is mapped to the
  public line number (`route_short_name`) with the GTFS snapshot valid on that
  date.
- **Validity.** Every window in `repeat_json` is used, clipped to the span of
  `is_active=1` snapshots.
- **Cancellations.** These are cancelled departures, and their `line` is
  already a public line number. They are kept out of the lift and summarised
  separately (`service_cancellations`).
- **Long-running alerts.** Alerts valid for at least 90% of the observed period
  have no comparable control period and are excluded. They are counted in
  `service_alert_inventory`.
- **Lift.** For each alert bucket, the controls are buckets in the same line,
  direction, local hour and day type with no active alert of any kind. A
  stratum needs at least 20 controls. Strata are weighted by their alert bucket
  counts, so `late5_lift_pp` is the alert minus control share over 5 minutes
  late, and `median_lift_min` is the change in median delay. `coverage_pct` is
  the share of alert buckets with usable strata. `late5_lift_lo/hi` is a 95%
  trip-bootstrap interval. The lift is an association, not a causal effect.

By default it analyzes the latest two days so notebooks and local scripts do
not load the full database into pandas:

```sh
uv run python analysis/service-alert-delay-correlation.py --alert-kind any --view grouped
uv run python analysis/service-alert-delay-correlation.py --alert-kind route --view line
```

Use explicit windows for longer or matched alert analyses:

```sh
uv run python analysis/service-alert-delay-correlation.py \
  --start 2026-05-06 \
  --end 2026-05-08 \
  --alert-kind any \
  --view both
```

`--full-history` is available for machines with enough memory, but it can be
large on multi-gigabyte databases.

Write any script result to CSV with `--output-csv`:

```sh
uv run python analysis/context-delay-metrics.py --output-csv outputs/context-delay-metrics.csv
```

Generated report artifacts under `reports/generated/` and cached analysis
outputs under `outputs/` are intentionally ignored by git.

## Metrics

Robust delay outputs use buckets, not raw poll rows, unless `--bucket poll` is
selected.

- `bucket_count`: number of analysis buckets.
- `raw_poll_count`: original VM rows represented by those buckets.
- `trip_count`, `day_count`: distinct trips and local service dates behind the
  buckets. Buckets from one trip are correlated, so `trip_count` is the
  effective sample size.
- `signed_mean_delay_min`: signed mean delay, retained for context but not used
  as the primary ranking.
- `median_delay_min`, `p75_delay_min`, `p90_delay_min`, `p95_delay_min`: signed
  robust delay percentiles in minutes.
- `pct_on_time`: share of buckets from 1 minute early to 3 minutes late
  (-60 to +180 seconds).
- `pct_late`: share of buckets with any positive delay.
- `pct_over_3_min_late`, `pct_over_5_min_late`: share of buckets more than 3 or
  5 minutes late.
- `pct_early`, `pct_over_1_min_early`, `pct_over_3_min_early`: early-running
  rates.
- `median_early_min_abs`, `p90_early_min_abs`: early-running magnitude in
  absolute minutes, among early buckets only.
- `*_ci_lo`, `*_ci_hi`: 95% percentile intervals from a trip-cluster Poisson
  bootstrap (300 replicates, seed derived from the group key). Line rankings
  carry intervals for `p90_delay_min`, `pct_over_5_min_late` and
  `pct_over_3_min_early`.

Late rankings sort by `p90_delay_min`, then `pct_over_5_min_late`, then
`bucket_count` (larger samples first). Early rankings sort by
`pct_over_3_min_early`, then `p90_early_min_abs`, then `bucket_count`.

Line quality (`line_quality`) reports for every line:

- `excluded_pct`: share of rows removed by the conservative filter;
- `data_fault_pct`: implausible, stale, post-trip or no-realtime rows;
- `trip_coverage_pct`: observed share of GTFS-scheduled trips on days with at
  least 95% collector coverage.

The public site marks a line as low quality when `data_fault_pct` is above 20
or `trip_coverage_pct` is below 80. The origin layover dominates
`excluded_pct`, so `excluded_pct` alone is not a fault measure.

## Notebooks

Start Jupyter from the project root:

```sh
uv run jupyter lab
```

The notebooks are in `notebooks/`:

```text
notebooks/01_hourly_delay_profile.ipynb
notebooks/02_line_delay_rankings.ipynb
notebooks/03_rush_impact.ipynb
notebooks/04_collector_blackouts.ipynb
notebooks/05_stop_delay_change.ipynb
notebooks/06_service_alert_delay_correlation.ipynb
notebooks/07_collector_missing_data_spots.ipynb
notebooks/08_data_quality_report.ipynb
notebooks/09_context_delay_metrics.ipynb
```

Notebooks 01, 02, 03, 08 and 09 read the shared DuckDB cache through
`analysis/cached_queries.py`, like the CLI scripts do by default. The first run
builds the cache and later runs reuse it, so each notebook stays under about
1.6 GB of memory. The legacy `load_observations` path loads every observation
into pandas. On the full database it needs more memory than a 15 GiB machine
has, so use it only with a line or time filter. Notebooks 05 and 06 load two
short recent windows by default.

Polars notebook counterparts live in `notebooks/polars/` with the same
filenames. They use `analysis/polars/` and the separate
`outputs/polars-report-cache/` cache path.
