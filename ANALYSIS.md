# Analysis Guide

Commands and metric notes for the bus lateness analysis scripts and notebooks.

## Defaults

Delay analytics now use conservative data quality filtering and trip-stop
buckets by default.

- Quality filtering starts from GTFS-matchable rows with non-null delay and
  line, then excludes implausible absolute delays over 120 minutes, stale or
  expired VM observations, pre-trip rows earlier than origin minus 15 minutes,
  and post-trip rows later than destination plus 30 minutes.
- Stop-call disagreement over 10 minutes is flagged by default. Add
  `--exclude-stop-call-disagreement` to exclude those rows too.
- `--quality-mode conservative|diagnostic|raw` controls filtering. Diagnostic
  mode adds flags without filtering; raw mode skips quality filtering.
- `--bucket trip-stop|vehicle-trip|line-hour|poll` controls pre-metric
  aggregation. `trip-stop` is the default and collapses repeated polls for the
  same vehicle trip and next stop.

## Scripts

Data-quality summary before delay metrics:

```sh
uv run python analysis/data-quality-report.py --view summary
```

Show quality issues by line or example flagged rows:

```sh
uv run python analysis/data-quality-report.py --view line --limit 20
uv run python analysis/data-quality-report.py --view examples --limit 20
```

Robust line delay ranking:

```sh
uv run python analysis/avg-line-delay.py --limit 20 --min-observations 50
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
```

Late and early line rankings:

```sh
uv run python analysis/line-delay-rankings.py --ranking both --limit 10
```

Rush-time impact with the default weekday windows of 07:00-09:00 and
15:00-18:00 in Europe/Helsinki local time:

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
second-half split is available only as:

```sh
uv run python analysis/stop-delay-change.py --legacy-midpoint
```

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
route/stop scope. Controls come from the same line, direction, local hour, and
weekday/weekend context:

```sh
uv run python analysis/service-alert-delay-correlation.py --alert-kind any --view grouped
uv run python analysis/service-alert-delay-correlation.py --alert-kind route --view line
```

Write any script result to CSV with `--output-csv`:

```sh
uv run python analysis/context-delay-metrics.py --output-csv outputs/context-delay-metrics.csv
```

## Metrics

Robust delay outputs use buckets, not raw poll rows, unless `--bucket poll` is
selected.

- `bucket_count`: number of analysis buckets.
- `raw_poll_count`: original VM rows represented by those buckets.
- `signed_mean_delay_min`: signed mean delay, retained for context but not used
  as the primary ranking.
- `median_delay_min`, `p75_delay_min`, `p90_delay_min`, `p95_delay_min`: signed
  robust delay percentiles in minutes.
- `pct_over_3_min_late`, `pct_over_5_min_late`: share of buckets more than 3 or
  5 minutes late.
- `pct_early`, `pct_over_1_min_early`, `pct_over_3_min_early`: early-running
  rates.
- `median_early_min_abs`, `p90_early_min_abs`: early-running magnitude in
  absolute minutes.

Default rankings sort by `p90_delay_min`, then `pct_over_5_min_late`, then
`bucket_count`.

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
