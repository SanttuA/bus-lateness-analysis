# Analysis Guide

Detailed commands and metric notes for the bus lateness analysis scripts and
notebooks.

## Scripts

Show the 10 lines with the highest average delay:

```sh
uv run python analysis/avg-line-delay.py
```

Use a different database, output limit, or minimum observation count:

```sh
uv run python analysis/avg-line-delay.py --db data/foli.db --limit 20 --min-observations 50
```

Hourly delay profile, ranked by the hours with the highest average delay:

```sh
uv run python analysis/hourly-delay-profile.py --limit 24
```

Hourly profile for one line:

```sh
uv run python analysis/hourly-delay-profile.py --line-ref 3 --limit 24
```

Late-only and early-only line rankings:

```sh
uv run python analysis/line-delay-rankings.py --limit 10 --min-observations 50
```

Rush-time impact with the default weekday windows of 07:00-09:00 and
15:00-18:00 in Europe/Helsinki local time:

```sh
uv run python analysis/rush-impact.py --limit 10 --min-observations 50
```

Tune rush windows:

```sh
uv run python analysis/rush-impact.py --rush-window 06:30-09:30 --rush-window 15:00-18:30
```

Collector blackout summary:

```sh
uv run python analysis/collector-blackouts.py
```

Stop-level delay changes between the first and second half of the collected
time range:

```sh
uv run python analysis/stop-delay-change.py --limit 20 --min-observations 100
```

Rank the strongest stop-level delay increases only:

```sh
uv run python analysis/stop-delay-change.py --sort-by increase
```

Compare specific periods. Naive timestamps are interpreted in the configured
local timezone and converted to UTC:

```sh
uv run python analysis/stop-delay-change.py \
  --baseline-start 2026-04-23 \
  --baseline-end 2026-04-25 \
  --comparison-start 2026-04-25 \
  --comparison-end 2026-04-27
```

Aggregate stop changes by city part with a mapping CSV:

```sh
uv run python analysis/stop-delay-change.py \
  --group-by city-part \
  --city-parts-csv data/stop-city-parts.csv
```

The city-part CSV must contain:

```text
stop_id,city_part
1,Satama
10,Keskusta
```

Service alert correlation with delays. By default, route alerts are matched to
vehicle observations through GTFS `routes.txt`:

```sh
uv run python analysis/service-alert-delay-correlation.py --limit 20 --min-observations 100
```

Use stop-level alert matching when needed:

```sh
uv run python analysis/service-alert-delay-correlation.py --alert-kind stop
```

Detailed collector missing-data summary and individual gaps:

```sh
uv run python analysis/collector-missing-data-spots.py --limit 20
```

Show only the missing spots for one collector source:

```sh
uv run python analysis/collector-missing-data-spots.py --source siri_vm --view spots
```

Write any script result to CSV with `--output-csv`:

```sh
uv run python analysis/line-delay-rankings.py --output-csv outputs/line-delay-rankings.csv
```

## Metrics

The delay analysis uses rows where `is_gtfs_matchable = 1`,
`delay_seconds IS NOT NULL`, and `line_ref IS NOT NULL`.

- `avg_delay_min`: average signed delay. Positive means late; negative means early.
- `median_delay_min`: median signed delay.
- `pct_late`: share of observations where `delay_seconds > 0`.
- Late-only rankings use observations where `delay_seconds > 0`.
- Early-only rankings use observations where `delay_seconds < 0` and report the
  absolute early deviation.
- Rush impact compares weekday rush windows with non-rush observations for the
  same line.
- Collector blackouts infer each source's expected cadence from the median gap
  between successful polls, then count successful-poll gaps larger than twice
  that cadence.
- Stop delay change compares baseline and comparison periods for observations
  grouped by `next_stop_point_ref`, or by `city_part` when a mapping CSV is
  supplied. Positive `delay_change_min` means delays increased in the comparison
  period.
- Service alert correlation compares observations during active alerts with
  observations without active alerts. Positive `delay_lift_min` means the alert
  period had higher average signed delay.
- Collector missing-data spots use the same median-success cadence idea as
  blackouts, but list each inferred gap and estimate missed polls and rows.

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
```
