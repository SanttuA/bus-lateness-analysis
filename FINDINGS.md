# Complete Bus Lateness Findings

This report summarizes the latest generated bus lateness analysis artifacts in
this repository. The primary source is the pandas/DuckDB report at
[`reports/generated/overall-results.md`](reports/generated/overall-results.md),
generated at `2026-05-26T10:11:03+00:00` from a rebuilt cache.

The cache was not rebuilt again for this narrative report.

## Executive Summary

- The latest generated report covers `2026-04-23T08:05:22Z` to
  `2026-05-23T13:24:20Z`, with `10,430,580` analysis rows from `10,532,270`
  raw vehicle observations.
- The default trip-stop bucketed dataset contains `3,746,770` buckets,
  representing `9,837,244` raw polls across `140` lines.
- Conservative quality filtering excludes `593,336` rows, or `5.69%` of
  analysis rows. If stop-call disagreement rows are also excluded, the excluded
  share rises to `7.90%`.
- Late-running pressure is led by lines `612`, `615`, and `614`, each with high
  p90 delay and thousands of buckets. Lines `25`, `25A`, `24`, and `21` have
  lower p90 delay than the top routes but much larger service volumes.
- Early running remains a separate reliability issue. `P6` is the clearest
  outlier, with `92.76%` of buckets early and `59.62%` more than three minutes
  early. Other notable early-running lines include `N10`, `75`, `L4`, `N7`,
  `L6`, `L1`, `711`, `L5`, and `L2`.
- The networkwide late-running peak is concentrated around `15:00-16:00` local
  time. Both hours have p90 delay of `4.40` minutes and the highest shares of
  buckets more than five minutes late.
- Rush-period impact is strongest for `612` and `615`. Line `612` has a
  `10.75` minute p90 rush lift and a `55.71` percentage-point increase in the
  share of buckets more than five minutes late.
- Collector coverage is no longer empty in the latest generated report. The
  collector tables identify major `siri_vm` and `siri_alerts` collection gaps,
  including multi-day gaps that can affect interpretation of affected periods.
- The Polars report is useful corroboration for the main operational findings,
  but pandas/DuckDB remains the better source for this narrative report because
  it is the established primary report and currently has fuller generated
  output.

## Scope And Methodology

| Item | Value |
| --- | --- |
| Database | `data/foli.db` |
| Generated report | `reports/generated/overall-results.md` |
| Cache manifest | `outputs/report-cache/manifest.json` |
| Cache built at | `2026-05-26T10:11:03+00:00` |
| Raw vehicle observations | `10,532,270` |
| Analysis rows | `10,430,580` |
| Cached trip-stop buckets | `3,746,770` |
| Raw polls represented by buckets | `9,837,244` |
| Lines represented | `140` |
| Representative bucket range | `2026-04-23T09:45:00Z` to `2026-05-23T13:39:00Z` |
| Quality mode | `conservative` |
| Bucket mode | `trip-stop` |
| Timezone | `Europe/Helsinki` |
| Minimum grouped observations | `30` |
| Rush windows | `07:00-09:00`, `15:00-18:00` local weekdays |

The analysis uses SIRI vehicle-monitoring delay values. These are estimated
vehicle state values, not measured stop arrival truth. Raw vehicle-monitoring
rows are repeated polls, so the default report collapses them into trip-stop
buckets before ranking lines, stops, hours, and alert contexts.

Primary delay findings use robust metrics:

- `median_delay_min`: typical signed delay in minutes.
- `p90_delay_min`: high-end signed delay; used for late-running rankings.
- `pct_over_5_min_late`: share of buckets more than five minutes late.
- `pct_over_3_min_early`: share of buckets more than three minutes early.
- `p90_early_min_abs`: high-end early-running magnitude in absolute minutes.

## Data Quality Findings

The default conservative filter removes implausible, stale, pre-trip, and
post-trip observations. Stop-call disagreement is flagged by default but not
removed unless explicitly requested.

| Quality check | Rows | Share |
| --- | ---: | ---: |
| Analysis rows | 10,430,580 | 100.00% |
| Implausible delay | 6,637 | 0.06% |
| Stale observation | 149,992 | 1.44% |
| Pre-trip observation | 343,245 | 3.29% |
| Post-trip observation | 201,581 | 1.93% |
| Stop-call disagreement | 320,371 | 3.07% |
| Conservative default excluded | 593,336 | 5.69% |
| Conservative with stop-call disagreement excluded | 823,567 | 7.90% |

Pre-trip observations are the largest individual default exclusion. Stop-call
disagreement is also material at `3.07%`, but leaving it as a flag keeps the
main findings from being narrowed more aggressively than the report default.

### Worst Quality Lines

| Line | Rows | Default excluded | Excluded share |
| --- | ---: | ---: | ---: |
| `P3` | 19,454 | 11,394 | 58.57% |
| `N6` | 13,832 | 7,905 | 57.15% |
| `79A` | 6,103 | 3,423 | 56.09% |
| `711` | 12,774 | 7,150 | 55.97% |
| `L13` | 6,377 | 3,426 | 53.72% |
| `N14` | 5,035 | 2,500 | 49.65% |
| `V2` | 3,970 | 1,726 | 43.48% |
| `P6` | 16,611 | 6,683 | 40.23% |
| `N10` | 16,349 | 6,291 | 38.48% |
| `67` | 12,071 | 4,556 | 37.74% |

Several lines with high exclusion rates also appear in operational findings.
`P6`, `N10`, and `711` should be read with particular caution because they also
rank as early-running outliers.

Full tables:
[`quality_summary.csv`](outputs/report-cache/quality_summary.csv),
[`quality_by_line.csv`](outputs/report-cache/quality_by_line.csv).

## Late-Running Line Findings

Late-running rankings use p90 delay as the main sort key. This highlights routes
where high-end delays are operationally meaningful even when the median remains
moderate.

| Rank | Line | Buckets | Median delay | p90 delay | >5 min late |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | `612` | 2,599 | 4.17 min | 14.12 min | 46.25% |
| 2 | `615` | 5,993 | 3.03 min | 13.40 min | 39.03% |
| 3 | `614` | 6,291 | 4.02 min | 10.18 min | 39.71% |
| 4 | `42A` | 3,527 | 1.33 min | 9.72 min | 22.51% |
| 5 | `V1` | 4,235 | 3.27 min | 9.59 min | 37.21% |
| 6 | `25` | 25,175 | 2.08 min | 8.10 min | 26.72% |
| 7 | `25A` | 29,265 | 2.78 min | 7.75 min | 24.31% |
| 8 | `24` | 45,581 | 1.02 min | 7.53 min | 18.74% |
| 9 | `720` | 3,030 | 2.50 min | 7.43 min | 24.36% |
| 10 | `42` | 3,196 | 0.83 min | 7.26 min | 18.09% |

The strongest late-running evidence is on `612`, `615`, and `614`: all three
combine high p90 delay with enough bucket volume to be stable screening
signals. `25`, `25A`, `24`, and `21` are also important because they have much
larger volumes; smaller p90 delay on those lines still affects many trips.

Full table: [`line_late_rankings.csv`](outputs/report-cache/line_late_rankings.csv).

## Early-Running Line Findings

Early running matters because it can create missed boardings even when average
delay appears acceptable. The ranking below emphasizes early frequency and
early magnitude rather than late delay.

| Rank | Line | Buckets | Median delay | Early | >3 min early | p90 early magnitude |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | `P6` | 3,081 | -4.20 min | 92.76% | 59.62% | 15.87 min |
| 2 | `901` | 14,098 | 0.00 min | 49.38% | 25.51% | 13.32 min |
| 3 | `903` | 1,760 | 0.00 min | 35.74% | 18.12% | 12.38 min |
| 4 | `N10` | 2,215 | -2.60 min | 71.92% | 46.68% | 9.23 min |
| 5 | `75` | 528 | -2.67 min | 94.51% | 46.59% | 8.48 min |
| 6 | `801` | 85,169 | 0.00 min | 48.61% | 19.68% | 8.47 min |
| 7 | `L4` | 2,669 | -1.50 min | 62.27% | 34.02% | 8.28 min |
| 8 | `N7` | 2,926 | -0.68 min | 55.23% | 33.77% | 8.27 min |
| 9 | `615` | 5,993 | 3.03 min | 21.54% | 7.83% | 8.26 min |
| 10 | `L6` | 2,120 | -2.20 min | 78.96% | 41.42% | 8.03 min |

`P6` is the dominant early-running signal by both frequency and magnitude.
`N10`, `75`, `L4`, `N7`, `L6`, `L1`, `711`, `L5`, and `L2` also have high early
shares. `615` appears in both late and early rankings, which suggests high
variability rather than a simple consistently-late profile.

Full table: [`line_early_rankings.csv`](outputs/report-cache/line_early_rankings.csv).

## Context Delay Hotspots

Context metrics group by line, direction, local hour, and weekday/weekend. They
are useful for finding specific operating conditions where delay is concentrated.

| Line | Direction | Hour | Day type | Buckets | Median delay | p90 delay | >5 min late |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: |
| `901` | 2 | 07:00 | weekend | 115 | 0.78 min | 43.34 min | 20.00% |
| `24` | 1 | 15:00 | weekend | 335 | 7.37 min | 28.72 min | 69.25% |
| `901` | 2 | 06:00 | weekend | 38 | 2.97 min | 28.23 min | 39.47% |
| `402` | 2 | 22:00 | weekday | 394 | -1.10 min | 27.83 min | 11.42% |
| `901` | 2 | 09:00 | weekend | 278 | 1.25 min | 24.65 min | 16.91% |
| `24` | 1 | 17:00 | weekend | 323 | 4.88 min | 23.23 min | 48.92% |
| `24` | 2 | 17:00 | weekend | 308 | 0.57 min | 20.14 min | 23.70% |
| `615` | 2 | 17:00 | weekday | 1,317 | 11.73 min | 18.90 min | 88.46% |
| `612` | 2 | 15:00 | weekday | 1,311 | 9.45 min | 16.45 min | 87.57% |
| `42A` | 2 | 13:00 | weekday | 668 | 5.23 min | 16.16 min | 51.80% |

The context table shows two types of signals. Some contexts are severe but
lower-volume, such as weekend `901` contexts. Others are operationally stronger
because both volume and severity are high, especially `615` direction `2`
weekday `17:00` and `612` direction `2` weekday `15:00`.

Full table: [`context_delay_metrics.csv`](outputs/report-cache/context_delay_metrics.csv).

## Hourly Delay Profile

Networkwide medians stay near zero for most hours, but p90 delay and
late-running shares rise sharply in the afternoon.

| Hour | Buckets | Median delay | p90 delay | >5 min late | Early |
| --- | ---: | ---: | ---: | ---: | ---: |
| 07:00 | 240,355 | 0.00 min | 2.50 min | 2.31% | 48.31% |
| 08:00 | 239,810 | 0.00 min | 2.90 min | 3.77% | 46.41% |
| 12:00 | 199,020 | 0.20 min | 3.23 min | 4.59% | 41.16% |
| 13:00 | 219,043 | 0.27 min | 3.64 min | 5.55% | 39.90% |
| 14:00 | 245,934 | 0.22 min | 3.67 min | 5.65% | 41.07% |
| 15:00 | 264,877 | 0.44 min | 4.40 min | 7.99% | 37.26% |
| 16:00 | 252,061 | 0.37 min | 4.40 min | 7.93% | 38.38% |
| 17:00 | 229,091 | -0.05 min | 2.83 min | 4.18% | 50.94% |
| 23:00 | 122,898 | -0.15 min | 1.98 min | 1.94% | 54.23% |

The clearest systemwide late-running peak is `15:00-16:00`. The morning peak is
visible but smaller: `08:00` has higher p90 delay than `07:00`, but both remain
well below the afternoon p90 values. Late evening and overnight periods show
more early running than late running.

Full table: [`hourly_delay_profile.csv`](outputs/report-cache/hourly_delay_profile.csv).

## Rush-Time Impact

Rush impact compares weekday rush windows with non-rush periods for the same
line. The table is sorted by p90 delay lift in the generated report.

| Line | Non-rush buckets | Rush buckets | Median lift | p90 lift | >5 min late lift |
| --- | ---: | ---: | ---: | ---: | ---: |
| `612` | 769 | 1,830 | 6.60 min | 10.75 min | 55.71 pp |
| `75` | 36 | 492 | 7.32 min | 6.97 min | 0.20 pp |
| `615` | 1,944 | 4,049 | 2.58 min | 6.57 min | 27.62 pp |
| `802` | 36 | 621 | 3.33 min | 4.14 min | 4.51 pp |
| `P1` | 1,077 | 525 | 0.35 min | 4.10 min | 11.94 pp |
| `903` | 628 | 1,132 | 1.20 min | 3.91 min | 12.36 pp |
| `220` | 63,801 | 17,469 | 2.43 min | 3.75 min | 21.15 pp |
| `721` | 2,650 | 1,491 | 2.42 min | 3.63 min | 32.15 pp |
| `72` | 4,073 | 2,207 | 1.67 min | 3.32 min | 16.54 pp |
| `25` | 20,496 | 4,679 | 1.88 min | 2.57 min | 15.32 pp |

`612` is the strongest rush-window finding. It has both a large p90 lift and a
large late-share lift. `615` is also substantial and has a larger rush sample.
`220`, `25`, `25A`, `24`, `28`, `722`, and `722S` matter because they combine
meaningful rush effects with high bucket volumes.

Full table: [`rush_impact.csv`](outputs/report-cache/rush_impact.csv).

## Stop-Level Midpoint Changes

The midpoint comparison splits the representative bucket range into two halves
and compares matched stop contexts. These findings show where observed delay
changed between the first and second halves of the cached range. They do not
prove that any particular intervention or incident caused the change.

The generated table is sorted by absolute p90-delay change, so it mixes
improvements and deteriorations.

| Stop | Baseline buckets | Comparison buckets | Median change | p90 change | >5 min late change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Kaamanen | 50 | 42 | 0.18 min | -7.91 min | -8.86 pp |
| Koverinlahdentie | 59 | 48 | -0.01 min | -4.33 min | -9.78 pp |
| Salonkylä | 49 | 35 | -0.26 min | -4.27 min | -9.39 pp |
| Tapaninkalliontie | 51 | 41 | 6.17 min | 3.68 min | 36.01 pp |
| Virola | 81 | 65 | 0.65 min | -3.49 min | 3.27 pp |
| Elinantie | 39 | 32 | 1.15 min | -3.45 min | -8.57 pp |
| Ruusukortteli | 109 | 87 | -0.92 min | -3.43 min | -15.34 pp |
| Nummenpakan koulu | 79 | 57 | 0.00 min | -3.43 min | -6.13 pp |
| Jalkapallostadion | 39 | 31 | -1.05 min | -3.36 min | -16.63 pp |
| Vajosuontie | 97 | 74 | 5.32 min | 3.30 min | 37.81 pp |

The clearest deterioration signals in the displayed table are
`Tapaninkalliontie` and `Vajosuontie`, where both median delay and the share of
buckets more than five minutes late increased sharply. Several other stops show
improvement in p90 delay. Treat this section as a drill-down list rather than a
causal conclusion.

Full table:
[`stop_midpoint_change.csv`](outputs/report-cache/stop_midpoint_change.csv).

## Service Alert Matched-Control Findings

Service alert analysis compares alert-matched observations with controls from
the same line, direction, local hour, and weekday/weekend context. Results are
associations, not causal effects.

### Group-Level Alert Effects

| Cause | Effect | Scope | Priority | Alert buckets | Median lift | p90 lift | >5 min late lift |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `OTHER_CAUSE` | `DETOUR` | route | 900 | 846,743 | 0.17 min | 0.90 min | 2.80 pp |
| `OTHER_CAUSE` | `UNKNOWN_EFFECT` | route | 900 | 891 | 0.34 min | 0.85 min | 3.92 pp |
| `OTHER_CAUSE` | `DETOUR` | route | 1000 | 415,259 | 0.09 min | 0.48 min | 1.63 pp |
| `OTHER_CAUSE` | `OTHER_EFFECT` | route | 1000 | 145,510 | 0.10 min | 0.40 min | 1.60 pp |
| `OTHER_CAUSE` | `DETOUR` | stop | 1000 | 428,653 | 0.07 min | 0.27 min | 0.71 pp |
| `ACCIDENT` | `Unknown` | stop | 1200 | 196,969 | 0.08 min | 0.08 min | -0.26 pp |
| `OTHER_CAUSE` | `Unknown` | route | 1200 | 1,265,592 | 0.03 min | 0.08 min | 0.13 pp |
| `TECHNICAL_PROBLEM` | `Unknown` | stop | 1200 | 1,047,788 | 0.00 min | -0.10 min | -0.31 pp |

At the grouped level, route detours have the clearest positive lift, but the
lift remains modest compared with the worst line-level and context-specific
delay findings. Alert presence alone does not explain most severe delay
patterns in the dataset.

### Largest Line-Level Alert Lifts

| Cause | Effect | Scope | Line | Alert buckets | Median lift | p90 lift | >5 min late lift |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| `ACCIDENT` | `Unknown` | stop | `28A` | 217 | -0.57 min | 10.68 min | 17.96 pp |
| `OTHER_CAUSE` | `DETOUR` | route | `704` | 108 | 7.00 min | 7.96 min | 78.13 pp |
| `OTHER_CAUSE` | `SIGNIFICANT_DELAYS` | stop | `K1` | 320 | 1.10 min | 7.79 min | 17.50 pp |
| `OTHER_CAUSE` | `DETOUR` | route | `706` | 125 | 7.22 min | 7.48 min | 62.27 pp |
| `OTHER_CAUSE` | `DETOUR` | stop | `N11` | 268 | -0.37 min | 6.36 min | 16.83 pp |
| `TECHNICAL_PROBLEM` | `Unknown` | stop | `21` | 25,186 | 3.45 min | 5.23 min | 28.57 pp |
| `OTHER_CAUSE` | `DETOUR` | route | `703` | 108 | 10.26 min | 4.80 min | 77.32 pp |
| `TECHNICAL_PROBLEM` | `Unknown` | stop | `701` | 85 | 0.87 min | 4.69 min | 12.90 pp |

The largest line-level alert lifts often have small alert bucket counts. The
most operationally credible high-volume signal in the displayed table is line
`21` during stop-scoped technical-problem alerts: `25,186` alert buckets,
`3.45` minutes of median lift, `5.23` minutes of p90 lift, and a `28.57`
percentage-point late-share lift.

Full tables:
[`service_alert_grouped.csv`](outputs/report-cache/service_alert_grouped.csv),
[`service_alert_by_line.csv`](outputs/report-cache/service_alert_by_line.csv).

## Collector Coverage Findings

The latest cache manifest contains `86,116` collector poll records. The current
collector outputs identify meaningful missing-data periods, so collection
coverage is a real caveat for affected date ranges.

| Source | Polls | Failures | Missing spots | Total missing | Largest missing | Estimated missed rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `siri_vm` | 78,203 | 191 | 71 | 4,156.67 min | 2,186.83 min | 1,122,360.85 |
| `siri_alerts` | 7,908 | 15 | 5 | 3,873.67 min | 2,178.08 min | 9,711.16 |
| `gtfs` | 5 | 0 | 0 | 0.00 min | 0.00 min | 0.00 |

The two largest visible gaps are:

| Source | Gap start | Gap end | Missing time | Estimated missed rows |
| --- | --- | --- | ---: | ---: |
| `siri_vm` | `2026-05-13T00:00:17Z` | `2026-05-14T12:27:37Z` | 2,186.83 min | 590,477.01 |
| `siri_alerts` | `2026-05-13T00:05:03Z` | `2026-05-14T12:28:08Z` | 2,178.08 min | 5,460.39 |
| `siri_vm` | `2026-05-09T10:02:27Z` | `2026-05-10T13:26:44Z` | 1,643.78 min | 443,845.56 |
| `siri_alerts` | `2026-05-09T10:01:41Z` | `2026-05-10T13:26:50Z` | 1,640.15 min | 4,111.81 |

These gaps mean the report should not be interpreted as complete continuous
coverage. They do not invalidate the full analysis, but they matter for
date-specific or incident-specific interpretation during gap periods.

Full tables:
[`collector_blackouts.csv`](outputs/report-cache/collector_blackouts.csv),
[`collector_missing_summary.csv`](outputs/report-cache/collector_missing_summary.csv),
[`collector_missing_spots.csv`](outputs/report-cache/collector_missing_spots.csv).

## Pandas/DuckDB vs Polars Source Comparison

The separate comparison report at
[`reports/generated/pandas-polars-comparison.md`](reports/generated/pandas-polars-comparison.md)
shows that both report paths use the same SQLite database, observation range,
analysis row count, raw observation count, bucket count, quality mode, bucket
mode, timezone, and minimum observation settings.

For real-world interpretation, the important point is that the primary
operational findings mostly agree: data quality totals, line late rankings,
line early rankings, rush impact, and many context metrics point to the same
routes and time windows. That makes the Polars output useful as a corroborating
source for broad conclusions.

Polars was faster in the existing generated runs, but only by `2.22` seconds
overall, about `1.0%`. Most runtime is cache/build work, so speed is not a
decisive difference for this current report workflow.

For this narrative report, pandas/DuckDB is the better source data path. It is
the established primary generated report, has fuller current report output for
the narrative sections, and is the safer source when alert, hourly, and
collector interpretation matters. Polars should be treated as useful
corroboration until the remaining output differences are resolved.

## Overall Interpretation

The most actionable reliability findings are concentrated in four areas:

1. Late-running priority lines: `612`, `615`, `614`, `42A`, `V1`, `25`, `25A`,
   `24`, `720`, and high-volume line `21`.
2. Early-running priority lines: `P6`, `N10`, `75`, `L4`, `N7`, `L6`, `L1`,
   `711`, `L5`, and `L2`.
3. Time-and-context hotspots: afternoon rush contexts, especially `612`
   direction `2` weekday `15:00` and `615` direction `2` weekday `17:00`.
4. Collector gaps: multi-hour to multi-day gaps in `siri_vm` and `siri_alerts`
   collection that can affect date-specific conclusions.

The top-level network median is usually close to zero, so averages alone would
hide the most important patterns. The operational issues appear in high-end
delay, early-running shares, line-direction-hour contexts, and collection
coverage.

Data quality is good enough for broad screening, but not perfect. Lines with
high conservative exclusion rates should be validated before making line-level
decisions. Stop-level midpoint and service-alert results are best used as
investigation leads because they depend on matched context definitions and can
be sensitive to sample size.

## Source Artifacts

- Generated markdown table report:
  [`reports/generated/overall-results.md`](reports/generated/overall-results.md)
- Polars generated report:
  [`reports/generated/overall-results-polars.md`](reports/generated/overall-results-polars.md)
- Pandas/DuckDB vs Polars comparison:
  [`reports/generated/pandas-polars-comparison.md`](reports/generated/pandas-polars-comparison.md)
- Cache manifest:
  [`outputs/report-cache/manifest.json`](outputs/report-cache/manifest.json)
- Cached result tables:
  [`quality_summary.csv`](outputs/report-cache/quality_summary.csv),
  [`quality_by_line.csv`](outputs/report-cache/quality_by_line.csv),
  [`line_late_rankings.csv`](outputs/report-cache/line_late_rankings.csv),
  [`line_early_rankings.csv`](outputs/report-cache/line_early_rankings.csv),
  [`context_delay_metrics.csv`](outputs/report-cache/context_delay_metrics.csv),
  [`hourly_delay_profile.csv`](outputs/report-cache/hourly_delay_profile.csv),
  [`rush_impact.csv`](outputs/report-cache/rush_impact.csv),
  [`stop_midpoint_change.csv`](outputs/report-cache/stop_midpoint_change.csv),
  [`service_alert_grouped.csv`](outputs/report-cache/service_alert_grouped.csv),
  [`service_alert_by_line.csv`](outputs/report-cache/service_alert_by_line.csv),
  [`collector_blackouts.csv`](outputs/report-cache/collector_blackouts.csv),
  [`collector_missing_summary.csv`](outputs/report-cache/collector_missing_summary.csv),
  [`collector_missing_spots.csv`](outputs/report-cache/collector_missing_spots.csv).

## Caveats

- SIRI VM delay is estimated vehicle-monitoring state, not measured stop arrival
  truth.
- Raw vehicle-monitoring rows are repeated polls; default results use trip-stop
  buckets so a visible vehicle is not overweighted just because it was polled
  repeatedly.
- Conservative filtering excludes implausible, stale, pre-trip, and post-trip
  rows. Stop-call disagreement is flagged but not excluded in the default cache.
- The latest generated source data ends at `2026-05-23T13:24:20Z`; later data is
  not included in this report.
- Service-alert and stop-midpoint findings are matched observational
  comparisons. They should not be interpreted as causal proof.
- Some high-ranked findings have low sample sizes near the `30` bucket minimum.
  These should be validated before prioritizing operational action.
- Collector blackout and missing-data outputs show meaningful gaps. Use caution
  when interpreting date-specific patterns during those periods.
