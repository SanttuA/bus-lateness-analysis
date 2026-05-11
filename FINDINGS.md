# Complete Bus Lateness Findings

This report summarizes all cached analysis outputs currently available in the
repository. It is a narrative companion to the generated table report at
[`reports/generated/overall-results.md`](reports/generated/overall-results.md)
and the detailed CSV artifacts in [`outputs/report-cache/`](outputs/report-cache/).

The cache was not refreshed for this report.

## Executive Summary

- The cached analysis covers `2026-04-23T08:05:22Z` to
  `2026-05-08T09:36:13Z`, with `5,585,585` analysis rows collapsed into
  `2,012,287` trip-stop buckets across `138` lines.
- Conservative default quality filtering excludes `310,081` rows, or `5.55%`
  of analysis rows. If stop-call disagreement rows are also excluded, the
  excluded share rises to `7.74%`.
- The strongest late-running line findings are led by `612`, `615`, `L8`,
  `614`, and `720` when ranked by p90 delay. Line `L8` has only `50` buckets,
  so its ranking is less stable than high-volume lines such as `612`, `615`,
  `614`, `25`, `24`, and `25A`.
- Early running is a separate operational issue. `P6` is the clearest outlier:
  `93.81%` of its buckets are early and `60.85%` are more than three minutes
  early. Other notable early-running lines include `N10`, `711`, `L4`, `L6`,
  `L1`, `L5`, and `L2`.
- Systemwide delay pressure is highest in the afternoon, especially
  `15:00-16:00` local time. Those hours have the largest p90 delay and the
  highest share of buckets more than five minutes late.
- Rush-window impact is concentrated on a smaller set of lines. `612` and
  `615` show the largest p90 rush delay lifts, with much higher late-running
  rates during rush periods.
- Stop-level midpoint changes identify locations whose observed p90 delay
  changed between the first and second half of the cached range. These are
  before/after indicators, not causal evidence.
- Service-alert matched-control analysis shows generally modest group-level
  delay lifts, but some line-level alert matches show larger p90 lifts. Small
  alert sample sizes should be interpreted cautiously.
- Collector blackout and missing-data report tables currently contain no
  matching rows under the configured definitions.

## Scope And Methodology

The source of truth for this report is the existing cache built at
`2026-05-11T05:32:44+00:00`. The manifest is
[`outputs/report-cache/manifest.json`](outputs/report-cache/manifest.json).

| Item | Value |
| --- | --- |
| Database | `data/foli.db` |
| Raw vehicle observations | `5,647,149` |
| Analysis rows | `5,585,585` |
| Cached trip-stop buckets | `2,012,287` |
| Lines represented | `138` |
| Representative bucket range | `2026-04-23T09:45:00Z` to `2026-05-08T09:50:00Z` |
| Quality mode | `conservative` |
| Bucket mode | `trip-stop` |
| Timezone | `Europe/Helsinki` |
| Minimum grouped observations | `30` |
| Rush windows | `07:00-09:00`, `15:00-18:00` local weekdays |

The analysis uses SIRI vehicle-monitoring delay values. These are estimated
vehicle state values, not measured stop arrival truth. Raw vehicle-monitoring
rows are repeated polls, so the default analysis collapses rows into trip-stop
buckets to avoid overweighting vehicles that remain visible for longer.

Primary delay findings use robust metrics:

- `median_delay_min`: typical signed delay in minutes.
- `p90_delay_min`: high-end signed delay; used for late-running rankings.
- `pct_over_5_min_late`: share of buckets more than five minutes late.
- `pct_over_3_min_early`: share of buckets more than three minutes early.
- `p90_early_min_abs`: high-end early-running magnitude in absolute minutes.

## Data Quality Findings

The default conservative filter removes rows that are implausible, stale,
pre-trip, or post-trip. Stop-call disagreement is flagged by default but not
removed unless explicitly requested.

| Quality check | Rows | Share |
| --- | ---: | ---: |
| Analysis rows | 5,585,585 | 100.00% |
| Implausible delay | 4,001 | 0.07% |
| Stale observation | 73,822 | 1.32% |
| Pre-trip observation | 186,131 | 3.33% |
| Post-trip observation | 101,019 | 1.81% |
| Stop-call disagreement | 173,682 | 3.11% |
| Conservative default excluded | 310,081 | 5.55% |
| Conservative with stop-call disagreement excluded | 432,344 | 7.74% |

Pre-trip and post-trip observations are the largest default quality exclusions.
Stop-call disagreement is also material at `3.11%`, but it is handled as a flag
in the default report so the core findings are not narrowed more aggressively.

### Worst Quality Lines

The worst data-quality rates are concentrated on a small set of lines. These
lines should be treated carefully in operational interpretation, especially
where delay rankings and data-quality flags overlap.

| Line | Rows | Default excluded | Excluded share |
| --- | ---: | ---: | ---: |
| `79A` | 3,512 | 2,244 | 63.90% |
| `N6` | 8,135 | 4,809 | 59.11% |
| `711` | 6,875 | 3,842 | 55.88% |
| `P3` | 9,659 | 5,292 | 54.79% |
| `V2` | 2,818 | 1,499 | 53.19% |
| `L13` | 3,525 | 1,841 | 52.23% |
| `P6` | 10,932 | 5,450 | 49.85% |
| `N10` | 8,461 | 3,190 | 37.70% |
| `71` | 7,264 | 2,715 | 37.38% |
| `L4` | 8,164 | 2,872 | 35.18% |

`P6`, `N10`, `711`, and `L4` also appear in early-running findings, so their
early-running results should be read together with these data-quality rates.

Full data-quality tables:
[`quality_summary.csv`](outputs/report-cache/quality_summary.csv),
[`quality_by_line.csv`](outputs/report-cache/quality_by_line.csv).

## Late-Running Line Findings

Late-running line rankings use `p90_delay_min` as the primary sort key. This
captures high-end delay more reliably than the signed mean.

| Rank | Line | Buckets | Median delay | p90 delay | >5 min late |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | `612` | 1,397 | 4.12 min | 15.38 min | 46.96% |
| 2 | `615` | 3,199 | 2.80 min | 14.79 min | 37.95% |
| 3 | `L8` | 50 | 3.66 min | 11.38 min | 36.00% |
| 4 | `614` | 3,393 | 4.00 min | 10.23 min | 39.91% |
| 5 | `720` | 1,601 | 3.12 min | 8.45 min | 29.42% |
| 6 | `25` | 13,306 | 2.06 min | 8.37 min | 26.01% |
| 7 | `V1` | 2,345 | 1.45 min | 8.31 min | 22.26% |
| 8 | `42A` | 1,899 | 0.87 min | 8.17 min | 18.96% |
| 9 | `24` | 24,585 | 1.03 min | 8.07 min | 20.39% |
| 10 | `25A` | 15,796 | 2.72 min | 7.95 min | 24.19% |

The strongest late-running evidence is on `612`, `615`, and `614`: they have
both high p90 delay and thousands of buckets. `L8` has severe p90 delay but only
`50` buckets, so it is a useful signal for follow-up rather than a stable
networkwide conclusion.

Lines `24`, `25`, and `25A` have lower p90 delay than the top few routes but
far more observations. Their findings matter because they represent repeated
delay across a much larger volume of service.

Full table: [`line_late_rankings.csv`](outputs/report-cache/line_late_rankings.csv).

## Early-Running Line Findings

Early running matters because it can cause missed boardings even when average
delay appears acceptable. The early-running ranking uses early magnitude and
early shares rather than p90 late delay.

| Rank | Line | Buckets | Median delay | Early | >3 min early | p90 early magnitude |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | `P6` | 1,663 | -4.23 min | 93.81% | 60.85% | 16.12 min |
| 2 | `903` | 995 | 0.00 min | 30.75% | 12.66% | 12.85 min |
| 3 | `901` | 8,076 | 0.00 min | 49.24% | 24.26% | 12.02 min |
| 4 | `615` | 3,199 | 2.80 min | 24.66% | 10.03% | 9.47 min |
| 5 | `N10` | 1,189 | -2.57 min | 70.14% | 46.43% | 8.88 min |
| 6 | `801` | 45,201 | 0.00 min | 48.25% | 19.39% | 8.57 min |
| 7 | `75` | 283 | -2.55 min | 93.29% | 46.64% | 8.45 min |
| 8 | `L4` | 1,445 | -1.57 min | 62.84% | 34.46% | 8.05 min |
| 9 | `N7` | 1,593 | -0.32 min | 53.86% | 31.64% | 7.96 min |
| 10 | `42` | 1,722 | 0.82 min | 34.67% | 8.94% | 7.91 min |

`P6` is the dominant early-running finding by both frequency and magnitude.
`N10`, `711`, `L4`, `L6`, `L1`, `L5`, and `L2` also show high rates of buckets
more than three minutes early. `615` appears in both late and early rankings,
which suggests high variability rather than a simple consistently late profile.

Full table: [`line_early_rankings.csv`](outputs/report-cache/line_early_rankings.csv).

## Context Delay Hotspots

Context metrics group by line, direction, local hour, and weekday/weekend. They
are useful for finding specific operating conditions where delay is concentrated.

| Line | Direction | Hour | Day type | Buckets | Median delay | p90 delay | >5 min late |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: |
| `705` | 1 | 16:00 | weekend | 30 | 0.06 min | 39.25 min | 23.33% |
| `24` | 1 | 07:00 | weekend | 167 | -1.68 min | 28.39 min | 12.57% |
| `402` | 2 | 22:00 | weekday | 216 | -1.10 min | 27.83 min | 20.37% |
| `24` | 1 | 17:00 | weekend | 181 | 6.15 min | 24.40 min | 58.01% |
| `24` | 2 | 17:00 | weekend | 178 | 1.03 min | 21.40 min | 24.72% |
| `25A` | 1 | 10:00 | weekday | 433 | 0.55 min | 21.20 min | 14.09% |
| `N12` | 2 | 07:00 | weekday | 50 | 0.00 min | 20.27 min | 16.00% |
| `615` | 2 | 17:00 | weekday | 695 | 12.55 min | 19.69 min | 89.06% |
| `901` | 2 | 12:00 | weekday | 34 | 4.42 min | 19.50 min | 44.12% |
| `612` | 2 | 15:00 | weekday | 689 | 10.22 min | 17.60 min | 91.44% |

The context table shows two different kinds of hotspots:

- Sparse but severe contexts, such as `705` direction `1` at weekend `16:00`,
  where the bucket count is exactly the minimum threshold.
- High-confidence recurring contexts, such as `615` direction `2` weekday
  `17:00` and `612` direction `2` weekday `15:00`, where hundreds of buckets
  show both high median delay and very high late-running shares.

Full table: [`context_delay_metrics.csv`](outputs/report-cache/context_delay_metrics.csv).

## Hourly Delay Profile

The hourly profile aggregates all lines by local hour. Networkwide medians stay
near zero, but p90 delay and late-running shares rise during the afternoon.

| Hour | Buckets | Median delay | p90 delay | >5 min late | Early |
| --- | ---: | ---: | ---: | ---: | ---: |
| 07:00 | 133,523 | 0.00 min | 2.57 min | 2.50% | 47.49% |
| 08:00 | 132,500 | 0.00 min | 2.90 min | 3.72% | 46.64% |
| 12:00 | 103,334 | 0.18 min | 3.23 min | 4.59% | 41.18% |
| 14:00 | 132,401 | 0.26 min | 3.77 min | 6.01% | 40.35% |
| 15:00 | 141,140 | 0.50 min | 4.58 min | 8.60% | 36.22% |
| 16:00 | 133,864 | 0.42 min | 4.48 min | 8.10% | 37.61% |
| 17:00 | 122,101 | -0.02 min | 2.98 min | 4.48% | 50.11% |
| 18:00 | 104,358 | 0.00 min | 2.73 min | 4.06% | 47.73% |
| 23:00 | 65,661 | -0.17 min | 1.92 min | 2.24% | 54.63% |

The clearest systemwide late-running peak is `15:00-16:00`. The morning peak is
visible but smaller in aggregate: `08:00` has a higher p90 than `07:00`, but
both remain below the afternoon p90 values. Late evening and overnight hours
have lower late-running rates but higher early-running shares.

Full table: [`hourly_delay_profile.csv`](outputs/report-cache/hourly_delay_profile.csv).

## Rush-Time Impact

Rush impact compares weekday rush windows (`07:00-09:00` and `15:00-18:00`) to
non-rush periods for the same line. The table is sorted by p90 delay lift.

| Line | Non-rush buckets | Rush buckets | Median lift | p90 lift | >5 min late lift |
| --- | ---: | ---: | ---: | ---: | ---: |
| `612` | 422 | 975 | 7.26 min | 12.27 min | 58.45 pp |
| `615` | 1,014 | 2,185 | 3.31 min | 9.44 min | 31.59 pp |
| `72` | 2,146 | 1,138 | 2.76 min | 5.21 min | 27.42 pp |
| `721` | 1,428 | 774 | 2.41 min | 3.73 min | 30.59 pp |
| `701` | 1,011 | 2,149 | 1.37 min | 3.59 min | 12.00 pp |
| `220` | 33,634 | 8,841 | 2.48 min | 3.47 min | 20.89 pp |
| `77` | 886 | 1,306 | 4.47 min | 3.39 min | -0.29 pp |
| `903` | 315 | 680 | 0.70 min | 3.31 min | 13.74 pp |
| `25A` | 12,112 | 3,684 | 2.10 min | 2.28 min | 22.58 pp |
| `24` | 19,337 | 5,248 | 1.70 min | 2.13 min | 11.56 pp |

`612` and `615` are the strongest rush-window findings. Their rush-period p90
delay is much higher than their non-rush p90 delay, and their late-running
shares increase sharply. `220`, `25A`, and `24` are also important because they
combine meaningful rush effects with large sample sizes.

Full table: [`rush_impact.csv`](outputs/report-cache/rush_impact.csv).

## Stop-Level Midpoint Changes

The midpoint comparison splits the representative bucket range into:

- Baseline: `2026-04-23T09:45:00Z` to `2026-04-30T21:47:30Z`
- Comparison: `2026-04-30T21:47:30Z` to `2026-05-08T09:50:00Z`

The comparison uses matched stop contexts. These findings show where observed
delay changed between the two halves of the available data. They do not prove
that any particular intervention or incident caused the change.

### Largest Deteriorations

| Stop | Baseline buckets | Comparison buckets | Median change | p90 change | >5 min late change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Betonimiehenkatu | 38 | 34 | -0.12 min | 9.74 min | 11.76 pp |
| Kallio | 39 | 35 | 5.07 min | 6.83 min | 32.31 pp |
| Vähätalontie | 36 | 32 | 4.47 min | 6.72 min | 41.32 pp |
| Laukolan koulu | 43 | 40 | 0.00 min | 6.68 min | 20.00 pp |
| Vajosuontie | 48 | 44 | 3.60 min | 5.89 min | 32.39 pp |
| Lavamäen kylätalo | 51 | 45 | 3.38 min | 4.99 min | 30.46 pp |

### Largest Improvements In The Reported Table

| Stop | Baseline buckets | Comparison buckets | Median change | p90 change | >5 min late change |
| --- | ---: | ---: | ---: | ---: | ---: |
| Uusikartano | 34 | 32 | -0.34 min | -4.72 min | -14.34 pp |
| Tammilehto | 40 | 36 | -1.23 min | -4.18 min | -26.94 pp |
| Montolantie | 50 | 45 | -0.04 min | -3.97 min | -21.33 pp |
| Salonkyläntie | 45 | 41 | -0.60 min | -3.87 min | -23.79 pp |
| Monnoistentie | 68 | 63 | -0.02 min | -3.86 min | -8.36 pp |
| Järvenpääntie | 68 | 64 | -0.29 min | -3.78 min | -9.83 pp |

The strongest deterioration signals are mostly low-to-moderate volume stop
contexts. They are good candidates for drill-down, but should be validated
against route, timetable, construction, and event context before drawing an
operational conclusion.

Full table: [`stop_midpoint_change.csv`](outputs/report-cache/stop_midpoint_change.csv).

## Service Alert Matched-Control Findings

Service alert analysis compares alert-matched observations with controls from
the same line, direction, local hour, and weekday/weekend context. Results are
associations, not causal effects.

### Group-Level Alert Effects

| Cause | Effect | Scope | Priority | Alert buckets | Median lift | p90 lift | >5 min late lift |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `OTHER_CAUSE` | `DETOUR` | route | 900 | 601,681 | 0.18 min | 0.78 min | 2.65 pp |
| `OTHER_CAUSE` | `UNKNOWN_EFFECT` | route | 900 | 891 | 0.25 min | 0.48 min | 2.10 pp |
| `OTHER_CAUSE` | `DETOUR` | route | 1000 | 415,259 | 0.08 min | 0.46 min | 1.64 pp |
| `OTHER_CAUSE` | `OTHER_EFFECT` | route | 1000 | 145,510 | 0.07 min | 0.37 min | 1.52 pp |
| `OTHER_CAUSE` | `Unknown` | route | 1200 | 454,964 | 0.04 min | 0.24 min | 0.89 pp |
| `OTHER_CAUSE` | `DETOUR` | stop | 1000 | 428,653 | 0.03 min | 0.20 min | 0.48 pp |
| `ACCIDENT` | `Unknown` | stop | 1200 | 91,613 | 0.12 min | 0.09 min | -0.07 pp |
| `TECHNICAL_PROBLEM` | `Unknown` | route | 1200 | 521,441 | -0.01 min | 0.00 min | 0.22 pp |

At the grouped level, route detours have the clearest positive lift, but the
lift is still modest compared with the worst line-level and context-specific
delay findings. Some alert groups have near-zero or negative p90 lift, which
suggests that alert presence alone is not enough to explain delay severity.

### Largest Line-Level Alert Lifts

| Cause | Effect | Scope | Line | Alert buckets | Median lift | p90 lift | >5 min late lift |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| `ACCIDENT` | `Unknown` | stop | `28A` | 50 | 0.18 min | 11.07 min | 20.00 pp |
| `TECHNICAL_PROBLEM` | `Unknown` | stop | `722` | 39 | -4.67 min | 6.05 min | 15.69 pp |
| `OTHER_CAUSE` | `DETOUR` | stop | `42A` | 600 | 1.13 min | 5.32 min | 21.02 pp |
| `OTHER_CAUSE` | `DETOUR` | stop | `N11` | 268 | -0.37 min | 5.30 min | 15.09 pp |
| `TECHNICAL_PROBLEM` | `Unknown` | stop | `21` | 11,675 | 3.63 min | 4.57 min | 28.37 pp |
| `ACCIDENT` | `Unknown` | stop | `220` | 312 | 1.74 min | 4.09 min | 18.35 pp |
| `OTHER_CAUSE` | `DETOUR` | stop | `25A` | 2,934 | 0.70 min | 3.78 min | 10.53 pp |
| `TECHNICAL_PROBLEM` | `Unknown` | stop | `82` | 467 | 1.52 min | 3.55 min | 18.08 pp |

The largest line-level p90 lifts often have small alert bucket counts. The most
operationally credible high-volume signal in this table is line `21` during
stop-scoped technical-problem alerts: `11,675` alert buckets, `3.63` minutes of
median lift, and `4.57` minutes of p90 lift.

Full tables:
[`service_alert_grouped.csv`](outputs/report-cache/service_alert_grouped.csv),
[`service_alert_by_line.csv`](outputs/report-cache/service_alert_by_line.csv).

## Collector Coverage Findings

The cache manifest contains `46,869` collector poll records, but the generated
collector result tables currently report no matching rows:

- Collector blackouts: no matching rows.
- Collector missing-data summary: no matching rows.
- Collector missing-data spots: no matching rows.

This means no blackouts or missing-data spots matched the current report
definitions. It should not be read as proof that collection was perfect; it
only describes the configured report outputs.

Full tables:
[`collector_blackouts.csv`](outputs/report-cache/collector_blackouts.csv),
[`collector_missing_summary.csv`](outputs/report-cache/collector_missing_summary.csv),
[`collector_missing_spots.csv`](outputs/report-cache/collector_missing_spots.csv).

## Overall Interpretation

The most actionable reliability findings are concentrated in three areas:

1. Late-running priority lines: `612`, `615`, `614`, `720`, `25`, `24`, and
   `25A`.
2. Early-running priority lines: `P6`, `N10`, `711`, `L4`, `L6`, `L1`, `L5`,
   and `L2`.
3. Time-and-context hotspots: afternoon rush contexts, especially `612`
   direction `2` at weekday `15:00` and `615` direction `2` at weekday `17:00`.

The top-level network median is usually close to zero, so averages alone would
hide the most important patterns. The operational issues appear in high-end
delay, early-running shares, and specific line-direction-hour contexts.

Data quality is good enough for broad screening, but not perfect. Lines with
high conservative exclusion rates should be validated before making line-level
decisions. Stop-level midpoint and service-alert results are best used as
investigation leads because they depend on matched context definitions and can
be sensitive to sample size.

## Source Artifacts

- Generated markdown table report:
  [`reports/generated/overall-results.md`](reports/generated/overall-results.md)
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

- SIRI VM delay is estimated vehicle-monitoring state, not measured arrival
  truth.
- The report uses trip-stop buckets by default. Results would differ if raw
  polls were treated as independent observations.
- Conservative quality filtering excludes implausible, stale, pre-trip, and
  post-trip rows. Stop-call disagreement is flagged but not excluded in the
  default cache.
- The cached data ends at `2026-05-08T09:36:13Z`; later data is not included.
- Service-alert and stop-midpoint findings are matched observational
  comparisons. They should not be interpreted as causal proof.
- Some high-ranked findings have low sample sizes near the `30` bucket minimum.
  These should be validated before prioritizing operational action.
