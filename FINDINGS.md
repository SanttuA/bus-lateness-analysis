# Complete Bus Lateness Findings

This report summarizes the latest generated bus lateness analysis artifacts in
this repository. The primary source is the pandas/DuckDB report at
[`reports/generated/overall-results.md`](reports/generated/overall-results.md),
generated at `2026-10-19T08:07:13+00:00` from a rebuilt cache. The Polars
report and the public site data were rebuilt from the same database and agree
with it table by table (see
[Pandas/DuckDB vs Polars](#pandasduckdb-vs-polars)).

This version replaces the May 2026 findings after an audit of the analysis
method. [What Changed In The October 2026 Audit](#what-changed-in-the-october-2026-audit)
lists the corrections and how they moved the headline numbers.

## Executive Summary

- The data covers `2026-04-23T08:05:22Z` to `2026-05-23T13:24:20Z`: `10,430,580`
  analysis rows from `10,532,270` raw vehicle observations.
- After quality filtering, the trip-stop dataset has `3,729,540` buckets ("stop
  passes"), representing `8,582,662` raw polls, `102,868` trips and `140` lines
  over `31` days.
- Conservative quality filtering excludes `1,847,918` rows, or `17.72%` of
  analysis rows. Most of the increase from the earlier `5.69%` comes from a new
  exclusion: `1,559,099` rows (`14.95%`) of buses waiting at their first stop
  before departure, which Föli reports with a placeholder delay of 0.
- Across the network, `65.41%` of stop passes are on time (1 minute early to 3
  minutes late), `3.80%` are more than 5 minutes late and `7.83%` are more than
  3 minutes early. The median delay is `0.02` minutes and the p90 is `2.88`
  minutes.
- Among frequent lines (at least 20 trips a day), late running is most common on
  `24` (`18.76%` of stop passes more than 5 minutes late), `21` (`16.86%`) and
  `220` (`10.07%`). Lines `612`, `615` and `614` have the highest p90 delays, but
  each runs only 2-4 trips a day, so their results rest on 37-74 trips.
- Early running is a separate problem. Among frequent lines it is most common on
  `K3` (`28.40%` of stop passes more than 3 minutes early), `801` (`19.57%`) and
  `28` (`19.27%`). The most extreme lines overall are infrequent service lines:
  `P6` (`59.84%`), `75` (`46.30%`) and `L2` (`44.14%`), each with 36-74 trips.
- On weekdays the late-running peak is at `16:00` (`8.73%` more than 5 minutes
  late) and `15:00` (`8.40%`). Weekend late running peaks at `15:00` (`6.44%`).
- Compared with weekday daytime service (09:00-15:00), the rush adds the most
  late running on `721` (+`32.13` percentage points, 95% interval
  `21.05`-`42.33`), `722S` (+`25.33`) and `614` (+`24.38`). The earlier rush
  leaders `612` and `615` run no weekday daytime service, so their rush effect
  cannot be measured this way.
- The clearest disruption signal is a route-level detour notice: `+2.16`
  percentage points more stop passes over 5 minutes late (95% interval
  `1.75`-`2.59`) across `370,654` affected stop passes. Single cancelled
  departures (`74` in total) are now counted separately instead of marking whole
  lines as disrupted until the end of the data.
- The worst day for late running was the May Day holiday, `2026-05-01` (`8.66%`
  more than 5 minutes late), followed by May Day eve, `2026-04-30` (`6.63%`).
  Collection gaps on 9-10 May and 13-14 May leave four days below 95%
  collector coverage.

## Scope And Methodology

| Item | Value |
| --- | --- |
| Database | `data/foli.db` |
| Generated report | `reports/generated/overall-results.md` |
| Cache manifest | `outputs/report-cache/manifest.json` |
| Report generated at | `2026-10-19T08:07:13+00:00` |
| Raw vehicle observations | `10,532,270` |
| Analysis rows | `10,430,580` |
| Cached trip-stop buckets | `3,729,540` |
| Raw polls represented by buckets | `8,582,662` |
| Distinct trips | `102,868` |
| Lines represented | `140` |
| Representative bucket range | `2026-04-23T09:45:00Z` to `2026-05-23T13:33:57Z` |
| Quality mode | `conservative` |
| Bucket mode | `trip-stop` |
| Timezone | `Europe/Helsinki` |
| Minimum grouped observations | `30` buckets and `10` distinct trips |
| On-time band | 1 minute early to 3 minutes late |
| Rush windows | `07:00-09:00`, `15:00-18:00` local weekdays |
| Off-peak baseline | `09:00-15:00` local weekdays |
| Reduced-service holidays in range | `2026-05-01`, `2026-05-14` |

The analysis uses SIRI vehicle-monitoring delay values. They are the vehicle's
estimated state while approaching the next stop, not measured stop arrival or
departure times. Raw rows are repeated 30-second polls, so the default report
collapses them into trip-stop buckets: one value per trip and next stop.

Method details that matter for reading the numbers:

- **Trips, not buckets, set the real sample size.** One trip contributes about 70
  correlated buckets. Every grouped table requires at least `10` distinct trips
  as well as `30` buckets. The 95% intervals come from a bootstrap that resamples
  whole trips (300 replicates, fixed seed).
- **Holidays count as weekend days.** A Monday-Friday date with fewer than 60% of
  the usual active GTFS services is treated as a holiday. In this period that
  catches May Day (`2026-05-01`) and Ascension Day (`2026-05-14`), which were
  previously counted as normal weekdays.
- **Shares, not only percentiles.** Rankings now use the share of stop passes more
  than 5 minutes late (`pct_over_5_min_late`) or more than 3 minutes early
  (`pct_over_3_min_early`). The p90 delay is still reported, with an interval.

## Data Quality Findings

The default conservative filter removes:

- implausible delays;
- stale observations;
- pre-trip and post-trip observations;
- two kinds of placeholder zero delay:
  - origin layovers: the next stop is the trip origin, recorded before the aimed
    departure;
  - trips without realtime data: every stop of the trip reports exactly 0.

Stop-call disagreement and the remaining placeholder zeros are flagged but not
removed.

| Quality check | Rows | Share |
| --- | ---: | ---: |
| Analysis rows | 10,430,580 | 100.00% |
| Implausible delay | 6,637 | 0.06% |
| Stale observation | 149,992 | 1.44% |
| Pre-trip observation | 343,245 | 3.29% |
| Post-trip observation | 201,581 | 1.93% |
| Origin layover (new) | 1,559,099 | 14.95% |
| Trip without realtime data (new) | 31,177 | 0.30% |
| Stop-call disagreement (flag only) | 320,371 | 3.07% |
| Placeholder zero (flag only, new) | 664,631 | 6.37% |
| Conservative default excluded | 1,847,918 | 17.72% |
| Conservative with stop-call disagreement excluded | 2,024,162 | 19.41% |

Origin layovers are the largest exclusion because a bus waiting at its first
stop is polled every 30 seconds, and Föli reports most of those rows with a
delay of exactly 0. Before this fix, exact zero appeared about 42 times as often
as each neighbouring second (1-5 seconds early or late) among accepted rows.
After it, the ratio is about 15. Removing these rows takes only `17,230`
buckets out of the data, because most of a waiting trip's polls collapse into
one bucket, but they no longer pull terminus stops and the network median
towards a perfect 0.

A smaller zero spike remains. Of the `477,453` accepted rows with exactly 0
delay, `199,285` disagree with the next stop-call times by more than a minute.
A row-level sensitivity check bounds their effect:

- On-time share falls from `63.14%` to between `62.27%` (if these rows are
  dropped) and `61.13%` (if they are replaced with the stop-call delay).
- The share more than 3 minutes early rises from `8.55%` to `9.33%` if they are
  replaced.
- The share more than 5 minutes late barely moves (`5.80%` to `5.86%`).

Treat on-time shares as up to about 2 percentage points optimistic.

### Lines With Incomplete Data

The excluded share now mostly reflects how long buses wait at the first stop,
so it is no longer a pure fault measure. A line is flagged as low quality when
more than 20% of its rows are data faults (implausible, stale, post-trip or
no-realtime rows), or when fewer than 80% of its scheduled trips appear on days
with full collector coverage.

`19` lines are flagged. They are kept in the data but left out of the frequent
line comparisons and the generated takeaways.

| Line | Rows | Excluded share | Data fault share |
| --- | ---: | ---: | ---: |
| `N14` | 5,035 | 72.41% | 48.90% |
| `L13` | 6,377 | 62.13% | 47.81% |
| `N10` | 16,349 | 47.51% | 31.97% |
| `P4` | 2,611 | 49.48% | 31.52% |
| `N6` | 13,832 | 63.01% | 26.53% |
| `79A` | 6,103 | 70.85% | 25.97% |
| `711` | 12,774 | 67.37% | 23.16% |
| `67` | 12,071 | 50.32% | 22.14% |

The other flagged lines (`203`, `2Y`, `76`, `79`, `82`, `K1`, `K2`, `K4`, `K6`,
`L8`, `L9`) are flagged for low trip coverage, not data faults. Line `P6`, the
most extreme early-running line, has a high excluded share (`47.15%`) but only
`7.46%` data faults and full trip coverage, so it is no longer flagged. Its
excluded rows are mostly waiting at the first stop.

Full tables:
[`quality_summary.csv`](outputs/report-cache/quality_summary.csv),
[`quality_by_line.csv`](outputs/report-cache/quality_by_line.csv),
[`line_quality.csv`](outputs/report-cache/line_quality.csv).

## Late-Running Line Findings

The generated report still sorts its late ranking by p90 delay. That puts lines
with only 2-4 trips a day first, so the table below shows the frequent lines
(at least 20 trips a day, no quality flag). Intervals are 95% trip-bootstrap
intervals.

| Line | Trips a day | Trips | On time | >5 min late (95% CI) | p90 delay, min (95% CI) |
| --- | ---: | ---: | ---: | ---: | ---: |
| `24` | 33.4 | 1,035 | 53.84% | 18.76% (16.61–20.60) | 7.55 (7.02–8.06) |
| `21` | 32.7 | 1,013 | 53.98% | 16.86% (15.03–19.01) | 6.47 (6.06–6.92) |
| `220` | 59.1 | 1,774 | 57.58% | 10.07% (9.04–11.12) | 5.03 (4.79–5.27) |
| `206` | 20.6 | 392 | 70.62% | 9.49% (7.19–11.75) | 4.93 (4.55–5.40) |
| `1A` | 24.5 | 736 | 81.77% | 4.83% (3.54–6.32) | 3.20 (2.85–3.61) |
| `6A` | 88.5 | 2,744 | 64.43% | 4.62% (4.03–5.18) | 3.37 (3.18–3.54) |
| `28` | 34.4 | 1,031 | 49.05% | 4.15% (3.52–4.92) | 3.40 (3.23–3.59) |
| `4` | 146.5 | 4,540 | 70.60% | 4.15% (3.74–4.59) | 3.02 (2.89–3.13) |

Lines `24` and `21` stand out clearly. About one stop pass in six is more than
5 minutes late, and their intervals do not overlap with any other frequent
line. Line `220` is next, with a narrower interval because it runs nearly
twice as many trips.

The p90 ranking in the generated report is led by:

- `612`: 14.12 minutes (11.08–16.04), 37 trips;
- `615`: 13.40 minutes, 49 trips;
- `614`: 10.20 minutes, 74 trips.

These are commuter lines running 2-4 trips a day. Their late running is real
(even the lower interval bounds are high), but it affects few departures.
Lines `25` and `25A` have a higher p90 than `24` (`8.10` and `7.75` minutes)
but run 17-18 trips a day, just under the frequent-line threshold.

Full table: [`line_late_rankings.csv`](outputs/report-cache/line_late_rankings.csv).

## Early-Running Line Findings

Early running matters because a bus that passes early can leave waiting
passengers behind, even when its average delay looks fine. The early ranking now
sorts by the share of stop passes more than 3 minutes early. Previously it
sorted by the size of early running among early buckets only, which put lines
like `615` in both the late and early top 10.

Frequent lines:

| Line | Trips a day | Trips | On time | >3 min early (95% CI) | Median delay, min |
| --- | ---: | ---: | ---: | ---: | ---: |
| `K3` | 22.9 | 550 | 40.30% | 28.40% (26.14–30.48) | -1.52 |
| `801` | 60.2 | 1,865 | 52.44% | 19.57% (18.57–20.54) | 0.03 |
| `28` | 34.4 | 1,031 | 49.05% | 19.27% (18.33–20.07) | -0.22 |
| `26` | 55.2 | 1,381 | 59.17% | 14.05% (12.84–15.17) | -0.35 |
| `3A` | 85.5 | 1,624 | 60.09% | 13.71% (12.89–14.49) | -0.10 |
| `23` | 32.7 | 1,013 | 63.85% | 13.69% (12.50–14.95) | 0.00 |

All lines, from the generated report:

| Rank | Line | Buckets | Trips | Median delay | >3 min early (95% CI) | p90 early magnitude |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | `P6` | 3,070 | 74 | -4.22 min | 59.84% (53.98–64.79) | 15.86 min |
| 2 | `75` | 527 | 36 | -2.64 min | 46.30% (41.79–51.51) | 8.45 min |
| 3 | `N10` | 2,192 | 168 | -2.40 min | 45.30% (39.82–50.16) | 9.30 min |
| 4 | `L13` | 579 | 30 | -2.32 min | 44.39% (37.36–50.97) | 7.80 min |
| 5 | `L2` | 759 | 37 | -2.30 min | 44.14% (40.28–47.78) | 6.68 min |
| 6 | `77` | 3,888 | 88 | -1.96 min | 43.00% (35.95–50.80) | 6.95 min |
| 7 | `L6` | 2,119 | 75 | -2.16 min | 41.10% (35.15–47.54) | 8.05 min |
| 8 | `L1` | 757 | 36 | -1.68 min | 37.38% (29.76–43.07) | 7.68 min |
| 9 | `711` | 2,156 | 88 | -1.73 min | 35.30% (29.35–40.98) | 7.30 min |
| 10 | `V4` | 479 | 35 | -2.13 min | 34.45% (28.23–40.56) | 4.60 min |

`P6` is the clearest early-running case by a wide margin. The interval is
wide but far from the rest, and it is not a data-quality artefact. `N10`, `L13`
and `711` are flagged for incomplete data, so their positions are less certain.
`K3` is the frequent line where early running affects the most departures.

Full table: [`line_early_rankings.csv`](outputs/report-cache/line_early_rankings.csv).

## Context Delay Hotspots

Context metrics group by line, direction, local hour and weekday or weekend.
Every row needs at least 10 distinct trips. Before this rule, some contexts
rested on 1-5 trips.

| Line | Direction | Hour | Day type | Buckets | Trips | Median delay | p90 delay | >5 min late |
| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `25` | 2 | 16:00 | weekend | 342 | 10 | 3.76 min | 34.18 min | 42.40% |
| `402` | 2 | 22:00 | weekday | 385 | 16 | -1.10 min | 27.83 min | 10.39% |
| `24` | 1 | 17:00 | weekend | 392 | 18 | 3.91 min | 23.65 min | 46.17% |
| `24` | 1 | 14:00 | weekend | 371 | 16 | 7.13 min | 22.95 min | 60.92% |
| `24` | 2 | 17:00 | weekend | 399 | 18 | 0.58 min | 22.07 min | 28.32% |
| `901` | 2 | 12:00 | weekday | 63 | 17 | 5.87 min | 19.50 min | 60.32% |
| `901` | 2 | 14:00 | weekday | 254 | 13 | 1.48 min | 19.50 min | 18.11% |
| `615` | 2 | 17:00 | weekday | 1,223 | 19 | 12.23 min | 19.12 min | 95.26% |
| `901` | 2 | 13:00 | weekday | 614 | 13 | 1.33 min | 19.08 min | 23.29% |
| `612` | 2 | 15:00 | weekday | 1,311 | 19 | 9.45 min | 16.45 min | 87.57% |

Most rows in this table rest on 10-19 trips. The most consistent signals are:

- `615` direction 2 at 17:00 and `612` direction 2 at 15:00 on weekdays, where
  nearly every stop pass is more than 5 minutes late;
- line `24` on weekend afternoons, which supports its place at the top of the
  frequent-line ranking.

Full table: [`context_delay_metrics.csv`](outputs/report-cache/context_delay_metrics.csv).

## Hourly Delay Profile

The hourly profile is now split into weekdays and weekends with holidays, and it
covers every hour that passes the minimums. Before this change, the Polars
version kept only the 20 hours with the highest p90, and both day types were
mixed.

Weekdays:

| Hour | Buckets | Trips | On time | >5 min late | >3 min early | p90 delay |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 07:00 | 198,257 | 8,898 | 67.35% | 2.69% | 7.91% | 2.73 min |
| 08:00 | 198,422 | 9,080 | 66.16% | 4.42% | 7.19% | 3.18 min |
| 12:00 | 143,067 | 6,624 | 67.88% | 4.67% | 5.86% | 3.32 min |
| 13:00 | 168,444 | 7,795 | 64.99% | 5.72% | 6.94% | 3.72 min |
| 14:00 | 195,645 | 9,040 | 64.49% | 5.68% | 6.71% | 3.72 min |
| 15:00 | 211,689 | 9,683 | 62.59% | 8.40% | 5.92% | 4.55 min |
| 16:00 | 197,433 | 8,877 | 62.21% | 8.73% | 5.36% | 4.67 min |
| 17:00 | 175,669 | 7,950 | 60.47% | 4.47% | 9.88% | 2.95 min |
| 18:00 | 142,192 | 6,461 | 64.57% | 3.65% | 9.52% | 2.72 min |

On weekdays, late running builds up through the day and peaks at 15:00-16:00.
At 17:00 it drops sharply while early running rises to `9.88%`, the highest
daytime share. Many buses seem to run ahead of their timetable once the
afternoon traffic eases. Weekends follow a flatter pattern, with late running
peaking at 15:00 (`6.44%`).

Full table: [`hourly_delay_profile.csv`](outputs/report-cache/hourly_delay_profile.csv).

## Rush-Time Impact

Rush impact compares weekday rush windows (07:00-09:00 and 15:00-18:00) with
weekday daytime service (09:00-15:00) on the same line. The earlier comparison
used every non-rush stop pass, including nights, weekends and holidays. That
mixed rush effects with differences between quiet and busy periods. Lines are
ranked by the change in the share more than 5 minutes late, with a 95%
trip-bootstrap interval.

| Line | Off-peak trips | Rush trips | >5 min late off-peak | >5 min late rush | Lift (95% CI) |
| --- | ---: | ---: | ---: | ---: | ---: |
| `721` | 34 | 19 | 7.03% | 39.17% | +32.13 pp (21.05–42.33) |
| `722S` | 18 | 36 | 2.75% | 28.07% | +25.33 pp (15.67–37.03) |
| `614` | 55 | 37 | 29.34% | 53.72% | +24.38 pp (12.09–35.06) |
| `72` | 94 | 55 | 11.37% | 27.92% | +16.55 pp (4.43–29.20) |
| `220` | 469 | 436 | 12.54% | 28.50% | +15.96 pp (12.18–19.40) |
| `P1` | 92 | 54 | 1.05% | 13.32% | +12.27 pp (6.05–19.40) |
| `722` | 91 | 78 | 11.84% | 22.19% | +10.36 pp (3.27–19.02) |
| `701` | 71 | 148 | 5.71% | 15.28% | +9.57 pp (1.68–15.60) |
| `28` | 261 | 256 | 2.82% | 11.19% | +8.37 pp (6.12–10.47) |
| `206` | 184 | 112 | 7.00% | 15.06% | +8.06 pp (1.72–13.04) |

`721`, `722S` and `614` have the largest rush lifts, but on small samples.
Line `220` is the strongest high-volume finding: hundreds of trips on both
sides and a narrow interval around a 16-point increase. Line `28` shows the
same pattern on a smaller scale.

Lines `612` and `615`, the earlier rush leaders, run no weekday 09:00-15:00
service. Their earlier rush lift compared rush hours with evenings and
weekends, so it is not in this table. Line `75`, previously second, rested on 36
baseline buckets. Its "lift" came from the line becoming less early, not
later.

Full table: [`rush_impact.csv`](outputs/report-cache/rush_impact.csv).

## Daily Network Profile

The stop-level midpoint comparison has been removed. Its two halves were
uneven: the second half held both multi-day collector gaps, and one holiday
fell in each half. The listed stops also had only 31-130 buckets per half, so
the largest changes were mostly noise. A daily network profile replaces it.

| Date | Day type | Trips | On time | >5 min late | >3 min early | Collector coverage |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2026-04-30 | weekday | 4,166 | 62.83% | 6.63% | 7.39% | 99.62% |
| 2026-05-01 | holiday | 2,467 | 64.11% | 8.66% | 5.38% | 99.71% |
| 2026-05-08 | weekday | 4,311 | 64.80% | 4.94% | 7.24% | 97.06% |
| 2026-05-09 | weekend | 1,400 | 67.00% | 1.37% | 8.52% | 53.83% |
| 2026-05-10 | weekend | 1,022 | 67.67% | 1.79% | 7.96% | 31.44% |
| 2026-05-13 | weekday | 111 | 52.96% | 0.18% | 17.94% | 12.55% |
| 2026-05-14 | holiday | 1,167 | 68.39% | 2.34% | 7.09% | 35.58% |
| 2026-05-19 | weekday | 4,326 | 64.90% | 4.55% | 7.55% | 99.27% |

On days with full collector coverage, the on-time share stays within
`62.83%`-`66.89%`. Typical weekdays have `3`-`5%` of stop passes more than 5
minutes late, and complete normal weekend days `1.4`-`2.6%`. The last day,
`2026-05-23`, ends in the afternoon, so it covers only part of a day.

May Day stands out. Late running on `2026-05-01` was the highest of the
period, even though it ran a holiday timetable, and May Day eve was the worst
regular weekday. Both fit the city's May Day crowds and events.

Days with low coverage should not be compared with the rest. `2026-05-13`
has only 111 trips of data.

Full table: [`daily_network_profile.csv`](outputs/report-cache/daily_network_profile.csv).

## Service Alert Matched-Control Findings

The earlier alert results were wrong in three ways:

1. **Cancellations.** The `1,832` cancellation rows have no validity period, so
   each cancelled departure marked its whole line and all its stops as
   disrupted until the end of the data. One cancellation group alone covered
   over a million buckets.
2. **Line numbers.** A message's line field is a GTFS route id, and a
   cancellation's line field is the public line number. The code mixed the two
   up. For example, a "Linja 43" notice flagged line `42`, and line 6
   cancellations landed on line `3`.
3. **Controls.** Controls were pooled, could include buckets under other
   alerts, and were not weighted by line and hour.

The rebuilt analysis:

- maps route ids to public line numbers using the GTFS snapshot valid on that
  date;
- uses every validity window, clipped to the time the alert was actually seen
  active;
- keeps cancellations out of the lift;
- excludes alerts valid for at least 90% of the period;
- compares alert buckets with buckets in the same line, direction, hour and day
  type that had no active alert, weighted by the alert buckets in each stratum
  and with trip-bootstrap intervals.

Of the 29 alerts seen, 14 are analyzed. 7 were long-running, 7 had no
matching targets and 1 was never active.

### Group-Level Alert Effects

| Cause | Effect | Scope | Priority | Alerts | Trips | Alert buckets | >5 min late alert | >5 min late control | Lift (95% CI) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `OTHER_CAUSE` | `UNKNOWN_EFFECT` | route | 900 | 1 | 35 | 881 | 15.47% | 12.46% | +3.01 pp (-1.07–7.81) |
| `OTHER_CAUSE` | `DETOUR` | route | 1000 | 1 | 9,565 | 370,654 | 6.14% | 3.98% | +2.16 pp (1.75–2.59) |
| `OTHER_CAUSE` | `OTHER_EFFECT` | route | 1000 | 1 | 2,513 | 144,739 | 4.95% | 3.19% | +1.76 pp (1.08–2.48) |
| `OTHER_CAUSE` | `DETOUR` | stop | 1000 | 1 | 11,853 | 423,483 | 4.79% | 4.03% | +0.76 pp (0.46–1.07) |
| `OTHER_CAUSE` | `DETOUR` | route | 900 | 5 | 1,416 | 56,809 | 4.62% | 3.95% | +0.67 pp (-0.73–1.85) |
| `OTHER_CAUSE` | `STOP_MOVED` | stop | 900 | 1 | 2,137 | 2,142 | 6.68% | 6.14% | +0.54 pp (-0.17–1.19) |
| `OTHER_CAUSE` | `SIGNIFICANT_DELAYS` | stop | 900 | 2 | 7,474 | 182,364 | 2.70% | 2.37% | +0.33 pp (-0.08–0.68) |

Route-level detours and other route changes come with a small but clear rise in
late running, about 2 percentage points. Stop-level detours add less than 1
point. The other groups' intervals include zero. Disruption notices explain
little of the network's late running. The matching gives an association, not
a causal effect.

### Largest Line-Level Alert Lifts

| Cause | Effect | Scope | Line | Trips | Alert buckets | Lift (95% CI) |
| --- | --- | --- | --- | ---: | ---: | ---: |
| `OTHER_CAUSE` | `DETOUR` | stop | `L8` | 10 | 47 | +19.35 pp (4.44–40.06) |
| `OTHER_CAUSE` | `DETOUR` | stop | `N11` | 60 | 253 | +19.31 pp (8.79–32.01) |
| `OTHER_CAUSE` | `DETOUR` | stop | `42` | 12 | 547 | +18.96 pp (-17.24–49.60) |
| `OTHER_CAUSE` | `SIGNIFICANT_DELAYS` | stop | `K1` | 12 | 320 | +17.50 pp (0.00–45.16) |
| `OTHER_CAUSE` | `DETOUR` | stop | `42A` | 12 | 600 | +13.43 pp (-14.67–39.35) |
| `OTHER_CAUSE` | `DETOUR` | route | `25` | 87 | 3,976 | +9.73 pp (0.77–19.57) |
| `OTHER_CAUSE` | `DETOUR` | route | `24` | 167 | 7,335 | +8.97 pp (3.72–14.81) |
| `OTHER_CAUSE` | `DETOUR` | route | `25A` | 87 | 4,807 | +7.92 pp (-1.39–16.77) |

The largest line-level lifts rest on 10-12 trips, and several intervals
include zero. The more credible line-level signals are the route detours on
`24` and `25`, with 167 and 87 affected trips and intervals above zero.
The earlier headline signal, line `21` during technical-problem alerts, came
from cancellations being treated as line-wide disruptions and has disappeared.

### Cancelled Departures

The data contains `74` cancelled departures (`46` technical problems, `26`
other causes, `2` accidents). The most affected lines are `1` (11), `2` (8),
and `180`, `3`, `39` and `4` (5 each). They are too few, and too short, to measure an
effect on punctuality.

Full tables:
[`service_alert_grouped.csv`](outputs/report-cache/service_alert_grouped.csv),
[`service_alert_by_line.csv`](outputs/report-cache/service_alert_by_line.csv),
[`service_alert_inventory.csv`](outputs/report-cache/service_alert_inventory.csv),
[`service_cancellations.csv`](outputs/report-cache/service_cancellations.csv).

## Collector Coverage Findings

The cache contains `86,116` collector poll records. Both engines now use one
shared gap detector, so the missing-data tables agree.

| Source | Polls | Failures | Missing spots | Total missing | Largest missing | Estimated missed rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `siri_vm` | 78,203 | 191 | 71 | 4,156.67 min | 2,186.83 min | 1,122,369 |
| `siri_alerts` | 7,908 | 15 | 5 | 3,873.67 min | 2,178.08 min | 9,711 |
| `gtfs` | 5 | 0 | 0 | 0.00 min | 0.00 min | 0 |

The largest gaps:

| Source | Gap start | Gap end | Missing time | Estimated missed rows |
| --- | --- | --- | ---: | ---: |
| `siri_vm` | `2026-05-13T00:00:17Z` | `2026-05-14T12:27:37Z` | 2,186.83 min | 590,481 |
| `siri_alerts` | `2026-05-13T00:05:03Z` | `2026-05-14T12:28:08Z` | 2,178.08 min | 5,460 |
| `siri_vm` | `2026-05-09T10:02:27Z` | `2026-05-10T13:26:44Z` | 1,643.78 min | 443,849 |
| `siri_alerts` | `2026-05-09T10:01:41Z` | `2026-05-10T13:26:50Z` | 1,640.15 min | 4,112 |

The data is not continuous. The daily profile shows which days are affected:
`2026-05-09`, `2026-05-10`, `2026-05-13` and `2026-05-14` are below 95%
coverage.

Full tables:
[`collector_blackouts.csv`](outputs/report-cache/collector_blackouts.csv),
[`collector_missing_summary.csv`](outputs/report-cache/collector_missing_summary.csv),
[`collector_missing_spots.csv`](outputs/report-cache/collector_missing_spots.csv).

## Pandas/DuckDB vs Polars

Both engines now share the same modules for:

- the holiday calendar;
- bootstrap intervals;
- alert matching;
- collector gap detection.

Their quality flags, trip minimums and sort orders were aligned. On the full
database:

- 15 of the 16 result tables match exactly.
- `context_delay_metrics` differs by at most `0.01` in one rounded column.
- `quality_by_line` has the same columns in a different order.

The public site uses the Polars build. This report uses the pandas/DuckDB build.
Their numbers are interchangeable.

Runtimes on this machine (15 GiB RAM):

| Build | Time | Peak memory |
| --- | ---: | ---: |
| DuckDB report rebuild | about 85 s | about 3.8 GB |
| Polars public-data rebuild | about 2 minutes | about 2.2 GB |

## What Changed In The October 2026 Audit

| Measure | May 2026 | October 2026 | Why it changed |
| --- | --- | --- | --- |
| Buckets / raw polls | 3,746,770 / 9,837,244 | 3,729,540 / 8,582,662 | Origin-layover and no-realtime rows removed |
| Conservative excluded | 5.69% | 17.72% | Same; waiting at the first stop alone is 14.95% of rows |
| Network median delay | 0.00 min | 0.02 min | Placeholder zeros no longer pin the median |
| Network on time | not reported | 65.41% | New on-time band (1 min early to 3 min late) |
| Top late line | `612` by p90, on 37 trips | `24` among frequent lines (18.76% >5 min late) | Ranking by share; frequent-line view; trip minimum and intervals |
| Top early line | `P6` by p90 early magnitude | `K3` among frequent lines; `P6` overall | Ranking by share >3 min early |
| Rush leader | `612`, +55.71 pp vs all non-rush | `721`, +32.13 pp vs weekday daytime | Off-peak baseline; holidays no longer weekdays |
| Largest alert signal | Line `21` technical problems, +28.57 pp | Route detours, +2.16 pp | Cancellations separated; route ids mapped; stratified controls |
| Stop changes | Midpoint split, largest change at Kaamanen | Removed; daily network profile | Uneven halves, 31-130 buckets per half |
| Hourly peak | 15:00-16:00, both day types mixed | Weekday 16:00 (8.73% >5 min late) | Split by day type; all hours kept |
| May 1 and May 14 | Weekdays | Holidays (weekend) | GTFS service calendar |

Other fixes:

- In both engines, ties between equal values now favour the larger sample.
- The pandas line-hour weekday is now computed in local time instead of UTC.
- The DuckDB route map is now date-aware.

Checked and found correct:

- the delay sign;
- duration parsing;
- trip keys and timestamps;
- the staleness filter;
- the collector code during the collection period;
- trip coverage against the GTFS schedule (98% of scheduled weekday trips appear);
- the quantile method.

## Overall Interpretation

The most actionable reliability findings are:

1. **Late running on frequent lines `24` and `21`.** It is clearly above every
   other frequent line, worst on weekday afternoons and weekend afternoons on
   `24`. Line `220` follows, with the strongest well-measured rush effect.
2. **Afternoon commuter lines `612`, `615` and `614`.** They have the highest
   late-running severity, but on 2-4 trips a day.
3. **Early running as its own problem.** Among frequent lines it is most common
   on `K3`, `801` and `28`, and most extreme on service line `P6`. Early running
   peaks at 17:00, as traffic eases.
4. **The May Day period.** It produced the worst late running of the month.
5. **Collector gaps on 9-10 May and 13-14 May.** These days should be excluded
   from day-to-day comparisons.

Disruption notices explain little of the late running: detours add about 2
percentage points where they apply. Several earlier headline numbers changed
substantially after the audit. In particular, the alert, rush and stop-change
sections no longer support their earlier claims. Older copies of the findings
should not be quoted.

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
  [`line_quality.csv`](outputs/report-cache/line_quality.csv),
  [`line_late_rankings.csv`](outputs/report-cache/line_late_rankings.csv),
  [`line_early_rankings.csv`](outputs/report-cache/line_early_rankings.csv),
  [`context_delay_metrics.csv`](outputs/report-cache/context_delay_metrics.csv),
  [`hourly_delay_profile.csv`](outputs/report-cache/hourly_delay_profile.csv),
  [`rush_impact.csv`](outputs/report-cache/rush_impact.csv),
  [`daily_network_profile.csv`](outputs/report-cache/daily_network_profile.csv),
  [`service_alert_grouped.csv`](outputs/report-cache/service_alert_grouped.csv),
  [`service_alert_by_line.csv`](outputs/report-cache/service_alert_by_line.csv),
  [`service_alert_inventory.csv`](outputs/report-cache/service_alert_inventory.csv),
  [`service_cancellations.csv`](outputs/report-cache/service_cancellations.csv),
  [`collector_blackouts.csv`](outputs/report-cache/collector_blackouts.csv),
  [`collector_missing_summary.csv`](outputs/report-cache/collector_missing_summary.csv),
  [`collector_missing_spots.csv`](outputs/report-cache/collector_missing_spots.csv).

## Caveats

- SIRI VM delay is the vehicle's estimated state while approaching the next
  stop, not a measured arrival or departure time.
- Raw vehicle-monitoring rows are repeated polls. Results use trip-stop buckets,
  but buckets from one trip are still correlated. Read trip counts and intervals,
  not bucket counts.
- Some placeholder zero delays remain. On-time shares may be up to about 2
  percentage points too high, and early-running shares slightly too low.
- Lines with only 2-4 trips a day can top the severity rankings. Their intervals
  are wide even when the trip minimum is met.
- The data ends at `2026-05-23T13:24:20Z` and covers one month in spring. It
  says nothing about winter conditions or later timetable changes.
- Service-alert results are matched observational comparisons, not causal
  effects.
- Collector gaps leave four days with incomplete coverage.
