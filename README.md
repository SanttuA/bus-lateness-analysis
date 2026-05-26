# Bus Lateness Analysis

Small Python project for exploring bus lateness in Turku region public
transport using data from the Föli API.

## Data

Input data is expected in a local database at:

```text
data/foli.db
```

The database is not committed to this repository. It should contain a
`vehicle_observations` table with fields such as `line_ref`, `delay_seconds`,
and `is_gtfs_matchable`. Some analyses also use `service_alerts`,
`collector_polls`, and extracted local GTFS snapshots under `data/gtfs/`.
GTFS directories should be named `gtfs_YYYY-MM-DD`; each snapshot is used from
that local date until the next snapshot date.

## Setup

This project uses `uv`.

```sh
uv sync
```

## Analysis

See [ANALYSIS.md](ANALYSIS.md) for script commands, notebook usage, and metric
definitions.

## Overall Results Report

Build one generated Markdown report that collects the main research outputs:

```sh
uv run python analysis/build-results-report.py
```

The report is written to `reports/generated/overall-results.md`. Intermediate
DuckDB tables and compact CSVs are stored under `outputs/report-cache/` so later
runs can reuse midpoint calculations and other summaries instead of loading the
full SQLite database into pandas. The cache is rebuilt when `data/foli.db` or
report settings change; pass `--force` to rebuild it manually. The command
prints simple progress updates and includes cache/build, render, and total run
timings in the generated report.

## Polars Analysis Option

The default analysis path above is unchanged. A secondary Polars CLI path is
available under `analysis/polars/` for performance comparisons:

```sh
uv run python analysis/polars/build-results-report.py
```

The Polars report is written to
`reports/generated/overall-results-polars.md`. Its Parquet cache and compact
CSVs are written separately under `outputs/polars-report-cache/`, so the
existing DuckDB/pandas outputs remain comparable.
The Polars command prints the same progress updates and timing section for
side-by-side runtime comparisons.
Polars notebook counterparts for the main exploratory notebooks are available
under `notebooks/polars/`.

Example Polars CLI commands:

```sh
uv run python analysis/polars/line-delay-rankings.py --ranking both --limit 10
uv run python analysis/polars/hourly-delay-profile.py --line-ref 3 --limit 24
```

## Dashboard

Run the local interactive dashboard with:

```sh
uv run streamlit run streamlit_app.py
```

A separate Polars-backed version is also available:

```sh
uv run streamlit run streamlit_app_polars.py
```

The dashboard uses the Polars base cache under `outputs/polars-report-cache/`.
If that cache is missing or stale, Streamlit builds it with the same
batch/partitioned cache path used by the Polars report command instead of
loading the full SQLite dataset into dashboard memory. It then joins filtered
trip-stop buckets to the date-matched GTFS snapshot under `data/gtfs/` and
visualizes robust delay by line, local hour, and next-stop location.
Observations before the first local GTFS snapshot keep SIRI stop names but have
no GTFS coordinates.

## Data Caveats

- SIRI VM delay is estimated vehicle-monitoring state, not measured arrival
  truth.
- Raw Föli VM rows are repeated polls, so treating every 30-second poll as an
  independent event overweights vehicles that remain visible for longer.
- The default analytics use conservative filtering for extreme, stale,
  pre-trip, and post-trip rows before drawing operational conclusions.
- Rows where VM delay strongly disagrees with next stop-call expected-vs-aimed
  times are flagged by default and can be excluded explicitly in CLI scripts.

## Data License And Attribution

Project code is licensed under the MIT License. See [LICENSE](LICENSE).

Data retrieved from the Föli API (`data.foli.fi`) is licensed under
[Creative Commons Attribution 4.0 International (CC BY 4.0)](https://creativecommons.org/licenses/by/4.0/deed.en).

The Föli API is public and free to use without registration, but applications
should use the API appropriately and avoid unnecessary load.

Attribution for the source data:

> Source: Turku Region Public Transport operating and schedule data. The data
> is maintained by the City of Turku public transport office. The data was
> downloaded from <http://data.foli.fi/> under the Creative Commons Attribution
> 4.0 International (CC BY 4.0) license.

See the Föli API documentation for the original Finnish attribution text and
terms: <https://data.foli.fi/doc/index>
