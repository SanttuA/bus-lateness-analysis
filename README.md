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
`collector_polls`, and the newest local GTFS archive under `data/gtfs/`.

## Setup

This project uses `uv`.

```sh
uv sync
```

## Analysis

See [ANALYSIS.md](ANALYSIS.md) for script commands, notebook usage, and metric
definitions.

## Dashboard

Run the local interactive dashboard with:

```sh
uv run streamlit run streamlit_app.py
```

The dashboard reads `data/foli.db`, joins observations to the newest local GTFS
snapshot under `data/gtfs/`, applies conservative quality filtering, collapses
repeated polls into trip-stop buckets, and visualizes robust delay by line,
local hour, and next-stop location.

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
