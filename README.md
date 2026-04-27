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
