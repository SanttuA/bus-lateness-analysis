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
and `is_gtfs_matchable`.

## Setup

This project uses `uv`.

```sh
uv sync
```

## Usage

Show the 10 lines with the highest average delay:

```sh
uv run python analysis/avg-line-delay.py
```

Use a different database, output limit, or minimum observation count:

```sh
uv run python analysis/avg-line-delay.py --db data/foli.db --limit 20 --min-observations 50
```

## Data License And Attribution

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
