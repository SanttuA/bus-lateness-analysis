from __future__ import annotations

import argparse

from _shared import (
    DELAY_FILTER_SQL,
    add_common_args,
    connect_readonly_db,
    print_or_empty,
    read_sql,
    round_numeric,
    write_optional_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show bus lines with the highest average delay."
    )
    add_common_args(parser)
    parser.set_defaults(limit=10, min_observations=1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    query = f"""
    SELECT
        line_ref,
        ROUND(AVG(delay_seconds) / 60.0, 2) AS avg_delay_min,
        COUNT(delay_seconds) AS obs_count
    FROM vehicle_observations
    WHERE {DELAY_FILTER_SQL}
    GROUP BY line_ref
    HAVING COUNT(delay_seconds) >= ?
    ORDER BY avg_delay_min DESC, obs_count DESC, line_ref
    LIMIT ?;
    """
    with connect_readonly_db(args.db) as con:
        df = read_sql(con, query, [args.min_observations, args.limit])

    df = round_numeric(df)
    print_or_empty(df)
    write_optional_csv(df, args.output_csv)


if __name__ == "__main__":
    main()
