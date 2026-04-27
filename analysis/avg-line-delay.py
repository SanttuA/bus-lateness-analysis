from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "foli.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show bus lines with the highest average delay."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"Path to the Foli SQLite database. Defaults to {DEFAULT_DB_PATH}",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of lines to show.",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=1,
        help="Only include lines with at least this many delay observations.",
    )
    return parser.parse_args()


def sql_string(value: Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def main() -> None:
    args = parse_args()
    db_path = args.db.expanduser()
    if not db_path.is_absolute():
        db_path = PROJECT_ROOT / db_path
    db_path = db_path.resolve()

    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH {sql_string(db_path)} AS foli (TYPE SQLITE);")

    query = """
    SELECT
        line_ref,
        ROUND(AVG(delay_seconds) / 60.0, 2) AS avg_delay_min,
        COUNT(delay_seconds) AS obs_count
    FROM foli.vehicle_observations
    WHERE
        is_gtfs_matchable = TRUE
        AND delay_seconds IS NOT NULL
        AND line_ref IS NOT NULL
    GROUP BY line_ref
    HAVING COUNT(delay_seconds) >= ?
    ORDER BY avg_delay_min DESC, obs_count DESC, line_ref
    LIMIT ?;
    """
    df = con.execute(query, [args.min_observations, args.limit]).df()

    if df.empty:
        print("No matching observations found.")
        return

    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
