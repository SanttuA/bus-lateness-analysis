from __future__ import annotations

import argparse

from _shared import add_common_args, print_or_empty, write_optional_csv
from report_cache import ReportSettings, build_collector_blackouts, load_collector_polls


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Summarize collector polling blackouts by source.")
    add_common_args(parser)
    parser.set_defaults(limit=20, min_observations=1)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = ReportSettings(db=args.db, limit=args.limit, min_observations=args.min_observations).resolved()
    result = build_collector_blackouts(load_collector_polls(settings), args.limit)
    print_or_empty(result, "No collector polls found.")
    write_optional_csv(result, args.output_csv)


if __name__ == "__main__":
    main()

