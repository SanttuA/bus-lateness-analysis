from __future__ import annotations

from argparse import Namespace

import polars as pl

from _shared import (
    QUALIFIED_DELAY_FILTER_SQL,
    aggregate_delay_buckets,
    apply_quality_filter,
    base_quality_query,
    read_sql,
)
from report_cache import (
    DELAY_BUCKETS_NAME,
    QUALITY_ROWS_NAME,
    ensure_analysis_cache,
    read_table,
    settings_from_args,
)


def load_quality_rows_for_args(args: Namespace) -> pl.DataFrame:
    if getattr(args, "no_cache", False):
        query = base_quality_query(where=QUALIFIED_DELAY_FILTER_SQL)
        rows = read_sql(args.db, query)
        return apply_quality_filter(
            rows,
            quality_mode=args.quality_mode,
            exclude_stop_call_disagreement=args.exclude_stop_call_disagreement,
        )
    settings = settings_from_args(args)
    ensure_analysis_cache(settings, force=getattr(args, "force_cache", False))
    return read_table(settings.cache_dir, QUALITY_ROWS_NAME).filter(pl.col("quality_pass"))


def load_delay_buckets_for_args(args: Namespace) -> pl.DataFrame:
    if getattr(args, "no_cache", False):
        quality_rows = load_quality_rows_for_args(args)
        return aggregate_delay_buckets(quality_rows, bucket=args.bucket, timezone=args.timezone)
    settings = settings_from_args(args)
    ensure_analysis_cache(settings, force=getattr(args, "force_cache", False))
    return read_table(settings.cache_dir, DELAY_BUCKETS_NAME)

