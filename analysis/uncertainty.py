"""Trip-cluster bootstrap intervals shared by the pandas/DuckDB and Polars paths.

Trip-stop buckets from the same trip are strongly correlated (one late trip is late at
most of its stops), so intervals resample whole trips. A Poisson bootstrap gives each
trip an independent Poisson(1) weight per replicate, which keeps every replicate O(n)
and lets weighted shares and quantiles reuse one sort of the data.
"""

from __future__ import annotations

from collections.abc import Sequence
import math
import zlib

import numpy as np

DEFAULT_BOOTSTRAP_REPS = 300
DEFAULT_BOOTSTRAP_SEED = 20260523
DEFAULT_ALPHA = 0.05


def stable_seed(*parts: object, base: int = DEFAULT_BOOTSTRAP_SEED) -> int:
    """Seed derived from a group key so results do not depend on processing order."""
    text = "|".join("" if part is None else str(part) for part in parts)
    return (zlib.crc32(text.encode("utf-8")) ^ base) & 0xFFFFFFFF


def cluster_codes(clusters: Sequence[object] | np.ndarray) -> tuple[np.ndarray, int]:
    """Dense integer codes for cluster labels, ordered by sorted label for determinism."""
    labels = np.asarray(clusters)
    if labels.size == 0:
        return np.zeros(0, dtype=np.int64), 0
    if not np.issubdtype(labels.dtype, np.integer):
        # Object dtype avoids fixed-width unicode copies of millions of trip keys.
        labels = labels.astype(object)
    _, codes = np.unique(labels, return_inverse=True)
    codes = codes.astype(np.int64).ravel()
    return codes, int(codes.max()) + 1


def _interval(samples: list[float], alpha: float) -> tuple[float | None, float | None]:
    finite = np.asarray([value for value in samples if value is not None and math.isfinite(value)])
    if finite.size < 2:
        return None, None
    lower, upper = np.quantile(finite, [alpha / 2, 1 - alpha / 2])
    return float(lower), float(upper)


def _replicate_weights(n_clusters: int, reps: int, seed: int):
    rng = np.random.default_rng(seed)
    for _ in range(reps):
        yield rng.poisson(1.0, n_clusters).astype(np.float64)


def bootstrap_share_ci(
    indicator: Sequence[bool] | np.ndarray,
    clusters: Sequence[object] | np.ndarray,
    *,
    reps: int = DEFAULT_BOOTSTRAP_REPS,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
    alpha: float = DEFAULT_ALPHA,
) -> tuple[float | None, float | None]:
    """Interval for the share of rows where indicator is true, in percent."""
    values = np.asarray(indicator, dtype=np.float64)
    codes, n_clusters = cluster_codes(clusters)
    if n_clusters < 2:
        return None, None
    hits = np.bincount(codes, weights=values, minlength=n_clusters)
    sizes = np.bincount(codes, minlength=n_clusters).astype(np.float64)
    samples = []
    for weights in _replicate_weights(n_clusters, reps, seed):
        total = float(weights @ sizes)
        samples.append(float(weights @ hits) / total * 100.0 if total > 0 else math.nan)
    return _interval(samples, alpha)


def bootstrap_quantile_ci(
    values: Sequence[float] | np.ndarray,
    clusters: Sequence[object] | np.ndarray,
    quantile: float,
    *,
    reps: int = DEFAULT_BOOTSTRAP_REPS,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
    alpha: float = DEFAULT_ALPHA,
) -> tuple[float | None, float | None]:
    """Interval for a quantile, using weighted inverse-CDF quantiles per replicate."""
    data = np.asarray(values, dtype=np.float64)
    codes, n_clusters = cluster_codes(clusters)
    if n_clusters < 2:
        return None, None
    order = np.argsort(data, kind="stable")
    sorted_values = data[order]
    sorted_codes = codes[order]
    samples = []
    for weights in _replicate_weights(n_clusters, reps, seed):
        row_weights = weights[sorted_codes]
        cumulative = np.cumsum(row_weights)
        total = cumulative[-1]
        if total <= 0:
            samples.append(math.nan)
            continue
        position = int(np.searchsorted(cumulative, quantile * total, side="left"))
        samples.append(float(sorted_values[min(position, sorted_values.size - 1)]))
    return _interval(samples, alpha)


def bootstrap_share_difference_ci(
    indicator: Sequence[bool] | np.ndarray,
    clusters: Sequence[object] | np.ndarray,
    in_second_group: Sequence[bool] | np.ndarray,
    *,
    reps: int = DEFAULT_BOOTSTRAP_REPS,
    seed: int = DEFAULT_BOOTSTRAP_SEED,
    alpha: float = DEFAULT_ALPHA,
) -> tuple[float | None, float | None]:
    """Interval for share(second group) - share(first group), in percentage points.

    A trip that appears in both groups keeps one weight, so shared trips stay paired.
    """
    values = np.asarray(indicator, dtype=np.float64)
    second = np.asarray(in_second_group, dtype=bool)
    codes, n_clusters = cluster_codes(clusters)
    if n_clusters < 2 or not second.any() or second.all():
        return None, None
    stats = []
    for mask in (~second, second):
        stats.append(
            (
                np.bincount(codes[mask], weights=values[mask], minlength=n_clusters),
                np.bincount(codes[mask], minlength=n_clusters).astype(np.float64),
            )
        )
    samples = []
    for weights in _replicate_weights(n_clusters, reps, seed):
        shares = []
        for hits, sizes in stats:
            total = float(weights @ sizes)
            shares.append(float(weights @ hits) / total if total > 0 else math.nan)
        samples.append((shares[1] - shares[0]) * 100.0)
    return _interval(samples, alpha)


LATE_RANKING_INTERVAL_COLUMNS = (
    "p90_delay_ci_lo",
    "p90_delay_ci_hi",
    "pct_over_5_min_late_ci_lo",
    "pct_over_5_min_late_ci_hi",
)
EARLY_RANKING_INTERVAL_COLUMNS = ("pct_over_3_min_early_ci_lo", "pct_over_3_min_early_ci_hi")


def line_ranking_intervals(
    ranking: str,
    line_ref: object,
    trips: Sequence[object] | np.ndarray,
    delay_seconds: Sequence[float] | np.ndarray,
    *,
    reps: int = DEFAULT_BOOTSTRAP_REPS,
) -> dict[str, float | None]:
    """Intervals for one ranked line; both engines call this with the line's buckets."""
    delay = np.asarray(delay_seconds, dtype=np.float64)
    seed = stable_seed(ranking, line_ref)
    if ranking == "early":
        low, high = bootstrap_share_ci(delay < -180, trips, reps=reps, seed=seed)
        return dict(zip(EARLY_RANKING_INTERVAL_COLUMNS, (low, high), strict=True))
    p90 = bootstrap_quantile_ci(delay / 60.0, trips, 0.9, reps=reps, seed=seed)
    late5 = bootstrap_share_ci(delay > 300, trips, reps=reps, seed=seed)
    return dict(zip(LATE_RANKING_INTERVAL_COLUMNS, (*p90, *late5), strict=True))


def rush_lift_interval(
    line_ref: object,
    trips: Sequence[object] | np.ndarray,
    delay_seconds: Sequence[float] | np.ndarray,
    is_rush: Sequence[bool] | np.ndarray,
    *,
    reps: int = DEFAULT_BOOTSTRAP_REPS,
) -> dict[str, float | None]:
    low, high = bootstrap_share_difference_ci(
        np.asarray(delay_seconds, dtype=np.float64) > 300,
        trips,
        is_rush,
        reps=reps,
        seed=stable_seed("rush", line_ref),
    )
    return {"rush_over_5_min_late_lift_ci_lo": low, "rush_over_5_min_late_lift_ci_hi": high}
