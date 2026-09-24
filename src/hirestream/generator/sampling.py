"""Sampling helpers shared by generator subsystems (pure functions over a numpy Generator)."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from itertools import pairwise
from typing import TypeVar

import numpy as np
import numpy.typing as npt

K = TypeVar("K")


def quota_counts(shares: Mapping[K, float], n: int) -> dict[K, int]:
    """Split `n` by `shares` with largest-remainder rounding: exact total, each count within 1.

    Deterministic: equal remainders are broken by the mapping's order.
    """
    if n < 0:
        raise ValueError("n must be non-negative")
    total = math.fsum(shares.values())
    if total <= 0:
        raise ValueError("shares must have a positive total")
    raw = {key: value / total * n for key, value in shares.items()}
    counts = {key: math.floor(value) for key, value in raw.items()}
    by_remainder = sorted(
        enumerate(shares), key=lambda item: (-(raw[item[1]] - counts[item[1]]), item[0])
    )
    for _, key in by_remainder[: n - sum(counts.values())]:
        counts[key] += 1
    return counts


def sample_piecewise_exponential(
    rng: np.random.Generator,
    breaks: Sequence[float],
    rates: Sequence[float],
    upper: float,
    size: int,
) -> npt.NDArray[np.float64]:
    """Draw from the density f(t) ∝ exp(-∫₀ᵗ r(u) du) on [0, upper).

    `r` is `rates[i]` on `[breaks[i], breaks[i + 1])`, and the last rate continues to `upper`,
    which may be `math.inf` when that rate is positive. This is the steady-state distribution of
    "time since an event" for piecewise-constant hazards (ADR-0004). Sampling is exact: pick a
    piece by its probability mass, then invert the truncated exponential within it.
    """
    if len(breaks) != len(rates) or not breaks or breaks[0] != 0:
        raise ValueError("breaks must start at 0 and match rates in length")
    if any(b2 <= b1 for b1, b2 in pairwise(breaks)):
        raise ValueError("breaks must be strictly increasing")
    if any(r < 0 for r in rates) or upper <= 0:
        raise ValueError("rates must be non-negative and upper positive")

    starts = [b for b in breaks if b < upper]
    ends = [*starts[1:], upper]
    piece_rates = list(rates[: len(starts)])
    if math.isinf(upper) and piece_rates[-1] == 0:
        raise ValueError("an unbounded last piece needs a positive rate")

    masses, cum_hazard = [], 0.0
    for start, end, rate in zip(starts, ends, piece_rates, strict=True):
        length = end - start
        if rate == 0:
            mass = length
        elif math.isinf(length):
            mass = 1.0 / rate
        else:
            mass = -math.expm1(-rate * length) / rate
        masses.append(math.exp(-cum_hazard) * mass)
        cum_hazard += rate * length if not math.isinf(length) else 0.0
    probs = np.asarray(masses) / math.fsum(masses)

    piece = np.minimum(
        np.searchsorted(np.cumsum(probs), rng.random(size), side="right"), len(probs) - 1
    )
    u = rng.random(size)
    start_arr = np.asarray(starts)[piece]
    length_arr = (np.asarray(ends) - np.asarray(starts))[piece]
    rate_arr = np.asarray(piece_rates, dtype=float)[piece]

    out = np.empty(size, dtype=np.float64)
    flat = rate_arr == 0
    out[flat] = start_arr[flat] + u[flat] * length_arr[flat]
    decay = ~flat
    with np.errstate(over="ignore"):
        tail_mass = -np.expm1(-rate_arr[decay] * length_arr[decay])  # 1.0 for infinite pieces
    out[decay] = start_arr[decay] - np.log1p(-u[decay] * tail_mass) / rate_arr[decay]
    return np.minimum(out, np.nextafter(upper, 0.0))  # float rounding must not reach `upper`


def truncated_pareto_mean(alpha: float, cap: float) -> float:
    """Mean of Pareto(alpha, x_m = 1) truncated to [1, cap]."""
    if alpha <= 0 or cap <= 1:
        raise ValueError("alpha must be positive and cap above 1")
    if alpha == 1:
        return math.log(cap) / (1 - 1 / cap)
    return alpha / (alpha - 1) * (1 - math.pow(cap, 1 - alpha)) / (1 - math.pow(cap, -alpha))


def sample_truncated_pareto(
    rng: np.random.Generator, alpha: float, cap: float, size: int
) -> npt.NDArray[np.float64]:
    """Pareto(alpha, x_m = 1) truncated to [1, cap], divided by its mean (so the mean is 1).

    Truncation keeps the heavy skew but gives finite variance: with alpha = 1.2 an untruncated
    draw can be thousands of times the mean, and sample means swing widely (ADR-0006).
    """
    u = rng.random(size)
    raw = np.power(1 - u * (1 - math.pow(cap, -alpha)), -1 / alpha)  # truncated inverse CDF
    return np.asarray(raw / truncated_pareto_mean(alpha, cap), dtype=np.float64)
