import math

import numpy as np
import pytest

from hirestream.generator.sampling import quota_counts, sample_piecewise_exponential


def test_quota_counts_are_exact_and_close_to_shares() -> None:
    shares = {"L3": 0.12, "L4": 0.30, "L5": 0.28, "L6": 0.18, "L7": 0.09, "L8": 0.03}
    for n in (0, 1, 7, 300, 3001, 25000):
        counts = quota_counts(shares, n)
        assert sum(counts.values()) == n
        assert all(abs(counts[k] - shares[k] * n) < 1 for k in shares)


def test_quota_counts_breaks_ties_by_order() -> None:
    assert quota_counts({"a": 0.5, "b": 0.5}, 3) == {"a": 2, "b": 1}
    assert quota_counts({"b": 0.5, "a": 0.5}, 3) == {"b": 2, "a": 1}


def test_quota_counts_normalises_near_one_totals() -> None:
    assert quota_counts({"x": 0.3333333, "y": 0.3333333, "z": 0.3333334}, 9) == {
        "x": 3,
        "y": 3,
        "z": 3,
    }


@pytest.mark.parametrize(("shares", "n"), [({"a": 1.0}, -1), ({"a": 0.0}, 5)])
def test_quota_counts_rejects_bad_input(shares: dict[str, float], n: int) -> None:
    with pytest.raises(ValueError, match="must"):
        quota_counts(shares, n)


def _analytic_cdf(breaks: list[float], rates: list[float], upper: float, t: float) -> float:
    """Independent check: numerically integrate exp(-H(u)) on a fine grid."""
    grid = np.linspace(0.0, upper, 400_001)
    idx = np.searchsorted(np.asarray(breaks), grid, side="right") - 1
    rate = np.asarray(rates)[idx]
    hazard = np.concatenate([[0.0], np.cumsum(rate[:-1] * np.diff(grid))])
    density = np.exp(-hazard)
    cdf = np.concatenate([[0.0], np.cumsum((density[1:] + density[:-1]) / 2 * np.diff(grid))])
    return float(np.interp(t, grid, cdf / cdf[-1]))


def test_matches_the_analytic_distribution() -> None:
    breaks, rates, upper = [0.0, 365.0], [0.23 / 365, 0.17 / 365], 8000.0
    draws = sample_piecewise_exponential(np.random.default_rng(3), breaks, rates, upper, 200_000)
    for t in (30.0, 180.0, 365.0, 1000.0, 3000.0, 6000.0):
        empirical = float(np.mean(draws < t))
        assert empirical == pytest.approx(_analytic_cdf(breaks, rates, upper, t), abs=0.005)
    assert draws.min() >= 0.0
    assert draws.max() < upper


def test_unbounded_tail_is_exponential() -> None:
    draws = sample_piecewise_exponential(np.random.default_rng(4), [0.0], [0.5], math.inf, 200_000)
    assert float(np.mean(draws)) == pytest.approx(2.0, rel=0.02)
    assert float(np.mean(draws < 1.0)) == pytest.approx(1 - math.exp(-0.5), abs=0.005)


def test_zero_rate_is_uniform() -> None:
    draws = sample_piecewise_exponential(np.random.default_rng(5), [0.0], [0.0], 10.0, 100_000)
    assert float(np.mean(draws < 2.5)) == pytest.approx(0.25, abs=0.005)


def test_breaks_beyond_upper_are_ignored() -> None:
    rng_a, rng_b = np.random.default_rng(6), np.random.default_rng(6)
    a = sample_piecewise_exponential(rng_a, [0.0, 50.0], [0.1, 0.9], 20.0, 1000)
    b = sample_piecewise_exponential(rng_b, [0.0], [0.1], 20.0, 1000)
    assert np.array_equal(a, b)


def test_same_generator_state_gives_same_draws() -> None:
    args = ([0.0, 365.0], [0.001, 0.002], 5000.0, 50)
    first = sample_piecewise_exponential(np.random.default_rng(7), *args)
    second = sample_piecewise_exponential(np.random.default_rng(7), *args)
    assert np.array_equal(first, second)


@pytest.mark.parametrize(
    ("breaks", "rates", "upper"),
    [
        ([1.0], [0.1], 10.0),  # must start at 0
        ([0.0, 5.0], [0.1], 10.0),  # length mismatch
        ([0.0, 5.0, 5.0], [0.1, 0.1, 0.1], 10.0),  # not increasing
        ([0.0], [-0.1], 10.0),  # negative rate
        ([0.0], [0.1], 0.0),  # empty support
        ([0.0], [0.0], math.inf),  # improper
    ],
)
def test_rejects_invalid_parameters(breaks: list[float], rates: list[float], upper: float) -> None:
    with pytest.raises(ValueError, match=r"must|needs"):
        sample_piecewise_exponential(np.random.default_rng(0), breaks, rates, upper, 1)
