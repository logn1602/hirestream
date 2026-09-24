import numpy as np
import pytest

from hirestream.generator.seeds import SUBSYSTEMS, SeedPlan

# First draw of each stream for seed 1602. If this changes, every generated dataset changes:
# a numpy upgrade altered PCG64/SeedSequence, or the key derivation changed. Neither is allowed
# to happen silently.
GOLDEN_1602 = {
    "world": 3249926588,
    "workforce": 1152729153,
    "requisitions": 3604156150,
    "jobboard": 693029927,
    "ats": 3861924298,
    "scheduling": 343047998,
    "chaos": 976485019,
    "hris_chaos": 1666423134,  # added in T1.3; the seven values above did not move
}


def _first(plan: SeedPlan, subsystem: str) -> int:
    return int(plan.rng(subsystem).integers(0, 2**32))


def test_golden_first_draws() -> None:
    plan = SeedPlan(1602)
    assert {s: _first(plan, s) for s in SUBSYSTEMS} == GOLDEN_1602


def test_same_seed_same_stream() -> None:
    a = SeedPlan(7).rng("ats").random(1000)
    b = SeedPlan(7).rng("ats").random(1000)
    assert np.array_equal(a, b)


def test_different_seed_different_stream() -> None:
    assert _first(SeedPlan(1), "ats") != _first(SeedPlan(2), "ats")


def test_subsystem_streams_are_distinct_and_uncorrelated() -> None:
    plan = SeedPlan(1602)
    draws = np.array([plan.rng(s).random(10_000) for s in SUBSYSTEMS])
    corr = np.corrcoef(draws)
    off_diagonal = corr[~np.eye(len(SUBSYSTEMS), dtype=bool)]
    assert np.all(np.abs(off_diagonal) < 0.05)


def test_consuming_one_stream_does_not_move_another() -> None:
    plan = SeedPlan(1602)
    plan.rng("jobboard").random(1_000_000)
    assert _first(plan, "ats") == GOLDEN_1602["ats"]


def test_unknown_subsystem_is_rejected() -> None:
    with pytest.raises(KeyError, match="add it to SUBSYSTEMS"):
        SeedPlan(1602).rng("weather")


def test_negative_seed_is_rejected() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        SeedPlan(-1)


def test_faker_seed_is_deterministic_and_independent_of_the_stream() -> None:
    plan = SeedPlan(1602)
    assert plan.faker_seed("world") == SeedPlan(1602).faker_seed("world") == 16279641977477481460
    assert plan.faker_seed("world") != plan.faker_seed("ats")
    assert plan.faker_seed("world") != _first(plan, "world")
