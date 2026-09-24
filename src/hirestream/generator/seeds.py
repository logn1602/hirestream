"""Seed plumbing: one independent random stream per generator subsystem (SPEC §6.1).

Streams are keyed by subsystem *name*, not by spawn order: each child is
`SeedSequence(seed, spawn_key=(sha256(name)[:4],))`. Adding, removing, or reordering a
subsystem therefore never reshuffles the others, and changing how one subsystem consumes
randomness leaves every other subsystem's output byte-identical.
"""

from __future__ import annotations

import hashlib

import numpy as np

SUBSYSTEMS: tuple[str, ...] = (
    "world",
    "workforce",
    "requisitions",
    "jobboard",
    "ats",
    "scheduling",
    "chaos",  # stream sources (T1.8)
    "hris_chaos",  # HRIS export chaos (ADR-0005)
)


def _spawn_key(name: str) -> int:
    # hashlib, not hash(): str hashes are salted per process and would break determinism.
    return int.from_bytes(hashlib.sha256(name.encode()).digest()[:4], "big")


class SeedPlan:
    def __init__(self, seed: int) -> None:
        if seed < 0:
            raise ValueError("seed must be non-negative")
        self.seed = seed

    def sequence(self, subsystem: str) -> np.random.SeedSequence:
        if subsystem not in SUBSYSTEMS:
            raise KeyError(f"unknown subsystem {subsystem!r}; add it to SUBSYSTEMS")
        return np.random.SeedSequence(self.seed, spawn_key=(_spawn_key(subsystem),))

    def rng(self, subsystem: str) -> np.random.Generator:
        """A fresh generator for `subsystem`; PCG64 is pinned so the stream never changes."""
        return np.random.Generator(np.random.PCG64(self.sequence(subsystem)))

    def faker_seed(self, subsystem: str) -> int:
        """An integer seed for Faker, derived from (but independent of) the subsystem's stream."""
        child = self.sequence(subsystem).spawn(1)[0]
        return int(child.generate_state(1, np.uint64)[0])
