"""Candidate and application ids shared by the job board and the ATS (ADR-0007 §6).

`apply_submit` must carry both ids the moment it is emitted, so the registry lives outside the
ATS. The ATS (T1.6) adds names, emails, and phones to candidates and allocates ids for its
direct-channel applications from the same registry.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Literal

import numpy as np

Channel = Literal["career_site", "referral", "sourced", "agency", "internal"]


@dataclass(slots=True)
class Candidate:
    """An ATS candidate (SPEC §7.4). Names and contacts are filled in by the ATS (T1.6)."""

    candidate_id: str
    is_internal: bool
    employee_id: str | None
    location_city: str
    first_applied_on: date
    first_name: str = ""
    last_name: str = ""
    email: str = ""
    phone: str = ""
    location_country: str = ""
    created_ms: int = 0


@dataclass(frozen=True, slots=True)
class Submission:
    """An application created by an `apply_submit`; the ATS picks it up the same day."""

    application_id: str
    candidate_id: str
    req_id: str
    channel: Channel
    applied_ts_ms: int
    employee_id: str | None = None


class CandidateRegistry:
    def __init__(self, reapply_probability: float) -> None:
        self.candidates: dict[str, Candidate] = {}
        self._reapply = reapply_probability
        self._by_visitor: dict[int, str] = {}  # only visitors who have applied
        self._by_employee: dict[str, str] = {}
        self._external: list[str] = []
        self._next_candidate = 1
        self._next_application = 1
        self.unnamed: list[str] = []  # new candidates the ATS has not filled in yet, in id order

    def external(self, rng: np.random.Generator, visitor: int, city: str, day: date) -> str:
        """The candidate behind an external visitor, created on their first application."""
        known = self._by_visitor.get(visitor)
        if known is not None:
            return known
        if self._external and rng.random() < self._reapply:  # an earlier candidate, new device
            candidate_id = self._external[int(rng.integers(len(self._external)))]
        else:
            candidate_id = self._new(is_internal=False, employee_id=None, city=city, day=day)
            self._external.append(candidate_id)
        self._by_visitor[visitor] = candidate_id
        return candidate_id

    def direct(self, rng: np.random.Generator, city: str, day: date) -> str:
        """A referral, sourced, or agency applicant: sometimes an earlier candidate (§6.6)."""
        if self._external and rng.random() < self._reapply:
            return self._external[int(rng.integers(len(self._external)))]
        candidate_id = self._new(is_internal=False, employee_id=None, city=city, day=day)
        self._external.append(candidate_id)
        return candidate_id

    def internal(self, employee_id: str, city: str, day: date) -> str:
        """One candidate per employee, created on their first internal application."""
        known = self._by_employee.get(employee_id)
        if known is None:
            known = self._new(is_internal=True, employee_id=employee_id, city=city, day=day)
            self._by_employee[employee_id] = known
        return known

    def new_application_id(self) -> str:
        application_id = f"A{self._next_application:09d}"
        self._next_application += 1
        return application_id

    def _new(self, *, is_internal: bool, employee_id: str | None, city: str, day: date) -> str:
        candidate_id = f"C{self._next_candidate:08d}"
        self._next_candidate += 1
        self.candidates[candidate_id] = Candidate(candidate_id, is_internal, employee_id, city, day)
        self.unnamed.append(candidate_id)
        return candidate_id
