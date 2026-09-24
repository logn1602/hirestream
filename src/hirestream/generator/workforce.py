"""Workforce dynamics: advance Halcyon's employees one simulated day at a time.

SPEC §6.3 sets the daily hazards (annual rate / 365); ADR-0005 fixes what the spec leaves
open: at most one hazard per employee per day, who each hazard applies to, what happens to the
reports of a manager who leaves, and that every change to an HRIS field sets
`job_effective_date` to the day it takes effect. Employees are mutated in place, so the world
always holds the true current state; the HRIS export decides what is visible when.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

import numpy as np
import numpy.typing as npt

from hirestream.generator.config import GeneratorConfig
from hirestream.generator.world import DAYS_PER_YEAR, MANAGER_MIN_LEVEL, Employee, Team, World

EventKind = Literal[
    "termination",
    "promotion",
    "lateral_move",
    "manager_change",
    "location_change",
    "leave_start",
    "leave_end",
    "reorg_move",
    "succession",  # took over an org or a team after its leader left
]
Cause = Literal["hazard", "succession", "reorg", "schedule"]

# Daily hazards in the order they are checked; an employee gets at most one per day (ADR-0005).
HAZARDS: tuple[EventKind, ...] = (
    "termination",
    "leave_start",
    "promotion",
    "lateral_move",
    "manager_change",
    "location_change",
)
EVENT_ORDER: tuple[EventKind, ...] = (*HAZARDS, "leave_end", "reorg_move", "succession")

ACTIVE, LEAVE, TERMINATED = 0, 1, 2
_STATUS = {"active": ACTIVE, "leave": LEAVE, "terminated": TERMINATED}


@dataclass(frozen=True, slots=True)
class WorkforceEvent:
    day: date
    employee_id: str
    kind: EventKind
    cause: Cause


class Workforce:
    def __init__(
        self, world: World, config: GeneratorConfig, rng: np.random.Generator, reorg_day: date
    ) -> None:
        self.world = world
        self.events: list[WorkforceEvent] = []
        self._config = config
        self._rng = rng
        self._reorg_day = reorg_day

        self._levels = list(config.org_model.levels)
        self._top = len(self._levels) - 1
        self._floor = self._levels.index(MANAGER_MIN_LEVEL)
        w, d = config.workforce, DAYS_PER_YEAR
        self._p_attrition = w.attrition_annual / d
        self._p_attrition_first_year = self._p_attrition * w.attrition_first_year_multiplier
        self._p = np.array(
            [
                0.0,  # attrition: per employee, depends on tenure
                w.leave_annual / d,
                w.promotion_annual / d,
                w.lateral_move_annual / d,
                w.manager_change_annual / d,
                w.location_change_annual / d,
            ]
        )
        self._cities = [loc.city for loc in config.org_model.locations]
        self._city_weights = np.array([loc.weight for loc in config.org_model.locations])

        emps = world.employees
        self._emps: list[Employee] = emps
        self._index = {emp.employee_id: i for i, emp in enumerate(emps)}
        self._status = np.array([_STATUS[e.employment_status] for e in emps], dtype=np.int8)
        self._hire = np.array([e.hire_date.toordinal() for e in emps], dtype=np.int64)
        self._level = np.array([self._levels.index(e.job_level) for e in emps], dtype=np.int64)
        # ADR-0004: the initial world starts every clock at the start of the current role.
        self._level_start = np.array(
            [e.job_effective_date.toordinal() for e in emps], dtype=np.int64
        )
        self._role_start = self._level_start.copy()
        self._reports: list[set[int]] = [set() for _ in emps]
        self._n_reports = np.zeros(len(emps), dtype=np.int64)
        for i, emp in enumerate(emps):
            if emp.manager_id is not None:
                self._reports[self._index[emp.manager_id]].add(i)
                self._n_reports[self._index[emp.manager_id]] += 1
        self._teams = {team.name: team for team in world.teams}
        self._orgs = {org.name: org for org in world.orgs}
        self._members: dict[str, set[int]] = {team.name: set() for team in world.teams}
        self._leave_ends: dict[int, list[int]] = {}
        for i, emp in enumerate(emps):
            self._members[emp.team].add(i)
            if emp.leave_end_date is not None:
                self._leave_ends.setdefault(emp.leave_end_date.toordinal(), []).append(i)

    # ------------------------------------------------------------------ public API

    def employee(self, employee_id: str) -> Employee:
        return self._emps[self._index[employee_id]]

    def step(self, day: date) -> list[WorkforceEvent]:
        """Advance one day: leave returns, the reorg (on its day), then the daily hazards."""
        first = len(self.events)
        self._return_from_leave(day)
        if day == self._reorg_day:
            self._reorg(day)
        self._daily_hazards(day)
        return self.events[first:]

    def event_counts(self) -> dict[EventKind, int]:
        counts = Counter(event.kind for event in self.events)
        return {kind: counts[kind] for kind in EVENT_ORDER if counts[kind]}

    # ------------------------------------------------------------------ daily steps

    def _return_from_leave(self, day: date) -> None:
        for i in self._leave_ends.pop(day.toordinal(), []):
            emp = self._emps[i]
            if self._status[i] != LEAVE or emp.leave_end_date != day:
                continue  # left the company while on leave
            self._status[i] = ACTIVE
            emp.employment_status = "active"
            emp.leave_end_date = None
            self._changed(i, day, "leave_end", "schedule")

    def _reorg(self, day: date) -> None:
        movable = [t for t in self.world.teams if not t.is_leadership and t.manager_id is not None]
        orgs = [org.name for org in self.world.orgs]
        if len(orgs) < 2 or not movable:
            return
        count = min(self._config.workforce.reorg.teams_moved, len(movable))
        for pick in self._rng.choice(len(movable), size=count, replace=False):
            team = movable[int(pick)]
            others = [org for org in orgs if org != team.org]
            team.org = others[int(self._rng.integers(len(others)))]
            assert team.manager_id is not None
            self._set_manager(self._index[team.manager_id], self._leader(team.org))
            for i in sorted(self._members[team.name]):
                self._emps[i].org = team.org
                self._changed(i, day, "reorg_move", "reorg")

    def _daily_hazards(self, day: date) -> None:
        n, today = len(self._emps), day.toordinal()
        draws = self._rng.random((len(HAZARDS), n))
        active = self._status == ACTIVE
        movable_ic = active & (self._n_reports == 0) & ~self._structural_mask()
        p_attrition = np.where(
            today - self._hire < DAYS_PER_YEAR, self._p_attrition_first_year, self._p_attrition
        )
        promotable = (self._level < self._top) & (
            today - self._level_start >= self._config.workforce.promotion_min_days_in_level
        )
        fired = np.stack(
            [
                (self._status != TERMINATED) & (draws[0] < p_attrition),
                active & (draws[1] < self._p[1]),
                active & promotable & (draws[2] < self._p[2]),
                movable_ic & (draws[3] < self._p[3]),
                movable_ic & (draws[4] < self._p[4]),
                active & (draws[5] < self._p[5]),
            ]
        )
        first = fired.argmax(axis=0)
        for i in np.flatnonzero(fired.any(axis=0)):
            self._apply(HAZARDS[int(first[i])], int(i), day)

    def _apply(self, kind: EventKind, i: int, day: date) -> None:
        # Earlier events today (a succession) can change eligibility, so re-check here.
        if kind == "termination":
            self._terminate(i, day)
        elif self._status[i] != ACTIVE:
            return
        elif kind == "leave_start":
            self._start_leave(i, day)
        elif kind == "promotion":
            self._promote_to(i, int(self._level[i]) + 1, day, "hazard")
        elif kind == "location_change":
            self._change_location(i, day)
        elif self._n_reports[i] == 0 and not self._is_structural(i):
            if kind == "lateral_move":
                self._lateral_move(i, day)
            else:
                self._manager_change(i, day)

    # ------------------------------------------------------------------ hazards

    def _terminate(self, i: int, day: date) -> None:
        if not self._vacate(i, day):
            return  # an org leader nobody can succeed stays (ADR-0005)
        emp = self._emps[i]
        self._detach(i)
        self._members[emp.team].discard(i)
        self._status[i] = TERMINATED
        emp.employment_status = "terminated"
        emp.termination_date = day
        emp.leave_end_date = None
        self._changed(i, day, "termination", "hazard")

    def _start_leave(self, i: int, day: date) -> None:
        lo, hi = self._config.workforce.leave_duration_days
        duration = int(self._rng.integers(lo, hi + 1))
        if duration < 1:
            return
        emp = self._emps[i]
        self._status[i] = LEAVE
        emp.employment_status = "leave"
        emp.leave_end_date = day + timedelta(days=duration)
        self._leave_ends.setdefault(emp.leave_end_date.toordinal(), []).append(i)
        self._changed(i, day, "leave_start", "hazard")

    def _change_location(self, i: int, day: date) -> None:
        emp = self._emps[i]
        others = [k for k, city in enumerate(self._cities) if city != emp.location_city]
        if not others:
            return
        weights = self._city_weights[others] / self._city_weights[others].sum()
        emp.location_city = self._cities[others[int(self._rng.choice(len(others), p=weights))]]
        self._changed(i, day, "location_change", "hazard")

    def _lateral_move(self, i: int, day: date) -> None:
        emp = self._emps[i]
        targets = [
            t
            for t in self.world.teams
            if not t.is_leadership and t.manager_id is not None and t.name != emp.team
        ]
        if not targets:
            return
        team = targets[int(self._rng.integers(len(targets)))]
        managers = self._people_managers(team) or [self._index[team.manager_id]]  # type: ignore[index]
        self._members[emp.team].discard(i)
        self._members[team.name].add(i)
        emp.team, emp.org = team.name, team.org
        self._set_manager(i, managers[int(self._rng.integers(len(managers)))])
        self._role_start[i] = day.toordinal()
        self._changed(i, day, "lateral_move", "hazard")

    def _manager_change(self, i: int, day: date) -> None:
        emp = self._emps[i]
        current = self._manager(i)
        options = [m for m in self._people_managers(self._teams[emp.team]) if m not in (i, current)]
        if not options:
            return
        self._set_manager(i, options[int(self._rng.integers(len(options)))])
        self._changed(i, day, "manager_change", "hazard")

    def _promote_to(self, i: int, level: int, day: date, cause: Cause) -> None:
        if level <= self._level[i]:
            return
        self._level[i] = level
        self._level_start[i] = self._role_start[i] = day.toordinal()
        self._emps[i].job_level = self._levels[level]
        self._changed(i, day, "promotion", cause)

    # ------------------------------------------------------------------ succession

    def _vacate(self, i: int, day: date) -> bool:
        """Re-home the direct reports of someone who is leaving their position (ADR-0005)."""
        direct = sorted(self._reports[i])
        emp = self._emps[i]
        org = self._orgs[emp.org]
        if org.leader_id == emp.employee_id:
            if not direct:
                return False
            successor = self._pick_successor(direct)
            old_team = self._teams[self._emps[successor].team]
            self._promote_to(successor, self._top, day, "succession")
            self._members[old_team.name].discard(successor)
            self._members[org.leadership_team].add(successor)
            self._emps[successor].team = org.leadership_team
            self._set_manager(successor, None)
            org.leader_id = self._emps[successor].employee_id
            self._changed(successor, day, "succession", "succession")
            for r in direct:
                if r != successor:
                    self._move_report(r, successor, day)
            self._replace_team_manager(old_team, successor, successor, day)
            return True
        team = self._teams[emp.team]
        if team.manager_id == emp.employee_id:
            self._replace_team_manager(team, i, self._leader(team.org), day)
            return True
        parent = self._manager(i)
        for r in direct:
            self._move_report(r, parent, day)
        return True

    def _replace_team_manager(self, team: Team, old: int, parent: int | None, day: date) -> None:
        """The departing team manager's highest-level report takes the team over."""
        direct = sorted(r for r in self._reports[old] if self._emps[r].team == team.name)
        if not direct:
            team.manager_id = None  # nobody else is left in the team
            return
        heir = self._pick_successor(direct)
        self._promote_to(heir, max(int(self._level[heir]), self._floor), day, "succession")
        team.manager_id = self._emps[heir].employee_id
        if self._set_manager(heir, parent):
            self._changed(heir, day, "succession", "succession")
        for r in direct:
            if r != heir:
                self._move_report(r, heir, day)

    def _pick_successor(self, candidates: list[int]) -> int:
        """Active before on leave, then the highest level; remaining ties are random."""
        keys = self._rng.random(len(candidates))
        best = max(
            range(len(candidates)),
            key=lambda k: (
                self._status[candidates[k]] == ACTIVE,
                self._level[candidates[k]],
                keys[k],
            ),
        )
        return candidates[best]

    def _move_report(self, i: int, manager: int | None, day: date) -> None:
        if self._set_manager(i, manager):
            self._changed(i, day, "manager_change", "succession")

    # ------------------------------------------------------------------ structure helpers

    def _changed(self, i: int, day: date, kind: EventKind, cause: Cause) -> None:
        """Record a change to an HRIS field; every such change sets job_effective_date."""
        emp = self._emps[i]
        emp.job_effective_date = day
        self.events.append(WorkforceEvent(day, emp.employee_id, kind, cause))

    def _set_manager(self, i: int, manager: int | None) -> bool:
        if self._manager(i) == manager:
            return False
        self._detach(i)
        if manager is not None:
            self._reports[manager].add(i)
            self._n_reports[manager] += 1
        self._emps[i].manager_id = None if manager is None else self._emps[manager].employee_id
        return True

    def _detach(self, i: int) -> None:
        current = self._manager(i)
        if current is not None and i in self._reports[current]:
            self._reports[current].discard(i)
            self._n_reports[current] -= 1

    def _manager(self, i: int) -> int | None:
        manager_id = self._emps[i].manager_id
        return None if manager_id is None else self._index[manager_id]

    def _leader(self, org: str) -> int:
        return self._index[self._orgs[org].leader_id]

    def _people_managers(self, team: Team) -> list[int]:
        return sorted(m for m in self._members[team.name] if self._n_reports[m] > 0)

    def _is_structural(self, i: int) -> bool:
        emp_id = self._emps[i].employee_id
        return (
            self._orgs[self._emps[i].org].leader_id == emp_id
            or self._teams[self._emps[i].team].manager_id == emp_id
        )

    def _structural_mask(self) -> npt.NDArray[np.bool_]:
        mask = np.zeros(len(self._emps), dtype=bool)
        for org in self.world.orgs:
            mask[self._index[org.leader_id]] = True
        for team in self.world.teams:
            if team.manager_id is not None:
                mask[self._index[team.manager_id]] = True
        return mask
