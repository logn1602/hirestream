"""Requisitions: open, hold, fill, cancel, and replenish job reqs day by day.

SPEC §6.4 names the three ways a req opens (backfill, growth, evergreen) and its lifecycle;
ADR-0006 fixes the rest: growth follows a monthly headcount plan, the ATS goes live with a
pipeline of open reqs, popularity is a capped Pareto, and reqs follow their team when the org
changes. Reqs cannot fill until the ATS (T1.6) calls `record_accept`.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal, TypeVar

import numpy as np

from hirestream.generator.config import GeneratorConfig
from hirestream.generator.sampling import sample_truncated_pareto
from hirestream.generator.workforce import Workforce, WorkforceEvent
from hirestream.generator.world import DAYS_PER_YEAR, Employee, Team

ReqStatus = Literal["open", "on_hold", "filled", "cancelled"]
ReqSource = Literal["backfill", "growth", "evergreen"]
CloseReason = Literal["filled", "cancelled", "expired", "team_dissolved"]
ReqEventKind = Literal[
    "opened",
    "on_hold",
    "resumed",
    "filled",
    "cancelled",
    "expired",
    "reopened",
    "replenished",
    "reassigned",
    "moved",
]
ACTIVE: frozenset[ReqStatus] = frozenset({"open", "on_hold"})
T = TypeVar("T")

ROLE_TITLES = {
    "software_engineering": "Software Engineer",
    "data_engineering": "Data Engineer",
    "data_science": "Data Scientist",
    "product_management": "Product Manager",
    "program_management": "Program Manager",
    "operations": "Operations Specialist",
    "finance": "Financial Analyst",
    "recruiting": "Recruiter",
    "design": "Product Designer",
    "sales": "Account Executive",
}
LEVEL_PREFIXES = {
    "L3": "Associate",
    "L4": "",
    "L5": "Senior",
    "L6": "Staff",
    "L7": "Principal",
    "L8": "Senior Principal",
}


def title_for(role_family: str, level: str) -> str:
    base = ROLE_TITLES.get(role_family, role_family.replace("_", " ").title())
    return f"{LEVEL_PREFIXES.get(level, level)} {base}".strip()


@dataclass(slots=True)
class Requisition:
    """One ATS requisition (SPEC §7.4, dates only) plus simulation state."""

    req_id: str
    title: str
    role_family: str
    job_level: str
    org: str
    team: str
    location_city: str
    headcount: int
    hiring_manager_id: str
    recruiter_id: str | None
    status: ReqStatus
    is_evergreen: bool
    is_internal_only: bool
    opened_on: date
    closed_on: date | None
    updated_on: date
    # simulation state, not an ATS column
    source: ReqSource
    popularity: float
    seats_open: int  # seats that can still take an accepted offer
    close_reason: CloseReason | None = None
    backfill_for: str | None = None  # employee_id of the leaver this req replaces
    expires_on: date | None = None  # None for evergreen reqs
    hold_on: date | None = None
    hold_until: date | None = None
    cancel_on: date | None = None


@dataclass(frozen=True, slots=True)
class ReqEvent:
    day: date
    req_id: str
    kind: ReqEventKind
    detail: str | None = None


@dataclass(frozen=True, slots=True)
class GrowthPlan:
    day: date
    target: float
    projected: int
    seats: int


@dataclass(frozen=True, slots=True)
class _Pending:
    """A req scheduled to open on `day` (backfills after a delay, growth across the month)."""

    day: date
    source: ReqSource
    team: str
    role_family: str
    job_level: str
    location_city: str
    headcount: int
    manager_hint: str | None
    backfill_for: str | None = None


class Requisitions:
    def __init__(
        self, config: GeneratorConfig, rng: np.random.Generator, workforce: Workforce
    ) -> None:
        self.reqs: dict[str, Requisition] = {}  # every req ever opened, in req_id order
        self.events: list[ReqEvent] = []
        self.plans: list[GrowthPlan] = []
        self._config = config
        self._cfg = config.requisitions
        self._rng = rng
        self._wf = workforce
        self._start, self._end = config.window.sim_start, config.window.sim_end
        self._teams = {team.name: team for team in workforce.world.teams}
        self._active: dict[str, Requisition] = {}  # open or on hold
        self._by_team: dict[str, set[str]] = {}
        self._by_manager: dict[str, set[str]] = {}
        self._by_recruiter: dict[str, set[str]] = {}
        self._unassigned: set[str] = set()  # active reqs waiting for a recruiter
        self._load: dict[str, int] = {}  # employed recruiters -> active reqs they own
        self._available: set[str] = set()  # recruiters not on leave
        for emp in workforce.world.employees:
            if emp.role_family == "recruiting" and emp.employment_status != "terminated":
                self._load[emp.employee_id] = 0
                if emp.employment_status == "active":
                    self._available.add(emp.employee_id)
        self._pending: dict[int, list[_Pending]] = {}
        self._pending_seats = 0
        self._agenda: dict[int, list[tuple[str, str]]] = {}
        self._next_id = 1
        self._h0 = self._alive_count()
        self._go_live()

    # ------------------------------------------------------------------ public API

    def step(self, day: date, workforce_events: Sequence[WorkforceEvent]) -> list[ReqEvent]:
        """Advance one day, after the workforce: react to its events, plan, open, age reqs."""
        first = len(self.events)
        self._follow_workforce(day, workforce_events)
        if self._unassigned and self._available:
            self._assign_waiting(day)
        if day == self._start or day.day == 1:
            self._refill_evergreen(day)
            self._plan_growth(day)
        self._open_due(day)
        self._run_agenda(day)
        return self.events[first:]

    def open_reqs(self) -> list[Requisition]:
        return [req for req in self._active.values() if req.status == "open"]

    def active_reqs(self) -> list[Requisition]:
        return list(self._active.values())

    def record_accept(self, req_id: str, day: date) -> None:
        """An offer on `req_id` was accepted: take a seat, and fill the req on its last seat."""
        req = self.reqs[req_id]
        if req.status not in ACTIVE or req.seats_open <= 0:
            raise ValueError(f"{req_id} has no open seat ({req.status}, {req.seats_open} left)")
        req.seats_open -= 1
        req.updated_on = day
        if req.seats_open == 0 and not req.is_evergreen:
            self._close(req, day, "filled", "filled")

    def reopen_seat(self, req_id: str, day: date) -> None:
        """An accepted offer became a no-start: give the seat back (ADR-0006 §5)."""
        req = self.reqs[req_id]
        if req.status == "filled" and req.expires_on is not None and day < req.expires_on:
            req.status, req.closed_on, req.close_reason = "open", None, None
            req.seats_open += 1
            req.updated_on = day
            self._index(req)
            self._log(day, req, "reopened")
            self._check_manager(req, day)
        elif req.status in ACTIVE and req.seats_open < req.headcount:
            req.seats_open += 1
            req.updated_on = day
            self._log(day, req, "reopened")

    def summary(self) -> dict[str, int]:
        kinds = Counter(event.kind for event in self.events)
        opened = Counter(req.source for req in self.reqs.values() if req.opened_on >= self._start)
        return {
            "go_live": sum(req.opened_on < self._start for req in self.reqs.values()),
            "opened": sum(opened.values()),
            "backfill": opened["backfill"],
            "growth": opened["growth"],
            "filled": kinds["filled"],
            "cancelled": kinds["cancelled"],
            "expired": kinds["expired"],
            "open_at_end": len(self._active),
        }

    # ------------------------------------------------------------------ go-live (ADR-0006 §2)

    def _go_live(self) -> None:
        members = self._alive_members()
        born: list[Requisition] = []
        evergreen = max(1, round(self._cfg.evergreen.per_1000_headcount * self._h0 / 1000))
        for _ in range(evergreen):
            ago = int(self._rng.integers(1, self._cfg.max_open_days + 1))
            born.append(self._make_evergreen(self._start - timedelta(days=ago), members))

        backfill_rate, growth_rate = self._opening_rates()
        days = self._cfg.initial_pipeline_days
        specs = [self._clone("backfill", members, 1) for _ in range(round(backfill_rate * days))]
        remaining = round(growth_rate * days)
        while remaining > 0:
            seats = min(self._draw_headcount(), remaining)
            specs.append(self._clone("growth", members, seats))
            remaining -= seats
        for spec in specs:
            ago = int(self._rng.integers(1, days + 1))
            req = self._make(spec, self._start - timedelta(days=ago))
            if req is not None and not (req.cancel_on is not None and req.cancel_on < self._start):
                born.append(req)

        keys = self._rng.random(len(born))
        order = sorted(range(len(born)), key=lambda i: (born[i].opened_on, keys[i]))
        for i in order:
            req = born[i]
            req.req_id = self._new_id()
            on_hold = req.hold_on is not None and req.hold_on < self._start
            if on_hold and req.hold_until is not None and req.hold_until <= self._start:
                req.hold_on = req.hold_until = None  # the hold ended before go-live
                on_hold = False
            self._activate(req, req.opened_on)
            if on_hold:
                assert req.hold_on is not None
                req.status = "on_hold"
                self._log(req.hold_on, req, "on_hold")

    def _opening_rates(self) -> tuple[float, float]:
        """Steady seats opened per day: (backfill, growth), from the configured rates."""
        w = self._config.workforce
        alive = [e for e in self._wf.world.employees if e.employment_status != "terminated"]
        first_year = sum((self._start - e.hire_date).days <= DAYS_PER_YEAR for e in alive)
        attrition = w.attrition_annual * (
            1 + (w.attrition_first_year_multiplier - 1) * first_year / max(len(alive), 1)
        )
        per_day = self._h0 / DAYS_PER_YEAR
        return (
            per_day * attrition * w.backfill_probability,
            per_day * (w.growth_annual + attrition * (1 - w.backfill_probability)),
        )

    # ------------------------------------------------------------------ daily steps

    def _follow_workforce(self, day: date, events: Sequence[WorkforceEvent]) -> None:
        moved_teams: set[str] = set()
        for event in events:
            emp = self._wf.employee(event.employee_id)
            if event.kind == "termination":
                self._maybe_backfill(day, emp)
                self._drop_recruiter(day, emp.employee_id)
            elif event.kind == "leave_start":
                self._available.discard(emp.employee_id)
            elif event.kind == "leave_end" and emp.employee_id in self._load:
                self._available.add(emp.employee_id)
            elif event.kind == "reorg_move":
                moved_teams.add(emp.team)
            for req_id in sorted(self._by_manager.get(emp.employee_id, ())):
                self._check_manager(self._active[req_id], day)
        for name in sorted(moved_teams):
            for req_id in sorted(self._by_team.get(name, ())):
                req = self._active[req_id]
                if req.org != self._teams[name].org:
                    req.org = self._teams[name].org
                    req.updated_on = day
                    self._log(day, req, "moved", req.org)

    def _maybe_backfill(self, day: date, emp: Employee) -> None:
        if self._teams[emp.team].is_leadership:
            return  # succession fills an org leader's role (ADR-0006 §4)
        if self._rng.random() >= self._config.workforce.backfill_probability:
            return
        lo, hi = self._config.workforce.backfill_open_delay_days
        self._schedule(
            _Pending(
                day=day + timedelta(days=int(self._rng.integers(lo, hi + 1))),
                source="backfill",
                team=emp.team,
                role_family=emp.role_family,
                job_level=emp.job_level,
                location_city=emp.location_city,
                headcount=1,
                manager_hint=emp.manager_id,
                backfill_for=emp.employee_id,
            )
        )

    def _plan_growth(self, day: date) -> None:
        """Open enough growth seats to be on the headcount plan by the next plan day."""
        following = (day.replace(day=1) + timedelta(days=32)).replace(day=1)
        years = (following - self._start).days / DAYS_PER_YEAR
        target = self._h0 * (1 + self._config.workforce.growth_annual) ** years
        open_seats = sum(r.seats_open for r in self._active.values() if not r.is_evergreen)
        projected = self._alive_count() + open_seats + self._pending_seats
        members = self._alive_members()
        seats = max(0, round(target - projected)) if members else 0  # no team left to grow
        self.plans.append(GrowthPlan(day, target, projected, seats))
        if seats == 0:
            return
        last = min(following - timedelta(days=1), self._end)
        while seats > 0:
            headcount = min(self._draw_headcount(), seats)
            spec = self._clone("growth", members, headcount)
            offset = int(self._rng.integers(0, (last - day).days + 1))
            self._schedule(_replace_day(spec, day + timedelta(days=offset)))
            seats -= headcount

    def _open_due(self, day: date) -> None:
        for spec in self._pending.pop(day.toordinal(), []):
            self._pending_seats -= spec.headcount
            req = self._make(spec, day)
            if req is None:
                continue  # the team emptied while the req was waiting to open
            req.req_id = self._new_id()
            self._activate(req, day)

    def _run_agenda(self, day: date) -> None:
        for action, req_id in self._agenda.pop(day.toordinal(), []):
            req = self._active.get(req_id)
            if req is None:
                continue
            if action == "hold" and req.status == "open":
                req.status, req.updated_on = "on_hold", day
                self._log(day, req, "on_hold")
            elif action == "resume" and req.status == "on_hold":
                req.status, req.updated_on = "open", day
                self._log(day, req, "resumed")
            elif action == "cancel":
                self._close(req, day, "cancelled", "cancelled")
            elif action == "expire":
                self._close(req, day, "cancelled", "expired")

    def _refill_evergreen(self, day: date) -> None:
        for req in self._active.values():
            if req.is_evergreen and req.seats_open < req.headcount:
                req.seats_open, req.updated_on = req.headcount, day
                self._log(day, req, "replenished")

    # ------------------------------------------------------------------ making reqs

    def _clone(self, source: ReqSource, members: list[Employee], headcount: int) -> _Pending:
        """Copy a random current employee's team, role family, level, and location."""
        emp = members[int(self._rng.integers(len(members)))]
        return _Pending(
            day=self._start,
            source=source,
            team=emp.team,
            role_family=emp.role_family,
            job_level=emp.job_level,
            location_city=emp.location_city,
            headcount=headcount,
            manager_hint=emp.manager_id,
        )

    def _make(self, spec: _Pending, opened: date) -> Requisition | None:
        team = self._teams[spec.team]
        manager = self._hiring_manager(team, spec.manager_hint)
        if manager is None:
            return None
        cfg = self._cfg
        popularity = float(self._popularity())
        internal = bool(self._rng.random() < cfg.internal_only_share)
        hold_on = hold_until = cancel_on = None
        if self._rng.random() < cfg.on_hold_probability:
            hold_on = opened + timedelta(days=int(self._rng.integers(0, cfg.max_open_days)))
            lo, hi = cfg.on_hold_days
            hold_until = hold_on + timedelta(days=int(self._rng.integers(lo, hi + 1)))
        if self._rng.random() < cfg.cancel_probability:
            cancel_on = opened + timedelta(days=int(self._rng.integers(0, cfg.max_open_days)))
        return Requisition(
            req_id="",
            title=title_for(spec.role_family, spec.job_level),
            role_family=spec.role_family,
            job_level=spec.job_level,
            org=team.org,
            team=team.name,
            location_city=spec.location_city,
            headcount=spec.headcount,
            hiring_manager_id=manager,
            recruiter_id=None,
            status="open",
            is_evergreen=False,
            is_internal_only=internal,
            opened_on=opened,
            closed_on=None,
            updated_on=opened,
            source=spec.source,
            popularity=popularity,
            seats_open=spec.headcount,
            backfill_for=spec.backfill_for,
            expires_on=opened + timedelta(days=cfg.max_open_days),
            hold_on=hold_on,
            hold_until=hold_until,
            cancel_on=cancel_on,
        )

    def _make_evergreen(self, opened: date, members: list[Employee]) -> Requisition:
        ever, org_model = self._cfg.evergreen, self._config.org_model
        team = self._teams[members[int(self._rng.integers(len(members)))].team]  # size-weighted
        role_family = self._weighted(
            ever.role_families, [org_model.role_families[r] for r in ever.role_families]
        )
        level = self._weighted(ever.levels, [org_model.levels[lvl] for lvl in ever.levels])
        cities = [loc.city for loc in org_model.locations]
        city = self._weighted(cities, [loc.weight for loc in org_model.locations])
        lo, hi = ever.headcount_range
        headcount = int(self._rng.integers(lo, hi + 1))
        assert team.manager_id is not None
        return Requisition(
            req_id="",
            title=title_for(role_family, level),
            role_family=role_family,
            job_level=level,
            org=team.org,
            team=team.name,
            location_city=city,
            headcount=headcount,
            hiring_manager_id=team.manager_id,
            recruiter_id=None,
            status="open",
            is_evergreen=True,
            is_internal_only=False,
            opened_on=opened,
            closed_on=None,
            updated_on=opened,
            source="evergreen",
            popularity=float(self._popularity()) * self._cfg.popularity.evergreen_multiplier,
            seats_open=headcount,
        )

    def _popularity(self) -> float:
        p = self._cfg.popularity
        return float(sample_truncated_pareto(self._rng, p.pareto_alpha, p.truncate_at, 1)[0])

    def _draw_headcount(self) -> int:
        dist = self._cfg.headcount_distribution
        return int(self._weighted(list(dist), list(dist.values())))

    def _weighted(self, values: Sequence[T], weights: Sequence[float]) -> T:
        w = np.asarray(weights, dtype=float)
        return values[int(self._rng.choice(len(values), p=w / w.sum()))]

    # ------------------------------------------------------------------ bookkeeping

    def _activate(self, req: Requisition, day: date) -> None:
        self.reqs[req.req_id] = req
        req.recruiter_id = self._pick_recruiter()
        self._index(req)
        self._log(day, req, "opened", req.source)
        for when, action in (
            (req.hold_on, "hold"),
            (req.hold_until, "resume"),
            (req.cancel_on, "cancel"),
            (req.expires_on, "expire"),
        ):
            if when is not None and when >= self._start:
                self._agenda.setdefault(when.toordinal(), []).append((action, req.req_id))

    def _index(self, req: Requisition) -> None:
        self._active[req.req_id] = req
        self._by_team.setdefault(req.team, set()).add(req.req_id)
        self._by_manager.setdefault(req.hiring_manager_id, set()).add(req.req_id)
        if req.recruiter_id is None:
            self._unassigned.add(req.req_id)
        else:
            self._by_recruiter.setdefault(req.recruiter_id, set()).add(req.req_id)
            self._load[req.recruiter_id] += 1

    def _close(self, req: Requisition, day: date, status: ReqStatus, reason: CloseReason) -> None:
        req.status, req.closed_on, req.close_reason, req.updated_on = status, day, reason, day
        del self._active[req.req_id]
        self._by_team[req.team].discard(req.req_id)
        self._by_manager[req.hiring_manager_id].discard(req.req_id)
        self._unassigned.discard(req.req_id)
        if req.recruiter_id is not None and req.recruiter_id in self._by_recruiter:
            self._by_recruiter[req.recruiter_id].discard(req.req_id)
            if req.recruiter_id in self._load:
                self._load[req.recruiter_id] -= 1
        kind: ReqEventKind = (
            "filled" if reason == "filled" else "expired" if reason == "expired" else "cancelled"
        )
        self._log(day, req, kind, reason)

    def _check_manager(self, req: Requisition, day: date) -> None:
        """Keep the hiring manager an employed member of the req's team (ADR-0006 §5)."""
        manager = self._wf.employee(req.hiring_manager_id)
        if manager.employment_status != "terminated" and manager.team == req.team:
            return
        team = self._teams[req.team]
        if team.manager_id is None:
            self._close(req, day, "cancelled", "team_dissolved")
            return
        self._by_manager[req.hiring_manager_id].discard(req.req_id)
        req.hiring_manager_id, req.updated_on = team.manager_id, day
        self._by_manager.setdefault(req.hiring_manager_id, set()).add(req.req_id)
        self._log(day, req, "reassigned", "hiring_manager")

    def _hiring_manager(self, team: Team, hint: str | None) -> str | None:
        if hint is not None:
            emp = self._wf.employee(hint)
            if emp.employment_status != "terminated" and emp.team == team.name:
                return hint
        return team.manager_id

    def _pick_recruiter(self) -> str | None:
        """The least-loaded available recruiter; ties are broken at random."""
        pool = [r for r in self._load if r in self._available]
        if not pool:
            return None
        low = min(self._load[r] for r in pool)
        ties = [r for r in pool if self._load[r] == low]
        return ties[int(self._rng.integers(len(ties)))]

    def _drop_recruiter(self, day: date, emp_id: str) -> None:
        if emp_id not in self._load:
            return
        del self._load[emp_id]
        self._available.discard(emp_id)
        for req_id in sorted(self._by_recruiter.pop(emp_id, set())):
            req = self._active[req_id]
            req.recruiter_id, req.updated_on = self._pick_recruiter(), day
            if req.recruiter_id is None:
                self._unassigned.add(req_id)
            else:
                self._by_recruiter.setdefault(req.recruiter_id, set()).add(req_id)
                self._load[req.recruiter_id] += 1
            self._log(day, req, "reassigned", "recruiter")

    def _assign_waiting(self, day: date) -> None:
        for req_id in sorted(self._unassigned):
            recruiter = self._pick_recruiter()
            if recruiter is None:
                return
            req = self._active[req_id]
            req.recruiter_id, req.updated_on = recruiter, day
            self._by_recruiter.setdefault(recruiter, set()).add(req_id)
            self._load[recruiter] += 1
            self._unassigned.discard(req_id)
            self._log(day, req, "reassigned", "recruiter")

    def _schedule(self, spec: _Pending) -> None:
        self._pending.setdefault(spec.day.toordinal(), []).append(spec)
        self._pending_seats += spec.headcount

    def _new_id(self) -> str:
        req_id = f"R{self._next_id:06d}"
        self._next_id += 1
        return req_id

    def _log(
        self, day: date, req: Requisition, kind: ReqEventKind, detail: str | None = None
    ) -> None:
        self.events.append(ReqEvent(day, req.req_id, kind, detail))

    def _alive_count(self) -> int:
        return sum(e.employment_status != "terminated" for e in self._wf.world.employees)

    def _alive_members(self) -> list[Employee]:
        return [
            e
            for e in self._wf.world.employees
            if e.employment_status != "terminated" and not self._teams[e.team].is_leadership
        ]


def _replace_day(spec: _Pending, day: date) -> _Pending:
    return _Pending(
        day=day,
        source=spec.source,
        team=spec.team,
        role_family=spec.role_family,
        job_level=spec.job_level,
        location_city=spec.location_city,
        headcount=spec.headcount,
        manager_hint=spec.manager_hint,
        backfill_for=spec.backfill_for,
    )
