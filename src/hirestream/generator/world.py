"""World builder: Halcyon's workforce as it stands on `sim_start` (SPEC §6.3, ADR-0004).

Everything is drawn from the `world` random stream (plus a Faker seed derived from it), in a
fixed order, so the same config and seed always build the same world.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from functools import partial
from typing import Literal

import numpy as np
import numpy.typing as npt

from hirestream.generator.config import GeneratorConfig
from hirestream.generator.people import EmailAllocator, PersonNamer
from hirestream.generator.sampling import quota_counts, sample_piecewise_exponential
from hirestream.generator.seeds import SeedPlan

MANAGER_MIN_LEVEL = "L6"  # SPEC §6.3: every team has a manager at L6 or above
DAYS_PER_YEAR = 365  # SPEC §6.3: a daily hazard is the annual rate / 365

# Team names are "<domain> <function>", unique across the company (ADR-0004).
TEAM_DOMAINS = (
    "Payments", "Search", "Identity", "Checkout", "Catalog", "Pricing", "Fulfillment",
    "Logistics", "Messaging", "Billing", "Analytics", "Security", "Storage", "Compute",
    "Networking", "Recommendations", "Mobile", "Web", "Growth", "Onboarding", "Compliance",
    "Tax", "Treasury", "Talent", "Workplace", "Support", "Reliability", "Developer Tools",
    "Data Platform", "Marketplace",
)  # fmt: skip
TEAM_FUNCTIONS = (
    "Platform", "Experience", "Services", "Insights", "Operations", "Engineering",
    "Foundations", "Tools",
)  # fmt: skip

EmploymentStatus = Literal["active", "leave", "terminated"]
IntArray = npt.NDArray[np.int64]


class WorldBuildError(ValueError):
    """The config cannot produce a world that satisfies SPEC §6.3 and ADR-0004."""


@dataclass(slots=True)
class Org:
    name: str
    leader_id: str
    leadership_team: str


@dataclass(slots=True)
class Team:
    name: str
    org: str
    manager_id: str
    is_leadership: bool = False  # the org leader's one-person team; the reorg skips it


@dataclass(slots=True)
class Employee:
    """One HRIS record (SPEC §7.5, minus `snapshot_date`) plus simulation state."""

    employee_id: str
    first_name: str
    last_name: str
    work_email: str
    hire_date: date
    org: str
    team: str
    role_family: str
    job_level: str
    manager_id: str | None
    location_city: str
    employment_status: EmploymentStatus
    termination_date: date | None
    job_effective_date: date
    ats_candidate_id: str | None
    leave_end_date: date | None = None  # simulation state, never exported: first day back


@dataclass(slots=True)
class World:
    orgs: list[Org]
    teams: list[Team]
    employees: list[Employee]  # in employee_id order

    def direct_reports(self) -> dict[str, list[str]]:
        reports: dict[str, list[str]] = {}
        for emp in self.employees:
            if emp.manager_id is not None:
                reports.setdefault(emp.manager_id, []).append(emp.employee_id)
        return reports

    def summary(self) -> str:
        teams = sum(not team.is_leadership for team in self.teams)
        on_leave = sum(emp.employment_status == "leave" for emp in self.employees)
        return (
            f"{len(self.orgs)} orgs, {teams} teams, {len(self.employees)} employees "
            f"({len(self.direct_reports())} managers, {on_leave} on leave)"
        )

    def fingerprint(self) -> str:
        """sha256 over every field of every org, team, and employee."""
        payload = {
            "orgs": [asdict(org) for org in self.orgs],
            "teams": [asdict(team) for team in self.teams],
            "employees": [asdict(emp) for emp in self.employees],
        }
        canonical = json.dumps(
            payload, sort_keys=True, separators=(",", ":"), default=date.isoformat
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


def build_world(config: GeneratorConfig, plan: SeedPlan) -> World:
    rng = plan.rng("world")
    org_names = list(config.org_model.orgs[: config.scale.org_count])
    team_names, teams_per_org, team_sizes = _plan_teams(config, rng, len(org_names))

    # People are indices until they get ids; parent -1 marks an org leader.
    parent: list[int] = []
    team_of: list[int] = []
    all_teams: list[tuple[str, int, bool]] = []  # (name, org index, is_leadership)

    def add_person(team: int, manager: int) -> int:
        parent.append(manager)
        team_of.append(team)
        return len(parent) - 1

    lo, hi = config.org_model.span_of_control
    leaders: list[int] = []
    team_managers: list[int] = []
    next_team = 0
    for org_idx, org in enumerate(org_names):
        all_teams.append((f"{org} Leadership", org_idx, True))
        leader = add_person(len(all_teams) - 1, -1)
        leaders.append(leader)
        for _ in range(teams_per_org[org_idx]):
            all_teams.append((team_names[next_team], org_idx, False))
            team = len(all_teams) - 1
            manager = add_person(team, leader)
            team_managers.append(manager)
            _grow(rng, manager, int(team_sizes[next_team]) - 1, lo, hi, partial(add_person, team))
            next_team += 1

    n = len(parent)
    levels = _assign_levels(config, rng, parent)
    role_families = _shuffled_quota(rng, config.org_model.role_families, n)
    cities = _shuffled_quota(rng, {loc.city: loc.weight for loc in config.org_model.locations}, n)
    tenure, in_role = _sample_tenure(config, rng, levels)
    on_leave, leave_back = _sample_leave(config, rng, tenure)

    order = np.lexsort((rng.permutation(n), -tenure))  # oldest hire first; random tie-break
    ids = [""] * n
    for rank, person in enumerate(order):
        ids[person] = f"E{rank + 1:06d}"

    namer = PersonNamer(config.meta.faker_locale_by_country, plan.faker_seed("world"))
    emails = EmailAllocator(config.meta.internal_email_domain)
    country = {loc.city: loc.country for loc in config.org_model.locations}
    day0 = config.window.sim_start - timedelta(days=1)
    employees: list[Employee] = []
    for person in (int(p) for p in order):
        first, last = namer.name(country[cities[person]])
        team_name, org_idx, _ = all_teams[team_of[person]]
        employees.append(
            Employee(
                employee_id=ids[person],
                first_name=first,
                last_name=last,
                work_email=emails.allocate(first, last),
                hire_date=day0 - timedelta(days=int(tenure[person])),
                org=org_names[org_idx],
                team=team_name,
                role_family=role_families[person],
                job_level=levels[person],
                manager_id=ids[parent[person]] if parent[person] >= 0 else None,
                location_city=cities[person],
                employment_status="leave" if on_leave[person] else "active",
                termination_date=None,
                job_effective_date=day0 - timedelta(days=int(in_role[person])),
                ats_candidate_id=None,
                leave_end_date=(
                    day0 + timedelta(days=int(leave_back[person])) if on_leave[person] else None
                ),
            )
        )

    orgs = [
        Org(name=org, leader_id=ids[leaders[i]], leadership_team=f"{org} Leadership")
        for i, org in enumerate(org_names)
    ]
    heads = iter(team_managers)
    teams = [
        Team(
            name=name,
            org=org_names[org_idx],
            manager_id=ids[leaders[org_idx]] if leadership else ids[next(heads)],
            is_leadership=leadership,
        )
        for name, org_idx, leadership in all_teams
    ]
    return World(orgs=orgs, teams=teams, employees=employees)


def _plan_teams(
    config: GeneratorConfig, rng: np.random.Generator, org_count: int
) -> tuple[list[str], list[int], IntArray]:
    """Team counts per org, unique team names, and team sizes (ADR-0004 §3)."""
    lo_teams, hi_teams = config.scale.teams_per_org
    per_org = [int(c) for c in rng.integers(lo_teams, hi_teams + 1, size=org_count)]
    total = sum(per_org)
    combos = [f"{domain} {function}" for domain in TEAM_DOMAINS for function in TEAM_FUNCTIONS]
    if total > len(combos):
        raise WorldBuildError(f"{total} teams requested but only {len(combos)} team names exist")
    names = [combos[int(i)] for i in rng.choice(len(combos), size=total, replace=False)]

    members = config.scale.initial_headcount - org_count
    sizes = rng.multinomial(members, [1.0 / total] * total).astype(np.int64)
    lo = config.org_model.span_of_control[0]
    if int(sizes.min()) < lo + 1:
        raise WorldBuildError(
            f"a team of {int(sizes.min())} can't have a manager with {lo}+ reports; "
            "raise scale.initial_headcount or lower scale.teams_per_org"
        )
    return names, per_org, sizes


def _grow(
    rng: np.random.Generator,
    manager: int,
    budget: int,
    lo: int,
    hi: int,
    add: Callable[[int], int],
) -> None:
    """Place `budget` people under `manager` so every manager has lo..hi direct reports.

    With budget <= hi they are all direct IC reports. Otherwise the manager takes k reports, j of
    them managers, and the other budget - k people split evenly across those j (ADR-0004 §3).
    """
    if budget <= hi:
        for _ in range(budget):
            add(manager)
        return
    k = int(rng.integers(lo, min(hi, budget - lo) + 1))  # leaves room for >= lo people below
    rest = budget - k
    fewest, most = max(1, math.ceil(rest / hi)), min(k, rest // lo)
    j = k if fewest > k else int(rng.integers(fewest, most + 1))
    base, extra = divmod(rest, j)
    for i in range(j):
        _grow(rng, add(manager), base + (1 if i < extra else 0), lo, hi, add)
    for _ in range(k - j):
        add(manager)


def _assign_levels(
    config: GeneratorConfig, rng: np.random.Generator, parent: Sequence[int]
) -> list[str]:
    """Exact level counts; leaders top level, managers L6+, ICs the rest (ADR-0004 §4)."""
    order = list(config.org_model.levels)  # junior to senior
    if MANAGER_MIN_LEVEL not in order:
        raise WorldBuildError(f"org_model.levels must include {MANAGER_MIN_LEVEL}")
    top = order[-1]
    n = len(parent)
    pool = Counter(quota_counts(config.org_model.levels, n))

    has_reports = [False] * n
    for manager in parent:
        if manager >= 0:
            has_reports[manager] = True
    manages_managers = [False] * n
    for person, manager in enumerate(parent):
        if manager >= 0 and has_reports[person]:
            manages_managers[manager] = True

    senior = order[order.index(MANAGER_MIN_LEVEL) :]  # e.g. L6, L7, L8
    second_line = senior[1:] + senior[:1]  # prefer L7, then L8, then L6
    level: list[str | None] = [None] * n

    def take(person: int, preferences: Sequence[str], role: str) -> None:
        for candidate in preferences:
            if pool[candidate] > 0:
                pool[candidate] -= 1
                level[person] = candidate
                return
        raise WorldBuildError(
            f"not enough {'/'.join(preferences)} levels for every {role}; "
            "check org_model.levels against span_of_control"
        )

    for person in range(n):
        if parent[person] < 0:
            take(person, [top], "org leader")
    groups = (
        ([p for p in range(n) if parent[p] >= 0 and manages_managers[p]], second_line),
        (
            [p for p in range(n) if parent[p] >= 0 and has_reports[p] and not manages_managers[p]],
            senior,
        ),
    )
    for group, preferences in groups:
        for idx in rng.permutation(len(group)):
            take(group[int(idx)], preferences, "manager")

    remaining = [lvl for lvl in order for _ in range(pool[lvl])]
    ics = [p for p in range(n) if level[p] is None]
    for person, idx in zip(ics, rng.permutation(len(remaining)), strict=True):
        level[person] = remaining[int(idx)]
    return [lvl for lvl in level if lvl is not None]


def _shuffled_quota(rng: np.random.Generator, shares: Mapping[str, float], n: int) -> list[str]:
    values = [key for key, count in quota_counts(shares, n).items() for _ in range(count)]
    return [values[int(i)] for i in rng.permutation(n)]


def _sample_tenure(
    config: GeneratorConfig, rng: np.random.Generator, levels: Sequence[str]
) -> tuple[IntArray, IntArray]:
    """Days since hire and days in the current role on sim_start - 1 (ADR-0004 §1)."""
    w = config.workforce
    n, d = len(levels), float(DAYS_PER_YEAR)
    max_tenure = (config.window.sim_start - config.meta.company_founded).days
    attrition, growth = w.attrition_annual / d, w.growth_annual / d
    first_year = attrition * w.attrition_first_year_multiplier
    tenure = np.floor(
        sample_piecewise_exponential(
            rng, [0.0, d], [growth + first_year, growth + attrition], max_tenure, n
        )
    ).astype(np.int64)

    change = (w.lateral_move_annual + w.location_change_annual) / d
    with_promotion = change + w.promotion_annual / d
    floor_days = float(w.promotion_min_days_in_level)

    def since_change(breaks: list[float], rates: list[float]) -> npt.NDArray[np.float64]:
        if rates[-1] == 0:  # nobody ever changes role: in role since hire
            return np.full(n, np.inf)
        return sample_piecewise_exponential(rng, breaks, rates, math.inf, n)

    promotable = (
        since_change([0.0, floor_days], [change, with_promotion])
        if floor_days > 0
        else since_change([0.0], [with_promotion])
    )
    top_level = since_change([0.0], [change])
    is_top = np.asarray(levels) == list(config.org_model.levels)[-1]
    in_role = np.minimum(np.floor(np.where(is_top, top_level, promotable)), tenure).astype(np.int64)
    return tenure, in_role


def _sample_leave(
    config: GeneratorConfig, rng: np.random.Generator, tenure: IntArray
) -> tuple[npt.NDArray[np.bool_], IntArray]:
    """Who is on leave on sim_start, and their return day as an offset from sim_start - 1."""
    w = config.workforce
    lo, hi = w.leave_duration_days
    n = len(tenure)
    on_leave = rng.random(n) < w.leave_annual * (lo + hi) / 2 / DAYS_PER_YEAR
    duration = rng.integers(lo, hi + 1, size=n).astype(np.int64)
    elapsed = np.minimum(np.floor(rng.random(n) * duration).astype(np.int64), tenure)
    return on_leave & (duration >= 1), duration - elapsed
