import math
import re
from collections import Counter
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from hirestream.generator.config import GeneratorConfig, load_config
from hirestream.generator.sampling import quota_counts
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.world import World, WorldBuildError, build_world

WriteConfig = Callable[[dict[str, Any]], Path]

# Fingerprint of the tiny world for seed 1602. It changes whenever the world builder, Faker,
# or numpy changes the generated people; update it deliberately, in the PR that changes the data.
GOLDEN_TINY_1602 = "14ec2ee87556214e1a854614f2150a1d2e5bea3cfb7111004029cbf90673cb27"


@pytest.fixture(scope="module", params=["tiny", "dev"])
def built(request: pytest.FixtureRequest, base_config_path: Path) -> tuple[GeneratorConfig, World]:
    cfg = load_config(base_config_path, request.param)
    return cfg, build_world(cfg, SeedPlan(1602))


def _custom(raw: dict[str, Any], write: WriteConfig) -> tuple[GeneratorConfig, World]:
    cfg = load_config(write(raw), "tiny")
    return cfg, build_world(cfg, SeedPlan(1602))


def _rank(cfg: GeneratorConfig) -> dict[str, int]:
    return {level: i for i, level in enumerate(cfg.org_model.levels)}


def test_headcount_orgs_and_teams(built: tuple[GeneratorConfig, World]) -> None:
    cfg, world = built
    assert len(world.employees) == cfg.scale.initial_headcount
    assert [org.name for org in world.orgs] == cfg.org_model.orgs[: cfg.scale.org_count]
    lo, hi = cfg.scale.teams_per_org
    for org in world.orgs:
        real = [t for t in world.teams if t.org == org.name and not t.is_leadership]
        assert lo <= len(real) <= hi
        assert [t.name for t in world.teams if t.org == org.name and t.is_leadership] == [
            org.leadership_team
        ]
    assert len({t.name for t in world.teams}) == len(world.teams)


def test_org_leaders(built: tuple[GeneratorConfig, World]) -> None:
    cfg, world = built
    by_id = {e.employee_id: e for e in world.employees}
    reports = world.direct_reports()
    top = list(cfg.org_model.levels)[-1]
    for org in world.orgs:
        leader = by_id[org.leader_id]
        assert (leader.job_level, leader.manager_id, leader.team) == (
            top,
            None,
            org.leadership_team,
        )
        assert [e.employee_id for e in world.employees if e.team == org.leadership_team] == [
            leader.employee_id
        ]
        heads = {t.manager_id for t in world.teams if t.org == org.name and not t.is_leadership}
        assert set(reports[leader.employee_id]) == heads
    assert sum(e.manager_id is None for e in world.employees) == len(world.orgs)


def test_managers_are_senior_and_spans_in_range(built: tuple[GeneratorConfig, World]) -> None:
    cfg, world = built
    rank, lo, hi = _rank(cfg), *cfg.org_model.span_of_control
    leaders = {org.leader_id for org in world.orgs}
    by_id = {e.employee_id: e for e in world.employees}
    for manager, direct in world.direct_reports().items():
        assert rank[by_id[manager].job_level] >= rank["L6"]
        if manager not in leaders:
            assert lo <= len(direct) <= hi, (manager, len(direct))


def test_reports_stay_in_their_team_and_org(built: tuple[GeneratorConfig, World]) -> None:
    _, world = built
    by_id = {e.employee_id: e for e in world.employees}
    team_heads = {t.manager_id: t for t in world.teams if not t.is_leadership}
    for emp in world.employees:
        if emp.manager_id is None:
            continue
        manager = by_id[emp.manager_id]
        assert manager.org == emp.org
        if emp.employee_id in team_heads:
            assert team_heads[emp.employee_id].name == emp.team
        else:
            assert manager.team == emp.team


def test_every_chain_ends_at_an_org_leader(built: tuple[GeneratorConfig, World]) -> None:
    _, world = built
    manager_of = {e.employee_id: e.manager_id for e in world.employees}
    leaders = {org.leader_id for org in world.orgs}
    for emp_id in manager_of:
        seen, current = set(), emp_id
        while manager_of[current] is not None:
            assert current not in seen, "cycle in the management tree"
            seen.add(current)
            current = manager_of[current]  # type: ignore[assignment]
        assert current in leaders


def test_shares_are_exact(built: tuple[GeneratorConfig, World]) -> None:
    cfg, world = built
    n = len(world.employees)
    assert Counter(e.job_level for e in world.employees) == Counter(
        quota_counts(cfg.org_model.levels, n)
    )
    assert Counter(e.role_family for e in world.employees) == Counter(
        quota_counts(cfg.org_model.role_families, n)
    )
    weights = {loc.city: loc.weight for loc in cfg.org_model.locations}
    assert Counter(e.location_city for e in world.employees) == Counter(quota_counts(weights, n))


def test_manager_level_preferences(built: tuple[GeneratorConfig, World]) -> None:
    _, world = built  # both presets have enough L6 and L7 for every manager's first choice
    reports = world.direct_reports()
    leaders = {org.leader_id for org in world.orgs}
    by_id = {e.employee_id: e for e in world.employees}
    for manager, direct in reports.items():
        if manager in leaders:
            continue
        second_line = any(d in reports for d in direct)
        assert by_id[manager].job_level == ("L7" if second_line else "L6")


def test_dates_and_statuses(built: tuple[GeneratorConfig, World]) -> None:
    cfg, world = built
    last_day = cfg.window.sim_start - timedelta(days=1)
    for emp in world.employees:
        assert cfg.meta.company_founded <= emp.hire_date <= emp.job_effective_date <= last_day
        assert emp.termination_date is None and emp.ats_candidate_id is None
        assert emp.employment_status in {"active", "leave"}
        assert (emp.employment_status == "leave") == (emp.leave_end_date is not None)
        if emp.leave_end_date is not None:
            assert cfg.window.sim_start <= emp.leave_end_date


def test_ids_follow_hire_order(built: tuple[GeneratorConfig, World]) -> None:
    _, world = built
    ids = [e.employee_id for e in world.employees]
    assert all(re.fullmatch(r"E\d{6}", i) for i in ids)
    assert ids == sorted(ids) and len(set(ids)) == len(ids)
    hires = [e.hire_date for e in world.employees]
    assert hires == sorted(hires)


def test_emails_are_unique_and_reserved(built: tuple[GeneratorConfig, World]) -> None:
    _, world = built
    emails = [e.work_email for e in world.employees]
    assert len(set(emails)) == len(emails)
    assert all(re.fullmatch(r"[a-z0-9]+\.[a-z0-9]+@halcyon\.example", e) for e in emails)


def test_first_year_share_matches_the_steady_state(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "dev")
    world = build_world(cfg, SeedPlan(1602))
    w, start = cfg.workforce, cfg.window.sim_start
    # Independent closed form (years): density exp(-a t) in year one, exp(-a - b (t - 1)) after.
    a = w.growth_annual + w.attrition_annual * w.attrition_first_year_multiplier
    b = w.growth_annual + w.attrition_annual
    span = (start - cfg.meta.company_founded).days / 365
    first = (1 - math.exp(-a)) / a
    rest = math.exp(-a) * (1 - math.exp(-b * (span - 1))) / b
    share = sum((start - e.hire_date).days <= 365 for e in world.employees) / len(world.employees)
    assert share == pytest.approx(first / (first + rest), abs=0.03)  # ~0.163; sd ~0.007 at n=3000
    in_role_long = sum((start - e.job_effective_date).days > 548 for e in world.employees) / len(
        world.employees
    )
    assert 0.5 < in_role_long < 0.75  # ADR-0004 prototype: 0.64 at full


def test_roles_never_change_means_in_role_since_hire(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    raw_config["workforce"].update(
        lateral_move_annual=0.0, location_change_annual=0.0, promotion_annual=0.0
    )
    _, world = _custom(raw_config, write_config)
    assert all(e.job_effective_date == e.hire_date for e in world.employees)


def test_leave_at_start(raw_config: dict[str, Any], write_config: WriteConfig) -> None:
    raw_config["workforce"]["leave_annual"] = 1.0  # p = 1.0 x 75 / 365, about 0.21
    cfg, world = _custom(raw_config, write_config)
    on_leave = [e for e in world.employees if e.employment_status == "leave"]
    assert 30 <= len(on_leave) <= 95  # ~62 expected of 300
    hi = cfg.workforce.leave_duration_days[1]
    last_day = cfg.window.sim_start - timedelta(days=1)
    for emp in on_leave:
        assert emp.leave_end_date is not None
        assert cfg.window.sim_start <= emp.leave_end_date <= last_day + timedelta(days=hi)


def test_no_leave_when_rate_is_zero(raw_config: dict[str, Any], write_config: WriteConfig) -> None:
    raw_config["workforce"]["leave_annual"] = 0.0
    _, world = _custom(raw_config, write_config)
    assert all(e.employment_status == "active" for e in world.employees)


def test_scarce_l7_falls_back_to_l8_then_l6(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    raw_config["org_model"]["levels"] = {
        "L3": 0.12, "L4": 0.30, "L5": 0.28, "L6": 0.26, "L7": 0.0, "L8": 0.04,
    }  # fmt: skip
    cfg, world = _custom(raw_config, write_config)
    rank = _rank(cfg)
    reports = world.direct_reports()
    by_id = {e.employee_id: e for e in world.employees}
    assert all(rank[by_id[m].job_level] >= rank["L6"] for m in reports)
    assert not any(e.job_level == "L7" for e in world.employees)


@pytest.mark.parametrize(
    ("levels", "message"),
    [
        ({"L3": 0.3, "L4": 0.3, "L5": 0.38, "L6": 0.01, "L7": 0.0, "L8": 0.01}, "every manager"),
        ({"L3": 0.3, "L4": 0.3, "L5": 0.2, "L6": 0.1, "L7": 0.1, "L8": 0.0}, "every org leader"),
    ],
)
def test_too_few_senior_levels_is_an_error(
    raw_config: dict[str, Any], write_config: WriteConfig, levels: dict[str, float], message: str
) -> None:
    raw_config["org_model"]["levels"] = levels
    with pytest.raises(WorldBuildError, match=message):
        _custom(raw_config, write_config)


def test_levels_must_include_the_manager_floor(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    raw_config["org_model"]["levels"] = {"L3": 0.2, "L4": 0.3, "L5": 0.3, "L7": 0.15, "L8": 0.05}
    with pytest.raises(WorldBuildError, match="must include L6"):
        _custom(raw_config, write_config)


def test_zero_promotion_floor(raw_config: dict[str, Any], write_config: WriteConfig) -> None:
    raw_config["workforce"]["promotion_min_days_in_level"] = 0  # breaks [0, 0] would be invalid
    cfg, world = _custom(raw_config, write_config)
    last_day = cfg.window.sim_start - timedelta(days=1)
    assert all(e.hire_date <= e.job_effective_date <= last_day for e in world.employees)


def test_teams_too_small_is_an_error(raw_config: dict[str, Any], write_config: WriteConfig) -> None:
    raw_config["presets"]["tiny"]["scale"]["initial_headcount"] = 20
    with pytest.raises(WorldBuildError, match="can't have a manager"):
        _custom(raw_config, write_config)


def test_too_many_teams_is_an_error(raw_config: dict[str, Any], write_config: WriteConfig) -> None:
    raw_config["presets"]["tiny"]["scale"].update(teams_per_org=[100, 100])
    with pytest.raises(WorldBuildError, match="team names exist"):
        _custom(raw_config, write_config)


def test_deterministic_and_golden(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    world = build_world(cfg, SeedPlan(1602))
    assert world.fingerprint() == build_world(cfg, SeedPlan(1602)).fingerprint()
    assert world.fingerprint() != build_world(cfg, SeedPlan(1603)).fingerprint()
    assert world.fingerprint() == GOLDEN_TINY_1602
    assert world.summary() == "3 orgs, 7 teams, 300 employees (41 managers, 2 on leave)"


@pytest.mark.slow
def test_full_world(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "full")
    world = build_world(cfg, SeedPlan(1602))
    rank, lo, hi = _rank(cfg), *cfg.org_model.span_of_control
    leaders = {org.leader_id for org in world.orgs}
    by_id = {e.employee_id: e for e in world.employees}
    assert len(world.employees) == 25_000
    for manager, direct in world.direct_reports().items():
        assert rank[by_id[manager].job_level] >= rank["L6"]
        assert manager in leaders or lo <= len(direct) <= hi
