from collections import Counter
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from hirestream.generator.calendar import build_calendar
from hirestream.generator.config import GeneratorConfig, load_config
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.workforce import Workforce, WorkforceEvent
from hirestream.generator.world import Employee, Org, Team, World, build_world

WriteConfig = Callable[[dict[str, Any]], Path]
Check = Callable[[Workforce, GeneratorConfig, date, list[WorkforceEvent]], None]


def _run(cfg: GeneratorConfig, seed: int = 1602, check: Check | None = None) -> Workforce:
    plan = SeedPlan(seed)
    workforce = Workforce(
        build_world(cfg, plan),
        cfg,
        plan.rng("workforce"),
        build_calendar(cfg)["workforce.reorg"].start,
    )
    day = cfg.window.sim_start
    while day <= cfg.window.sim_end:
        events = workforce.step(day)
        if check is not None:
            check(workforce, cfg, day, events)
        day += timedelta(days=1)
    return workforce


def assert_invariants(world: World, cfg: GeneratorConfig) -> None:
    """The org rules from SPEC §6.3 / ADR-0005 that every simulated day must satisfy."""
    by_id = {e.employee_id: e for e in world.employees}
    alive = {i for i, e in by_id.items() if e.employment_status != "terminated"}
    rank = {level: k for k, level in enumerate(cfg.org_model.levels)}
    teams = {t.name: t for t in world.teams}
    leaders = {org.name: org.leader_id for org in world.orgs}
    for org in world.orgs:
        leader = by_id[org.leader_id]
        assert org.leader_id in alive
        assert (leader.job_level, leader.team, leader.manager_id, leader.org) == (
            list(cfg.org_model.levels)[-1],
            org.leadership_team,
            None,
            org.name,
        )
    for team in world.teams:
        if team.is_leadership or team.manager_id is None:
            continue
        head = by_id[team.manager_id]
        assert team.manager_id in alive
        assert (head.team, head.org, head.manager_id) == (team.name, team.org, leaders[team.org])
        assert rank[head.job_level] >= rank["L6"]
    members = Counter(by_id[i].team for i in alive)
    for team in world.teams:
        if team.manager_id is None:
            assert members[team.name] == 0
    for emp_id in alive:
        emp = by_id[emp_id]
        assert emp.org == teams[emp.team].org
        if emp.manager_id is None:
            assert leaders[emp.org] == emp_id
            continue
        manager = by_id[emp.manager_id]
        assert emp.manager_id in alive, f"{emp_id} reports to departed {emp.manager_id}"
        assert rank[manager.job_level] >= rank["L6"]
        if teams[emp.team].manager_id == emp_id:
            assert emp.manager_id == leaders[emp.org]
        else:
            assert manager.team == emp.team
        seen, current = set(), emp_id
        while by_id[current].manager_id is not None:
            assert current not in seen, "cycle in the management tree"
            seen.add(current)
            current = by_id[current].manager_id  # type: ignore[assignment]
        assert current == leaders[emp.org]


def _daily_invariants(
    wf: Workforce, cfg: GeneratorConfig, day: date, _: list[WorkforceEvent]
) -> None:
    assert_invariants(wf.world, cfg)


def test_invariants_hold_every_day(base_config_path: Path) -> None:
    _run(load_config(base_config_path, "tiny"), check=_daily_invariants)


def test_invariants_survive_heavy_churn(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    # Annual rates are capped at 1.0 by the config, so stretch tiny to a full year instead.
    raw_config["presets"]["tiny"]["window"]["sim_end"] = "2025-12-31"
    raw_config["workforce"].update(
        attrition_annual=1.0,
        lateral_move_annual=1.0,
        manager_change_annual=1.0,
        promotion_annual=1.0,
    )
    cfg = load_config(write_config(raw_config), "tiny")
    leaders_before = [org.leader_id for org in build_world(cfg, SeedPlan(1602)).orgs]
    wf = _run(cfg, check=_daily_invariants)
    assert [org.leader_id for org in wf.world.orgs] != leaders_before  # an org leader left
    kinds = Counter((e.kind, e.cause) for e in wf.events)
    assert kinds[("succession", "succession")] > 0
    assert kinds[("promotion", "succession")] > 0  # an heir was raised to L6 or L8


def test_rates_track_the_config(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "dev")
    wf = _run(cfg)
    counts, n = wf.event_counts(), cfg.scale.initial_headcount
    # 20-seed means at dev (no hires until T1.6, so headcount shrinks): 350, 206, 54, 29.
    assert 0.10 < counts["termination"] / n < 0.14
    assert 0.05 < counts["promotion"] / n < 0.09
    assert 35 <= counts["leave_start"] <= 80
    assert 12 <= counts["location_change"] <= 50
    assert_invariants(wf.world, cfg)


def test_reorg_moves_one_whole_team(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    reorg_day = build_calendar(cfg)["workforce.reorg"].start
    before = {t.name: t.org for t in build_world(cfg, SeedPlan(1602)).teams}
    wf = _run(cfg)
    moved = [e for e in wf.events if e.kind == "reorg_move"]
    assert moved and {e.day for e in moved} == {reorg_day}
    teams = {wf.employee(e.employee_id).team for e in moved}
    assert len(teams) == 1
    team = next(t for t in wf.world.teams if t.name in teams)
    assert team.org != before[team.name]
    alive_members = [
        e for e in wf.world.employees if e.team == team.name and e.employment_status != "terminated"
    ]
    assert all(e.org == team.org for e in alive_members)


def test_every_change_sets_job_effective_date(base_config_path: Path) -> None:
    def check(wf: Workforce, cfg: GeneratorConfig, day: date, events: list[WorkforceEvent]) -> None:
        for event in events:
            assert wf.employee(event.employee_id).job_effective_date == day

    _run(load_config(base_config_path, "dev"), check=check)


def test_at_most_one_hazard_per_employee_per_day(base_config_path: Path) -> None:
    wf = _run(load_config(base_config_path, "dev"))
    hazards = Counter((e.day, e.employee_id) for e in wf.events if e.cause == "hazard")
    assert max(hazards.values()) == 1


def test_terminations_keep_the_last_record(base_config_path: Path) -> None:
    wf = _run(load_config(base_config_path, "dev"))
    for event in (e for e in wf.events if e.kind == "termination"):
        emp = wf.employee(event.employee_id)
        assert emp.employment_status == "terminated"
        assert emp.termination_date == emp.job_effective_date == event.day
        assert emp.leave_end_date is None


def test_leave_starts_and_ends(raw_config: dict[str, Any], write_config: WriteConfig) -> None:
    raw_config["workforce"].update(leave_annual=1.0, leave_duration_days=[5, 10])
    cfg = load_config(write_config(raw_config), "tiny")
    expected_back: dict[str, date] = {}
    returns: list[str] = []

    def check(wf: Workforce, cfg: GeneratorConfig, day: date, events: list[WorkforceEvent]) -> None:
        for event in events:
            emp = wf.employee(event.employee_id)
            if event.kind == "leave_start":
                assert emp.employment_status == "leave" and emp.leave_end_date is not None
                assert 5 <= (emp.leave_end_date - day).days <= 10
                expected_back[event.employee_id] = emp.leave_end_date
            elif event.kind == "leave_end":
                assert emp.employment_status == "active" and emp.leave_end_date is None
                # Initial leavers (ADR-0004) return without a leave_start in the window.
                assert expected_back.pop(event.employee_id, day) == day
                returns.append(event.employee_id)

    _run(cfg, check=check)
    assert len(returns) > 20


def test_no_hazard_promotions_before_the_floor(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    raw_config["workforce"]["promotion_min_days_in_level"] = 100_000
    wf = _run(load_config(write_config(raw_config), "dev"))
    assert not [e for e in wf.events if e.kind == "promotion" and e.cause == "hazard"]


def test_same_seed_same_events(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    assert _run(cfg).events == _run(cfg).events
    assert _run(cfg).events != _run(cfg, seed=1603).events


def test_event_counts_follow_a_fixed_order(base_config_path: Path) -> None:
    counts = _run(load_config(base_config_path, "tiny")).event_counts()
    assert list(counts) == [
        k for k in ("termination", "leave_start", "promotion", "lateral_move", "manager_change",
                    "location_change", "leave_end", "reorg_move", "succession") if k in counts
    ]  # fmt: skip
    assert all(v > 0 for v in counts.values())


def _edge_world() -> World:
    """One org, one real team of three: small enough that every edge case happens."""
    hired = date(2010, 1, 4)

    def person(emp_id: str, team: str, level: str, manager: str | None) -> Employee:
        return Employee(
            employee_id=emp_id,
            first_name="Test",
            last_name=emp_id,
            work_email=f"{emp_id.lower()}@halcyon.example",
            hire_date=hired,
            org="Commerce",
            team=team,
            role_family="operations",
            job_level=level,
            manager_id=manager,
            location_city="Seattle",
            employment_status="active",
            termination_date=None,
            job_effective_date=hired,
            ats_candidate_id=None,
        )

    return World(
        orgs=[Org("Commerce", "E000001", "Commerce Leadership")],
        teams=[
            Team("Commerce Leadership", "Commerce", "E000001", is_leadership=True),
            Team("Payments Platform", "Commerce", "E000002"),
        ],
        employees=[
            person("E000001", "Commerce Leadership", "L8", None),
            person("E000002", "Payments Platform", "L6", "E000001"),
            person("E000003", "Payments Platform", "L4", "E000002"),
            person("E000004", "Payments Platform", "L3", "E000002"),
        ],
    )


def test_degenerate_org_keeps_its_invariants(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    raw_config["presets"]["tiny"]["window"]["sim_end"] = "2029-12-31"  # five years
    raw_config["org_model"]["locations"] = [
        {"city": "Seattle", "country": "US", "tz": "America/Los_Angeles", "weight": 1.0}
    ]
    raw_config["workforce"].update(
        attrition_annual=1.0,
        lateral_move_annual=1.0,  # no other team to move to
        location_change_annual=1.0,  # no other city to move to
        leave_annual=1.0,
        leave_duration_days=[0, 0],  # zero-length leave never starts
    )
    cfg = load_config(write_config(raw_config), "tiny")
    world = _edge_world()
    wf = Workforce(
        world, cfg, SeedPlan(1602).rng("workforce"), build_calendar(cfg)["workforce.reorg"].start
    )
    day = cfg.window.sim_start
    while day <= cfg.window.sim_end:
        wf.step(day)
        assert_invariants(world, cfg)
        day += timedelta(days=1)
    kinds = Counter(e.kind for e in wf.events)
    assert not kinds["lateral_move"] and not kinds["location_change"] and not kinds["leave_start"]
    assert not kinds["reorg_move"]  # a single org has nowhere to move a team to
    assert kinds["succession"] > 0
    team = next(t for t in world.teams if not t.is_leadership)
    assert team.manager_id is None  # everyone in the team left
    leader = wf.employee(world.orgs[0].leader_id)
    assert leader.employment_status == "active"  # the last org leader has nobody to hand over to


def test_active_mask_and_days_in_role(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    plan = SeedPlan(1602)
    world = build_world(cfg, plan)
    wf = Workforce(world, cfg, plan.rng("workforce"), build_calendar(cfg)["workforce.reorg"].start)
    start = cfg.window.sim_start
    assert list(wf.active_mask()) == [e.employment_status == "active" for e in world.employees]
    assert list(wf.days_in_role(start)) == [
        (start - e.job_effective_date).days for e in world.employees
    ]
    roles_before = wf.days_in_role(start)
    day = start
    promoted: set[str] = set()
    while day <= cfg.window.sim_end:
        promoted |= {e.employee_id for e in wf.step(day) if e.kind == "promotion"}
        day += timedelta(days=1)
    end = cfg.window.sim_end
    index = {e.employee_id: i for i, e in enumerate(world.employees)}
    elapsed = (end - start).days
    assert promoted
    for emp_id in promoted:  # a promotion restarts the role clock
        assert wf.days_in_role(end)[index[emp_id]] < roles_before[index[emp_id]] + elapsed
    assert list(wf.active_mask()) == [e.employment_status == "active" for e in world.employees]
