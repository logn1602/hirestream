from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

from hirestream.generator.calendar import build_calendar
from hirestream.generator.config import GeneratorConfig, load_config
from hirestream.generator.requisitions import ACTIVE, ReqEvent, Requisitions, title_for
from hirestream.generator.sampling import truncated_pareto_mean
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.workforce import Workforce
from hirestream.generator.world import Employee, Org, Team, World, build_world

WriteConfig = Callable[[dict[str, Any]], Path]
Check = Callable[[Requisitions, Workforce, date, list[ReqEvent]], None]


def _setup(cfg: GeneratorConfig, seed: int = 1602) -> tuple[Workforce, Requisitions]:
    plan = SeedPlan(seed)
    wf = Workforce(
        build_world(cfg, plan),
        cfg,
        plan.rng("workforce"),
        build_calendar(cfg)["workforce.reorg"].start,
    )
    return wf, Requisitions(cfg, plan.rng("requisitions"), wf)


def _run(
    cfg: GeneratorConfig, seed: int = 1602, check: Check | None = None, until: date | None = None
) -> tuple[Workforce, Requisitions]:
    wf, rq = _setup(cfg, seed)
    day = cfg.window.sim_start
    while day <= (until or cfg.window.sim_end):
        events = rq.step(day, wf.step(day))
        if check is not None:
            check(rq, wf, day, events)
        day += timedelta(days=1)
    return wf, rq


def assert_req_invariants(rq: Requisitions, wf: Workforce, day: date) -> None:
    teams = {t.name: t for t in wf.world.teams}
    active = set()
    for req in rq.reqs.values():
        if req.status in ACTIVE:
            active.add(req.req_id)
            team = teams[req.team]
            assert team.manager_id is not None and not team.is_leadership
            assert req.org == team.org
            manager = wf.employee(req.hiring_manager_id)
            assert manager.employment_status != "terminated" and manager.team == req.team
            if req.recruiter_id is not None:
                recruiter = wf.employee(req.recruiter_id)
                assert recruiter.role_family == "recruiting"
                assert recruiter.employment_status != "terminated"
            assert 0 <= req.seats_open <= req.headcount
            assert req.closed_on is None and req.close_reason is None
            if not req.is_evergreen:
                assert req.expires_on is not None and day < req.expires_on
        else:
            assert not req.is_evergreen or req.close_reason == "team_dissolved"
            assert (
                req.closed_on is not None and req.closed_on <= day and req.close_reason is not None
            )
    assert active == {r.req_id for r in rq.active_reqs()}


def _daily(rq: Requisitions, wf: Workforce, day: date, _: list[ReqEvent]) -> None:
    assert_req_invariants(rq, wf, day)


def test_invariants_hold_every_day(base_config_path: Path) -> None:
    _run(load_config(base_config_path, "tiny"), check=_daily)
    wf, rq = _run(load_config(base_config_path, "dev"))
    assert_req_invariants(rq, wf, load_config(base_config_path, "dev").window.sim_end)


def test_go_live_pipeline(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "dev")
    _, rq = _setup(cfg)
    start = cfg.window.sim_start
    evergreen = [r for r in rq.active_reqs() if r.is_evergreen]
    others = [r for r in rq.active_reqs() if not r.is_evergreen]
    assert len(evergreen) == 2  # max(1, round(0.8 x 3000 / 1000))
    assert all(start - timedelta(days=180) <= r.opened_on < start for r in evergreen)
    assert all(start - timedelta(days=45) <= r.opened_on < start for r in others)
    seats = sum(r.seats_open for r in others)
    assert 55 <= seats <= 70  # ADR-0006: ~66 at dev before go-live cancellations are dropped
    assert {r.source for r in others} == {"backfill", "growth"}


def test_ids_follow_open_order(base_config_path: Path) -> None:
    _, rq = _run(load_config(base_config_path, "dev"))
    ids = list(rq.reqs)
    assert ids == sorted(ids) == [f"R{i:06d}" for i in range(1, len(ids) + 1)]
    opened = [rq.reqs[i].opened_on for i in ids]
    assert opened == sorted(opened)


def test_backfills_follow_departures(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "dev")
    wf, rq = _run(cfg)
    teams = {t.name: t for t in wf.world.teams}
    by_leaver = {r.backfill_for: r for r in rq.reqs.values() if r.backfill_for is not None}
    cutoff = cfg.window.sim_end - timedelta(days=30)  # later backfills may open after the window
    eligible = [
        e
        for e in wf.events
        if e.kind == "termination"
        and e.day <= cutoff
        and not teams[wf.employee(e.employee_id).team].is_leadership
    ]
    share = sum(e.employee_id in by_leaver for e in eligible) / len(eligible)
    assert share == pytest.approx(0.70, abs=0.07)
    for event in eligible:
        req = by_leaver.get(event.employee_id)
        if req is None:
            continue
        leaver = wf.employee(event.employee_id)
        assert 0 <= (req.opened_on - event.day).days <= 30
        assert (req.team, req.role_family, req.job_level, req.location_city, req.headcount) == (
            leaver.team, leaver.role_family, leaver.job_level, leaver.location_city, 1
        )  # fmt: skip
    leaders = {e.employee_id for e in wf.events if e.kind == "termination"} & {
        emp.employee_id for emp in wf.world.employees if teams[emp.team].is_leadership
    }
    assert not leaders & set(by_leaver)


def test_growth_plan_opens_exactly_the_gap(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "dev")
    _, rq = _run(cfg)
    h0 = cfg.scale.initial_headcount
    opened: defaultdict[tuple[int, int], int] = defaultdict(int)
    for req in rq.reqs.values():
        if req.source == "growth" and req.opened_on >= cfg.window.sim_start:
            opened[(req.opened_on.year, req.opened_on.month)] += req.headcount
    for plan in rq.plans:
        following = (plan.day.replace(day=1) + timedelta(days=32)).replace(day=1)
        assert plan.target == pytest.approx(
            h0 * 1.05 ** ((following - cfg.window.sim_start).days / 365)
        )
        assert plan.seats == max(0, round(plan.target - plan.projected))
        assert opened[(plan.day.year, plan.day.month)] == plan.seats
    assert any(plan.seats > 0 for plan in rq.plans)


def test_headcount_internal_share_and_popularity(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "dev")
    _, rq = _run(cfg)
    reqs = list(rq.reqs.values())
    assert {r.headcount for r in reqs if r.source == "backfill"} == {1}
    growth = Counter(r.headcount for r in reqs if r.source == "growth")
    assert set(growth) <= {1, 2, 3} and growth[1] / sum(growth.values()) > 0.75
    regular = [r for r in reqs if not r.is_evergreen]
    assert sum(r.is_internal_only for r in regular) / len(regular) == pytest.approx(0.10, abs=0.04)
    top = 200 / truncated_pareto_mean(1.2, 200)
    assert max(r.popularity for r in regular) <= top
    for req in (r for r in reqs if r.is_evergreen):
        assert 25 / truncated_pareto_mean(1.2, 200) <= req.popularity <= 25 * top
        assert not req.is_internal_only and 20 <= req.headcount <= 50


def test_every_req_goes_on_hold_once(raw_config: dict[str, Any], write_config: WriteConfig) -> None:
    raw_config["requisitions"].update(on_hold_probability=1.0, cancel_probability=0.0)
    cfg = load_config(write_config(raw_config), "dev")
    _, rq = _run(cfg)
    holds: defaultdict[str, list[tuple[str, date]]] = defaultdict(list)
    for event in rq.events:
        if event.kind in ("on_hold", "resumed"):
            holds[event.req_id].append((event.kind, event.day))
    assert len(holds) > 100
    for req_id, seq in holds.items():
        kinds = [k for k, _ in seq]
        assert kinds in (["on_hold"], ["on_hold", "resumed"]), req_id
        if kinds == ["on_hold", "resumed"]:
            assert 7 <= (seq[1][1] - seq[0][1]).days <= 30


def test_random_cancellations(raw_config: dict[str, Any], write_config: WriteConfig) -> None:
    raw_config["requisitions"].update(on_hold_probability=0.0, cancel_probability=1.0)
    cfg = load_config(write_config(raw_config), "dev")
    _, rq = _run(cfg)
    for req in (
        r for r in rq.reqs.values() if not r.is_evergreen and r.opened_on >= cfg.window.sim_start
    ):
        assert req.cancel_on is not None
        if req.cancel_on <= cfg.window.sim_end:
            assert (req.status, req.close_reason, req.closed_on) == (
                "cancelled",
                "cancelled",
                req.cancel_on,
            )
    assert not [e for e in rq.events if e.kind == "expired"]


def test_unfilled_reqs_expire_on_time(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    raw_config["requisitions"].update(on_hold_probability=0.0, cancel_probability=0.0)
    cfg = load_config(write_config(raw_config), "dev")
    _, rq = _run(cfg)
    expired = [r for r in rq.reqs.values() if r.close_reason == "expired"]
    assert len(expired) > 100
    horizon = cfg.window.sim_end - timedelta(days=180)
    for req in rq.reqs.values():
        due = not req.is_evergreen and req.opened_on <= horizon
        if due and req.close_reason != "team_dissolved":
            assert req.close_reason == "expired"
            assert req.closed_on == req.opened_on + timedelta(days=180)


def test_accepting_offers_fills_and_reopens(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    wf, rq = _run(cfg, until=date(2025, 1, 10))
    req = next(r for r in rq.open_reqs() if not r.is_evergreen)
    for _ in range(req.headcount):
        rq.record_accept(req.req_id, date(2025, 1, 10))
    assert (req.status, req.close_reason, req.closed_on, req.seats_open) == (
        "filled", "filled", date(2025, 1, 10), 0
    )  # fmt: skip
    assert req.req_id not in {r.req_id for r in rq.active_reqs()}
    with pytest.raises(ValueError, match="no open seat"):
        rq.record_accept(req.req_id, date(2025, 1, 10))
    rq.reopen_seat(req.req_id, date(2025, 1, 12))  # a no-start gives the seat back
    assert (req.status, req.seats_open, req.closed_on) == ("open", 1, None)
    assert_req_invariants(rq, wf, date(2025, 1, 12))


def test_evergreen_seats_refill_monthly(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    wf, rq = _setup(cfg)
    evergreen = next(r for r in rq.active_reqs() if r.is_evergreen)
    day = cfg.window.sim_start
    while day <= date(2025, 2, 1):
        rq.step(day, wf.step(day))
        if day == date(2025, 1, 15):
            for _ in range(3):
                rq.record_accept(evergreen.req_id, day)
            assert evergreen.seats_open == evergreen.headcount - 3 and evergreen.status == "open"
        day += timedelta(days=1)
    assert evergreen.seats_open == evergreen.headcount
    assert ReqEvent(date(2025, 2, 1), evergreen.req_id, "replenished") in rq.events


def test_reassignments_under_heavy_churn(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    raw_config["presets"]["tiny"]["window"]["sim_end"] = "2025-12-31"
    raw_config["workforce"]["attrition_annual"] = 1.0
    cfg = load_config(write_config(raw_config), "tiny")
    _, rq = _run(cfg, check=_daily)
    details = Counter(e.detail for e in rq.events if e.kind == "reassigned")
    assert details["hiring_manager"] > 0 and details["recruiter"] > 0


def test_reqs_follow_their_team_in_a_reorg(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "dev")
    wf, rq = _run(cfg)
    reorg_day = build_calendar(cfg)["workforce.reorg"].start
    moved = [e for e in rq.events if e.kind == "moved"]
    assert moved and {e.day for e in moved} == {reorg_day}
    teams = {t.name: t for t in wf.world.teams}
    for event in moved:
        req = rq.reqs[event.req_id]
        assert event.detail == teams[req.team].org


def test_requisitions_never_move_the_workforce(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    plan = SeedPlan(1602)
    alone = Workforce(
        build_world(cfg, plan),
        cfg,
        plan.rng("workforce"),
        build_calendar(cfg)["workforce.reorg"].start,
    )
    day = cfg.window.sim_start
    while day <= cfg.window.sim_end:
        alone.step(day)
        day += timedelta(days=1)
    together, _ = _run(cfg)
    assert together.events == alone.events


def test_same_seed_same_reqs(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    assert _run(cfg)[1].events == _run(cfg)[1].events
    assert _run(cfg)[1].events != _run(cfg, seed=1603)[1].events


@pytest.mark.parametrize(
    ("role_family", "level", "title"),
    [
        ("software_engineering", "L5", "Senior Software Engineer"),
        ("recruiting", "L4", "Recruiter"),
        ("data_science", "L3", "Associate Data Scientist"),
        ("customer_success", "L9", "L9 Customer Success"),
    ],
)
def test_titles(role_family: str, level: str, title: str) -> None:
    assert title_for(role_family, level) == title


def test_summary_matches_the_event_log(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "dev")
    _, rq = _run(cfg)
    summary = rq.summary()
    kinds = Counter(e.kind for e in rq.events)
    assert summary["filled"] == kinds["filled"] and summary["expired"] == kinds["expired"]
    assert summary["cancelled"] == kinds["cancelled"]
    assert summary["go_live"] + summary["opened"] == len(rq.reqs)
    assert summary["backfill"] + summary["growth"] == summary["opened"]
    assert summary["open_at_end"] == len(rq.active_reqs())


def test_reopening_a_seat_on_an_open_req(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    _, rq = _run(cfg, until=date(2025, 1, 5))
    evergreen = next(r for r in rq.active_reqs() if r.is_evergreen)
    rq.record_accept(evergreen.req_id, date(2025, 1, 5))
    rq.record_accept(evergreen.req_id, date(2025, 1, 5))
    rq.reopen_seat(evergreen.req_id, date(2025, 1, 6))
    assert evergreen.seats_open == evergreen.headcount - 1
    rq.reopen_seat(evergreen.req_id, date(2025, 1, 6))
    rq.reopen_seat(evergreen.req_id, date(2025, 1, 6))  # never above the headcount
    assert evergreen.seats_open == evergreen.headcount


def test_reqs_wait_for_a_recruiter(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    plan = SeedPlan(1602)
    world = build_world(cfg, plan)
    back = cfg.window.sim_start + timedelta(days=5)
    for emp in world.employees:  # every recruiter is on leave at go-live
        if emp.role_family == "recruiting":
            emp.employment_status, emp.leave_end_date = "leave", back
    wf = Workforce(world, cfg, plan.rng("workforce"), build_calendar(cfg)["workforce.reorg"].start)
    rq = Requisitions(cfg, plan.rng("requisitions"), wf)
    assert all(r.recruiter_id is None for r in rq.active_reqs())
    day = cfg.window.sim_start
    while day <= back:
        rq.step(day, wf.step(day))
        day += timedelta(days=1)
    assert all(r.recruiter_id is not None for r in rq.active_reqs())
    assert_req_invariants(rq, wf, back)


def _one_team_world() -> World:
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


def test_reqs_close_when_their_team_dissolves(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    raw_config["presets"]["tiny"]["window"]["sim_end"] = "2029-12-31"
    raw_config["workforce"]["attrition_annual"] = 1.0
    cfg = load_config(write_config(raw_config), "tiny")
    world = _one_team_world()
    wf = Workforce(world, cfg, SeedPlan(1602).rng("workforce"), date(2030, 1, 1))
    rq = Requisitions(cfg, SeedPlan(1602).rng("requisitions"), wf)
    day = cfg.window.sim_start
    while day <= cfg.window.sim_end:
        rq.step(day, wf.step(day))
        assert_req_invariants(rq, wf, day)
        day += timedelta(days=1)
    assert world.teams[1].manager_id is None  # everyone in the team left
    reasons = Counter(r.close_reason for r in rq.reqs.values())
    assert reasons["team_dissolved"] >= 1  # at least the evergreen req
    assert not rq.active_reqs()


def test_a_vacancy_opens_a_backfill(raw_config: dict[str, Any], write_config: WriteConfig) -> None:
    raw_config["workforce"].update(backfill_probability=1.0, backfill_open_delay_days=[3, 3])
    cfg = load_config(write_config(raw_config), "tiny")
    wf, rq = _setup(cfg)
    team = next(t for t in wf.world.teams if not t.is_leadership)
    day = cfg.window.sim_start
    rq.vacancy(
        day, team=team.name, role_family="design", job_level="L5", location_city="London",
        manager_hint=team.manager_id, leaver_id="E000042",
    )  # fmt: skip
    for offset in range(4):
        today = day + timedelta(days=offset)
        rq.step(today, wf.step(today))
    req = next(r for r in rq.reqs.values() if r.backfill_for == "E000042")
    assert (req.opened_on, req.team, req.role_family, req.job_level, req.location_city) == (
        day + timedelta(days=3), team.name, "design", "L5", "London"
    )  # fmt: skip


def test_recruiter_pool_follows_hires_and_transfers(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    wf, rq = _setup(cfg)
    day = cfg.window.sim_start
    team = next(t for t in wf.world.teams if not t.is_leadership)
    new = wf.hire(
        day, first_name="Rae", last_name="Kim", team=team.name, role_family="recruiting",
        job_level="L4", location_city="Boston", manager_id=None, ats_candidate_id="C1",
    )  # fmt: skip
    rq.step(day, wf.events[-1:])
    assert new.employee_id in rq._load and new.employee_id in rq._available
    owner = next(r.recruiter_id for r in rq.active_reqs() if r.recruiter_id is not None)
    assert owner is not None
    other = next(
        t for t in wf.world.teams if not t.is_leadership and t.name != wf.employee(owner).team
    )
    assert wf.transfer(
        day, owner, team=other.name, role_family="sales", job_level="L5", manager_id=None
    )
    rq.step(day, wf.events[-1:])
    assert owner not in rq._load
    assert all(r.recruiter_id != owner for r in rq.active_reqs())
    assert_req_invariants(rq, wf, day)
