import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest
import yaml

from hirestream.generator.ats import (
    ATS,
    BEFORE_ONSITE,
    STAGES,
    LeadTimeInterviews,
    StageChange,
    accept_probability,
    ht4_bucket,
)
from hirestream.generator.calendar import build_calendar
from hirestream.generator.candidates import CandidateRegistry
from hirestream.generator.config import GeneratorConfig, load_config
from hirestream.generator.events import CountingSink
from hirestream.generator.jobboard import JobBoard
from hirestream.generator.people import fictional_phone
from hirestream.generator.requisitions import Requisitions
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.workforce import Workforce
from hirestream.generator.world import build_world

FINAL = {"rejected", "withdrawn", "hired", "offer_declined", "no_start"}


@dataclass
class Run:
    cfg: GeneratorConfig
    wf: Workforce
    rq: Requisitions
    ats: ATS
    registry: CandidateRegistry
    views: float = 0.0  # summed expected external views over the window
    closed: dict[str, date] = field(default_factory=dict)


def _run(cfg: GeneratorConfig, seed: int = 1602) -> Run:
    plan = SeedPlan(seed)
    cal = build_calendar(cfg)
    wf = Workforce(build_world(cfg, plan), cfg, plan.rng("workforce"), cal["workforce.reorg"].start)
    rq = Requisitions(cfg, plan.rng("requisitions"), wf)
    registry = CandidateRegistry(cfg.ats.reapply_probability)
    jb = JobBoard(cfg, cal, plan.rng("jobboard"), wf, rq, registry)
    ats = ATS(cfg, plan.rng("ats"), plan.faker_seed("ats"), wf, rq, registry)
    run = Run(cfg, wf, rq, ats, registry)
    sink = CountingSink()
    day = cfg.window.sim_start
    while day <= cfg.window.sim_end:
        changes = wf.step(day)
        req_events = rq.step(day, changes)
        submissions = jb.step(day, sink)
        run.views += sum(jb.expected_views.values())
        mark = len(wf.events)
        ats.step(day, submissions, jb.expected_views, changes, req_events)
        rq.follow(day, wf.events[mark:])
        day += timedelta(days=1)
    for event in rq.events:
        if event.kind in ("filled", "cancelled", "expired"):
            run.closed[event.req_id] = event.day
    return run


@pytest.fixture(scope="module")
def run(base_config_path: Path) -> Run:
    return _run(load_config(base_config_path, "tiny"))


def _history(ats: ATS) -> dict[str, list[StageChange]]:
    history: dict[str, list[StageChange]] = defaultdict(list)
    for change in ats.changes:
        history[change.application_id].append(change)
    return history


def test_stage_changes_follow_the_state_machine(run: Run) -> None:
    ids = [c.change_id for c in run.ats.changes]
    assert ids == list(range(1, len(ids) + 1))
    for app_id, changes in _history(run.ats).items():
        first, *rest = changes
        opening = (
            first.from_stage,
            first.to_stage,
            first.from_status,
            first.to_status,
            first.reason,
        )
        assert opening == (None, "applied", None, "active", "submitted")
        status, stage = "active", "applied"
        for change in rest:
            assert (change.from_stage, change.from_status) == (stage, status), app_id
            if change.to_status == "active":  # an advance to the next stage
                assert status == "active"
                assert STAGES.index(change.to_stage) == STAGES.index(stage) + 1
            else:  # a close keeps the stage
                assert change.to_stage == stage and change.to_status in FINAL
                assert status == "active" or (status, change.to_status) == ("hired", "no_start")
            status, stage = change.to_status, change.to_stage
        app = run.ats.applications[app_id]
        assert (app.status, app.stage) == (status, stage)


def test_offers_match_their_applications(run: Run) -> None:
    assert run.ats.offers
    for offer in run.ats.offers.values():
        app = run.ats.applications[offer.application_id]
        assert app.stage == "offer"
        if offer.status == "accepted":
            assert app.status in ("hired", "no_start")
            assert offer.start_date is not None and offer.decided_ms is not None
        elif offer.status == "declined":
            assert app.status == "offer_declined"
        elif offer.status == "rescinded":
            assert app.status in ("rejected", "withdrawn")


def test_no_req_takes_more_hires_than_seats(run: Run) -> None:
    taken: Counter[str] = Counter()
    for offer in run.ats.offers.values():
        app = run.ats.applications[offer.application_id]
        if offer.status == "accepted" and app.status == "hired":
            taken[app.req_id] += 1
    for req_id, n in taken.items():
        req = run.rq.reqs[req_id]
        if not req.is_evergreen:
            assert n <= req.headcount


def test_hires_join_the_workforce_on_their_start_date(run: Run) -> None:
    by_candidate = {e.ats_candidate_id: e for e in run.wf.world.employees if e.ats_candidate_id}
    transfers = {(e.employee_id, e.day) for e in run.wf.events if e.kind == "transfer"}
    started = 0
    for offer in run.ats.offers.values():
        app = run.ats.applications[offer.application_id]
        if offer.status != "accepted" or app.status != "hired" or offer.start_date is None:
            continue
        if offer.start_date > run.cfg.window.sim_end:
            continue
        started += 1
        if app.employee_id is None:
            emp = by_candidate[app.candidate_id]
            req = run.rq.reqs[app.req_id]
            assert emp.hire_date == offer.start_date
            assert (emp.role_family, emp.job_level) == (req.role_family, req.job_level)
        else:
            assert (app.employee_id, offer.start_date) in transfers
    assert started > 5
    assert sum(run.ats.truth.hires.values()) == started


def test_closed_reqs_reject_early_applications(run: Run) -> None:
    for app in run.ats.applications.values():
        closed = run.closed.get(app.req_id)
        if app.status_reason in ("position_filled", "req_cancelled") and app.stage in BEFORE_ONSITE:
            assert closed is not None
            rejected = max(
                c.changed_ms for c in run.ats.changes if c.application_id == app.application_id
            )
            gap = (date.fromtimestamp(rejected / 1000) - closed).days
            assert -1 <= gap <= 6  # 0-5 days, give or take a timezone
    active_on_closed = [
        a for a in run.ats.applications.values()
        if a.status == "active" and a.stage in BEFORE_ONSITE and a.req_id in run.closed
        and run.closed[a.req_id] + timedelta(days=5) < run.cfg.window.sim_end
    ]  # fmt: skip
    assert not active_on_closed


def test_referrals_pass_the_first_screen_more_often(run: Run) -> None:
    gate = run.ats.truth.first_gate

    def pass_rate(channel: str) -> float:
        total = sum(gate[(channel, o)] for o in ("advance", "withdraw", "reject"))
        return gate[(channel, "advance")] / total

    ratio = pass_rate("referral") / pass_rate("career_site")
    assert 1.5 <= ratio <= 2.1  # SPEC §13.2 HT2 band (expected 1.8)


def test_direct_applications_track_expected_views(run: Run) -> None:
    rates = run.cfg.ats.direct_apps_per_1000_external_views
    for channel in ("referral", "sourced", "agency"):
        expected = run.views * rates[channel] / 1000
        observed = run.ats.truth.applications[channel]
        assert abs(observed - expected) <= 4 * np.sqrt(expected), channel


def test_candidates_are_filled_in(run: Run) -> None:
    emails = [c.email for c in run.registry.candidates.values() if not c.is_internal]
    assert len(emails) == len(set(emails))
    for cand in run.registry.candidates.values():
        assert cand.first_name and cand.last_name and cand.created_ms > 0
        assert cand.location_country in {"US", "GB", "IN"}
        if cand.is_internal:
            assert cand.employee_id is not None
            assert cand.email == run.wf.employee(cand.employee_id).work_email
        else:
            assert re.fullmatch(r"[a-z0-9.]+@example\.(com|net|org)", cand.email)
        assert re.fullmatch(r"\+1-\d{3}-555-01\d\d|\+44 7700 900\d{3}|\+00 0\d{9}", cand.phone)


def test_internal_applicants_who_leave_are_withdrawn(run: Run) -> None:
    for app in run.ats.applications.values():
        left = (
            app.employee_id and run.wf.employee(app.employee_id).employment_status == "terminated"
        )
        if left:
            assert app.status != "active"


def test_same_seed_same_ats(base_config_path: Path, tmp_path: Path) -> None:
    raw = yaml.safe_load(base_config_path.read_text())
    raw["jobboard"]["external"]["base_daily_views_per_open_req"] = 3.0
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw))
    cfg = load_config(path, "tiny")
    assert _run(cfg).ats.changes == _run(cfg).ats.changes
    assert _run(cfg).ats.changes != _run(cfg, seed=1603).ats.changes


@pytest.mark.parametrize(
    ("days", "internal", "expected"),
    [(10, False, 0.82), (30, False, 0.82), (40, False, 0.78), (90, False, 0.58),
     (200, False, 0.45), (40, True, 0.88)],
)  # fmt: skip
def test_accept_probability_decays_after_the_threshold(
    base_config_path: Path, days: int, internal: bool, expected: float
) -> None:
    cfg = load_config(base_config_path, "tiny")
    assert accept_probability(cfg, days, internal) == pytest.approx(expected)


def test_ht4_buckets() -> None:
    assert [ht4_bucket(d) for d in (0, 30, 31, 45, 46, 60, 61)] == [
        "<=30", "<=30", "31-45", "31-45", "46-60", "46-60", ">60"
    ]  # fmt: skip


def test_lead_time_interviews(base_config_path: Path) -> None:
    timing = LeadTimeInterviews(load_config(base_config_path, "tiny"))
    rng = np.random.default_rng(6)
    phone = [timing.stage_days(rng, "phone_screen") for _ in range(4000)]
    onsite = [timing.stage_days(rng, "onsite") for _ in range(4000)]
    assert min(phone) >= 1 and min(onsite) >= 1
    assert 4 <= np.median(phone) <= 8 and 8 <= np.median(onsite) <= 12  # lead + ~1 d + 1.5 d


def test_fictional_phones() -> None:
    rng = np.random.default_rng(7)
    assert re.fullmatch(r"\+1-\d{3}-555-01\d\d", fictional_phone(rng, "US"))
    assert re.fullmatch(r"\+44 7700 900\d{3}", fictional_phone(rng, "GB"))
    assert re.fullmatch(r"\+00 0\d{9}", fictional_phone(rng, "IN"))


def _busy(base_config_path: Path, tmp_path: Path, **ats: object) -> GeneratorConfig:
    raw = yaml.safe_load(base_config_path.read_text())
    raw["jobboard"]["external"]["base_daily_views_per_open_req"] = 6.0
    raw["jobboard"]["internal"]["p_employee_browses_per_day"] = 0.3  # plenty of internal applicants
    raw["ats"]["offer"].update(ats)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(raw))
    return load_config(path, "tiny")


def test_internal_hires_transfer_and_open_a_backfill(
    base_config_path: Path, tmp_path: Path
) -> None:
    run = _run(_busy(base_config_path, tmp_path, no_start_probability=0.0))
    summary = run.ats.truth.summary()
    assert summary["hires_internal"] > 0
    no_starts = [a for a in run.ats.applications.values() if a.status == "no_start"]
    assert len(no_starts) == summary["no_starts"]
    assert all(a.status_reason == "could_not_start" for a in no_starts)  # left before starting
    transfers = [e.employee_id for e in run.wf.events if e.kind == "transfer"]
    assert len(transfers) == summary["hires_internal"]  # some people move twice at this rate
    movers = set(transfers)
    backfills = {r.backfill_for for r in run.rq.reqs.values() if r.backfill_for}
    assert movers & backfills  # a transfer leaves a seat that may be backfilled (p = 0.7)
    assert summary["applications"] == sum(run.ats.truth.applications.values())


def test_no_starts_give_their_seat_back(base_config_path: Path, tmp_path: Path) -> None:
    run = _run(_busy(base_config_path, tmp_path, no_start_probability=1.0))
    flipped = [a for a in run.ats.applications.values() if a.status == "no_start"]
    assert flipped and run.ats.truth.no_starts == len(flipped)
    assert not [e for e in run.wf.events if e.kind == "hire"]  # nobody actually starts
    reopened = {e.req_id for e in run.rq.events if e.kind == "reopened"}
    assert {a.req_id for a in flipped} <= reopened
