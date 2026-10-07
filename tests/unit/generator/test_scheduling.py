import hashlib
import json
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from hirestream.generator.ats import ATS, StageChange
from hirestream.generator.calendar import build_calendar
from hirestream.generator.candidates import CandidateRegistry
from hirestream.generator.config import GeneratorConfig, load_config
from hirestream.generator.events import CollectingSink, CountingSink, StreamEvent
from hirestream.generator.jobboard import JobBoard
from hirestream.generator.requisitions import Requisitions
from hirestream.generator.scheduling import PRODUCER_VERSION, SOURCE, Scheduler
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.workforce import Workforce
from hirestream.generator.world import build_world

ENVELOPE_KEYS = {
    "event_id", "event_type", "schema_version", "source", "producer_version", "event_ts",
    "sent_ts", "payload",
}  # fmt: skip
PAYLOAD_KEYS = {
    "interview_scheduled": {
        "interview_id", "application_id", "req_id", "interview_type", "loop_id", "session_index",
        "interviewer_id", "scheduled_start", "duration_minutes", "timezone", "coordinator_id",
    },
    "interview_rescheduled": {
        "interview_id", "previous_start", "new_start", "timezone", "reason", "initiated_by",
    },
    "interview_cancelled": {"interview_id", "reason"},
    "interview_completed": {"interview_id", "actual_start", "actual_end"},
    "interview_no_show": {"interview_id", "no_show_party"},
    "feedback_submitted": {
        "feedback_id", "interview_id", "interviewer_id", "recommendation", "word_count",
    },
    "feedback_updated": {"feedback_id", "interview_id", "interviewer_id", "recommendation"},
}  # fmt: skip
ENUMS = {
    "interview_type": {"phone_screen", "onsite"},
    "reason": {
        "candidate_conflict", "interviewer_conflict", "other", "candidate_withdrew",
        "position_filled", "interviewer_unavailable",
    },
    "initiated_by": {"candidate", "interviewer", "coordinator"},
    "no_show_party": {"candidate", "interviewer"},
    "recommendation": {"strong_hire", "hire", "no_hire", "strong_no_hire"},
}  # fmt: skip
POSITIVE = {"strong_hire", "hire"}


@dataclass
class Booking:
    """An `interview_scheduled` event and who the interviewer was on the day it was booked."""

    payload: dict[str, Any]
    level: int
    org: str
    req_org: str  # both on the booking day: a reorg can move either later
    active: bool
    trained: bool
    applicant: str | None


@dataclass
class Run:
    cfg: GeneratorConfig
    wf: Workforce
    rq: Requisitions
    ats: ATS
    scheduler: Scheduler
    events: list[StreamEvent] = field(default_factory=list)
    bookings: list[Booking] = field(default_factory=list)


def _run(cfg: GeneratorConfig, seed: int = 1602) -> Run:
    plan = SeedPlan(seed)
    cal = build_calendar(cfg)
    wf = Workforce(build_world(cfg, plan), cfg, plan.rng("workforce"), cal["workforce.reorg"].start)
    rq = Requisitions(cfg, plan.rng("requisitions"), wf)
    registry = CandidateRegistry(cfg.ats.reapply_probability)
    jb = JobBoard(cfg, cal, plan.rng("jobboard"), wf, rq, registry)
    scheduler = Scheduler(cfg, plan.rng("scheduling"), wf, rq)
    ats = ATS(cfg, plan.rng("ats"), plan.faker_seed("ats"), wf, rq, registry, scheduler=scheduler)
    run = Run(cfg, wf, rq, ats, scheduler)
    sink = CollectingSink()
    day = cfg.window.sim_start
    while day <= cfg.window.sim_end:
        changes = wf.step(day)
        req_events = rq.step(day, changes)
        submissions = jb.step(day, CountingSink())
        mark = len(wf.events)
        ats.step(day, submissions, jb.expected_views, changes, req_events)
        seen = len(sink.events)
        scheduler.step(day, sink)
        _note_bookings(run, sink.events[seen:])
        rq.follow(day, wf.events[mark:])
        day += timedelta(days=1)
    run.events = sink.events
    return run


def _note_bookings(run: Run, events: list[StreamEvent]) -> None:
    levels = run.wf.level_ranks()
    for event in events:
        if event.event_type != "interview_scheduled":
            continue
        payload = event.body["payload"]
        emp = run.wf.employee(payload["interviewer_id"])
        i = run.wf.index_of(emp.employee_id)
        app = run.ats.applications[payload["application_id"]]
        run.bookings.append(
            Booking(
                payload,
                int(levels[i]),
                emp.org,
                run.rq.reqs[payload["req_id"]].org,
                emp.employment_status == "active",
                bool(run.scheduler._trained[i]),
                app.employee_id,
            )
        )


@pytest.fixture(scope="module")
def run(base_config_path: Path) -> Run:
    return _run(load_config(base_config_path, "tiny"))


def _by_interview(run: Run) -> dict[str, list[StreamEvent]]:
    timeline: dict[str, list[StreamEvent]] = defaultdict(list)
    for event in sorted(run.events, key=lambda e: e.event_ts_ms):
        timeline[event.partition_key].append(event)
    return timeline


def _utc(text: str) -> datetime:
    assert text.endswith("Z")
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def test_envelope_and_payload_match_the_contract(run: Run) -> None:
    assert run.events
    for event in run.events:
        body = event.body
        assert set(body) == ENVELOPE_KEYS
        assert uuid.UUID(body["event_id"]).version == 4
        assert body["source"] == SOURCE and body["event_type"] == event.event_type
        assert body["schema_version"] == 1 and body["producer_version"] == PRODUCER_VERSION
        assert _ms(_utc(body["event_ts"])) == event.event_ts_ms
        assert 0 <= _ms(_utc(body["sent_ts"])) - event.event_ts_ms <= 5000
        payload = body["payload"]
        assert set(payload) == PAYLOAD_KEYS[event.event_type]
        assert payload["interview_id"] == event.partition_key
        for key, allowed in ENUMS.items():
            if key in payload:
                assert payload[key] in allowed, (event.event_type, key, payload[key])
        if event.event_type == "interview_completed":
            assert _utc(payload["actual_start"]) < _utc(payload["actual_end"])
    assert len({e.body["event_id"] for e in run.events}) == len(run.events)


def test_local_starts_carry_the_offset_of_their_timezone(run: Run) -> None:
    starts = 0
    for event in run.events:
        payload = event.body["payload"]
        for key in ("scheduled_start", "previous_start", "new_start"):
            if key in payload:
                local = datetime.fromisoformat(payload[key])
                assert (
                    local.utcoffset() == local.astimezone(ZoneInfo(payload["timezone"])).utcoffset()
                )
                starts += 1
    assert starts > len(run.bookings)


def test_interviews_are_on_weekdays_in_business_hours(run: Run) -> None:
    lo, hi = run.cfg.scheduling.business_hours_local
    for event in run.events:
        payload = event.body["payload"]
        start = payload.get("scheduled_start") or payload.get("new_start")
        if start is None:
            continue
        local = datetime.fromisoformat(start)
        duration = run.scheduler.interviews[payload["interview_id"]].duration_minutes
        assert local.weekday() < 5 and local.minute % 15 == 0 and local.second == 0
        assert lo * 60 <= local.hour * 60 + local.minute <= hi * 60 - duration


def test_timelines_are_ordered_and_consistent(run: Run) -> None:
    max_times = run.cfg.scheduling.reschedule.max_times
    for interview_id, events in _by_interview(run).items():
        kinds = [e.event_type for e in events]
        assert kinds[0] == "interview_scheduled", interview_id
        assert len({e.event_ts_ms for e in events}) == len(events)  # strictly ordered
        assert kinds.count("interview_rescheduled") <= max_times
        assert kinds.count("interview_completed") <= 1 and kinds.count("interview_cancelled") <= 1
        start = events[0].body["payload"]["scheduled_start"]
        for event in events:
            payload = event.body["payload"]
            if event.event_type == "interview_rescheduled":
                assert payload["previous_start"] == start
                start = payload["new_start"]
            if event.event_type in ("interview_scheduled", "interview_rescheduled"):
                assert event.event_ts_ms < _ms(datetime.fromisoformat(start))  # booked ahead
        if "interview_cancelled" in kinds:
            assert kinds[-1] == "interview_cancelled"
        if "feedback_submitted" in kinds:
            assert kinds.index("interview_completed") < kinds.index("feedback_submitted")
        if "feedback_updated" in kinds:
            assert kinds.index("feedback_submitted") < kinds.index("feedback_updated")
        iv = run.scheduler.interviews[interview_id]
        assert iv.start_ms == _ms(datetime.fromisoformat(start))


def test_interviewers_are_eligible(run: Run) -> None:
    levels = list(run.cfg.org_model.levels)
    picked: list[bool] = []
    for b in run.bookings:
        req = run.rq.reqs[b.payload["req_id"]]
        assert b.payload["interviewer_id"] != b.applicant  # nobody interviews themselves
        if b.payload["interviewer_id"] == req.hiring_manager_id:
            continue  # the last-resort fallback
        assert b.active and b.level >= levels.index(req.job_level)
        picked.append(b.trained)
    assert sum(picked) / len(picked) > 0.95  # untrained employees only step in when needed
    loops: dict[str, list[str]] = defaultdict(list)
    for b in run.bookings:
        if b.payload["loop_id"] is not None:
            loops[b.payload["loop_id"]].append(b.payload["interviewer_id"])
    for interviewers in loops.values():
        assert len(set(interviewers)) == len(interviewers)  # replacements included
    # 0.7 by design, plus chance; tiny's saturated pools spill into other orgs (dev: 0.74).
    assert _same_org_share(run) > 0.55


def _same_org_share(run: Run) -> float:
    same = sum(b.org == b.req_org for b in run.bookings)
    return same / len(run.bookings)


def test_loops_and_phone_screens_are_shaped_by_the_config(run: Run) -> None:
    lo, hi = run.cfg.scheduling.onsite.sessions
    loops: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    tz = {loc.city: loc.tz for loc in run.cfg.org_model.locations}
    for b in run.bookings:
        payload = b.payload
        req = run.rq.reqs[payload["req_id"]]
        if payload["interview_type"] == "phone_screen":
            assert payload["loop_id"] is None and payload["session_index"] is None
            city = run.wf.employee(payload["interviewer_id"]).location_city
            assert payload["duration_minutes"] == run.cfg.scheduling.phone_screen.duration_minutes
            assert payload["timezone"] == tz[city]  # in the interviewer's timezone
        else:
            sessions = loops[payload["loop_id"]]
            index = payload["session_index"]
            if index not in sessions or payload["interview_id"] < sessions[index]["interview_id"]:
                sessions[index] = payload  # the original booking, not a replacement
            assert payload["timezone"] == tz[req.location_city]  # at the req's office
            assert payload["coordinator_id"] == (req.recruiter_id or req.hiring_manager_id)
    assert loops
    same_day = 0
    for sessions in loops.values():
        assert sorted(sessions) == list(range(1, len(sessions) + 1))
        assert lo <= len(sessions) <= hi
        days = {datetime.fromisoformat(p["scheduled_start"]).date() for p in sessions.values()}
        same_day += len(days) == 1
    share = same_day / len(loops)
    assert abs(share - run.cfg.scheduling.onsite.same_day_probability) < 0.2


def test_disruptions_happen_at_their_configured_rates(run: Run) -> None:
    cfg = run.cfg.scheduling
    truth = run.scheduler.truth
    booked = sum(truth.interviews.values())
    assert truth.reschedules / booked == pytest.approx(cfg.reschedule.probability, abs=0.05)
    assert truth.cancelled["other"] / booked == pytest.approx(cfg.cancel_probability, abs=0.03)
    happened = truth.completed + sum(truth.no_shows.values())
    expected = cfg.no_show.candidate + cfg.no_show.interviewer
    assert sum(truth.no_shows.values()) / happened == pytest.approx(expected, abs=0.03)
    initiated = Counter(
        e.body["payload"]["initiated_by"]
        for e in run.events
        if e.event_type == "interview_rescheduled"
    )
    assert initiated["candidate"] > initiated["interviewer"] > 0


def test_unavailable_interviewers_are_replaced(run: Run) -> None:
    timeline = _by_interview(run)
    unavailable = [
        e for e in run.events
        if e.event_type == "interview_cancelled"
        and e.body["payload"]["reason"] == "interviewer_unavailable"
    ]  # fmt: skip
    assert unavailable
    for event in unavailable:
        iv = run.scheduler.interviews[event.partition_key]
        later = [
            b.payload for b in run.bookings
            if b.payload["application_id"] == iv.application_id
            and b.payload["interview_type"] == iv.interview_type
            and b.payload["session_index"] == iv.session_index
            and b.payload["interviewer_id"] != iv.interviewer_id
            and timeline[b.payload["interview_id"]][0].event_ts_ms >= event.event_ts_ms
        ]  # fmt: skip
        assert later, iv.interview_id


def _decisions(changes: list[StageChange]) -> list[StageChange]:
    return [
        c for c in changes
        if c.from_stage in ("phone_screen", "onsite") and c.reason in ("advanced", "not_selected")
    ]  # fmt: skip


def test_the_ats_decides_after_feedback_or_the_cap(run: Run) -> None:
    cap = timedelta(days=run.cfg.ats.feedback_wait_cap_days)
    by_stage: dict[tuple[str, str], list[str]] = defaultdict(list)
    for iv in run.scheduler.interviews.values():
        by_stage[(iv.application_id, iv.interview_type)].append(iv.interview_id)
    timeline = _by_interview(run)
    decided = _decisions(run.ats.changes)
    assert decided
    for change in decided:
        assert change.from_stage is not None
        interviews = [timeline[i] for i in by_stage[(change.application_id, change.from_stage)]]
        done = [
            e.event_ts_ms for events in interviews for e in events
            if e.event_type in ("interview_completed", "interview_no_show")
        ]  # fmt: skip
        assert done and max(done) < change.changed_ms
        feedback_in = all(
            any(e.event_type == "feedback_submitted" and e.event_ts_ms < change.changed_ms
                for e in events)
            for events in interviews
            if any(e.event_type == "interview_completed" for e in events)
        )  # fmt: skip
        last_done = datetime.fromtimestamp(max(done) / 1000, UTC).date()
        decided_on = datetime.fromtimestamp(change.changed_ms / 1000, UTC).date()
        assert feedback_in or decided_on >= last_done + cap


def test_recommendations_follow_the_decision(run: Run) -> None:
    outcome = {
        (c.application_id, c.from_stage): c.reason == "advanced"
        for c in _decisions(run.ats.changes)
    }
    positive: dict[bool, list[bool]] = {True: [], False: []}
    for event in run.events:
        if event.event_type != "feedback_submitted":
            continue
        iv = run.scheduler.interviews[event.partition_key]
        key = (iv.application_id, iv.interview_type)
        if key in outcome:
            positive[outcome[key]].append(event.body["payload"]["recommendation"] in POSITIVE)
    assert sum(positive[True]) / len(positive[True]) > 0.7  # 0.80 hire or better
    assert sum(positive[False]) / len(positive[False]) < 0.3  # 0.20


def test_closed_applications_cancel_pending_interviews(run: Run) -> None:
    closed = {
        c.application_id: c for c in run.ats.changes
        if c.from_stage in ("phone_screen", "onsite")
        and c.reason in ("candidate_withdrew", "left_company", "position_filled")
    }  # fmt: skip
    reasons: Counter[str] = Counter()
    for event in run.events:
        if event.event_type != "interview_cancelled":
            continue
        reason = event.body["payload"]["reason"]
        if reason in ("candidate_withdrew", "position_filled"):
            iv = run.scheduler.interviews[event.partition_key]
            change = closed[iv.application_id]
            expected = "position_filled" if change.reason == "position_filled" else reason
            assert reason == expected
            assert event.event_ts_ms > change.changed_ms or event.event_ts_ms == iv.start_ms
            reasons[reason] += 1
    assert reasons["candidate_withdrew"] and reasons["position_filled"]
    for app_id, change in closed.items():  # nothing happens after the application closed
        for iv in run.scheduler.interviews.values():
            if iv.application_id == app_id and iv.interview_type == change.from_stage:
                assert iv.status != "scheduled"


def test_weekly_loads_match_the_event_stream(run: Run) -> None:
    """The warehouse's `interviewer_weekly_load`, rebuilt from events, is what HT1 is drawn on."""
    final: dict[str, tuple[str, str, bool]] = {}
    for interview_id, events in _by_interview(run).items():
        payload = events[0].body["payload"]
        start = payload["scheduled_start"]
        cancelled = False
        for event in events:
            if event.event_type == "interview_rescheduled":
                start = event.body["payload"]["new_start"]
            cancelled |= event.event_type == "interview_cancelled"
        final[interview_id] = (payload["interviewer_id"], start, cancelled)
    load: Counter[tuple[str, int, int]] = Counter()
    for interviewer, start, cancelled in final.values():
        if not cancelled:
            year, week, _ = datetime.fromisoformat(start).astimezone(UTC).isocalendar()
            load[(interviewer, year, week)] += 1
    assert load == +run.scheduler._load
    cap = run.cfg.scheduling.interviewer_selection.weekly_soft_cap
    assert max(load.values()) <= cap + 8  # the compounding penalty keeps overload bounded


def test_nothing_is_left_hanging(run: Run) -> None:
    end_ms = _ms(datetime.combine(run.cfg.window.sim_end, datetime.min.time(), UTC))
    for iv in run.scheduler.interviews.values():
        if iv.status == "scheduled":
            assert iv.start_ms >= end_ms  # only interviews after the window are still ahead
    summary = run.scheduler.summary()
    assert summary["feedback"] + run.scheduler.truth.never_submitted <= summary["completed"]
    assert 0 < summary["overloaded_feedback"] < summary["feedback"]
    assert 0 < summary["within_48h"] < 1 and summary["ht1_ratio"] > 1


def _digest(events: list[StreamEvent]) -> str:
    lines = (json.dumps(e.body, sort_keys=True) for e in events)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def test_same_seed_same_schedule(base_config_path: Path, run: Run) -> None:
    cfg = load_config(base_config_path, "tiny")
    assert _digest(_run(cfg).events) == _digest(run.events)
    assert _digest(_run(cfg, seed=7).events) != _digest(run.events)


@pytest.mark.slow
def test_dev_calibration(base_config_path: Path) -> None:
    """HT1 and the 48-hour feedback share at dev, the scale they are calibrated for."""
    run = _run(load_config(base_config_path, "dev"))
    summary = run.scheduler.summary()
    assert 1.6 <= summary["ht1_ratio"] <= 2.4
    assert summary["overloaded_feedback"] >= 300
    assert 0.70 <= _same_org_share(run) <= 0.85
    lo, hi = run.cfg.calibration_targets.feedback_within_48h_share
    assert lo <= summary["within_48h"] <= hi


def test_a_day_with_nothing_booked_emits_nothing(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    plan = SeedPlan(1602)
    wf = Workforce(
        build_world(cfg, plan),
        cfg,
        plan.rng("workforce"),
        build_calendar(cfg)["workforce.reorg"].start,
    )
    scheduler = Scheduler(
        cfg, plan.rng("scheduling"), wf, Requisitions(cfg, plan.rng("requisitions"), wf)
    )
    sink = CollectingSink()
    scheduler.step(date(2025, 1, 2), sink)
    assert sink.events == [] and scheduler.summary()["ht1_ratio"] == 0.0


def test_late_calls_change_nothing(run: Run) -> None:
    """A second cancel, an unknown application, or a closed application's callback is a no-op."""
    closed = next(app_id for app_id, st in run.scheduler._stages.items() if st.closed)
    run.scheduler.cancel(0, closed, "other")
    run.scheduler.cancel(0, "A-unknown", "other")
    assert not run.scheduler._outbox
    app = run.ats.applications[closed]
    assert app.status != "active"
    run.ats.stage_ready(closed, run.cfg.window.sim_end)
    assert app.due is None
