import hashlib
import json
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pytest
import yaml

from hirestream.generator.calendar import build_calendar
from hirestream.generator.candidates import CandidateRegistry, Submission
from hirestream.generator.config import GeneratorConfig, load_config
from hirestream.generator.events import CollectingSink, CountingSink, EventSink, StreamEvent
from hirestream.generator.jobboard import (
    BOT_USER_AGENTS,
    PRODUCER_VERSIONS,
    JobBoard,
    diurnal_start_ms,
    view_weights,
)
from hirestream.generator.requisitions import Requisition, Requisitions
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.workforce import Workforce
from hirestream.generator.world import build_world

PAYLOAD_KEYS = {
    "page_view": {"page_type", "req_id"},
    "job_search": {"query_text", "filter_location", "filter_role_family", "results_count"},
    "job_view": {"req_id", "position_in_results"},
    "job_save": {"req_id"},
    "apply_start": {"req_id"},
    "apply_submit": {"req_id", "application_id", "candidate_id"},
}
PAGE_TYPES = {"home", "search_results", "job_detail", "apply_form", "confirmation"}
ENVELOPE_KEYS = {
    "event_id",
    "event_type",
    "schema_version",
    "source",
    "producer_version",
    "event_ts",
    "sent_ts",
    "context",
    "payload",
}
REFERRERS = {"direct", "search_engine", "social", "email", "internal_portal"}


@dataclass
class Run:
    cfg: GeneratorConfig
    wf: Workforce
    rq: Requisitions
    jb: JobBoard
    events: list[StreamEvent]
    submissions: list[Submission] = field(default_factory=list)
    expected: Counter[str] = field(default_factory=Counter)  # req_id -> summed daily views


def _config(base_config_path: Path, tmp: Path, **overrides: Any) -> GeneratorConfig:
    raw = yaml.safe_load(base_config_path.read_text())
    raw["jobboard"]["external"]["base_daily_views_per_open_req"] = (
        6.0  # keep collected events small
    )
    for dotted, value in overrides.items():
        node = raw
        *parents, leaf = dotted.split(".")
        for part in parents:
            node = node[part]
        node[leaf] = value
    path = tmp / "config.yaml"
    path.write_text(yaml.safe_dump(raw))
    return load_config(path, "tiny")


def _simulate(
    cfg: GeneratorConfig, sink: EventSink, seed: int = 1602, jobboard: bool = True
) -> Run:
    plan = SeedPlan(seed)
    cal = build_calendar(cfg)
    wf = Workforce(build_world(cfg, plan), cfg, plan.rng("workforce"), cal["workforce.reorg"].start)
    rq = Requisitions(cfg, plan.rng("requisitions"), wf)
    jb = JobBoard(
        cfg, cal, plan.rng("jobboard"), wf, rq, CandidateRegistry(cfg.ats.reapply_probability)
    )
    run = Run(cfg, wf, rq, jb, events=sink.events if isinstance(sink, CollectingSink) else [])
    day = cfg.window.sim_start
    while day <= cfg.window.sim_end:
        rq.step(day, wf.step(day))
        if jobboard:
            run.submissions += jb.step(day, sink)
            run.expected.update(jb.expected_views)
        day += timedelta(days=1)
    return run


@pytest.fixture(scope="module")
def run(base_config_path: Path, tmp_path_factory: pytest.TempPathFactory) -> Run:
    return _simulate(_config(base_config_path, tmp_path_factory.mktemp("cfg")), CollectingSink())


def _sessions(events: list[StreamEvent]) -> dict[str, list[StreamEvent]]:
    grouped: dict[str, list[StreamEvent]] = defaultdict(list)
    for event in events:  # events are sorted by time and ties keep session order
        grouped[event.partition_key].append(event)
    return grouped


def _is_bot(session: list[StreamEvent]) -> bool:
    return sum(e.event_type == "job_view" for e in session) >= 30  # humans cap at 12 views


def test_envelope_and_payload_match_the_contract(run: Run) -> None:
    assert run.events
    for event in run.events:
        body = event.body
        assert set(body) == ENVELOPE_KEYS
        assert uuid.UUID(body["event_id"]).version == 4
        assert body["source"] == "jobboard-web" and body["event_type"] == event.event_type
        assert isinstance(body["event_ts"], int) and body["event_ts"] == event.event_ts_ms
        sent = datetime.fromisoformat(body["sent_ts"].replace("Z", "+00:00"))
        assert body["sent_ts"].endswith("Z") and sent.tzinfo == UTC
        assert 0 <= int(sent.timestamp() * 1000) - body["event_ts"] <= 5000
        assert body["producer_version"] == PRODUCER_VERSIONS[body["schema_version"]]
        context = body["context"]
        assert (
            context["session_id"] == event.partition_key and context["referrer_type"] in REFERRERS
        )
        assert set(body["payload"]) == PAYLOAD_KEYS[event.event_type]
        if event.event_type == "page_view":
            assert body["payload"]["page_type"] in PAGE_TYPES
            has_req = body["payload"]["page_type"] in {"job_detail", "apply_form", "confirmation"}
            assert (body["payload"]["req_id"] is not None) == has_req
    assert len({e.body["event_id"] for e in run.events}) == len(run.events)


def test_schema_v2_switches_on_its_date(run: Run) -> None:
    switch = build_calendar(run.cfg)["chaos.schema_v2.jobboard"].start
    switch_ms = int(datetime(switch.year, switch.month, switch.day, tzinfo=UTC).timestamp() * 1000)
    versions: Counter[int] = Counter()
    for event in run.events:
        body = event.body
        versions[body["schema_version"]] += 1
        assert ("device_type" in body["context"]) == (body["schema_version"] == 2)
        if body["schema_version"] == 2:
            assert body["context"]["device_type"] in {"desktop", "mobile", "tablet"}
        if event.event_ts_ms >= switch_ms + 86_400_000:
            assert body["schema_version"] == 2
        if event.event_ts_ms < switch_ms - 86_400_000:
            assert body["schema_version"] == 1
    assert versions[1] and versions[2]


def test_sessions_are_well_formed(run: Run) -> None:
    for sid, session in _sessions(run.events).items():
        contexts = {
            json.dumps({k: v for k, v in e.body["context"].items() if k != "device_type"})
            for e in session
        }
        assert len(contexts) == 1, sid
        stamps = [e.event_ts_ms for e in session]
        assert stamps == sorted(stamps)
        started: set[str] = set()
        for prev, event in zip([None, *session], session, strict=False):
            payload = event.body["payload"]
            if event.event_type == "job_view":
                assert prev is not None and prev.body["payload"] == {
                    "page_type": "job_detail",
                    "req_id": payload["req_id"],
                }
            if event.event_type == "apply_start":
                assert prev is not None and prev.body["payload"] == {
                    "page_type": "apply_form",
                    "req_id": payload["req_id"],
                }
                started.add(payload["req_id"])
            if event.event_type == "apply_submit":
                assert payload["req_id"] in started
        internal = session[0].body["context"]["employee_id"] is not None
        assert (session[0].body["context"]["referrer_type"] == "internal_portal") == internal


def test_bots_crawl_fast_and_never_apply(run: Run) -> None:
    bot_events = 0
    for session in _sessions(run.events).values():
        if not _is_bot(session):
            continue
        bot_events += len(session)
        assert {e.event_type for e in session} == {"page_view", "job_view"}
        assert session[0].body["context"]["referrer_type"] == "direct"
        views = [e.event_ts_ms for e in session if e.event_type == "job_view"]
        assert 30 <= len(views) <= 300
        if len(views) > 60:  # fast enough for gold's "> 60 views in 10 minutes" rule
            assert any(views[i + 60] - views[i] <= 600_000 for i in range(len(views) - 60))
    assert bot_events == run.jb.truth.events["bot"]
    agents = {
        s[0].body["context"]["user_agent"] for s in _sessions(run.events).values() if _is_bot(s)
    }
    assert agents & set(BOT_USER_AGENTS) and agents - set(BOT_USER_AGENTS)  # known and spoofed


def test_bot_share_is_inside_the_calibration_band(base_config_path: Path) -> None:
    """At default volume: with a third of the views there are too few bot sessions to judge."""
    truth = _simulate(load_config(base_config_path, "tiny"), CountingSink()).jb.truth
    share = truth.events["bot"] / sum(truth.events.values())
    assert 0.05 <= share <= 0.15  # calibration_targets.clickstream_bot_event_share
    sessions = truth.sessions["bot"] / (truth.sessions["bot"] + truth.sessions["external"])
    assert 0.0015 <= sessions <= 0.0045  # session_share 0.003, loose for ~100 bot sessions


def test_who_sees_which_reqs(run: Run) -> None:
    moved = {e.employee_id for e in run.wf.events if e.kind in ("lateral_move", "reorg_move")}
    for event in run.events:
        rid = event.body["payload"].get("req_id")
        if rid is None:
            continue
        req = run.rq.reqs[rid]
        employee_id = event.body["context"]["employee_id"]
        if employee_id is None:
            assert not req.is_internal_only
        elif employee_id not in moved:
            assert req.team != run.wf.employee(employee_id).team


def test_views_track_expected_views(run: Run) -> None:
    views: Counter[str] = Counter()
    for session in _sessions(run.events).values():
        if _is_bot(session) or session[0].body["context"]["employee_id"] is not None:
            continue
        views.update(e.body["payload"]["req_id"] for e in session if e.event_type == "job_view")
    total = sum(run.expected.values())
    assert sum(views.values()) / total == pytest.approx(1.0, abs=0.05)
    evergreen = {r.req_id for r in run.rq.reqs.values() if r.is_evergreen}
    share = sum(v for rid, v in views.items() if rid in evergreen) / sum(views.values())
    assert share == pytest.approx(sum(run.expected[r] for r in evergreen) / total, abs=0.03)


def test_returning_visitors(run: Run) -> None:
    seen: set[str] = set()
    returning = external = 0
    for session in _sessions(run.events).values():
        context = session[0].body["context"]
        if context["employee_id"] is not None or _is_bot(session):
            continue
        external += 1
        returning += context["visitor_id"] in seen
        seen.add(context["visitor_id"])
    assert returning / external == pytest.approx(0.30, abs=0.03)


def test_every_apply_submit_is_a_submission(run: Run) -> None:
    submits = [e for e in run.events if e.event_type == "apply_submit"]
    assert len(submits) == len(run.submissions) > 50
    by_id = {s.application_id: s for s in run.submissions}
    assert len(by_id) == len(run.submissions)
    registry = run.jb._candidates
    for event in submits:
        payload, context = event.body["payload"], event.body["context"]
        sub = by_id[payload["application_id"]]
        assert (sub.candidate_id, sub.req_id, sub.applied_ts_ms, sub.employee_id) == (
            payload["candidate_id"], payload["req_id"], event.event_ts_ms, context["employee_id"]
        )  # fmt: skip
        assert sub.channel == ("internal" if context["employee_id"] else "career_site")
        assert registry.candidates[sub.candidate_id].is_internal == (sub.channel == "internal")


def test_ht3_long_tenure_applies_about_three_times_as_often(
    base_config_path: Path, tmp_path: Path
) -> None:
    cfg = _config(
        base_config_path, tmp_path,
        **{"jobboard.internal.p_employee_browses_per_day": 0.3,
           "jobboard.external.base_daily_views_per_open_req": 1.0},
    )  # fmt: skip
    truth = _simulate(cfg, CountingSink()).jb.truth
    apps: Counter[str] = Counter()
    days: Counter[str] = Counter()
    for (_, group), n in truth.ht3_applications.items():
        apps[group] += n
    for (_, group), n in truth.ht3_employee_days.items():
        days[group] += n
    ratio = (apps["long"] / days["long"]) / (apps["short"] / days["short"])
    assert 2.4 <= ratio <= 3.6  # SPEC §13.2 band; expected 2.0 x 1.5 = 3.0


def test_view_weights() -> None:
    cfg = load_config(Path(__file__).parents[3] / "config" / "generator" / "base.yaml", "tiny")

    def req(opened: date, popularity: float, evergreen: bool) -> Requisition:
        return Requisition(
            req_id="R1", title="t", role_family="design", job_level="L4", org="o", team="t",
            location_city="Seattle", headcount=1, hiring_manager_id="E1", recruiter_id=None,
            status="open", is_evergreen=evergreen, is_internal_only=False, opened_on=opened,
            closed_on=None, updated_on=opened, source="growth", popularity=popularity, seats_open=1,
        )  # fmt: skip

    monday, saturday = date(2025, 1, 6), date(2025, 1, 11)
    fresh = [req(monday, 2.0, False)]
    assert view_weights(fresh, monday, cfg)[0] == pytest.approx(2.0 * 1.25 * 1.15)
    ratio = view_weights(fresh, saturday, cfg)[0] / view_weights(fresh, monday, cfg)[0]
    assert ratio == pytest.approx(0.55 / 1.15 * 0.5 ** (5 / 21))  # weekday and five days of decay
    aged = [
        req(monday - timedelta(days=21), 1.0, False),
        req(monday - timedelta(days=210), 1.0, True),
    ]
    weights = view_weights(aged, monday, cfg)
    assert weights[0] == pytest.approx(0.5 * 1.25 * 1.15)  # one half-life
    assert weights[1] == pytest.approx(1.25 * 1.15)  # evergreen reqs never decay


def test_diurnal_starts_have_two_peaks() -> None:
    starts = diurnal_start_ms(
        np.random.default_rng(3), np.zeros(200_000, dtype=np.int64), [11, 20], 1.0
    )
    hours = (starts // 3_600_000) % 24
    density = np.bincount(hours, minlength=24) / len(hours)
    assert density[10:12].sum() > 0.25 and density[19:21].sum() > 0.25
    assert density[3:6].sum() < 0.01
    assert starts.min() >= 0 and starts.max() < 86_400_000


def test_same_seed_same_traffic_and_isolation(base_config_path: Path, tmp_path: Path) -> None:
    cfg = _config(
        base_config_path, tmp_path, **{"jobboard.external.base_daily_views_per_open_req": 1.0}
    )

    def digest(run: Run) -> str:
        return hashlib.sha256(json.dumps([e.body for e in run.events]).encode()).hexdigest()

    a, b = _simulate(cfg, CollectingSink()), _simulate(cfg, CollectingSink())
    assert digest(a) == digest(b)
    assert digest(a) != digest(_simulate(cfg, CollectingSink(), seed=1603))
    quiet = _simulate(cfg, CollectingSink(), jobboard=False)
    assert quiet.wf.events == a.wf.events and quiet.rq.events == a.rq.events


def test_counting_sink_agrees_with_the_truth(base_config_path: Path, tmp_path: Path) -> None:
    cfg = _config(
        base_config_path, tmp_path, **{"jobboard.external.base_daily_views_per_open_req": 1.0}
    )
    sink = CountingSink()
    run = _simulate(cfg, sink)
    assert sink.total == sum(run.jb.truth.events.values())
    assert sink.counts[("jobboard-web", "apply_submit")] == len(run.submissions)
