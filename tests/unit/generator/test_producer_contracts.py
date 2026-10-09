"""SPEC 6.11, literally: every event both producers emit at `tiny` meets its contract (ADR-0014).

Slow (about 260k events), so it runs with `-m slow`. The default suite validates every scheduling
event and a sample of job-board events. Before chaos: injected faults are test_chaos's business.
"""

from collections import Counter
from datetime import timedelta
from pathlib import Path

import pytest

from hirestream.generator.ats import ATS
from hirestream.generator.calendar import build_calendar
from hirestream.generator.candidates import CandidateRegistry
from hirestream.generator.chaos import STARTS_EVENTS
from hirestream.generator.config import load_config
from hirestream.generator.jobboard import JobBoard
from hirestream.generator.requisitions import Requisitions
from hirestream.generator.scheduling import Scheduler
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.workforce import Workforce
from hirestream.generator.world import build_world
from tests.contract_checks import STARTS, VALID, Contracts, ValidatingSink

pytestmark = pytest.mark.slow
BUG_VIOLATIONS = frozenset(
    {("pattern", f"/payload/{key}") for key in STARTS} | {("required", "/payload/timezone")}
)


@pytest.mark.no_cover
def test_every_event_at_tiny_meets_its_contract(
    base_config_path: Path, contracts: Contracts
) -> None:
    cfg = load_config(base_config_path, "tiny")
    plan = SeedPlan(cfg.meta.seed)
    cal = build_calendar(cfg)
    wf = Workforce(build_world(cfg, plan), cfg, plan.rng("workforce"), cal["workforce.reorg"].start)
    rq = Requisitions(cfg, plan.rng("requisitions"), wf)
    registry = CandidateRegistry(cfg.ats.reapply_probability)
    jb = JobBoard(cfg, cal, plan.rng("jobboard"), wf, rq, registry)
    scheduler = Scheduler(cfg, cal, plan.rng("scheduling"), plan.rng("scheduling_chaos"), wf, rq)
    ats = ATS(cfg, plan.rng("ats"), plan.faker_seed("ats"), wf, rq, registry, scheduler=scheduler)
    sink = ValidatingSink(contracts)
    day = cfg.window.sim_start
    while day <= cfg.window.sim_end:  # simulation.simulate's loop, with the validating sink
        changes = wf.step(day)
        req_events = rq.step(day, changes)
        submissions = jb.step(day, sink)
        mark = len(wf.events)
        ats.step(day, submissions, jb.expected_views, changes, req_events)
        scheduler.step(day, sink)
        rq.follow(day, wf.events[mark:])
        day += timedelta(days=1)

    bug = cfg.chaos.scheduling_tz_bug.producer_version
    naive: Counter[str] = Counter()
    dropped: Counter[str] = Counter()
    per_source: Counter[str] = Counter()
    for (source, event_type, _, producer, signature), n in sink.counts.items():
        per_source[source] += n
        if producer == bug and event_type in STARTS_EVENTS:
            assert signature, (event_type, n)  # never clean: every start lost its offset
            assert signature <= BUG_VIOLATIONS, signature
            naive[event_type] += n
            dropped[event_type] += n * (("required", "/payload/timezone") in signature)
        else:
            assert signature == VALID, (source, event_type, producer, signature, n)
    truth = scheduler.truth
    assert +naive == truth.naive_starts and +dropped == truth.missing_timezone
    assert per_source["jobboard-web"] == jb.truth.summary()["events"]
    assert per_source["jobboard-web"] > 150_000 and per_source["scheduling-service"] > 3_000
