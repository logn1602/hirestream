"""The day loop that drives every generator subsystem (SPEC §6.1, ADR-0005 §1).

Each simulated day, subsystems advance in a fixed order (workforce, requisitions, job board, ATS)
and the sinks write that day's output. The ATS's hires and transfers happen after the workforce's
own step, so requisitions and HRIS see them the same day. T1.7+ (scheduling) join this loop.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from hirestream.generator.ats import ATS
from hirestream.generator.ats_sink import AtsSnapshot
from hirestream.generator.calendar import CalendarEvent
from hirestream.generator.candidates import CandidateRegistry
from hirestream.generator.config import GeneratorConfig
from hirestream.generator.events import CountingSink
from hirestream.generator.hris import HrisExport
from hirestream.generator.jobboard import JobBoard
from hirestream.generator.manifest import FileEntry
from hirestream.generator.requisitions import ReqEvent, Requisitions
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.workforce import EventKind, Workforce, WorkforceEvent
from hirestream.generator.world import World

ONE_DAY = timedelta(days=1)


@dataclass(frozen=True)
class SimulationResult:
    events: list[WorkforceEvent]
    event_counts: dict[EventKind, int]
    files: list[FileEntry]
    req_events: list[ReqEvent]
    req_summary: dict[str, int]
    jobboard_summary: dict[str, int]
    ats_summary: dict[str, int]
    ats_snapshot: AtsSnapshot  # the ATS's final state, for the Postgres sink


def simulate(
    config: GeneratorConfig,
    plan: SeedPlan,
    calendar: Mapping[str, CalendarEvent],
    world: World,
    lake_root: Path,
) -> SimulationResult:
    """Run the whole window. `world` is advanced in place and ends in its final state."""
    workforce = Workforce(world, config, plan.rng("workforce"), calendar["workforce.reorg"].start)
    requisitions = Requisitions(config, plan.rng("requisitions"), workforce)
    candidates = CandidateRegistry(config.ats.reapply_probability)
    jobboard = JobBoard(config, calendar, plan.rng("jobboard"), workforce, requisitions, candidates)
    ats = ATS(config, plan.rng("ats"), plan.faker_seed("ats"), workforce, requisitions, candidates)
    stream_sink = CountingSink()  # T1.8 replaces this with delivery, chaos, and file sinks
    hris = HrisExport(config, calendar, lake_root, plan.rng("hris_chaos"), workforce)
    day = config.window.sim_start
    while day <= config.window.sim_end:
        changes = workforce.step(day)
        req_events = requisitions.step(day, changes)
        submissions = jobboard.step(day, stream_sink)
        mark = len(workforce.events)
        ats.step(day, submissions, jobboard.expected_views, changes, req_events)
        starts = workforce.events[mark:]  # today's hires and transfers
        requisitions.follow(day, starts)
        hris.publish(day, [*changes, *starts])
        day += ONE_DAY
    return SimulationResult(
        events=workforce.events,
        event_counts=workforce.event_counts(),
        files=hris.files,
        req_events=requisitions.events,
        req_summary=requisitions.summary(),
        jobboard_summary=jobboard.truth.summary(),
        ats_summary=ats.truth.summary(),
        ats_snapshot=AtsSnapshot(
            candidates=list(candidates.candidates.values()),
            requisitions=list(requisitions.reqs.values()),
            applications=list(ats.applications.values()),
            offers=list(ats.offers.values()),
            changes=ats.changes,
        ),
    )
