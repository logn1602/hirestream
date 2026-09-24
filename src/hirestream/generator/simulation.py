"""The day loop that drives every generator subsystem (SPEC §6.1, ADR-0005 §1).

Each simulated day, subsystems advance in a fixed order (workforce, then requisitions) and the
sinks write that day's output. T1.5+ (job board, ATS, scheduling) join this loop.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

from hirestream.generator.calendar import CalendarEvent
from hirestream.generator.config import GeneratorConfig
from hirestream.generator.hris import HrisExport
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
    hris = HrisExport(config, calendar, lake_root, plan.rng("hris_chaos"), workforce)
    day = config.window.sim_start
    while day <= config.window.sim_end:
        changes = workforce.step(day)
        requisitions.step(day, changes)
        hris.publish(day, changes)
        day += ONE_DAY
    return SimulationResult(
        events=workforce.events,
        event_counts=workforce.event_counts(),
        files=hris.files,
        req_events=requisitions.events,
        req_summary=requisitions.summary(),
    )
