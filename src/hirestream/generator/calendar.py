"""Resolve special events placed by fraction of the window to concrete dates (SPEC §6.1).

`at: 0.0` is `sim_start`, `at: 1.0` is `sim_end`, and anything between maps to
`sim_start + floor(at x (n_days - 1))`. Placing events by fraction means every preset,
including CI's 90-day `tiny`, hits every code path. The resolved calendar goes into the manifest.
"""

from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import date, timedelta

from pydantic import BaseModel, ConfigDict

from hirestream.generator.config import INCIDENT_NAMES, GeneratorConfig, Window

# Absorbs float error such as 0.7 x 100 = 69.99999999999999, which would otherwise floor to 69.
_EPSILON = 1e-9


class CalendarEvent(BaseModel):
    """An event in effect from `start` through `end`, inclusive (one day when they are equal)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    start: date
    end: date
    clipped: bool = False  # True when the configured duration ran past sim_end


def resolve_fraction(window: Window, at: float) -> date:
    if not 0.0 <= at <= 1.0:
        raise ValueError(f"fraction {at} is outside [0, 1]")
    offset = min(math.floor(at * (window.n_days - 1) + _EPSILON), window.n_days - 1)
    return window.sim_start + timedelta(days=offset)


def _span(window: Window, at: float, days: int) -> CalendarEvent:
    start = resolve_fraction(window, at)
    end = start + timedelta(days=days - 1)
    if end > window.sim_end:
        return CalendarEvent(start=start, end=window.sim_end, clipped=True)
    return CalendarEvent(start=start, end=end)


def _point(window: Window, at: float) -> CalendarEvent:
    return _span(window, at, 1)


def _from(window: Window, at: float) -> CalendarEvent:
    """A switch that stays in effect until the end of the window."""
    return CalendarEvent(start=resolve_fraction(window, at), end=window.sim_end)


def build_calendar(
    config: GeneratorConfig, incidents: Iterable[str] = ()
) -> dict[str, CalendarEvent]:
    """Every dated event for a run, in a fixed order. Only enabled incidents are included."""
    enabled = set(incidents)
    if unknown := enabled - set(INCIDENT_NAMES):
        raise ValueError(f"unknown incidents {sorted(unknown)}; choose from {list(INCIDENT_NAMES)}")

    w, chaos, inc = config.window, config.chaos, config.incidents
    calendar: dict[str, CalendarEvent] = {
        "workforce.reorg": _point(w, config.workforce.reorg.at),
        "chaos.scheduling_tz_bug": _span(
            w, chaos.scheduling_tz_bug.at, chaos.scheduling_tz_bug.duration_days
        ),
        "chaos.schema_v2.scheduling": _from(w, chaos.schema_v2.scheduling_at),
        "chaos.schema_v2.jobboard": _from(w, chaos.schema_v2.jobboard_at),
    }
    for i, at in enumerate(chaos.hris.missing_snapshot_at):
        calendar[f"chaos.hris.missing_snapshot.{i}"] = _point(w, at)
    rename = chaos.hris.column_rename
    calendar["chaos.hris.column_rename"] = _span(w, rename.at, rename.days)
    calendar["chaos.hris.duplicate_row"] = _point(w, chaos.hris.duplicate_row_at)

    # Hour-long incidents are dated here; their start hour is sampled by the chaos layer (T1.8).
    spans = {
        "duplicate_storm": _point(w, inc.duplicate_storm.at),
        "silent_schema_break": _span(w, inc.silent_schema_break.at, inc.silent_schema_break.days),
        "late_burst": _point(w, inc.late_burst.at),
        "hris_partial_file": _point(w, inc.hris_partial_file.at),
    }
    for name in INCIDENT_NAMES:
        if name in enabled:
            calendar[f"incidents.{name}"] = spans[name]
    return calendar
