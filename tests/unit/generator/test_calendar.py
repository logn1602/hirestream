from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hirestream.generator.calendar import CalendarEvent, build_calendar, resolve_fraction
from hirestream.generator.config import INCIDENT_NAMES, Window, load_config

TINY = Window(sim_start=date(2025, 1, 1), sim_end=date(2025, 3, 31))  # 90 days


@pytest.mark.parametrize(
    ("at", "expected"),
    [(0.0, date(2025, 1, 1)), (1.0, date(2025, 3, 31)), (0.5, date(2025, 2, 14))],
)
def test_resolve_fraction(at: float, expected: date) -> None:
    assert resolve_fraction(TINY, at) == expected


def test_resolve_fraction_absorbs_float_error() -> None:
    window = Window(sim_start=date(2025, 1, 1), sim_end=date(2025, 4, 11))  # n_days - 1 = 100
    # 0.29 * 100 == 28.999999999999996; a bare floor would land a day early.
    assert resolve_fraction(window, 0.29) == date(2025, 1, 30)


def test_single_day_window() -> None:
    window = Window(sim_start=date(2025, 5, 1), sim_end=date(2025, 5, 1))
    assert resolve_fraction(window, 0.0) == resolve_fraction(window, 1.0) == date(2025, 5, 1)


@pytest.mark.parametrize("at", [-0.01, 1.01])
def test_resolve_fraction_rejects_out_of_range(at: float) -> None:
    with pytest.raises(ValueError, match="outside"):
        resolve_fraction(TINY, at)


def test_tiny_calendar_dates(base_config_path: Path) -> None:
    cal = build_calendar(load_config(base_config_path, "tiny"))
    assert cal["workforce.reorg"] == CalendarEvent(start=date(2025, 3, 8), end=date(2025, 3, 8))
    assert cal["chaos.scheduling_tz_bug"] == CalendarEvent(
        start=date(2025, 1, 27), end=date(2025, 2, 9)
    )
    assert cal["chaos.schema_v2.scheduling"] == CalendarEvent(
        start=date(2025, 2, 14), end=date(2025, 3, 31)
    )
    assert not any(name.startswith("incidents.") for name in cal)


@pytest.mark.parametrize("preset", ["tiny", "dev", "full"])
def test_every_event_falls_inside_the_window(base_config_path: Path, preset: str) -> None:
    cfg = load_config(base_config_path, preset)
    cal = build_calendar(cfg, INCIDENT_NAMES)
    assert len(cal) == 6 + len(cfg.chaos.hris.missing_snapshot_at) + len(INCIDENT_NAMES)
    for event in cal.values():
        assert cfg.window.sim_start <= event.start <= event.end <= cfg.window.sim_end


def test_span_past_sim_end_is_clipped(
    raw_config: dict[str, Any], write_config: Callable[[dict[str, Any]], Path]
) -> None:
    raw_config["chaos"]["scheduling_tz_bug"].update(at=0.95, duration_days=30)
    cal = build_calendar(load_config(write_config(raw_config), "tiny"))
    assert cal["chaos.scheduling_tz_bug"] == CalendarEvent(
        start=date(2025, 3, 26), end=date(2025, 3, 31), clipped=True
    )


def test_only_enabled_incidents_are_dated(base_config_path: Path) -> None:
    cal = build_calendar(load_config(base_config_path, "tiny"), ["late_burst"])
    assert [name for name in cal if name.startswith("incidents.")] == ["incidents.late_burst"]


def test_unknown_incident_is_rejected(base_config_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown incidents"):
        build_calendar(load_config(base_config_path, "tiny"), ["meteor_strike"])


def test_calendar_order_is_fixed(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "dev")
    forward = build_calendar(cfg, ["late_burst", "duplicate_storm"])
    backward = build_calendar(cfg, ["duplicate_storm", "late_burst"])
    assert list(forward.items()) == list(backward.items())
