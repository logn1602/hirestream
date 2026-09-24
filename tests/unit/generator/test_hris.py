import csv
import gzip
import hashlib
import io
import math
import os
from collections import Counter
from collections.abc import Callable, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

from hirestream.generator.calendar import build_calendar
from hirestream.generator.config import GeneratorConfig, load_config
from hirestream.generator.hris import COLUMNS, _csv_line, _write_gzip, snapshot_path
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.simulation import SimulationResult, simulate
from hirestream.generator.world import Employee, build_world

WriteConfig = Callable[[dict[str, Any]], Path]
Rows = dict[str, list[str]]  # employee_id -> row


def _simulate(cfg: GeneratorConfig, lake: Path, incidents: Sequence[str] = ()) -> SimulationResult:
    plan = SeedPlan(1602)
    return simulate(cfg, plan, build_calendar(cfg, incidents), build_world(cfg, plan), lake)


def _read(lake: Path, day: date) -> tuple[list[str], list[list[str]]]:
    with gzip.open(snapshot_path(lake, day), "rt", encoding="utf-8", newline="") as fh:
        header, *rows = list(csv.reader(fh))
    return header, rows


def _by_id(lake: Path, day: date) -> Rows:
    return {row[1]: row for row in _read(lake, day)[1]}


@pytest.fixture(scope="module")
def tiny(
    tmp_path_factory: pytest.TempPathFactory, base_config_path: Path
) -> tuple[GeneratorConfig, Path, SimulationResult]:
    cfg = load_config(base_config_path, "tiny")
    lake = tmp_path_factory.mktemp("lake")
    return cfg, lake, _simulate(cfg, lake)


def _days(cfg: GeneratorConfig) -> list[date]:
    n = cfg.window.n_days
    return [cfg.window.sim_start + timedelta(days=i) for i in range(n)]


def test_one_file_per_day_except_the_missing_one(
    tiny: tuple[GeneratorConfig, Path, SimulationResult],
) -> None:
    cfg, lake, result = tiny
    missing = build_calendar(cfg)["chaos.hris.missing_snapshot.0"].start
    expected = [d for d in _days(cfg) if d != missing]
    assert [e.path for e in result.files] == [
        snapshot_path(lake, d).relative_to(lake).as_posix() for d in expected
    ]
    assert not snapshot_path(lake, missing).exists()
    for entry in result.files:
        data = (lake / entry.path).read_bytes()
        assert entry.sha256 == hashlib.sha256(data).hexdigest() and entry.bytes == len(data)
        assert entry.records == gzip.decompress(data).count(b"\n") - 1


def test_header_format_and_rename_day(tiny: tuple[GeneratorConfig, Path, SimulationResult]) -> None:
    cfg, lake, _ = tiny
    rename = build_calendar(cfg)["chaos.hris.column_rename"].start
    assert _read(lake, cfg.window.sim_start)[0] == list(COLUMNS)
    assert _read(lake, rename)[0] == [("mgr_id" if c == "manager_id" else c) for c in COLUMNS]
    raw = snapshot_path(lake, cfg.window.sim_start).read_bytes()
    assert raw[4:8] == b"\x00\x00\x00\x00"  # gzip mtime 0
    assert raw[3] & 0x08 == 0  # no embedded file name
    text = gzip.decompress(raw).decode("utf-8")
    assert "\r" not in text and text.endswith("\n")


def test_first_snapshot_matches_the_initial_world(
    tiny: tuple[GeneratorConfig, Path, SimulationResult], base_config_path: Path
) -> None:
    cfg, lake, result = tiny
    world = build_world(cfg, SeedPlan(1602))
    first_day = cfg.window.sim_start
    changed_on_day_one = {e.employee_id for e in result.events if e.day == first_day}
    rows = _by_id(lake, first_day)
    assert list(rows) == [e.employee_id for e in world.employees]
    for emp in world.employees:
        if emp.employee_id in changed_on_day_one:
            continue
        row = rows[emp.employee_id]
        assert row[0] == first_day.isoformat()
        assert row[2:6] == [
            emp.first_name,
            emp.last_name,
            emp.work_email,
            emp.hire_date.isoformat(),
        ]
        assert row[10] == (emp.manager_id or "") and row[14] == emp.job_effective_date.isoformat()


def test_exactly_one_duplicate_on_the_duplicate_day(
    tiny: tuple[GeneratorConfig, Path, SimulationResult],
) -> None:
    cfg, lake, _ = tiny
    duplicate_day = build_calendar(cfg)["chaos.hris.duplicate_row"].start
    counts = Counter(tuple(r) for r in _read(lake, duplicate_day)[1])
    assert sorted(counts.values())[-2:] == [1, 2]
    assert sum(v == 2 for v in counts.values()) == 1
    other = duplicate_day - timedelta(days=1)
    assert max(Counter(tuple(r) for r in _read(lake, other)[1]).values()) == 1


def test_no_row_is_effective_after_its_snapshot(
    tiny: tuple[GeneratorConfig, Path, SimulationResult],
) -> None:
    _, lake, result = tiny
    for entry in result.files:
        for row in _read(lake, date.fromisoformat(entry.path.split("=")[1][:10]))[1]:
            assert row[14] <= row[0]


def _single_change(
    result: SimulationResult, cfg: GeneratorConfig, gap: int
) -> list[tuple[str, date]]:
    """Employees changed exactly once, early enough that the next `gap` days are in the window."""
    per_emp = Counter(e.employee_id for e in result.events)
    missing = build_calendar(cfg)["chaos.hris.missing_snapshot.0"].start
    return [
        (e.employee_id, e.day)
        for e in result.events
        if per_emp[e.employee_id] == 1
        and e.day + timedelta(days=gap) <= cfg.window.sim_end
        and not (e.day <= missing <= e.day + timedelta(days=gap))
    ]


def test_late_exports_carry_their_true_date(
    raw_config: dict[str, Any], write_config: WriteConfig, tmp_path: Path
) -> None:
    raw_config["chaos"]["hris"].update(retro_effective_share=1.0, retro_effective_days=[3, 3])
    cfg = load_config(write_config(raw_config), "tiny")
    result = _simulate(cfg, tmp_path)
    cases = _single_change(result, cfg, 3)
    assert len(cases) > 10
    for emp_id, day in cases:
        before = _by_id(tmp_path, day + timedelta(days=2)).get(emp_id)
        after = _by_id(tmp_path, day + timedelta(days=3))[emp_id]
        assert before is not None and before[14] < day.isoformat()  # still the old row
        assert after[14] == day.isoformat()  # appears late, with its true effective date


def test_changes_appear_the_same_day_when_nothing_is_late(
    raw_config: dict[str, Any], write_config: WriteConfig, tmp_path: Path
) -> None:
    raw_config["chaos"]["hris"]["retro_effective_share"] = 0.0
    cfg = load_config(write_config(raw_config), "tiny")
    result = _simulate(cfg, tmp_path)
    cases = _single_change(result, cfg, 0)
    assert len(cases) > 10
    for emp_id, day in cases:
        assert _by_id(tmp_path, day)[emp_id][14] == day.isoformat()


def test_terminated_rows_stay_for_the_retention_window(
    raw_config: dict[str, Any], write_config: WriteConfig, tmp_path: Path
) -> None:
    raw_config["chaos"]["hris"]["retro_effective_share"] = 0.0
    raw_config["workforce"].update(terminated_retention_days=5, attrition_annual=1.0)
    cfg = load_config(write_config(raw_config), "tiny")
    result = _simulate(cfg, tmp_path)
    written = {date.fromisoformat(e.path.split("=")[1][:10]) for e in result.files}
    checked = 0
    for event in (e for e in result.events if e.kind == "termination"):
        window = [event.day + timedelta(days=k) for k in range(6)]
        if window[-1] > cfg.window.sim_end:
            continue
        rows = [_by_id(tmp_path, d).get(event.employee_id) for d in window[:5] if d in written]
        assert rows and all(row is not None and row[12] == "terminated" for row in rows)
        if window[5] in written:
            assert event.employee_id not in _by_id(tmp_path, window[5])
        checked += 1
    assert checked > 10


def test_partial_file_incident_truncates_one_day(
    tiny: tuple[GeneratorConfig, Path, SimulationResult], tmp_path: Path
) -> None:
    cfg, _, result = tiny
    partial = _simulate(cfg, tmp_path, incidents=["hris_partial_file"])
    day = build_calendar(cfg, ["hris_partial_file"])["incidents.hris_partial_file"].start
    full = {e.path: e for e in result.files}
    for entry in partial.files:
        if entry.path == snapshot_path(tmp_path, day).relative_to(tmp_path).as_posix():
            complete = full[entry.path].records
            assert complete is not None and entry.records == math.floor(complete * 0.6)
        else:
            assert entry.sha256 == full[entry.path].sha256


def test_same_seed_writes_identical_bytes(
    tiny: tuple[GeneratorConfig, Path, SimulationResult], tmp_path: Path
) -> None:
    cfg, _, result = tiny
    again = _simulate(cfg, tmp_path)
    assert [(e.path, e.sha256) for e in again.files] == [(e.path, e.sha256) for e in result.files]


def test_hris_chaos_never_moves_the_workforce(
    raw_config: dict[str, Any], write_config: WriteConfig, tmp_path: Path,
    tiny: tuple[GeneratorConfig, Path, SimulationResult],
) -> None:  # fmt: skip
    raw_config["chaos"]["hris"]["retro_effective_share"] = 1.0
    changed = _simulate(load_config(write_config(raw_config), "tiny"), tmp_path)
    assert changed.events == tiny[2].events
    assert [e.sha256 for e in changed.files] != [e.sha256 for e in tiny[2].files]


def test_csv_quotes_awkward_names() -> None:
    emp = Employee(
        employee_id="E000001",
        first_name='Jo "JJ"',
        last_name="Smith, Jr",
        work_email="jo@halcyon.example",
        hire_date=date(2020, 1, 1),
        org="Commerce",
        team="Payments Platform",
        role_family="design",
        job_level="L4",
        manager_id=None,
        location_city="Seattle",
        employment_status="active",
        termination_date=None,
        job_effective_date=date(2020, 1, 1),
        ats_candidate_id=None,
    )
    parsed = next(csv.reader(io.StringIO(_csv_line(emp))))
    assert parsed[1:3] == ['Jo "JJ"', "Smith, Jr"] and parsed[9] == "" and parsed[14] == ""


def test_failed_write_leaves_no_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(src: str, dst: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", boom)
    target = snapshot_path(tmp_path, date(2025, 1, 1))
    with pytest.raises(OSError, match="disk full"):
        _write_gzip(target, b"x\n")
    assert list(target.parent.iterdir()) == []
