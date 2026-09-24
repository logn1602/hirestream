"""HRIS daily snapshot export and its always-on chaos (SPEC §6.8, §6.9, §7.5, ADR-0005).

The export keeps its own *published* view of each employee, one cached CSV line per person.
Normally a change is published the day it happens; a late ("retro") change stays hidden for a
few days and then appears carrying its true `job_effective_date`. Lines are rebuilt only when an
employee changes, so writing a 25,000-row file is a join plus gzip.
"""

from __future__ import annotations

import csv
import gzip
import io
import math
import os
import tempfile
from collections.abc import Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path

import numpy as np

from hirestream.generator.calendar import CalendarEvent
from hirestream.generator.config import GeneratorConfig
from hirestream.generator.manifest import FileEntry, file_entry
from hirestream.generator.workforce import Workforce, WorkforceEvent
from hirestream.generator.world import Employee

HRIS_PREFIX = Path("bronze") / "hris"
COLUMNS = (
    "snapshot_date",
    "employee_id",
    "first_name",
    "last_name",
    "work_email",
    "hire_date",
    "org",
    "team",
    "role_family",
    "job_level",
    "manager_id",
    "location_city",
    "employment_status",
    "termination_date",
    "job_effective_date",
    "ats_candidate_id",
)
GZIP_LEVEL = 6


def snapshot_path(lake_root: Path, day: date) -> Path:
    return lake_root / HRIS_PREFIX / f"snapshot_date={day}" / f"employees_{day:%Y%m%d}.csv.gz"


class HrisExport:
    def __init__(
        self,
        config: GeneratorConfig,
        calendar: Mapping[str, CalendarEvent],
        lake_root: Path,
        rng: np.random.Generator,
        workforce: Workforce,
    ) -> None:
        self.files: list[FileEntry] = []
        self._lake_root = lake_root
        self._rng = rng
        self._workforce = workforce
        self._retention = timedelta(days=config.workforce.terminated_retention_days)
        chaos = config.chaos.hris
        self._retro_share = chaos.retro_effective_share
        self._retro_days = chaos.retro_effective_days
        self._missing = {
            e.start for k, e in calendar.items() if k.startswith("chaos.hris.missing_")
        }
        self._rename = calendar["chaos.hris.column_rename"]
        self._header = self._header_line(None)
        self._renamed_header = self._header_line(
            (chaos.column_rename.from_, chaos.column_rename.to)
        )
        self._duplicate_day = calendar["chaos.hris.duplicate_row"].start
        partial = calendar.get("incidents.hris_partial_file")
        self._partial_day = partial.start if partial else None
        self._keep = config.incidents.hris_partial_file.keep_fraction

        self._lines: dict[str, str] = {}  # published view; insertion order is employee_id order
        self._drop: dict[str, date] = {}  # terminated rows leave the export on this day
        self._pending: dict[str, date] = {}  # late changes and the day they appear
        for emp in workforce.world.employees:
            self._publish(emp)

    def publish(self, day: date, events: Sequence[WorkforceEvent]) -> FileEntry | None:
        """Apply today's changes to the published view and write today's file (if any)."""
        for emp_id in dict.fromkeys(event.employee_id for event in events):
            if emp_id in self._pending:  # a newer change exports the full current state
                del self._pending[emp_id]
                self._publish(self._workforce.employee(emp_id))
            elif self._rng.random() < self._retro_share:
                lo, hi = self._retro_days
                self._pending[emp_id] = day + timedelta(days=int(self._rng.integers(lo, hi + 1)))
            else:
                self._publish(self._workforce.employee(emp_id))
        for emp_id, due in list(self._pending.items()):
            if due <= day:
                del self._pending[emp_id]
                self._publish(self._workforce.employee(emp_id))
        for emp_id, drop in list(self._drop.items()):
            if drop <= day:
                del self._drop[emp_id]
                del self._lines[emp_id]
        if day in self._missing:
            return None
        return self._write(day)

    def _publish(self, emp: Employee) -> None:
        self._lines[emp.employee_id] = _csv_line(emp)
        if emp.termination_date is not None:
            self._drop[emp.employee_id] = emp.termination_date + self._retention

    def _write(self, day: date) -> FileEntry:
        stamp = day.isoformat()
        rows = [f"{stamp},{line}\n" for line in self._lines.values()]
        if day == self._partial_day:
            rows = rows[: math.floor(len(rows) * self._keep)]
        if day == self._duplicate_day and rows:
            j = int(self._rng.integers(len(rows)))
            rows.insert(j + 1, rows[j])
        renamed = self._rename.start <= day <= self._rename.end
        content = (self._renamed_header if renamed else self._header) + "".join(rows)
        path = snapshot_path(self._lake_root, day)
        _write_gzip(path, content.encode("utf-8"))
        entry = file_entry(path, self._lake_root, records=len(rows))
        self.files.append(entry)
        return entry

    @staticmethod
    def _header_line(rename: tuple[str, str] | None) -> str:
        columns = [rename[1] if rename and c == rename[0] else c for c in COLUMNS]
        return ",".join(columns) + "\n"


def _csv_line(emp: Employee) -> str:
    """One employee's columns after `snapshot_date`, CSV-quoted, without the newline."""
    buffer = io.StringIO()
    csv.writer(buffer, lineterminator="").writerow(
        [
            emp.employee_id,
            emp.first_name,
            emp.last_name,
            emp.work_email,
            emp.hire_date.isoformat(),
            emp.org,
            emp.team,
            emp.role_family,
            emp.job_level,
            emp.manager_id or "",
            emp.location_city,
            emp.employment_status,
            emp.termination_date.isoformat() if emp.termination_date else "",
            emp.job_effective_date.isoformat(),
            emp.ats_candidate_id or "",
        ]
    )
    return buffer.getvalue()


def _write_gzip(path: Path, content: bytes) -> None:
    """Deterministic gzip (no timestamp, no embedded name), written atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".hris-", suffix=".tmp")
    try:
        with (
            os.fdopen(fd, "wb") as raw,
            gzip.GzipFile(
                filename="", mode="wb", fileobj=raw, mtime=0, compresslevel=GZIP_LEVEL
            ) as gz,
        ):
            gz.write(content)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
