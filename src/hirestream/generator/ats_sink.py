"""Bulk-load the ATS's final state into Postgres with COPY (SPEC §6.6, §7.4, ADR-0008).

Rows are formatted as CSV here, not by the driver, so the exact bytes sent to COPY are known and
hashed into the run manifest: the determinism check then covers database contents too. The load
runs in one transaction, so `--overwrite` replaces the tables atomically.
"""

from __future__ import annotations

import csv
import hashlib
import io
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg

from hirestream.generator.ats import Application, Offer, StageChange
from hirestream.generator.ats_db import AtsDbError, describe
from hirestream.generator.candidates import Candidate
from hirestream.generator.errors import OutputExistsError
from hirestream.generator.events import iso_utc_ms
from hirestream.generator.manifest import TableEntry
from hirestream.generator.requisitions import Requisition

DDL = Path(__file__).resolve().parents[3] / "sql" / "ats_source" / "001_schema.sql"
CHUNK_ROWS = 20_000

Row = Sequence[str | None]

# Load order follows the foreign keys; each table's columns match sql/ats_source/001_schema.sql.
COLUMNS: dict[str, tuple[str, ...]] = {
    "candidates": (
        "candidate_id", "first_name", "last_name", "email", "phone", "location_city",
        "location_country", "is_internal", "employee_id", "created_at", "updated_at",
    ),
    "requisitions": (
        "req_id", "title", "role_family", "job_level", "org", "team", "location_city", "headcount",
        "hiring_manager_id", "recruiter_id", "status", "is_evergreen", "is_internal_only",
        "opened_at", "closed_at", "created_at", "updated_at",
    ),
    "applications": (
        "application_id", "candidate_id", "req_id", "source_channel", "applied_at",
        "current_stage", "status", "status_reason", "updated_at",
    ),
    "offers": (
        "offer_id", "application_id", "extended_at", "status", "decided_at", "start_date",
        "updated_at",
    ),
    "application_stage_changes": (
        "change_id", "application_id", "from_stage", "to_stage", "from_status", "to_status",
        "reason", "changed_at", "changed_by",
    ),
}  # fmt: skip


@dataclass(frozen=True)
class AtsSnapshot:
    """The ATS's final state after a backfill: what goes into the source database."""

    candidates: Sequence[Candidate]
    requisitions: Sequence[Requisition]
    applications: Sequence[Application]
    offers: Sequence[Offer]
    changes: Sequence[StageChange]


class RequisitionClock:
    """Times of day for requisitions, which only track dates (ADR-0006).

    A time in business hours in the req's city, derived from a hash of (seed, req_id, field): no
    random stream is consumed, so adding the sink changes no generator output.
    """

    def __init__(self, seed: int, zones: Mapping[str, ZoneInfo], hours: tuple[int, int]) -> None:
        self._seed = seed
        self._zones = zones
        self._lo, self._hi = hours[0] * 3_600_000, hours[1] * 3_600_000

    def at(self, req: Requisition, field: str, day: date) -> int:
        digest = hashlib.sha256(f"{self._seed}:{req.req_id}:{field}".encode()).digest()
        offset = self._lo + int.from_bytes(digest[:8], "big") % (self._hi - self._lo)
        local = datetime(day.year, day.month, day.day, tzinfo=self._zones[req.location_city])
        return int(local.timestamp() * 1000) + offset

    def times(self, req: Requisition) -> tuple[int, int | None, int]:
        """(opened_at, closed_at, updated_at), ordered so updated >= closed >= opened."""
        opened = self.at(req, "opened", req.opened_on)
        closed = (
            None if req.closed_on is None else max(opened, self.at(req, "closed", req.closed_on))
        )
        updated = max(self.at(req, "updated", req.updated_on), closed or opened)
        return opened, closed, updated


def pg_ts(epoch_ms: int | None) -> str | None:
    """Naive UTC timestamp text, e.g. 2025-01-01 10:15:30.123 (SPEC §7.4)."""
    return None if epoch_ms is None else iso_utc_ms(epoch_ms)[:-1].replace("T", " ")


def _bool(value: bool) -> str:
    return "true" if value else "false"


def table_rows(
    snapshot: AtsSnapshot, clock: RequisitionClock
) -> dict[str, Callable[[], Iterator[Row]]]:
    """Each table's rows, in load order, as lazy generators (memory stays flat at full)."""

    def candidates() -> Iterator[Row]:
        for c in snapshot.candidates:
            yield (c.candidate_id, c.first_name, c.last_name, c.email, c.phone, c.location_city,
                   c.location_country, _bool(c.is_internal), c.employee_id, pg_ts(c.created_ms),
                   pg_ts(c.created_ms))  # fmt: skip

    def requisitions() -> Iterator[Row]:
        for r in snapshot.requisitions:
            opened, closed, updated = clock.times(r)
            yield (r.req_id, r.title, r.role_family, r.job_level, r.org, r.team, r.location_city,
                   str(r.headcount), r.hiring_manager_id, r.recruiter_id, r.status,
                   _bool(r.is_evergreen), _bool(r.is_internal_only), pg_ts(opened), pg_ts(closed),
                   pg_ts(opened), pg_ts(updated))  # fmt: skip

    def applications() -> Iterator[Row]:
        for a in snapshot.applications:
            yield (a.application_id, a.candidate_id, a.req_id, a.channel, pg_ts(a.applied_ms),
                   a.stage, a.status, a.status_reason, pg_ts(a.updated_ms))  # fmt: skip

    def offers() -> Iterator[Row]:
        for o in snapshot.offers:
            yield (o.offer_id, o.application_id, pg_ts(o.extended_ms), o.status,
                   pg_ts(o.decided_ms), o.start_date.isoformat() if o.start_date else None,
                   pg_ts(o.updated_ms))  # fmt: skip

    def changes() -> Iterator[Row]:
        for s in snapshot.changes:
            yield (str(s.change_id), s.application_id, s.from_stage, s.to_stage, s.from_status,
                   s.to_status, s.reason, pg_ts(s.changed_ms), s.changed_by)  # fmt: skip

    return {
        "candidates": candidates,
        "requisitions": requisitions,
        "applications": applications,
        "offers": offers,
        "application_stage_changes": changes,
    }


def csv_chunks(rows: Iterable[Row]) -> Iterator[tuple[bytes, int]]:
    """CSV for COPY: None becomes an empty unquoted field (NULL). Yields (bytes, row count)."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    count = 0
    for row in rows:
        writer.writerow(row)
        count += 1
        if count == CHUNK_ROWS:
            yield buffer.getvalue().encode("utf-8"), count
            buffer.seek(0)
            buffer.truncate()
            count = 0
    if count:
        yield buffer.getvalue().encode("utf-8"), count


def payload_digests(snapshot: AtsSnapshot, clock: RequisitionClock) -> dict[str, TableEntry]:
    """Row counts and payload hashes without a database (what `load` would record)."""
    stats = {}
    for table, rows in table_rows(snapshot, clock).items():
        digest, total = hashlib.sha256(), 0
        for chunk, count in csv_chunks(rows()):
            digest.update(chunk)
            total += count
        stats[table] = TableEntry(rows=total, sha256=digest.hexdigest())
    return stats


class PostgresSink:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def check(self) -> None:
        """Fail fast, before a long simulation, if the ats-db can't be reached."""
        try:
            with psycopg.connect(self._dsn, connect_timeout=5) as conn:
                conn.execute("SELECT 1")
        except psycopg.Error as exc:
            raise AtsDbError(
                f"cannot reach the ats-db at {describe(self._dsn)} ({type(exc).__name__}); "
                "start it with `make up`, or pass --skip-ats-db"
            ) from exc

    def has_data(self) -> bool:
        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            return self._has_rows(cur)

    def ensure_writable(self, overwrite: bool) -> None:
        """Refuse before simulating if the tables already hold a run (ADR-0005 §7)."""
        if not overwrite and self.has_data():
            raise OutputExistsError(self._occupied())

    def load(
        self, snapshot: AtsSnapshot, clock: RequisitionClock, overwrite: bool
    ) -> dict[str, TableEntry]:
        """Replace the ATS tables with `snapshot` in one transaction; return counts and hashes."""
        stats = {}
        with psycopg.connect(self._dsn) as conn, conn.cursor() as cur:
            if not overwrite and self._has_rows(cur):
                raise OutputExistsError(self._occupied())
            cur.execute("DROP TABLE IF EXISTS " + ", ".join(reversed(COLUMNS)) + " CASCADE")
            cur.execute(DDL.read_text())
            for table, rows in table_rows(snapshot, clock).items():
                digest, total = hashlib.sha256(), 0
                columns = ", ".join(COLUMNS[table])
                with cur.copy(f"COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv)") as copy:
                    for chunk, count in csv_chunks(rows()):
                        copy.write(chunk)
                        digest.update(chunk)
                        total += count
                stats[table] = TableEntry(rows=total, sha256=digest.hexdigest())
            cur.execute(
                "SELECT setval(pg_get_serial_sequence('application_stage_changes', 'change_id'),"
                " COALESCE(max(change_id), 1), max(change_id) IS NOT NULL)"
                " FROM application_stage_changes"
            )
        return stats

    def _occupied(self) -> str:
        return f"the ats-db at {describe(self._dsn)} already holds generated data; pass --overwrite"

    @staticmethod
    def _has_rows(cur: psycopg.Cursor[tuple[object, ...]]) -> bool:
        row = cur.execute("SELECT to_regclass('requisitions') IS NOT NULL").fetchone()
        if not row or not row[0]:
            return False
        row = cur.execute("SELECT EXISTS (SELECT 1 FROM requisitions)").fetchone()
        return bool(row and row[0])
