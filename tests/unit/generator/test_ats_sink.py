import os
import re
import secrets
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from hirestream.generator import ats_sink
from hirestream.generator.ats import Application, Offer, StageChange
from hirestream.generator.ats_db import describe, resolve_ats_dsn
from hirestream.generator.ats_sink import (
    COLUMNS,
    DDL,
    AtsSnapshot,
    PostgresSink,
    RequisitionClock,
    csv_chunks,
    payload_digests,
    pg_ts,
    table_rows,
)
from hirestream.generator.calendar import build_calendar
from hirestream.generator.candidates import Candidate
from hirestream.generator.config import load_config
from hirestream.generator.errors import OutputExistsError
from hirestream.generator.requisitions import Requisition
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.simulation import simulate
from hirestream.generator.world import build_world

ZONES = {"Seattle": ZoneInfo("America/Los_Angeles"), "London": ZoneInfo("Europe/London")}
MS = 1_735_726_530_123  # 2025-01-01 10:15:30.123 UTC


def _snapshot() -> AtsSnapshot:
    cand = Candidate(
        "C00000001", False, None, "Seattle", date(2025, 1, 1), "Jo", "Smith, Jr",
        "jo.smith@example.com", "+1-206-555-0142", "US", MS,
    )  # fmt: skip
    req = Requisition(
        req_id="R000001", title="Senior Data Engineer", role_family="data_engineering",
        job_level="L5", org="Commerce", team="Payments Platform", location_city="London",
        headcount=1, hiring_manager_id="E000010", recruiter_id=None, status="filled",
        is_evergreen=False, is_internal_only=False, opened_on=date(2025, 1, 1),
        closed_on=date(2025, 2, 10), updated_on=date(2025, 2, 10), source="growth",
        popularity=1.0, seats_open=0,
    )  # fmt: skip
    app = Application(
        "A000000001", "C00000001", "R000001", "career_site", MS, date(2025, 1, 1), None,
        stage="offer", status="hired", status_reason="offer_accepted", updated_ms=MS + 1000,
    )  # fmt: skip
    offer = Offer(
        "O000000001", "A000000001", MS, date(2025, 2, 10), True, 40, status="accepted",
        decided_ms=MS + 500, start_date=date(2025, 3, 12), updated_ms=MS + 500,
    )  # fmt: skip
    changes = [
        StageChange(1, "A000000001", None, "applied", None, "active", "submitted", MS, "candidate"),
        StageChange(2, "A000000001", "offer", "offer", "active", "hired", "offer_accepted",
                    MS + 1000, "candidate"),
    ]  # fmt: skip
    return AtsSnapshot([cand], [req], [app], [offer], changes)


def _clock() -> RequisitionClock:
    return RequisitionClock(1602, ZONES, (9, 17))


def _lines(table: str) -> list[str]:
    rows = table_rows(_snapshot(), _clock())[table]
    return b"".join(chunk for chunk, _ in csv_chunks(rows())).decode().splitlines()


def test_rows_render_as_copy_csv() -> None:
    assert _lines("candidates") == [
        'C00000001,Jo,"Smith, Jr",jo.smith@example.com,+1-206-555-0142,Seattle,US,false,,'
        "2025-01-01 10:15:30.123,2025-01-01 10:15:30.123"
    ]  # None -> empty unquoted field (NULL); commas are quoted
    assert _lines("applications") == [
        "A000000001,C00000001,R000001,career_site,2025-01-01 10:15:30.123,offer,hired,"
        "offer_accepted,2025-01-01 10:15:31.123"
    ]
    assert _lines("offers") == [
        "O000000001,A000000001,2025-01-01 10:15:30.123,accepted,2025-01-01 10:15:30.623,"
        "2025-03-12,2025-01-01 10:15:30.623"
    ]
    assert _lines("application_stage_changes")[0] == (
        "1,A000000001,,applied,,active,submitted,2025-01-01 10:15:30.123,candidate"
    )


def test_requisition_times_are_business_hours_and_ordered() -> None:
    clock = _clock()
    req = _snapshot().requisitions[0]
    opened, closed, updated = clock.times(req)
    assert closed is not None and opened <= closed <= updated
    for ms, day in ((opened, req.opened_on), (closed, req.closed_on)):
        local = datetime.fromtimestamp(ms / 1000, UTC).astimezone(ZONES["London"])
        assert local.date() == day and 9 <= local.hour < 17
    assert clock.times(req) == (opened, closed, updated)  # deterministic
    assert RequisitionClock(7, ZONES, (9, 17)).times(req) != (opened, closed, updated)
    line = _lines("requisitions")[0].split(",")
    assert line[13] == pg_ts(opened) and line[14] == pg_ts(closed) and line[16] == pg_ts(updated)


def test_payload_digests_are_deterministic() -> None:
    a, b = payload_digests(_snapshot(), _clock()), payload_digests(_snapshot(), _clock())
    assert a == b and list(a) == list(COLUMNS)
    assert {name: e.rows for name, e in a.items()} == {
        "candidates": 1, "requisitions": 1, "applications": 1, "offers": 1,
        "application_stage_changes": 2,
    }  # fmt: skip


def test_chunking_doesnt_change_the_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    rows = [(str(i), "x") for i in range(10)]
    whole = b"".join(chunk for chunk, _ in csv_chunks(rows))
    monkeypatch.setattr(ats_sink, "CHUNK_ROWS", 3)
    chunks = list(csv_chunks(rows))
    assert [n for _, n in chunks] == [3, 3, 3, 1]
    assert b"".join(chunk for chunk, _ in chunks) == whole


def test_ddl_columns_match_the_loader() -> None:
    tables = re.findall(r"CREATE TABLE (\w+) \((.*?)\n\);", DDL.read_text(), flags=re.S)
    assert [name for name, _ in tables] == list(COLUMNS)
    for name, body in tables:
        columns = [
            line.split()[0]
            for line in body.splitlines()
            if line.strip() and not line.strip().startswith(("CONSTRAINT", "CHECK", "--"))
            and line.startswith("    ") and not line.startswith("     ")
        ]  # fmt: skip
        assert tuple(columns) == COLUMNS[name], name


def test_dsn_resolution() -> None:
    assert resolve_ats_dsn({}) is None
    assert resolve_ats_dsn({"ATS_DB_NAME": "ats", "ATS_DB_USER": "ats"}) is None
    assert resolve_ats_dsn({"HIRESTREAM_ATS_DSN": "dbname=x", "ATS_DB_NAME": "ats"}) == "dbname=x"
    dsn = resolve_ats_dsn({"ATS_DB_NAME": "ats", "ATS_DB_USER": "u", "ATS_DB_PASSWORD": "p"})
    assert dsn is not None
    assert conninfo_to_dict(dsn) == {
        "host": "127.0.0.1", "port": "15432", "dbname": "ats", "user": "u", "password": "p"
    }  # fmt: skip
    assert describe(dsn) == "127.0.0.1:15432/ats"


# ---------------------------------------------------------------------- integration

TEST_DSN = os.environ.get("HIRESTREAM_TEST_ATS_DSN")
integration = pytest.mark.skipif(TEST_DSN is None, reason="HIRESTREAM_TEST_ATS_DSN is not set")


@pytest.fixture
def fresh_db() -> Iterator[str]:
    """A throwaway database on the test server, dropped afterwards."""
    assert TEST_DSN is not None
    name = f"hs_test_{secrets.token_hex(4)}"
    with psycopg.connect(TEST_DSN, autocommit=True) as conn:
        conn.execute(f"CREATE DATABASE {name}")
    try:
        yield make_conninfo(TEST_DSN, dbname=name)
    finally:
        with psycopg.connect(TEST_DSN, autocommit=True) as conn:
            conn.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


@pytest.fixture(scope="module")
def tiny_snapshot(base_config_path: Path, tmp_path_factory: pytest.TempPathFactory) -> AtsSnapshot:
    cfg = load_config(base_config_path, "tiny")
    plan = SeedPlan(1602)
    result = simulate(
        cfg, plan, build_calendar(cfg), build_world(cfg, plan), tmp_path_factory.mktemp("lake")
    )
    return result.ats_snapshot


def _count(dsn: str, table: str) -> int:
    with psycopg.connect(dsn) as conn:
        row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()
    assert row is not None
    return int(row[0])


@pytest.mark.integration
@integration
def test_load_writes_every_table(fresh_db: str, tiny_snapshot: AtsSnapshot) -> None:
    sink = PostgresSink(fresh_db)
    sink.check()
    assert not sink.has_data()
    stats = sink.load(tiny_snapshot, _clock_for(tiny_snapshot), overwrite=False)
    assert stats == payload_digests(tiny_snapshot, _clock_for(tiny_snapshot))
    expected = {
        "candidates": len(tiny_snapshot.candidates),
        "requisitions": len(tiny_snapshot.requisitions),
        "applications": len(tiny_snapshot.applications),
        "offers": len(tiny_snapshot.offers),
        "application_stage_changes": len(tiny_snapshot.changes),
    }
    assert (
        {t: _count(fresh_db, t) for t in COLUMNS}
        == expected
        == {t: e.rows for t, e in stats.items()}
    )
    with psycopg.connect(fresh_db) as conn:  # the sequence continues after the loaded ids
        nxt = conn.execute("SELECT nextval('application_stage_changes_change_id_seq')").fetchone()
    assert nxt is not None and nxt[0] == len(tiny_snapshot.changes) + 1


@pytest.mark.integration
@integration
def test_overwrite_rules_and_atomic_replacement(fresh_db: str, tiny_snapshot: AtsSnapshot) -> None:
    sink = PostgresSink(fresh_db)
    clock = _clock_for(tiny_snapshot)
    sink.load(tiny_snapshot, clock, overwrite=False)
    with pytest.raises(OutputExistsError, match="pass --overwrite"):
        sink.ensure_writable(overwrite=False)
    with pytest.raises(OutputExistsError):
        sink.load(tiny_snapshot, clock, overwrite=False)
    broken = AtsSnapshot(  # an application whose candidate doesn't exist: the FK must refuse it
        candidates=[], requisitions=tiny_snapshot.requisitions,
        applications=tiny_snapshot.applications[:1], offers=[], changes=[],
    )  # fmt: skip
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        sink.load(broken, clock, overwrite=True)
    assert _count(fresh_db, "applications") == len(tiny_snapshot.applications)  # rolled back
    sink.load(tiny_snapshot, clock, overwrite=True)
    assert _count(fresh_db, "candidates") == len(tiny_snapshot.candidates)


def _clock_for(snapshot: AtsSnapshot) -> RequisitionClock:
    cfg = load_config(Path(__file__).parents[3] / "config" / "generator" / "base.yaml", "tiny")
    zones = {loc.city: ZoneInfo(loc.tz) for loc in cfg.org_model.locations}
    return RequisitionClock(1602, zones, cfg.scheduling.business_hours_local)


@pytest.mark.integration
@integration
def test_backfill_loads_the_ats_db(
    fresh_db: str, tmp_path: Path, base_config_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from typer.testing import CliRunner

    from hirestream.cli import app
    from hirestream.generator.manifest import read_manifest

    monkeypatch.setenv("HIRESTREAM_ATS_DSN", fresh_db)
    args = ["generate", "backfill", "--config", str(base_config_path), "--preset", "tiny",
            "--lake-root", str(tmp_path), "--run-id", "db"]  # fmt: skip
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    assert "ats-db: 5 tables loaded into" in result.output
    manifest = read_manifest(tmp_path / "_runs" / "db" / "manifest.json")
    assert manifest.ats_tables["applications"].rows == _count(fresh_db, "applications") > 0
    again = CliRunner().invoke(app, [*args[:-1], "db2"])  # without --overwrite
    assert again.exit_code == 2 and "already holds generated data" in " ".join(again.output.split())
