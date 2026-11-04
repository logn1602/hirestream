"""ATS silver on tiny's real ATS (ADR-0022): the generator's snapshot loaded into Postgres,
extracted and built into silver; then changed in the source, extracted again and rebuilt
incrementally. Each time, every silver row must equal its source row after the PII rule, and the
incremental tables must equal a bulk rebuild of the same bronze."""

import shutil
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
import pytest
from pyspark.sql import SparkSession

from hirestream.generator.ats_sink import PostgresSink, RequisitionClock
from hirestream.generator.config import load_config
from hirestream.generator.run import BackfillResult
from hirestream.ingest.ats_extract import extract
from hirestream.transforms import silver_ats
from hirestream.transforms.common.ats import TABLES
from hirestream.transforms.common.lake import Lake, LakePaths
from hirestream.transforms.common.lineage import RunContext, content_fingerprint
from hirestream.transforms.common.manifest import Mode
from hirestream.transforms.silver_ats import CHANGE_DATE, SCHEMAS, SILVER_TABLES, AtsSilverRun
from tests.pg import integration, throwaway_db
from tests.pii import TEST_KEY, expected_email_hash

pytestmark = [pytest.mark.integration, integration]

STARTED = datetime(2026, 1, 1, tzinfo=UTC)
Rows = dict[Any, tuple[Any, ...]]  # key -> row
Snapshot = dict[str, Rows]  # silver table -> rows


def _source(dsn: str) -> Snapshot:
    """The source tables as silver should hold them: candidates without names or phone, and the
    email replaced by its hash. Keyed in Python, so no collation decides an order."""
    out = {}
    with psycopg.connect(dsn) as conn:
        for table in TABLES:
            rows = conn.execute(f"SELECT {', '.join(table.columns)} FROM {table.name}").fetchall()
            if table.name == "candidates":
                rows = [(r[0], expected_email_hash(r[3]), *r[5:]) for r in rows]
            out[SILVER_TABLES[table.name]] = {r[0]: tuple(r) for r in rows}
    return out


def _silver(lake: Lake) -> Snapshot:
    out = {}
    for table in TABLES:
        schema = SCHEMAS[table.name]
        columns = [c for c in schema.fieldNames() if not c.startswith("_") and c != CHANGE_DATE]
        df = lake.read_parquet(lake.paths.silver(SILVER_TABLES[table.name]), schema)
        out[SILVER_TABLES[table.name]] = {r[0]: tuple(r) for r in df.select(*columns).collect()}
    return out


def _fingerprints(lake: Lake) -> dict[str, str]:
    return {
        name: content_fingerprint(lake.read_parquet(lake.paths.silver(name), SCHEMAS[source]))
        for source, name in SILVER_TABLES.items()
    }


def _silver_run(lake: Lake, run_id: str, mode: Mode = "incremental") -> AtsSilverRun:
    return silver_ats.run(lake, TEST_KEY, RunContext(run_id, STARTED), mode=mode, as_of=STARTED)


def _one(conn: psycopg.Connection[tuple[Any, ...]], query: str) -> tuple[Any, ...]:
    row = conn.execute(query).fetchone()
    assert row is not None
    return row


def _change_the_source(dsn: str) -> str:
    """What a day in the ATS does: an application withdrawn, with its stage change; a
    requisition's headcount raised; a candidate's email re-typed in capitals with a trailing
    space, which must not change its hash. Returns that candidate's id."""
    with psycopg.connect(dsn) as conn:
        app_id, stage, when = _one(
            conn,
            "SELECT application_id, current_stage,"
            " (SELECT max(updated_at) FROM applications) + interval '1 hour'"
            " FROM applications WHERE status = 'active' ORDER BY application_id LIMIT 1",
        )
        conn.execute(
            "UPDATE applications SET status = 'withdrawn', updated_at = %s"
            " WHERE application_id = %s",
            (when, app_id),
        )
        conn.execute(
            "INSERT INTO application_stage_changes (application_id, from_stage, to_stage,"
            " from_status, to_status, reason, changed_at, changed_by)"
            " VALUES (%s, %s, %s, 'active', 'withdrawn', 'candidate_withdrew', %s, 'system')",
            (app_id, stage, stage, when),
        )
        (req_id,) = _one(conn, "SELECT req_id FROM requisitions ORDER BY req_id LIMIT 1")
        conn.execute(
            "UPDATE requisitions SET headcount = headcount + 1,"
            " updated_at = (SELECT max(updated_at) + interval '1 hour' FROM requisitions)"
            " WHERE req_id = %s",
            (req_id,),
        )
        (candidate_id,) = _one(
            conn, "SELECT candidate_id FROM candidates ORDER BY candidate_id LIMIT 1"
        )
        conn.execute(
            "UPDATE candidates SET email = upper(email) || ' ',"
            " updated_at = (SELECT max(updated_at) + interval '1 hour' FROM candidates)"
            " WHERE candidate_id = %s",
            (candidate_id,),
        )
    return str(candidate_id)


@dataclass
class Tiny:
    retyped: str = ""  # the candidate whose email changed case
    source: dict[str, Snapshot] = field(default_factory=dict)  # moment -> what silver should hold
    silver: dict[str, Snapshot] = field(default_factory=dict)  # moment -> what it holds
    runs: dict[str, AtsSilverRun] = field(default_factory=dict)
    fingerprints: dict[str, dict[str, str]] = field(default_factory=dict)


@pytest.fixture(scope="module")
def tiny(
    backfill: BackfillResult,
    base_config_path: Path,
    tmp_path_factory: pytest.TempPathFactory,
    spark: SparkSession,
) -> Iterator[Tiny]:
    config = load_config(base_config_path, "tiny")
    zones = {loc.city: ZoneInfo(loc.tz) for loc in config.org_model.locations}
    clock = RequisitionClock(backfill.manifest.seed, zones, config.scheduling.business_hours_local)
    with throwaway_db() as dsn:
        PostgresSink(dsn).load(backfill.simulation.ats_snapshot, clock, overwrite=True)
        root = Path(tmp_path_factory.mktemp("tiny-ats"))
        lake = Lake(spark, LakePaths.from_root(root))
        tiny = Tiny()
        extract(lake, dsn, "x1")  # the first extract is full
        tiny.runs["s1"] = _silver_run(lake, "s1")
        tiny.source["s1"], tiny.silver["s1"] = _source(dsn), _silver(lake)

        tiny.retyped = _change_the_source(dsn)
        time.sleep(1.05)  # extracts are named by the second they start in
        extract(lake, dsn, "x2")
        tiny.runs["s2"] = _silver_run(lake, "s2")
        tiny.source["s2"], tiny.silver["s2"] = _source(dsn), _silver(lake)
        tiny.fingerprints["incremental"] = _fingerprints(lake)

        bulk_root = Path(tmp_path_factory.mktemp("tiny-ats-bulk"))
        shutil.copytree(root / "bronze", bulk_root / "bronze")
        shutil.copytree(root / "state" / "ats_watermarks", bulk_root / "state" / "ats_watermarks")
        bulk = Lake(spark, LakePaths.from_root(bulk_root))
        tiny.runs["bulk"] = _silver_run(bulk, "bulk", "bulk")
        tiny.fingerprints["bulk"] = _fingerprints(bulk)
        yield tiny


def test_silver_holds_the_source_after_a_full_extract(tiny: Tiny) -> None:
    assert tiny.silver["s1"] == tiny.source["s1"]
    counts = tiny.runs["s1"].record.counts
    assert {t: c["written"] for t, c in counts.items()} == {
        t: len(rows) for t, rows in tiny.source["s1"].items()
    }
    assert all(c["superseded"] == 0 for c in counts.values())  # one extract, no copies


def test_silver_follows_the_source_through_an_incremental_extract(tiny: Tiny) -> None:
    assert tiny.silver["s2"] == tiny.source["s2"] != tiny.source["s1"]
    record = tiny.runs["s2"].record
    assert record.rebuilt and len(record.new_extracts) == 1 and not record.baseline_changed
    # The overlap re-read rows from the hour before each watermark: superseded, not duplicated.
    assert sum(c["superseded"] for c in record.counts.values()) > 0
    changes = (len(tiny.source[m]["ats_stage_changes"]) for m in ("s1", "s2"))
    assert next(changes) + 1 == next(changes)


def test_a_retyped_email_keeps_its_hash(tiny: Tiny) -> None:
    before, after = (tiny.silver[m]["ats_candidates"][tiny.retyped] for m in ("s1", "s2"))
    assert before[1] == after[1] is not None  # the same email_hash
    assert before[-1] < after[-1]  # updated_at moved: the row was re-read and replaced


def test_incremental_runs_equal_a_bulk_rebuild(tiny: Tiny) -> None:
    assert tiny.fingerprints["bulk"] == tiny.fingerprints["incremental"]
