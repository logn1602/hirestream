"""ATS silver (ADR-0022) on hand-made extracts. One module-scoped history drives most tests: a
full extract; an incremental one with an overlap copy, an update, a row the source changed without
moving `updated_at`, and a stage change read twice; a crashed extract's orphan directory; a run
with nothing new; a crashed run and its retry; a bulk rebuild of the same bronze in a second lake;
and last, a new full extract from a reloaded source."""

import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession

from hirestream.transforms import silver_ats
from hirestream.transforms.common.ats import TABLES, ExtractRecord, bronze_key
from hirestream.transforms.common.lake import Lake, LakePaths
from hirestream.transforms.common.lineage import RunContext, content_fingerprint
from hirestream.transforms.common.manifest import Mode, record_key
from hirestream.transforms.common.pii import PiiKey, PiiKeyError
from hirestream.transforms.silver_ats import (
    SCHEMAS,
    SILVER_TABLES,
    AtsSilverError,
    AtsSilverRecord,
    AtsSilverRun,
)
from tests.pii import TEST_KEY, expected_email_hash
from tests.unit.transforms.ats_bronze import (
    application,
    candidate,
    change,
    land,
    offer,
    requisition,
)

E1 = datetime(2025, 3, 2, tzinfo=UTC)  # a full extract
E2 = E1 + timedelta(hours=1)
ORPHAN = E2 + timedelta(minutes=30)  # an extract that landed its rows and crashed
E3 = E1 + timedelta(hours=2)
E4 = E1 + timedelta(hours=3)  # a full extract after the source was reloaded
ID = {"e1": "20250302T000000Z", "e2": "20250302T010000Z", "e3": "20250302T020000Z",
      "e4": "20250302T030000Z"}  # fmt: skip
AS_OF = datetime(2025, 3, 2, 6, tzinfo=UTC)
STARTED = datetime(2026, 1, 1, tzinfo=UTC)
OTHER_KEY = PiiKey("another-salt-of-at-least-thirty-two-chars")
MARCH_1 = date(2025, 3, 1)


def t(hour: int, minute: int = 0) -> datetime:
    """A naive UTC source timestamp on 2025-03-01."""
    return datetime(2025, 3, 1, hour, minute)


Tables = dict[str, dict[object, Row]]


@dataclass
class History:
    lake: Lake
    bulk_lake: Lake
    extracts: dict[str, ExtractRecord]
    runs: dict[str, AtsSilverRun]
    tables: dict[str, Tables]  # moment -> silver table -> key -> row
    fingerprints: dict[str, dict[str, str]]


def run(
    lake: Lake, run_id: str, mode: Mode = "incremental", key: PiiKey = TEST_KEY
) -> AtsSilverRun:
    return silver_ats.run(lake, key, RunContext(run_id, STARTED), mode=mode, as_of=AS_OF)


def tables(lake: Lake) -> Tables:
    out = {}
    for table in TABLES:
        name = SILVER_TABLES[table.name]
        rows = lake.read_parquet(lake.paths.silver(name), SCHEMAS[table.name]).collect()
        out[name] = {row[table.key]: row for row in rows}
    return out


def fingerprints(lake: Lake) -> dict[str, str]:
    return {
        name: content_fingerprint(lake.read_parquet(lake.paths.silver(name), SCHEMAS[source]))
        for source, name in SILVER_TABLES.items()
    }


@pytest.fixture(scope="module")
def history(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> History:
    lake = Lake(spark, LakePaths.from_root(tmp_path_factory.mktemp("ats-incremental")))
    extracts: dict[str, ExtractRecord] = {}
    runs: dict[str, AtsSilverRun] = {}
    snapshots: dict[str, Tables] = {}
    prints: dict[str, dict[str, str]] = {}

    extracts["e1"] = land(lake, E1, {
        "candidates": [candidate("c1", " Ann@X.Example\t", t(9)),
                       candidate("c2", "bo@x.example", t(10), internal="E7")],
        "requisitions": [requisition("r1", t(8))],
        "applications": [application("a1", "c1", t(9, 30)), application("a2", "c2", t(10))],
        "offers": [offer("o1", t(9, 45))],
        "application_stage_changes": [change(1, datetime(2025, 2, 28, 23)),
                                      change(2, t(9, 30), to_stage="recruiter_screen")],
    }, mode="full")  # fmt: skip
    runs["s1"] = run(lake, "s1")
    runs["s1-retry"] = run(lake, "s1")
    snapshots["s1"] = tables(lake)

    extracts["e2"] = land(lake, E2, {
        "candidates": [candidate("c2", "bo@x.example", t(10), internal="E7")],  # overlap copy
        "requisitions": [requisition("r1", t(8), title="Senior Engineer")],  # same updated_at
        "applications": [application("a1", "c1", t(11), status="hired", stage="offer")],
        "application_stage_changes": [change(2, t(9, 30), to_stage="recruiter_screen"),  # again
                                      change(3, t(11), to_stage="offer")],
    })  # fmt: skip
    land(lake, ORPHAN, {"candidates": [candidate("c9", "zed@x.example", t(12))]}, commit=False)
    runs["s2"] = run(lake, "s2")
    snapshots["s2"] = tables(lake)
    prints["s2"] = fingerprints(lake)
    runs["s3"] = run(lake, "s3")  # no new extract
    prints["s3"] = fingerprints(lake)

    extracts["e3"] = land(lake, E3, {
        "offers": [offer("o1", t(12), status="accepted")],
        "application_stage_changes": [change(4, datetime(2025, 3, 2, 1))],
    })  # fmt: skip
    runs["s4"] = run(lake, "s4")
    prints["s4"] = fingerprints(lake)
    # A crash after the writes but before the commit: no record, no touched file.
    lake.delete_tree(record_key(lake, "ats", "s4"))
    lake.delete_tree(lake.paths.state("touched_partitions", "s4"))
    runs["s4-after-crash"] = run(lake, "s4")
    prints["s4-after-crash"] = fingerprints(lake)

    # The same bronze and extract records (orphan included), rebuilt in bulk in a second lake.
    root = Path(lake.paths.root.removeprefix("file:"))
    bulk_root = Path(tmp_path_factory.mktemp("ats-bulk"))
    shutil.copytree(root / "bronze", bulk_root / "bronze")
    shutil.copytree(root / "state" / "ats_watermarks", bulk_root / "state" / "ats_watermarks")
    bulk_lake = Lake(spark, LakePaths.from_root(bulk_root))
    runs["b1"] = run(bulk_lake, "b1", "bulk")
    prints["bulk"] = fingerprints(bulk_lake)

    extracts["e4"] = land(lake, E4, {
        "candidates": [candidate("c2", "bo@x.example", t(10), internal="E7")],
        "requisitions": [requisition("r1", t(8))],
        "applications": [application("a2", "c2", t(10))],
    }, mode="full")  # fmt: skip
    runs["s5"] = run(lake, "s5")
    snapshots["s5"] = tables(lake)
    return History(lake, bulk_lake, extracts, runs, snapshots, prints)


def test_the_first_run_builds_every_table_from_the_full_extract(history: History) -> None:
    record = history.runs["s1"].record
    assert (record.baseline, record.baseline_changed, record.rebuilt) == (ID["e1"], True, True)
    assert record.extracts == record.new_extracts == (ID["e1"],)
    assert record.salt_fingerprint == TEST_KEY.fingerprint
    assert record.counts == {
        "ats_candidates": {"read": 2, "written": 2, "superseded": 0},
        "ats_requisitions": {"read": 1, "written": 1, "superseded": 0},
        "ats_applications": {"read": 2, "written": 2, "superseded": 0},
        "ats_offers": {"read": 1, "written": 1, "superseded": 0},
        "ats_stage_changes": {"read": 2, "written": 2, "superseded": 0},
    }
    assert {name: set(rows) for name, rows in history.tables["s1"].items()} == {
        "ats_candidates": {"c1", "c2"},
        "ats_requisitions": {"r1"},
        "ats_applications": {"a1", "a2"},
        "ats_offers": {"o1"},
        "ats_stage_changes": {1, 2},
    }


def test_candidates_lose_their_pii_and_keep_a_hashed_email(history: History) -> None:
    columns = SCHEMAS["candidates"].fieldNames()
    assert not {"first_name", "last_name", "email", "phone"} & set(columns)
    assert columns[:2] == ["candidate_id", "email_hash"]  # the hash takes the email's place
    c1, c2 = (history.tables["s1"]["ats_candidates"][c] for c in ("c1", "c2"))
    assert c1.email_hash == expected_email_hash("ann@x.example")  # tab, spaces and case stripped
    assert c2.email_hash == expected_email_hash("bo@x.example")
    assert (c2.is_internal, c2.employee_id, c2.location_city) == (True, "E7", "Seattle")


def test_lineage_names_the_bronze_file_and_its_extract(history: History) -> None:
    row = history.tables["s1"]["ats_offers"]["o1"]
    assert row._source_file.startswith(f"{bronze_key('offers', E1)}/part-")
    assert row._source_file.endswith(".parquet") and history.lake.exists(row._source_file)
    assert (row._arrival_ts, row._run_id) == (E1.replace(tzinfo=None), "s1")
    assert row._ingested_at == STARTED.replace(tzinfo=None)


def test_the_latest_version_of_each_row_wins(history: History) -> None:
    s2 = history.tables["s2"]
    a1 = s2["ats_applications"]["a1"]
    assert (a1.status, a1.current_stage, a1._arrival_ts) == (
        "hired",
        "offer",
        E2.replace(tzinfo=None),
    )
    # A tie on updated_at goes to the later extract: an overlap copy, and a row the source
    # changed without moving updated_at, whose later copy is the truth.
    assert s2["ats_candidates"]["c2"]._arrival_ts == E2.replace(tzinfo=None)
    assert s2["ats_requisitions"]["r1"].title == "Senior Engineer"
    assert s2["ats_candidates"]["c1"]._arrival_ts == E1.replace(tzinfo=None)  # only in E1


def test_a_stage_change_keeps_its_first_copy(history: History) -> None:
    changes = history.tables["s2"]["ats_stage_changes"]
    assert set(changes) == {1, 2, 3}
    assert changes[2]._arrival_ts == E1.replace(tzinfo=None)  # read again in E2, kept from E1
    assert changes[1].change_date == date(2025, 2, 28)


def test_an_orphan_directory_is_never_read(history: History) -> None:
    assert history.lake.exists(bronze_key("candidates", ORPHAN))
    for moment in ("s2", "s5"):
        assert "c9" not in history.tables[moment]["ats_candidates"]


def test_counts_reconcile_and_name_what_was_superseded(history: History) -> None:
    for name, result in history.runs.items():
        for table, c in result.record.counts.items():
            assert c["read"] == c["written"] + c["superseded"], (name, table)
    assert history.runs["s2"].record.counts == {
        "ats_candidates": {"read": 3, "written": 2, "superseded": 1},
        "ats_requisitions": {"read": 2, "written": 1, "superseded": 1},
        "ats_applications": {"read": 3, "written": 2, "superseded": 1},
        "ats_offers": {"read": 1, "written": 1, "superseded": 0},
        "ats_stage_changes": {"read": 4, "written": 3, "superseded": 1},
    }


def test_touched_dates_come_from_the_new_extracts(history: History) -> None:
    assert history.runs["s1"].record.touched["ats_stage_changes"] == (date(2025, 2, 28), MARCH_1)
    assert history.runs["s2"].record.touched == {
        "ats_candidates": (MARCH_1,),
        "ats_requisitions": (MARCH_1,),
        "ats_applications": (MARCH_1,),
        "ats_offers": (),  # E2 had no offer rows
        "ats_stage_changes": (MARCH_1,),
    }
    lake = history.lake
    assert lake.read_json(lake.paths.state("touched_partitions", "s4", "ats.json")) == {
        "run_id": "s4",
        "source": "ats",
        "mode": "incremental",
        "baseline_changed": False,
        "tables": {
            "silver/ats_candidates": {"updated_dates": []},
            "silver/ats_requisitions": {"updated_dates": []},
            "silver/ats_applications": {"updated_dates": []},
            "silver/ats_offers": {"updated_dates": ["2025-03-01"]},
            "silver/ats_stage_changes": {"change_dates": ["2025-03-02"]},
        },
    }


def test_a_run_with_nothing_new_rebuilds_nothing(history: History) -> None:
    record = history.runs["s3"].record
    assert (record.rebuilt, record.new_extracts, record.counts, record.touched) == (
        False,
        (),
        {},
        {},
    )
    assert record.extracts == (ID["e1"], ID["e2"]) and not record.baseline_changed
    assert history.fingerprints["s3"] == history.fingerprints["s2"]
    lake = history.lake
    touched = lake.read_json(lake.paths.state("touched_partitions", "s3", "ats.json"))
    assert (touched["baseline_changed"], touched["tables"]) == (False, {})


def test_a_finished_run_is_not_run_again(history: History) -> None:
    retry = history.runs["s1-retry"]
    assert retry.skipped and retry.record == history.runs["s1"].record


def test_a_run_retried_after_a_crash_rebuilds_the_same_tables(history: History) -> None:
    rerun = history.runs["s4-after-crash"]
    assert not rerun.skipped
    assert rerun.record == history.runs["s4"].record  # same new extract, counts and touched dates
    assert history.fingerprints["s4-after-crash"] == history.fingerprints["s4"]


def test_incremental_runs_equal_one_bulk_run(history: History) -> None:
    bulk = history.runs["b1"].record
    assert bulk.new_extracts == (ID["e1"], ID["e2"], ID["e3"]) and bulk.baseline_changed
    assert history.fingerprints["bulk"] == history.fingerprints["s4-after-crash"]


def test_stage_changes_are_partitioned_by_change_date(history: History) -> None:
    lake = history.bulk_lake
    files = [f.key for f in lake.list_files(lake.paths.silver("ats_stage_changes"))]
    parts = sorted({key.split("/")[2] for key in files if key.endswith(".parquet")})
    assert parts == ["change_date=2025-02-28", "change_date=2025-03-01", "change_date=2025-03-02"]


def test_a_new_baseline_replaces_everything(history: History) -> None:
    record = history.runs["s5"].record
    assert (record.baseline, record.baseline_changed) == (ID["e4"], True)
    assert record.extracts == record.new_extracts == (ID["e4"],)
    # The reloaded source no longer has c1, a1, o1 or any stage change: neither does silver.
    assert {name: set(rows) for name, rows in history.tables["s5"].items()} == {
        "ats_candidates": {"c2"},
        "ats_requisitions": {"r1"},
        "ats_applications": {"a2"},
        "ats_offers": set(),
        "ats_stage_changes": set(),
    }


def test_the_record_round_trips(history: History) -> None:
    record = history.runs["s2"].record
    assert AtsSilverRecord.from_json(record.to_json()) == record
    on_disk = history.lake.read_json(record_key(history.lake, "ats", "s2"))
    assert on_disk == record.to_json() and on_disk["as_of"] == "2025-03-02T06:00:00Z"
    with pytest.raises(ValueError, match="unknown ATS silver record version"):
        AtsSilverRecord.from_json({**on_disk, "version": 99})


def test_another_salt_is_refused_before_anything_is_written(history: History) -> None:
    with pytest.raises(PiiKeyError, match="not the salt"):
        run(history.lake, "s6", key=OTHER_KEY)
    assert not history.lake.exists(record_key(history.lake, "ats", "s6"))


def test_a_lake_with_no_extract_has_nothing_to_build(lake: Lake) -> None:
    with pytest.raises(AtsSilverError, match="run `hirestream ingest ats` first"):
        run(lake, "nothing")
    assert not lake.exists("state/pii")  # no salt is pinned by a run that hashed nothing


def test_a_bulk_run_with_another_salt_deletes_nothing(history: History) -> None:
    lake = history.bulk_lake
    with pytest.raises(PiiKeyError):
        run(lake, "b2", "bulk", key=OTHER_KEY)
    assert history.runs["b1"].record.rebuilt and lake.exists(lake.paths.silver("ats_offers"))
    assert lake.exists(record_key(lake, "ats", "b1"))


def test_bronze_that_disagrees_with_its_record_stops_the_run(
    spark: SparkSession, lake: Lake
) -> None:
    land(lake, E1, {"candidates": [candidate("c1", "ann@x.example", t(9))]}, mode="full")
    extra = spark.createDataFrame(
        [candidate("c8", "eve@x.example", t(9))], TABLES[0].schema
    )  # written behind the extract's back
    extra.write.mode("append").parquet(lake.paths.uri(bronze_key("candidates", E1)))
    with pytest.raises(AtsSilverError, match=r"bronze holds 2 candidates rows .* records say 1"):
        run(lake, "tampered")


def test_an_unknown_mode_is_refused(lake: Lake) -> None:
    with pytest.raises(ValueError, match="mode must be one of"):
        run(lake, "x", mode="sideways")  # type: ignore[arg-type]
