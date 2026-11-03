"""The stream silver runner on hand-made scheduling bronze. One module-scoped history drives most
tests: three hourly batches, a late object, a retried run and a crashed one, then a bulk rebuild of
the same bronze in a second lake. Each run is a dozen small Spark jobs, so runs are shared."""

import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession

from hirestream.transforms import silver_scheduling
from hirestream.transforms.common import manifest, stream_silver
from hirestream.transforms.common.lake import Lake, LakePaths
from hirestream.transforms.common.lineage import RunContext, content_fingerprint
from hirestream.transforms.common.manifest import BronzeChangedError, Mode
from hirestream.transforms.common.stream_silver import SilverRun, StreamSource
from tests.unit.transforms.bronze import event_at, example, put_object

H1 = datetime(2025, 3, 1, 10, tzinfo=UTC)
H2 = H1 + timedelta(hours=1)
H3 = datetime(2025, 3, 2, 0, tzinfo=UTC)  # the next UTC day
STARTED = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass
class History:
    lake: Lake
    bulk_lake: Lake
    source: StreamSource
    events: dict[str, dict[str, Any]]
    keys: dict[str, str]
    runs: dict[str, SilverRun]
    fingerprints: dict[str, tuple[str, str]]  # moment -> (silver, quarantine)


def run(
    lake: Lake, source: StreamSource, run_id: str, mode: Mode, as_of: datetime, window: int = 2
) -> SilverRun:
    return stream_silver.run(
        lake,
        source,
        RunContext(run_id, STARTED),
        mode=mode,
        as_of=as_of,
        window=window,
    )


def tables(lake: Lake, source: StreamSource) -> tuple[str, str]:
    silver = lake.read_parquet(lake.paths.silver(source.table), source.schema)
    rejected = lake.read_parquet(lake.paths.quarantine(source.name), source.quarantine_schema)
    return content_fingerprint(silver), content_fingerprint(rejected)


@pytest.fixture(scope="module")
def history(spark: SparkSession, tmp_path_factory: pytest.TempPathFactory) -> History:
    lake = Lake(spark, LakePaths.from_root(tmp_path_factory.mktemp("incremental")))
    source = silver_scheduling.stream_source(spark)
    e = {name: event_at(H1 + timedelta(minutes=5 * i)) for i, name in enumerate("abc", start=1)}
    e["c"] = event_at(H2 + timedelta(minutes=5))
    e["d"] = event_at(H3 + timedelta(minutes=5))
    e["e"] = event_at(H1 + timedelta(minutes=20))
    bad_enum = example(
        "interview_cancelled",
        event_ts="2025-03-01T11:06:00.000Z",
        sent_ts="2025-03-01T11:06:00.000Z",
        payload__reason="BAD",
    )
    keys = {
        "h1": put_object(lake, H1, [e["a"], e["a"], e["b"], '{"event_id": "trunc']),
        "h2": put_object(lake, H2, [e["c"], e["a"], bad_enum]),
        "h3": put_object(lake, H3, [e["d"]]),
    }
    runs = {
        "r1": run(lake, source, "r1", "incremental", H1 + timedelta(hours=1)),
        "r2": run(lake, source, "r2", "incremental", H2 + timedelta(hours=1)),
    }
    # An object lands in H1's prefix after r2 listed it, as Firehose can deliver late. Its mtime
    # is when it was written, so it is also the latest copy of anything it repeats.
    keys["late"] = put_object(lake, H1, [e["e"], e["c"]], part=1, arrived=H2 + timedelta(hours=1.5))
    runs["r3"] = run(lake, source, "r3", "incremental", H3 + timedelta(hours=1), window=24)
    runs["r3-retry"] = run(lake, source, "r3", "incremental", H3 + timedelta(hours=1), window=24)
    fingerprints = {"before-crash": tables(lake, source)}
    # A crash after the writes but before the commit: no record, no touched file.
    lake.delete_tree(manifest.record_key(lake, source.name, "r3"))
    lake.delete_tree(lake.paths.state("touched_partitions", "r3"))
    runs["r3-after-crash"] = run(
        lake, source, "r3", "incremental", H3 + timedelta(hours=1), window=24
    )
    fingerprints["after-crash"] = tables(lake, source)

    bulk_root = Path(tmp_path_factory.mktemp("bulk"))
    shutil.copytree(Path(lake.paths.root.removeprefix("file:")) / "bronze", bulk_root / "bronze")
    bulk_lake = Lake(spark, LakePaths.from_root(bulk_root))
    runs["b1"] = run(bulk_lake, source, "b1", "bulk", H3 + timedelta(hours=1))
    fingerprints["bulk"] = tables(bulk_lake, source)
    return History(lake, bulk_lake, source, e, keys, runs, fingerprints)


def test_each_run_takes_only_the_objects_it_has_not_seen(history: History) -> None:
    objects = {name: [f.key for f in r.record.objects] for name, r in history.runs.items()}
    k = history.keys
    assert objects["r1"] == [k["h1"]]
    assert objects["r2"] == [k["h2"]]  # h1 is in the window, but already processed
    assert objects["r3"] == sorted([k["late"], k["h3"]])  # the late object, still in the window


def test_counts_reconcile_for_every_run(history: History) -> None:
    for name, result in history.runs.items():
        c = result.record.counts
        assert c["lines"] == c["written"] + c["duplicates"] + sum(c["quarantined"].values()), name
    assert history.runs["r1"].record.counts == {
        "objects": 1,
        "lines": 4,
        "written": 2,
        "duplicates": 1,
        "quarantined": {"MALFORMED_JSON": 1},
    }
    r2 = history.runs["r2"].record.counts
    assert (r2["written"], r2["duplicates"], r2["quarantined"]) == (1, 1, {"INVALID_ENUM": 1})


def test_a_row_already_in_silver_wins_over_a_later_copy(history: History) -> None:
    silver = history.lake.read_parquet(
        history.lake.paths.silver(history.source.table), history.source.schema
    )
    rows = {row.event_id: row for row in silver.collect()}
    assert set(rows) == {history.events[n]["event_id"] for n in "abcde"}
    a, c = rows[history.events["a"]["event_id"]], rows[history.events["c"]["event_id"]]
    assert (a._source_file, a._source_line, a._run_id) == (history.keys["h1"], 1, "r1")
    assert (c._source_file, c._run_id) == (history.keys["h2"], "r2")


def test_touched_partitions_cover_the_batch_and_name_the_run(history: History) -> None:
    lake = history.lake
    assert history.runs["r2"].record.touched == (date(2025, 3, 1),)
    assert history.runs["r3"].record.touched == (date(2025, 3, 1), date(2025, 3, 2))
    assert lake.read_json(lake.paths.state("touched_partitions", "r3", "scheduling.json")) == {
        "run_id": "r3",
        "source": "scheduling",
        "table": "silver/scheduling_events",
        "mode": "incremental",
        "event_dates": ["2025-03-01", "2025-03-02"],
    }


def test_a_finished_run_is_not_run_again(history: History) -> None:
    retry = history.runs["r3-retry"]
    assert retry.skipped and retry.record == history.runs["r3"].record


def test_a_run_retried_after_a_crash_adds_nothing_twice(history: History) -> None:
    rerun = history.runs["r3-after-crash"].record
    assert not history.runs["r3-after-crash"].skipped
    assert (rerun.counts["written"], rerun.counts["duplicates"]) == (0, 3)
    assert rerun.touched == history.runs["r3"].record.touched  # gold still hears about them
    assert history.fingerprints["after-crash"] == history.fingerprints["before-crash"]


def test_incremental_runs_equal_one_bulk_run(history: History) -> None:
    assert history.runs["b1"].record.counts["written"] == 5
    assert history.fingerprints["bulk"] == history.fingerprints["after-crash"]


def test_a_null_partition_holds_lines_without_a_date(history: History) -> None:
    lake, source = history.lake, history.source
    rejected = lake.read_parquet(lake.paths.quarantine(source.name), source.quarantine_schema)
    found = {(row.reason_code, row.event_date) for row in rejected.collect()}
    assert found == {("MALFORMED_JSON", None), ("INVALID_ENUM", date(2025, 3, 1))}


def test_bulk_rebuilds_from_what_bronze_holds_now(history: History) -> None:
    lake, source = history.bulk_lake, history.source
    Path(lake.paths.uri(history.keys["h3"]).removeprefix("file:")).unlink()  # regenerated bronze
    rebuilt = run(lake, source, "b2", "bulk", H3 + timedelta(hours=1))
    silver = lake.read_parquet(lake.paths.silver(source.table), source.schema)
    assert history.events["d"]["event_id"] not in {row.event_id for row in silver.collect()}
    assert rebuilt.record.counts["written"] == 4
    assert [r.run_id for r in manifest.records(lake, source.name)] == ["b2"]


def test_a_processed_object_that_changes_stops_incremental_runs(
    spark: SparkSession, tmp_path: Path
) -> None:
    lake = Lake(spark, LakePaths.from_root(tmp_path))
    source = silver_scheduling.stream_source(spark)
    put_object(lake, H1, [event_at(H1)])
    run(lake, source, "r1", "incremental", H1 + timedelta(hours=1))
    put_object(lake, H1, [event_at(H1), event_at(H1)])  # rewritten: bronze is append-only
    with pytest.raises(BronzeChangedError, match="rebuild with --mode bulk"):
        run(lake, source, "r2", "incremental", H1 + timedelta(hours=1))


def test_an_unexpected_bronze_key_stops_the_run(spark: SparkSession, tmp_path: Path) -> None:
    lake = Lake(spark, LakePaths.from_root(tmp_path))
    stray = Path(lake.paths.uri("bronze/scheduling/stray.jsonl.gz").removeprefix("file:"))
    stray.parent.mkdir(parents=True)
    stray.write_bytes(b"{}\n")  # an empty file has no split, so Spark never lists it
    with pytest.raises(ValueError, match="unexpected bronze key"):
        run(lake, silver_scheduling.stream_source(spark), "b1", "bulk", H1)
