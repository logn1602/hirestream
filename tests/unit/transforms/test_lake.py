import gzip
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, LongType, StringType, StructField, StructType

from hirestream.transforms.common.lake import LINE_SCHEMA, FileInfo, Lake, LakePaths
from hirestream.transforms.common.lineage import ARRIVAL_TS, SOURCE_FILE

ARRIVAL_MS = 1_767_229_200_000  # 2026-01-01T01:00:00Z
TABLE = StructType(
    [
        StructField("event_id", StringType()),
        StructField("n", LongType()),
        StructField("event_date", DateType()),
    ]
)


def put(lake: Lake, key: str, lines: list[str], mtime_ms: int = ARRIVAL_MS) -> Path:
    """A bronze-style file at `key`: gzip when the key ends in .gz, with a set mtime."""
    path = Path(lake.paths.uri(key).removeprefix("file:"))
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "".join(f"{line}\n" for line in lines).encode()
    path.write_bytes(gzip.compress(body, mtime=0) if key.endswith(".gz") else body)
    os.utime(path, ns=(mtime_ms * 1_000_000, mtime_ms * 1_000_000))
    return path


# -- LakePaths --------------------------------------------------------------------------------


def test_local_roots_become_absolute_file_uris(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    assert LakePaths.from_root("data/lake").root == f"file:{tmp_path}/data/lake"
    assert LakePaths.from_root(tmp_path / "lake/").root == f"file:{tmp_path}/lake"
    assert LakePaths.from_root(f"file://{tmp_path}/lake").root == f"file:{tmp_path}/lake"
    assert LakePaths.from_root(f"file:{tmp_path}/a/../lake").root == f"file:{tmp_path}/lake"


def test_object_store_roots_are_kept() -> None:
    paths = LakePaths.from_root("s3a://hirestream-lake/prefix/")
    assert paths.root == "s3a://hirestream-lake/prefix"
    assert paths.uri(paths.silver("scheduling_events")) == (
        "s3a://hirestream-lake/prefix/silver/scheduling_events"
    )
    with pytest.raises(ValueError, match="names no bucket"):
        LakePaths.from_root("s3a://")


def test_layer_keys_and_their_round_trip() -> None:
    paths = LakePaths.from_root("/lake")
    assert paths.bronze("scheduling") == "bronze/scheduling"
    assert paths.gold("fct_interview") == "gold/fct_interview"
    assert paths.quarantine("jobboard") == "quarantine/jobboard"
    assert paths.state("bronze_manifest", "scheduling") == "state/bronze_manifest/scheduling"
    key = "bronze/hris/snapshot_date=2026-01-01/employees_20260101.csv.gz"
    assert paths.key(paths.uri(key)) == key


@pytest.mark.parametrize("key", ["", "/bronze", "bronze/", "bronze//x", "bronze/../state", "."])
def test_invalid_keys_are_rejected(key: str) -> None:
    with pytest.raises(ValueError, match="invalid lake key"):
        LakePaths.from_root("/lake").uri(key)


def test_unknown_layers_and_foreign_uris_are_rejected() -> None:
    paths = LakePaths.from_root("/lake")
    with pytest.raises(ValueError, match="unknown layer"):
        paths.layer("platinum", "x")
    for uri in ("file:/other/bronze", "file:/lake", "file:/lake/", "file:/lakehouse/x"):
        with pytest.raises(ValueError, match="not under the lake root"):
            paths.key(uri)


# -- filesystem -------------------------------------------------------------------------------


def test_list_files_skips_what_spark_skips(lake: Lake) -> None:
    prefix = lake.paths.bronze("scheduling")
    visible = [
        f"{prefix}/yyyy=2026/mm=01/dd=01/hh=01/part-00000.jsonl.gz",
        f"{prefix}/yyyy=2026/mm=01/dd=01/hh=00/part-00001.jsonl.gz",
        f"{prefix}/_partition=1/part-00000.jsonl.gz",  # "_" with "=" is a partition directory
    ]
    hidden = [
        f"{prefix}/yyyy=2026/mm=01/dd=01/hh=01/.part-00002.jsonl.gz.tmp",
        f"{prefix}/yyyy=2026/mm=01/dd=01/hh=01/_SUCCESS",
        f"{prefix}/_firehose_errors/part-00000.jsonl.gz",
    ]
    sizes = {}
    for i, key in enumerate(visible + hidden):
        sizes[key] = put(lake, key, ["{}"] * (i + 1), mtime_ms=ARRIVAL_MS + i).stat().st_size
    found = lake.list_files(prefix)
    assert [f.key for f in found] == sorted(visible)
    assert found[0] == FileInfo(visible[2], size=sizes[visible[2]], mtime_ms=ARRIVAL_MS + 2)
    assert lake.list_files(f"{prefix}/_firehose_errors")[0].key == hidden[2]  # asked for
    assert lake.list_files(lake.paths.bronze("jobboard")) == []


def test_state_files_are_written_once(lake: Lake) -> None:
    key = lake.paths.state("touched_partitions", "run-1.json")
    value = {"scheduling": ["2026-01-02", "2026-01-01"], "a": 1}
    assert not lake.exists(key)
    lake.write_new_json(key, value)
    assert lake.exists(key)
    assert lake.read_json(key) == value
    assert lake.read_text(key).startswith('{\n  "a": 1,')  # sorted keys: stable bytes
    with pytest.raises(FileExistsError):
        lake.write_new_json(key, {})
    assert lake.read_json(key) == value
    assert [f.key for f in lake.list_files(lake.paths.state())] == [key]  # no temp left behind


class _LateWriter:
    """A filesystem on which another writer creates the target after our existence check."""

    def __init__(self, fs: Any, target: Any) -> None:
        self._fs, self._target, self._checked = fs, target, False

    def exists(self, path: Any) -> bool:
        if path.equals(self._target) and not self._checked:
            self._checked = True
            return False
        return bool(self._fs.exists(path))

    def __getattr__(self, name: str) -> Any:
        return getattr(self._fs, name)


def test_a_writer_that_loses_the_race_clobbers_nothing(
    lake: Lake, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = lake.paths.state("touched_partitions", "run-1.json")
    lake.write_new_json(key, {"winner": True})
    fs, path = lake._resolve(key)
    monkeypatch.setattr(lake, "_resolve", lambda _: (_LateWriter(fs, path), path))
    with pytest.raises(FileExistsError):
        lake.write_new_json(key, {"winner": False})
    monkeypatch.undo()
    assert lake.read_json(key) == {"winner": True}
    assert [f.key for f in lake.list_files(lake.paths.state())] == [key]


# -- tables -----------------------------------------------------------------------------------


@pytest.mark.parametrize("root", ["lake", "my lake+1"])  # Spark URL-encodes the space
def test_read_lines_carries_key_and_arrival(spark: SparkSession, tmp_path: Path, root: str) -> None:
    lake = Lake(spark, LakePaths.from_root(tmp_path / root))
    gz = "bronze/scheduling/yyyy=2026/mm=01/dd=01/hh=01/part-00000.jsonl.gz"
    plain = "bronze/scheduling/yyyy=2026/mm=01/dd=01/hh=01/part-00001.jsonl"
    put(lake, gz, ['{"a":1}', "not json"])
    put(lake, plain, ['{"a":2}'], mtime_ms=ARRIVAL_MS + 1500)
    rows = sorted(lake.read_lines([f.key for f in lake.list_files("bronze")]).collect())
    assert rows == [
        ("not json", gz, datetime(2026, 1, 1, 1, 0)),
        ('{"a":1}', gz, datetime(2026, 1, 1, 1, 0)),
        ('{"a":2}', plain, datetime(2026, 1, 1, 1, 0, 1, 500_000)),
    ]


def test_read_lines_of_nothing_is_empty(lake: Lake) -> None:
    df = lake.read_lines([])
    assert df.schema == LINE_SCHEMA
    assert df.columns == ["value", SOURCE_FILE, ARRIVAL_TS]
    assert df.count() == 0


def test_read_parquet_before_the_first_write_is_empty(lake: Lake) -> None:
    df = lake.read_parquet(lake.paths.silver("missing"), TABLE)
    assert df.schema == TABLE
    assert df.count() == 0


def test_write_partitioned_replaces_only_the_partitions_it_writes(
    spark: SparkSession, lake: Lake
) -> None:
    key = lake.paths.silver("events")
    day1, day2, day3 = date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)
    first = [("a", 1, day1), ("b", 2, day2), ("c", 3, day2)]
    lake.write_partitioned(spark.createDataFrame(first, TABLE), key, ["event_date"], ["event_id"])
    rerun = [("c", 30, day2), ("b", 20, day2), ("d", 4, day3)]
    lake.write_partitioned(spark.createDataFrame(rerun, TABLE), key, ["event_date"], ["event_id"])

    rows = lake.read_parquet(key, TABLE).collect()
    assert sorted(rows) == [("a", 1, day1), ("b", 20, day2), ("c", 30, day2), ("d", 4, day3)]
    files = [f.key for f in lake.list_files(key)]
    assert [k.split("/")[2] for k in files] == [f"event_date={d}" for d in (day1, day2, day3)]
    part = spark.read.parquet(lake.paths.uri(files[1]))
    assert [r.event_id for r in part.collect()] == ["b", "c"]  # one file, rows sorted


def test_write_partitioned_needs_partition_columns(spark: SparkSession, lake: Lake) -> None:
    df = spark.createDataFrame([], TABLE)
    with pytest.raises(ValueError, match="replace the whole table"):
        lake.write_partitioned(df, lake.paths.silver("events"), [])
