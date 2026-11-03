"""Running a stream source's silver job: discovery, the merge, and the run's records (ADR-0019).

A run, in order:
1. If its manifest record exists, it already finished: make sure its touched-partitions file
   exists, and stop.
2. Bulk only: delete the source's silver table, quarantine and manifest, which a rebuild replaces.
3. Discover new objects, read their lines once, and build rows and quarantine rows.
4. Silver: anti-join the batch's rows against the rows already in the partitions they fall in,
   then rewrite only the partitions that gained a row. A row already in silver wins.
5. Quarantine: the same, keyed on the line (`_source_file`, `_source_line`).
6. Commit the manifest record (objects, counts, touched partitions), then write the touched
   partitions file from it.

A crash before step 6 leaves no record, so a retry processes the objects again. The merges make
that harmless: nothing is added twice, and the touched set is a function of the batch.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from functools import reduce
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from hirestream.transforms.common import manifest
from hirestream.transforms.common.lake import Lake
from hirestream.transforms.common.lineage import SOURCE_FILE, SOURCE_LINE, RunContext
from hirestream.transforms.common.manifest import Mode, RunRecord

Build = Callable[[DataFrame, RunContext], tuple[DataFrame, DataFrame]]
_SPILL = StorageLevel.MEMORY_AND_DISK


@dataclass(frozen=True)
class StreamSource:
    """What the runner needs to know about one stream source."""

    name: str  # the folder under bronze/ and quarantine/, e.g. "scheduling"
    table: str  # the silver table, e.g. "scheduling_events"
    schema: StructType  # the silver table, partition column last
    quarantine_schema: StructType  # the quarantine table, partition column last
    build: Build  # lines -> (rows deduplicated within the batch, quarantine rows)
    key: str = "event_id"
    partition: str = "event_date"


@dataclass(frozen=True)
class SilverRun:
    record: RunRecord
    skipped: bool  # the run had already finished: nothing was done


def run(
    lake: Lake,
    source: StreamSource,
    run_context: RunContext,
    *,
    mode: Mode,
    as_of: datetime,
    window: int,
) -> SilverRun:
    committed = manifest.record_key(lake, source.name, run_context.run_id)
    if lake.exists(committed):
        record = RunRecord.from_json(lake.read_json(committed))
        _write_touched(lake, source, record)
        return SilverRun(record, skipped=True)
    if mode == "bulk":
        for key in (
            lake.paths.silver(source.table),
            lake.paths.quarantine(source.name),
            lake.paths.state("bronze_manifest", source.name),
        ):
            lake.delete_tree(key)
    objects = manifest.discover(lake, source.name, mode=mode, as_of=as_of, window=window)
    # Bronze is read once, and the batch's rows and rejects are computed once: each is used by
    # several actions (dates, anti-join, count, write).
    lines = lake.read_lines([f.key for f in objects]).persist(_SPILL)
    rows, rejected = source.build(lines, run_context)
    rows, rejected = rows.persist(_SPILL), rejected.persist(_SPILL)
    try:
        counts, touched = _merge(lake, source, lines, rows, rejected)
    finally:
        for df in (rows, rejected, lines):
            df.unpersist()
    record = RunRecord(
        run_id=run_context.run_id,
        source=source.name,
        mode=mode,
        as_of=as_of,
        objects=tuple(objects),
        counts={"objects": len(objects), **counts},
        touched=touched,
    )
    lake.write_new_json(committed, record.to_json())  # the commit point
    _write_touched(lake, source, record)
    return SilverRun(record, skipped=False)


def _merge(
    lake: Lake, source: StreamSource, lines: DataFrame, rows: DataFrame, rejected: DataFrame
) -> tuple[dict[str, Any], tuple[date, ...]]:
    batch_dates = _dates(rows, source.partition)
    new_rows = _add(
        lake,
        lake.paths.silver(source.table),
        source.schema,
        rows,
        on=[source.key],
        partition=source.partition,
        sort_by=[source.key],
        dates=batch_dates,
    )
    quarantined = {
        r.reason_code: int(r["count"]) for r in rejected.groupBy("reason_code").count().collect()
    }
    _add(
        lake,
        lake.paths.quarantine(source.name),
        source.quarantine_schema,
        rejected,
        on=[SOURCE_FILE, SOURCE_LINE],
        partition=source.partition,
        sort_by=[SOURCE_FILE, SOURCE_LINE],
        dates=_dates(rejected, source.partition),
    )
    read = lines.count()
    counts = {
        "lines": read,
        "written": new_rows,
        "duplicates": read - sum(quarantined.values()) - new_rows,
        "quarantined": dict(sorted(quarantined.items())),
    }
    return counts, tuple(d for d in batch_dates if d is not None)


def _add(
    lake: Lake,
    key: str,
    schema: StructType,
    batch: DataFrame,
    *,
    on: list[str],
    partition: str,
    sort_by: list[str],
    dates: list[date | None],
) -> int:
    """Add the batch's rows that aren't in the table yet; rewrite only the partitions they fall
    in, keeping those partitions' existing rows. Returns how many rows were added."""
    if not dates:
        return 0
    existing = lake.read_parquet(key, schema).where(_in(F.col(partition), dates))
    new = batch.join(existing.select(*on), on=on, how="left_anti").select(*schema.fieldNames())
    new = new.persist(_SPILL)
    try:
        added = new.count()
        if added:
            gained = _dates(new, partition)
            keep = existing.where(_in(F.col(partition), gained))
            lake.write_partitioned(keep.unionByName(new), key, [partition], sort_by)
    finally:
        new.unpersist()
    return added


def _dates(df: DataFrame, partition: str) -> list[date | None]:
    found = [row[0] for row in df.select(partition).distinct().collect()]
    return sorted(found, key=lambda d: (d is not None, d or date.min))


def _in(column: Column, dates: list[date | None]) -> Column:
    """`column` is one of `dates`, where a None matches a null partition."""
    known = [d for d in dates if d is not None]
    tests = [column.isin(*known)] if known else []
    if None in dates:
        tests.append(column.isNull())
    return reduce(lambda a, b: a | b, tests, F.lit(False))


def _write_touched(lake: Lake, source: StreamSource, record: RunRecord) -> None:
    """SPEC §9.1's record for gold, one file per source in the run's folder (ADR-0019 §9)."""
    lake.write_once_json(
        lake.paths.state("touched_partitions", record.run_id, f"{source.name}.json"),
        {
            "run_id": record.run_id,
            "source": source.name,
            "table": lake.paths.silver(source.table),
            "mode": record.mode,
            f"{source.partition}s": [d.isoformat() for d in record.touched],  # "event_dates"
        },
    )
