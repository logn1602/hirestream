"""ATS silver: `silver.ats_*`, the source's current state, rebuilt from the current baseline
(SPEC §9.2, ADR-0022).

The extract (ADR-0021) lands every table once per extract and commits a record naming the
directories that are complete. The current baseline is the latest full extract and the
incrementals after it. A rebuild reads the baseline's directories and keeps:
- candidates, requisitions, applications, offers: one row per primary key, the latest
  `updated_at`, a tie going to the later extract. The overlap re-reads rows, so ties are common,
  and if the source ever changed a row without moving `updated_at`, the later copy is the truth.
- application_stage_changes: one row per `change_id`, the first extract's copy, as stream silver
  keeps the earliest copy of an event. Partitioned by `change_date` (SPEC §9.1).
Candidates lose their names, phone and email; `email_hash` replaces the email (`pii.py`). Rows
are never quarantined: the source enforces types, enums, keys and foreign keys, and integrity
across extracts is for DQ to report (S-ATS-01 to -03), not for silver to hide by dropping rows.

Why a rebuild rather than a merge into the existing tables: Parquet has no row-level MERGE, so an
unpartitioned table is rewritten either way, and a merge would read the table it overwrites.
Reading only bronze also makes every incremental run equal a bulk one, and a new baseline drops
rows a reloaded source no longer has. The cost is reading the whole baseline each time.

A run, in order:
1. If its record exists, it already finished: make sure its touched file exists, and stop.
2. Read the committed extracts; with none, stop. Then check the salt against the one this lake's
   hashes were made with, before anything is deleted or written.
3. Bulk only: delete the ATS silver tables and records, which a rebuild replaces.
4. Incremental: if every extract of the current baseline is already in a record of that baseline
   and every table exists, nothing changed: commit a record saying so.
5. Otherwise rebuild the five tables from the baseline, checking that bronze holds exactly the
   rows the extract records say it does.
6. Commit the record (extracts, counts, touched dates, salt fingerprint), then write the touched
   file from it.
A crash before step 6 leaves no record, so the retry rebuilds again: harmless, because a rebuild
is a function of the baseline.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Final

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from hirestream.transforms.common import pii
from hirestream.transforms.common.ats import (
    TABLES,
    AtsTable,
    ExtractRecord,
    committed_records,
    current_baseline,
    extract_id,
)
from hirestream.transforms.common.lake import Lake
from hirestream.transforms.common.lineage import (
    ARRIVAL_TS,
    INGESTED_AT,
    RUN_ID,
    SOURCE_FILE,
    RunContext,
    with_run_lineage,
)
from hirestream.transforms.common.manifest import MODES, Mode, record_key
from hirestream.transforms.common.pii import PiiKey

SOURCE: Final = "ats"
SILVER_TABLES: Final[Mapping[str, str]] = {  # source table -> silver table (SPEC §9.2)
    "candidates": "ats_candidates",
    "requisitions": "ats_requisitions",
    "applications": "ats_applications",
    "offers": "ats_offers",
    "application_stage_changes": "ats_stage_changes",
}
DROPPED: Final = ("first_name", "last_name", "email", "phone")  # candidates' PII (SPEC §9.1)
EMAIL_HASH: Final = "email_hash"
CHANGE_DATE: Final = "change_date"
RECORD_VERSION: Final = 1
_NEW: Final = "_new"  # the row came from an extract no earlier record of the baseline built from
_SPILL = StorageLevel.MEMORY_AND_DISK


class AtsSilverError(RuntimeError):
    """ATS silver can't run: nothing was extracted yet, or bronze doesn't match its records."""


def silver_schema(table: AtsTable) -> StructType:
    """A silver table: the source's columns with the PII replaced, then lineage (SPEC §9.1), and
    the partition column last."""
    fields: list[StructField] = []
    for column in table.schema.fields:
        if table.name == "candidates" and column.name in DROPPED:
            if column.name == "email":
                fields.append(StructField(EMAIL_HASH, StringType(), True))  # null when blank
            continue
        fields.append(column)
    fields += [
        StructField(SOURCE_FILE, StringType(), False),  # the bronze Parquet file
        StructField(ARRIVAL_TS, TimestampType(), False),  # its extract's extract_ts
        StructField(RUN_ID, StringType(), False),
        StructField(INGESTED_AT, TimestampType(), False),
    ]
    if table.append_only:
        fields.append(StructField(CHANGE_DATE, DateType(), False))
    return StructType(fields)


SCHEMAS: Final[Mapping[str, StructType]] = {t.name: silver_schema(t) for t in TABLES}


@dataclass(frozen=True)
class AtsSilverRecord:
    """What a run committed: `state/bronze_manifest/ats/<run_id>.json`."""

    run_id: str
    mode: Mode
    as_of: datetime
    baseline: str  # the id of the baseline's full extract
    baseline_changed: bool  # no earlier record was built from this baseline
    extracts: tuple[str, ...]  # the baseline's extracts, oldest first: what the tables hold
    new_extracts: tuple[str, ...]  # the ones no earlier record of this baseline built from
    rebuilt: bool
    salt_fingerprint: str
    counts: Mapping[str, Mapping[str, int]] = field(default_factory=dict)  # read, written, ...
    touched: Mapping[str, tuple[date, ...]] = field(default_factory=dict)  # silver table -> dates

    def to_json(self) -> dict[str, Any]:
        return {
            "version": RECORD_VERSION,
            "run_id": self.run_id,
            "source": SOURCE,
            "mode": self.mode,
            "as_of": self.as_of.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "baseline": self.baseline,
            "baseline_changed": self.baseline_changed,
            "extracts": list(self.extracts),
            "new_extracts": list(self.new_extracts),
            "rebuilt": self.rebuilt,
            "salt_fingerprint": self.salt_fingerprint,
            "counts": {table: dict(counts) for table, counts in self.counts.items()},
            "touched": {
                table: [d.isoformat() for d in days] for table, days in self.touched.items()
            },
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> AtsSilverRecord:
        if data.get("version") != RECORD_VERSION:
            raise ValueError(f"unknown ATS silver record version {data.get('version')!r}")
        return cls(
            run_id=data["run_id"],
            mode=data["mode"],
            as_of=datetime.fromisoformat(data["as_of"].replace("Z", "+00:00")),
            baseline=data["baseline"],
            baseline_changed=data["baseline_changed"],
            extracts=tuple(data["extracts"]),
            new_extracts=tuple(data["new_extracts"]),
            rebuilt=data["rebuilt"],
            salt_fingerprint=data["salt_fingerprint"],
            counts={table: dict(counts) for table, counts in data["counts"].items()},
            touched={
                table: tuple(date.fromisoformat(d) for d in days)
                for table, days in data["touched"].items()
            },
        )


@dataclass(frozen=True)
class AtsSilverRun:
    record: AtsSilverRecord
    skipped: bool  # the run had already finished: nothing was done


def records(lake: Lake) -> list[AtsSilverRecord]:
    """Every committed ATS silver record, in run-id order."""
    found = lake.read_json_files(lake.paths.state("bronze_manifest", SOURCE))
    return sorted((AtsSilverRecord.from_json(v) for v in found), key=lambda r: r.run_id)


def run(
    lake: Lake, key: PiiKey, run_context: RunContext, *, mode: Mode, as_of: datetime
) -> AtsSilverRun:
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
    committed = record_key(lake, SOURCE, run_context.run_id)
    if lake.exists(committed):
        record = AtsSilverRecord.from_json(lake.read_json(committed))
        _write_touched(lake, record)
        return AtsSilverRun(record, skipped=True)
    baseline = current_baseline(committed_records(lake))
    if not baseline:
        raise AtsSilverError(
            "no committed ATS extract to build from: run `hirestream ingest ats` first"
        )
    pii.register(lake, key)  # before anything is deleted or written
    if mode == "bulk":
        for table in SILVER_TABLES.values():
            lake.delete_tree(lake.paths.silver(table))
        lake.delete_tree(lake.paths.state("bronze_manifest", SOURCE))
    ids = tuple(extract_id(r.extract_ts) for r in baseline)
    built = {e for r in records(lake) if r.baseline == ids[0] for e in r.extracts}
    new = tuple(i for i in ids if i not in built)
    missing = any(not lake.exists(lake.paths.silver(t)) for t in SILVER_TABLES.values())
    rebuild = mode == "bulk" or bool(new) or missing
    counts, touched = _rebuild(lake, key, run_context, baseline, set(new)) if rebuild else ({}, {})
    record = AtsSilverRecord(
        run_id=run_context.run_id,
        mode=mode,
        as_of=as_of,
        baseline=ids[0],
        baseline_changed=not built,
        extracts=ids,
        new_extracts=new,
        rebuilt=rebuild,
        salt_fingerprint=key.fingerprint,
        counts=counts,
        touched=touched,
    )
    lake.write_new_json(committed, record.to_json())  # the commit point
    _write_touched(lake, record)
    return AtsSilverRun(record, skipped=False)


def _rebuild(
    lake: Lake,
    key: PiiKey,
    run_context: RunContext,
    baseline: Sequence[ExtractRecord],
    new: set[str],
) -> tuple[dict[str, dict[str, int]], dict[str, tuple[date, ...]]]:
    counts: dict[str, dict[str, int]] = {}
    touched: dict[str, tuple[date, ...]] = {}
    for table in TABLES:
        name = SILVER_TABLES[table.name]
        rows = _read(lake, table, baseline, new).persist(_SPILL)
        try:
            # One job gives both the row count and the dates of the rows from new extracts.
            day = F.to_date("changed_at" if table.append_only else "updated_at")
            summary = rows.groupBy(F.col(_NEW).alias("new"), day.alias("day")).count().collect()
            read = sum(int(r["count"]) for r in summary)
            expected = sum(r.tables[table.name].rows for r in baseline)
            if read != expected:
                raise AtsSilverError(
                    f"bronze holds {read:,} {table.name} rows for the baseline, but its extract "
                    f"records say {expected:,}: bronze is append-only, so something changed it"
                )
            silver = _to_silver(_current(rows, table), table, key, run_context)
            target = lake.paths.silver(name)
            partition = [CHANGE_DATE] if table.append_only else []
            lake.write_table(silver, target, partition_by=partition, sort_by=[table.key])
            written = lake.read_parquet(target, SCHEMAS[table.name]).count()  # Parquet metadata
            counts[name] = {"read": read, "written": written, "superseded": read - written}
            touched[name] = tuple(sorted({r.day for r in summary if r.new}))
        finally:
            rows.unpersist()
    return counts, touched


def _read(
    lake: Lake, table: AtsTable, baseline: Sequence[ExtractRecord], new: set[str]
) -> DataFrame:
    """Every row the baseline landed for `table`, with its extract's time as `_arrival_ts` and
    whether that extract is new to silver."""
    landed = [
        (entry.key, r.extract_ts, extract_id(r.extract_ts) in new)
        for r in baseline
        if (entry := r.tables[table.name]).key is not None
    ]
    rows = lake.read_parquet_files([key for key, _, _ in landed], table.schema)
    origin = lake.spark.createDataFrame(
        landed, f"_dir string, {ARRIVAL_TS} timestamp, {_NEW} boolean"
    )
    directory = F.regexp_replace(F.col(SOURCE_FILE), "/[^/]+$", "")
    return rows.join(F.broadcast(origin), directory == F.col("_dir")).drop("_dir")


def _current(rows: DataFrame, table: AtsTable) -> DataFrame:
    """One row per key: the latest version of a mutable row, the first copy of a stage change."""
    if table.append_only:
        order = [F.col(ARRIVAL_TS).asc()]
    else:
        order = [F.col("updated_at").desc(), F.col(ARRIVAL_TS).desc()]
    rank = F.row_number().over(Window.partitionBy(table.key).orderBy(*order))
    return rows.withColumn("_rank", rank).where(F.col("_rank") == 1).drop("_rank")


def _to_silver(
    current: DataFrame, table: AtsTable, key: PiiKey, run_context: RunContext
) -> DataFrame:
    df = current
    if table.name == "candidates":
        df = df.withColumn(EMAIL_HASH, key.email_hash(F.col("email")))
    if table.append_only:
        df = df.withColumn(CHANGE_DATE, F.to_date("changed_at"))
    return with_run_lineage(df, run_context).select(*SCHEMAS[table.name].fieldNames())


def _write_touched(lake: Lake, record: AtsSilverRecord) -> None:
    """SPEC §9.1's record for gold (ADR-0019 §9's layout). Per silver table, the dates its rows
    from new extracts fall on; reporting a date too many only costs gold a rebuild. A changed
    baseline means rows may also have vanished, so gold rebuilds what it took from ATS silver."""
    lake.write_once_json(
        lake.paths.state("touched_partitions", record.run_id, f"{SOURCE}.json"),
        {
            "run_id": record.run_id,
            "source": SOURCE,
            "mode": record.mode,
            "baseline_changed": record.baseline_changed,
            "tables": {
                lake.paths.silver(name): {
                    ("change_dates" if name == "ats_stage_changes" else "updated_dates"): [
                        d.isoformat() for d in days
                    ]
                }
                for name, days in record.touched.items()
            },
        },
    )
