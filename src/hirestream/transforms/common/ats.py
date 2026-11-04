"""The ATS extract's tables and its per-extract records (SPEC §7.4, §8, ADR-0021).

Shared by the extract (`hirestream.ingest.ats_extract`, which talks to Postgres) and ATS silver
(a Spark job), so it imports only the standard library, pyspark and the lake.

An extract lands each table under `bronze/ats/<table>/extract_ts=YYYYMMDDTHHMMSSZ/`, then writes
`state/ats_watermarks/<extract_ts>.json`. That record is the extract's commit point: it holds the
watermark each table reached and which of the extract's directories hold rows. A directory
without a record is an extract that crashed before committing, and nothing reads it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, Literal

from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hirestream.transforms.common.lake import Lake

ExtractMode = Literal["full", "incremental"]
EXTRACT_MODES: Final = ("full", "incremental")
EXTRACT_TS_FORMAT: Final = "%Y%m%dT%H%M%SZ"
OVERLAP_HOURS: Final = 1  # SPEC §8: updated_at > watermark - 1 h catches in-flight transactions
RECORD_VERSION: Final = 1


@dataclass(frozen=True)
class AtsTable:
    """One source table: its key, the column its watermark follows, and its bronze schema."""

    name: str
    key: str
    cursor: Literal["updated_at", "change_id"]
    schema: StructType

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(field.name for field in self.schema.fields)

    @property
    def append_only(self) -> bool:
        return self.cursor == "change_id"


def _schema(*fields: tuple[str, DataType, bool]) -> StructType:
    return StructType([StructField(name, kind, nullable) for name, kind, nullable in fields])


S, B, N, L, T, D = (
    StringType(),
    BooleanType(),
    IntegerType(),
    LongType(),
    TimestampType(),
    DateType(),
)

# Columns in sql/ats_source/001_schema.sql order; nullability as the DDL declares it. Bronze keeps
# the vendor's columns unchanged (PII included); silver drops and hashes (SPEC §9.1).
TABLES: Final[tuple[AtsTable, ...]] = (
    AtsTable("candidates", "candidate_id", "updated_at", _schema(
        ("candidate_id", S, False), ("first_name", S, False), ("last_name", S, False),
        ("email", S, False), ("phone", S, True), ("location_city", S, False),
        ("location_country", S, False), ("is_internal", B, False), ("employee_id", S, True),
        ("created_at", T, False), ("updated_at", T, False),
    )),
    AtsTable("requisitions", "req_id", "updated_at", _schema(
        ("req_id", S, False), ("title", S, False), ("role_family", S, False),
        ("job_level", S, False), ("org", S, False), ("team", S, False),
        ("location_city", S, False), ("headcount", N, False), ("hiring_manager_id", S, False),
        ("recruiter_id", S, True), ("status", S, False), ("is_evergreen", B, False),
        ("is_internal_only", B, False), ("opened_at", T, False), ("closed_at", T, True),
        ("created_at", T, False), ("updated_at", T, False),
    )),
    AtsTable("applications", "application_id", "updated_at", _schema(
        ("application_id", S, False), ("candidate_id", S, False), ("req_id", S, False),
        ("source_channel", S, False), ("applied_at", T, False), ("current_stage", S, False),
        ("status", S, False), ("status_reason", S, True), ("updated_at", T, False),
    )),
    AtsTable("offers", "offer_id", "updated_at", _schema(
        ("offer_id", S, False), ("application_id", S, False), ("extended_at", T, False),
        ("status", S, False), ("decided_at", T, True), ("start_date", D, True),
        ("updated_at", T, False),
    )),
    AtsTable("application_stage_changes", "change_id", "change_id", _schema(
        ("change_id", L, False), ("application_id", S, False), ("from_stage", S, True),
        ("to_stage", S, False), ("from_status", S, True), ("to_status", S, False),
        ("reason", S, False), ("changed_at", T, False), ("changed_by", S, False),
    )),
)  # fmt: skip
TABLES_BY_NAME: Final[Mapping[str, AtsTable]] = {table.name: table for table in TABLES}

Watermark = datetime | int  # max updated_at (naive UTC in the source, aware here) or change_id


def extract_id(extract_ts: datetime) -> str:
    """`20261103T142501Z`: the name of an extract's directories and of its record."""
    return extract_ts.astimezone(UTC).strftime(EXTRACT_TS_FORMAT)


def bronze_key(table: str, extract_ts: datetime) -> str:
    return f"bronze/ats/{table}/extract_ts={extract_id(extract_ts)}"


def record_key(extract_ts: datetime) -> str:
    return f"state/ats_watermarks/{extract_id(extract_ts)}.json"


RECORD_PREFIX: Final = "state/ats_watermarks"


@dataclass(frozen=True)
class TableExtract:
    """What one extract pulled from one table."""

    rows: int
    watermark_from: Watermark | None  # the previous watermark (None: everything was pulled)
    watermark: Watermark | None  # max cursor value seen so far (None: the table is still empty)
    key: str | None  # the bronze directory holding the rows; None when no rows were pulled

    def to_json(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "watermark_from": _wm_json(self.watermark_from),
            "watermark": _wm_json(self.watermark),
            "key": self.key,
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> TableExtract:
        return cls(
            rows=int(data["rows"]),
            watermark_from=_wm_from_json(data["watermark_from"]),
            watermark=_wm_from_json(data["watermark"]),
            key=data["key"],
        )


@dataclass(frozen=True)
class ExtractRecord:
    """The commit record of one extract: `state/ats_watermarks/<extract_ts>.json`."""

    extract_ts: datetime  # the source transaction's start, UTC
    run_id: str
    mode: ExtractMode  # full: a new baseline that ignores earlier watermarks
    source_ids: Mapping[str, int]  # each table's Postgres oid: a reloaded source changes them
    tables: Mapping[str, TableExtract]

    def to_json(self) -> dict[str, Any]:
        return {
            "version": RECORD_VERSION,
            "extract_ts": _iso(self.extract_ts),
            "run_id": self.run_id,
            "mode": self.mode,
            "overlap_hours": OVERLAP_HOURS,
            "source_ids": dict(self.source_ids),
            "tables": {name: entry.to_json() for name, entry in self.tables.items()},
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> ExtractRecord:
        if data.get("version") != RECORD_VERSION:
            raise ValueError(f"unknown ATS watermark record version {data.get('version')!r}")
        mode = data["mode"]
        if mode not in EXTRACT_MODES:
            raise ValueError(f"unknown extract mode {mode!r}")
        return cls(
            extract_ts=_parse_iso(data["extract_ts"]),
            run_id=data["run_id"],
            mode=mode,
            source_ids={k: int(v) for k, v in data["source_ids"].items()},
            tables={k: TableExtract.from_json(v) for k, v in data["tables"].items()},
        )


def check_monotonic(records: Sequence[ExtractRecord]) -> None:
    """S-ATS-04 on the committed history: extract times strictly increase, and within a baseline
    (a full extract and the incrementals after it) no table's watermark ever moves back."""
    previous: ExtractRecord | None = None
    for record in records:
        if previous is not None:
            if record.extract_ts <= previous.extract_ts:
                raise ValueError(
                    f"extract {extract_id(record.extract_ts)} is not after "
                    f"{extract_id(previous.extract_ts)}"
                )
            if record.mode == "incremental":
                for name, entry in record.tables.items():
                    before = previous.tables[name].watermark
                    if before is not None and not _at_least(entry.watermark, before):
                        raise ValueError(
                            f"{name}'s watermark moved back in extract "
                            f"{extract_id(record.extract_ts)}: {before} -> {entry.watermark}"
                        )
        previous = record


def committed_records(lake: Lake) -> list[ExtractRecord]:
    """Every committed extract, oldest first, checked for monotonic watermarks (S-ATS-04)."""
    records = [
        ExtractRecord.from_json(lake.read_json(info.key))
        for info in lake.list_files(RECORD_PREFIX)
        if info.key.endswith(".json")
    ]
    records.sort(key=lambda r: r.extract_ts)
    check_monotonic(records)
    return records


def current_baseline(records: Sequence[ExtractRecord]) -> list[ExtractRecord]:
    """The latest full extract and every extract after it: what silver is built from. Earlier
    extracts came from a source that has since been reloaded (or simply re-baselined)."""
    for index in range(len(records) - 1, -1, -1):
        if records[index].mode == "full":
            return list(records[index:])
    return []


def _at_least(value: Watermark | None, floor: Watermark) -> bool:
    if isinstance(value, datetime) and isinstance(floor, datetime):
        return value >= floor
    if isinstance(value, int) and isinstance(floor, int):
        return value >= floor
    return False  # missing, or a timestamp where an id was recorded (or the reverse)


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_iso(text: str) -> datetime:
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"{text!r} has no zone")
    return parsed.astimezone(UTC)


def _wm_json(value: Watermark | None) -> str | int | None:
    if isinstance(value, datetime):
        return _iso(value)
    return value


def _wm_from_json(value: str | int | None) -> Watermark | None:
    if isinstance(value, str):
        return _parse_iso(value)
    return value
