"""Hand-made ATS extracts for silver's tests, landed the way `hirestream ingest ats` lands them
(ADR-0021): a bronze directory per table with rows, then the commit record. Rows are tuples in
each table's column order; timestamps are naive UTC, as the source stores them."""

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any

from hirestream.transforms.common.ats import (
    TABLES,
    ExtractRecord,
    TableExtract,
    Watermark,
    bronze_key,
    committed_records,
    extract_id,
    record_key,
)
from hirestream.transforms.common.lake import Lake

Row = tuple[Any, ...]
SOURCE_IDS = {table.name: 16000 + i for i, table in enumerate(TABLES)}


def land(
    lake: Lake,
    at: datetime,
    rows: Mapping[str, Sequence[Row]],
    *,
    mode: str = "incremental",
    commit: bool = True,
) -> ExtractRecord:
    """One extract started at `at`. Its watermarks follow the extract's rule: the largest cursor
    value seen, never moving back within a baseline. Without `commit`, the directories land but
    the record doesn't: an extract that crashed."""
    history = committed_records(lake)
    previous = history[-1] if history and mode == "incremental" else None
    tables = {}
    for table in TABLES:
        got = list(rows.get(table.name, ()))
        before = previous.tables[table.name].watermark if previous else None
        key = None
        if got:
            key = bronze_key(table.name, at)
            df = lake.spark.createDataFrame(got, table.schema)
            lake.write_new_parquet(df, key, sort_by=[table.key])
        cursor = table.columns.index(table.cursor)
        seen = [_watermark(row[cursor]) for row in got]
        watermark = max([*seen, *([before] if before is not None else [])], default=None)
        tables[table.name] = TableExtract(len(got), before, watermark, key)
    record = ExtractRecord(at, f"extract-{extract_id(at)}", mode, SOURCE_IDS, tables)  # type: ignore[arg-type]
    if commit:
        lake.write_new_json(record_key(at), record.to_json())
    return record


def _watermark(value: datetime | int) -> Watermark:
    return value.replace(tzinfo=UTC) if isinstance(value, datetime) else value


def candidate(
    candidate_id: str,
    email: str,
    updated: datetime,
    *,
    internal: str | None = None,
    city: str = "Seattle",
) -> Row:
    """`internal` is the employee id of an internal candidate."""
    return (candidate_id, "Ann", "Lee", email, "555-0100", city, "US", internal is not None,
            internal, datetime(2025, 1, 1), updated)  # fmt: skip


def requisition(req_id: str, updated: datetime, *, title: str = "Engineer") -> Row:
    opened = datetime(2025, 1, 2)
    return (req_id, title, "engineering", "L4", "Platform", "Data", "Seattle", 2, "E100", None,
            "open", False, False, opened, None, opened, updated)  # fmt: skip


def application(
    application_id: str,
    candidate_id: str,
    updated: datetime,
    *,
    status: str = "active",
    stage: str = "applied",
) -> Row:
    return (application_id, candidate_id, "r1", "career_site", datetime(2025, 2, 1), stage,
            status, None, updated)  # fmt: skip


def offer(offer_id: str, updated: datetime, *, status: str = "extended") -> Row:
    decided = updated if status != "extended" else None
    start = date(2025, 4, 1) if status == "accepted" else None
    return (offer_id, "a1", datetime(2025, 3, 1, 9), status, decided, start, updated)


def change(change_id: int, changed: datetime, *, to_stage: str = "applied") -> Row:
    return (change_id, "a1", None, to_stage, None, "active", "advance", changed, "system")
