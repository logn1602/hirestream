"""`hirestream ingest ats`: pull the ATS into bronze by watermark (SPEC §8, ADR-0021).

One extract reads all five tables in a single `REPEATABLE READ, READ ONLY` transaction, so the
tables agree with each other (every application's candidate is in the same snapshot). Each table
is exported with Postgres's own `COPY … TO STDOUT` into a local CSV file, which Spark reads with
the table's fixed schema and writes to `bronze/ats/<table>/extract_ts=…/` as Parquet. The commit
record `state/ats_watermarks/<extract_ts>.json` is written last; only then does the watermark
move.

- Mutable tables: `updated_at > watermark - 1 h`. The overlap re-reads rows a transaction still
  in flight at the last extract may have committed with an earlier `updated_at`; silver resolves
  the duplicates it causes.
- `application_stage_changes`: `change_id > last max`, as the spec says. A `bigserial` id can
  commit out of order, so a strict cursor can miss a row; ADR-0021 records the limitation.
- A watermark is the largest value seen in the data, never the clock, and never moves back.
- A reloaded source (`generate backfill` drops and recreates the tables) is detected by its
  table oids, and an incremental extract then refuses: run with `--full` to start a new baseline.
"""

from __future__ import annotations

import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
from psycopg import sql

from hirestream.generator.ats_db import AtsDbError, describe
from hirestream.transforms.common.ats import (
    OVERLAP_HOURS,
    RECORD_PREFIX,
    TABLES,
    AtsTable,
    ExtractMode,
    ExtractRecord,
    TableExtract,
    Watermark,
    bronze_key,
    check_monotonic,
    extract_id,
    record_key,
)
from hirestream.transforms.common.lake import Lake

PG_TIMESTAMP = 'YYYY-MM-DD"T"HH24:MI:SS.US'  # fixed width: what Spark parses below
SPARK_TIMESTAMP = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
NULL = r"\N"  # COPY's NULL marker, unquoted; FORCE_QUOTE * quotes every other value


class AtsExtractError(RuntimeError):
    """The extract can't run safely (no source, reloaded source, clock gone backwards)."""


@dataclass(frozen=True)
class ExtractResult:
    record: ExtractRecord
    key: str  # the record's lake key


@dataclass(frozen=True)
class _Pulled:
    rows: int
    watermark: Watermark | None
    csv: Path


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


def extract(lake: Lake, dsn: str, run_id: str, mode: ExtractMode = "incremental") -> ExtractResult:
    """Run one extract and commit it. `full` ignores the watermarks and starts a new baseline;
    an incremental extract with no earlier one is a full one."""
    records = committed_records(lake)
    previous = records[-1] if records else None
    with tempfile.TemporaryDirectory(prefix="ats-extract-") as scratch:
        try:
            with psycopg.connect(dsn, connect_timeout=5) as conn:
                conn.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
                conn.read_only = True
                with conn.transaction(), conn.cursor() as cur:
                    extract_ts, source_ids = _snapshot_identity(cur)
                    mode = _effective_mode(mode, previous, source_ids, extract_ts)
                    pulled = {
                        table.name: _pull(
                            cur,
                            table,
                            None
                            if mode == "full" or previous is None
                            else previous.tables[table.name].watermark,
                            Path(scratch) / f"{table.name}.csv",
                        )
                        for table in TABLES
                    }
        except psycopg.OperationalError as exc:
            raise AtsDbError(
                f"cannot reach the ats-db at {describe(dsn)} ({type(exc).__name__}); "
                "start it with `make up`"
            ) from exc
        # The snapshot is on local disk; the source transaction is closed. Land each table.
        tables = {}
        for table in TABLES:
            got = pulled[table.name]
            before = (
                None
                if mode == "full" or previous is None
                else (previous.tables[table.name].watermark)
            )
            key = _land(lake, table, got, extract_ts) if got.rows else None
            tables[table.name] = TableExtract(
                rows=got.rows,
                watermark_from=before,
                watermark=got.watermark if got.watermark is not None else before,
                key=key,
            )
    record = ExtractRecord(extract_ts, run_id, mode, source_ids, tables)
    check_monotonic([*records, record])
    key = record_key(extract_ts)
    lake.write_new_json(key, record.to_json())  # the commit point
    return ExtractResult(record, key)


def _snapshot_identity(cur: psycopg.Cursor[tuple[object, ...]]) -> tuple[datetime, dict[str, int]]:
    row = cur.execute("SELECT transaction_timestamp()").fetchone()
    assert row is not None and isinstance(row[0], datetime)
    extract_ts = row[0].astimezone(UTC)
    names = [table.name for table in TABLES]
    rows = cur.execute(
        "SELECT c.relname, c.oid::bigint FROM pg_class c WHERE c.relkind = 'r'"
        " AND c.relname = ANY(%s) AND pg_table_is_visible(c.oid)",
        (names,),
    ).fetchall()
    found = {str(name): int(oid) for name, oid in rows}  # type: ignore[call-overload]
    if missing := [name for name in names if name not in found]:
        raise AtsExtractError(f"the ats-db has no {', '.join(missing)} table: is it loaded?")
    return extract_ts, {name: found[name] for name in names}


def _effective_mode(
    mode: ExtractMode,
    previous: ExtractRecord | None,
    source_ids: Mapping[str, int],
    extract_ts: datetime,
) -> ExtractMode:
    if previous is None:
        return "full"
    if extract_id(extract_ts) <= extract_id(previous.extract_ts):
        raise AtsExtractError(
            f"extract {extract_id(extract_ts)} would not follow the last one "
            f"({extract_id(previous.extract_ts)}): extracts are at least a second apart"
        )
    if mode == "incremental" and dict(previous.source_ids) != dict(source_ids):
        raise AtsExtractError(
            "the ats-db tables were recreated since extract "
            f"{extract_id(previous.extract_ts)} (a new `generate backfill`?): their watermarks "
            "no longer apply; run `hirestream ingest ats --full` to start a new baseline"
        )
    return mode


def _pull(
    cur: psycopg.Cursor[tuple[object, ...]],
    table: AtsTable,
    watermark: Watermark | None,
    csv: Path,
) -> _Pulled:
    """COPY the table's new rows to `csv` and return their count and their max cursor value."""
    where, params = _filter(table, watermark)
    name, cursor = sql.Identifier(table.name), sql.Identifier(table.cursor)
    select = sql.SQL("SELECT {columns} FROM {table}{where} ORDER BY {key}").format(
        columns=sql.SQL(", ").join(_export(field.name, field.dataType.typeName())
                                   for field in table.schema.fields),
        table=name,
        where=where,
        key=sql.Identifier(table.key),
    )  # fmt: skip
    # FORCE_QUOTE *: with a NULL marker other than '', COPY writes '' unquoted, and Spark would
    # read that empty field as null. Quoting every non-null value keeps '' and NULL apart.
    copy = sql.SQL(
        "COPY ({select}) TO STDOUT WITH (FORMAT csv, NULL {null}, FORCE_QUOTE *)"
    ).format(select=select, null=sql.Literal(NULL))
    with csv.open("wb") as out, cur.copy(copy, params) as stream:
        for chunk in stream:
            out.write(chunk)
    stats = sql.SQL("SELECT count(*), max({cursor}) FROM {table}{where}").format(
        cursor=cursor, table=name, where=where
    )
    row = cur.execute(stats, params).fetchone()
    assert row is not None
    rows, top = int(row[0]), row[1]  # type: ignore[call-overload]
    if isinstance(top, datetime):
        top = top.replace(tzinfo=UTC)  # timestamp without time zone holding UTC (SPEC §7.4)
    return _Pulled(rows, top if isinstance(top, datetime | int) else None, csv)


def _filter(
    table: AtsTable, watermark: Watermark | None
) -> tuple[sql.Composable, tuple[object, ...]]:
    if watermark is None:
        return sql.SQL(""), ()
    cursor = sql.Identifier(table.cursor)
    if isinstance(watermark, datetime):
        # Naive, so psycopg sends a `timestamp`: an aware value would be a timestamptz and
        # Postgres would compare it through the session's TimeZone.
        since = (watermark - timedelta(hours=OVERLAP_HOURS)).astimezone(UTC).replace(tzinfo=None)
        return sql.SQL(" WHERE {} > %s").format(cursor), (since,)
    return sql.SQL(" WHERE {} > %s").format(cursor), (watermark,)


def _export(column: str, spark_type: str) -> sql.Composable:
    """How a column is written to CSV so Spark reads it back exactly."""
    ident = sql.Identifier(column)
    if spark_type == "timestamp":
        return sql.SQL("to_char({c}, {f}) AS {c}").format(c=ident, f=sql.Literal(PG_TIMESTAMP))
    if spark_type == "date":
        return sql.SQL("to_char({c}, 'YYYY-MM-DD') AS {c}").format(c=ident)
    if spark_type == "boolean":
        return sql.SQL("{c}::text AS {c}").format(c=ident)  # 'true' / 'false', not COPY's t / f
    return ident


def _land(lake: Lake, table: AtsTable, pulled: _Pulled, extract_ts: datetime) -> str:
    """Write the CSV as one Parquet file in the extract's bronze directory; check the count."""
    frame = (
        lake.spark.read.schema(table.schema)
        .option("mode", "FAILFAST")  # a value that doesn't fit its column is our bug: stop
        .option("multiLine", "true")  # COPY quotes embedded newlines; keep them in their field
        .option("escape", '"')  # COPY doubles quotes inside a quoted field
        .option("nullValue", NULL)
        .option("timestampFormat", SPARK_TIMESTAMP)
        .option("dateFormat", "yyyy-MM-dd")
        .csv(pulled.csv.resolve().as_uri())
    )
    key = bronze_key(table.name, extract_ts)
    lake.write_new_parquet(frame, key, sort_by=[table.key])
    landed = lake.spark.read.parquet(lake.paths.uri(key)).count()
    if landed != pulled.rows:
        raise AtsExtractError(
            f"{table.name}: {pulled.rows:,} rows exported but {landed:,} landed in {key}"
        )
    return key
