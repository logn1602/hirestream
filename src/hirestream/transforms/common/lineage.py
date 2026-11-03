"""Lineage columns, and the content fingerprint that idempotency is checked with (SPEC §9.1).

`_source_file`, `_source_line` and `_arrival_ts` describe where a row came from and are a function
of the inputs. `_run_id` and `_ingested_at` name the run that wrote the row. They are run-scoped:
the caller passes them in as a `RunContext` (a transform never reads the clock), and idempotency
compares everything except them (ADR-0018).
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SOURCE_FILE: Final = "_source_file"  # lake-relative key of the bronze object (ADR-0018)
SOURCE_LINE: Final = "_source_line"  # the line's number in that object, from 1 (ADR-0019)
ARRIVAL_TS: Final = "_arrival_ts"  # the object's mtime locally, LastModified on S3 (ADR-0012)
RUN_ID: Final = "_run_id"
INGESTED_AT: Final = "_ingested_at"
LINEAGE_COLUMNS: Final = (SOURCE_FILE, SOURCE_LINE, ARRIVAL_TS, RUN_ID, INGESTED_AT)
RUN_SCOPED_COLUMNS: Final = (RUN_ID, INGESTED_AT)


@dataclass(frozen=True)
class RunContext:
    """The run a job belongs to: its id and its start time, chosen once by the caller."""

    run_id: str
    started_at: datetime

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("run_id must be non-empty")
        if self.started_at.tzinfo is None or self.started_at.utcoffset() is None:
            raise ValueError(f"started_at must be timezone-aware, got {self.started_at!r}")


def with_run_lineage(df: DataFrame, run: RunContext) -> DataFrame:
    """Stamp each row with the run that writes it. `_ingested_at` is the run's start time, so
    every row a run writes carries the same value."""
    return df.withColumns({RUN_ID: F.lit(run.run_id), INGESTED_AT: F.lit(run.started_at)})


def content_fingerprint(df: DataFrame, exclude: Iterable[str] = RUN_SCOPED_COLUMNS) -> str:
    """A fingerprint of the frame's schema and its rows as a multiset, ignoring row order and
    partitioning. Two frames with equal fingerprints are, in practice, equal.

    The run-scoped lineage columns are excluded by default: a re-run on the same inputs must
    produce the same fingerprint even though it is a different run.
    """
    skip = set(exclude)
    columns = [name for name in df.columns if name not in skip]
    if not columns:
        raise ValueError("nothing to fingerprint: every column is excluded")
    # to_json spells nulls by omitting the key, so (null, 'a') and ('a', null) still differ.
    row_hash = F.xxhash64(F.to_json(F.struct(*columns))).cast("decimal(38,0)")
    stats = df.select(row_hash.alias("h")).agg(F.count("*").alias("n"), F.sum("h").alias("s"))
    n, total = stats.first() or (0, None)
    schema = df.select(*columns).schema.json()
    return hashlib.sha256(f"{schema}|{n}|{total or 0}".encode()).hexdigest()
