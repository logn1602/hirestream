"""Quarantine reason codes and row shape (SPEC §9.1). Nothing is silently dropped: a row silver
can't accept goes to `quarantine/<source>/` with one of these codes."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hirestream.transforms.common.lineage import ARRIVAL_TS, RUN_ID, SOURCE_FILE, SOURCE_LINE


class ReasonCode(StrEnum):
    """SPEC §9.1's reasons. The generator's ground truth counts the same codes, and a test keeps
    the two lists identical."""

    MALFORMED_JSON = "MALFORMED_JSON"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    INVALID_ENUM = "INVALID_ENUM"
    UNPARSEABLE_TIMESTAMP = "UNPARSEABLE_TIMESTAMP"
    UNRESOLVABLE_TIMEZONE = "UNRESOLVABLE_TIMEZONE"
    UNKNOWN_SCHEMA_VERSION = "UNKNOWN_SCHEMA_VERSION"
    OUT_OF_RANGE_TIMESTAMP = "OUT_OF_RANGE_TIMESTAMP"


PRECEDENCE: Final = (
    ReasonCode.MALFORMED_JSON,
    ReasonCode.MISSING_REQUIRED_FIELD,
    ReasonCode.INVALID_ENUM,
    ReasonCode.UNKNOWN_SCHEMA_VERSION,
    ReasonCode.UNPARSEABLE_TIMESTAMP,
    ReasonCode.UNRESOLVABLE_TIMEZONE,
    ReasonCode.OUT_OF_RANGE_TIMESTAMP,
)
"""When a line breaks several rules, the first code here wins: ADR-0014 §5's order, with the two
timestamp codes it left open placed by ADR-0019 (a bad time before a missing zone; the plausibility
check last, once everything parses)."""

QUARANTINE_SCHEMA: Final = StructType(
    [
        StructField("raw_line", StringType()),
        StructField("reason_code", StringType(), nullable=False),
        StructField("reason_detail", StringType()),
        StructField(SOURCE_FILE, StringType()),
        StructField(SOURCE_LINE, IntegerType()),
        StructField(ARRIVAL_TS, TimestampType()),
        StructField(RUN_ID, StringType()),
    ]
)
"""The columns every quarantine table shares; each source adds its partition column. A line is
identified by (`_source_file`, `_source_line`), so a retried run can't quarantine it twice."""


def quarantine_table_schema(partition: str = "event_date") -> StructType:
    """A source's quarantine table: the shared columns plus its date partition, null when the
    line has no readable date (ADR-0019 §9)."""
    return StructType([*QUARANTINE_SCHEMA.fields, StructField(partition, DateType())])
