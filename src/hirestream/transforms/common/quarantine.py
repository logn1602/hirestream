"""Quarantine reason codes and row shape (SPEC §9.1). Nothing is silently dropped: a row silver
can't accept goes to `quarantine/<source>/` with one of these codes."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from hirestream.transforms.common.lineage import ARRIVAL_TS, RUN_ID, SOURCE_FILE


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


QUARANTINE_SCHEMA: Final = StructType(
    [
        StructField("raw_line", StringType()),
        StructField("reason_code", StringType(), nullable=False),
        StructField("reason_detail", StringType()),
        StructField(SOURCE_FILE, StringType()),
        StructField(ARRIVAL_TS, TimestampType()),
        StructField(RUN_ID, StringType()),
    ]
)
"""The columns every quarantine table shares; each source adds its partition column (T2.2+)."""
