"""Helpers every stream-event silver job shares: parsing lines, choosing one reason code per line,
keeping one copy per event, and the timezones the JVM knows (SPEC §9.1, ADR-0019)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from hirestream.transforms.common.lineage import ARRIVAL_TS, SOURCE_FILE, SOURCE_LINE
from hirestream.transforms.common.quarantine import PRECEDENCE, ReasonCode
from hirestream.transforms.common.session import jvm

PARSED = "_json"  # the line as a VARIANT; null when it isn't JSON


def parse_lines(lines: DataFrame) -> DataFrame:
    """Add each line parsed as a VARIANT. `try_parse_json` returns null for invalid JSON,
    including duplicate keys; a valid line that isn't an object (`null`, `[1]`) is nulled here, so
    a null `_json` means exactly "not a JSON object" (MALFORMED_JSON)."""
    parsed = F.try_parse_json("value")
    is_object = F.col("value").rlike(r"^\s*\{")
    return lines.withColumn(PARSED, F.when(is_object, parsed))


def first_reason(checks: Mapping[ReasonCode, tuple[Column, Column]]) -> tuple[Column, Column]:
    """(reason_code, reason_detail) from {code: (failed, detail)}: the first failed check in
    PRECEDENCE order, or nulls when the line passes everything. A null `failed` counts as passed."""
    if missing := set(PRECEDENCE) - set(checks):
        raise ValueError(f"a check is needed for every reason code: {sorted(missing)}")
    code: Column | None = None
    detail: Column | None = None
    for reason in PRECEDENCE:
        failed, why = checks[reason]
        failed = F.coalesce(failed, F.lit(False))
        code = (
            F.when(failed, F.lit(reason.value)) if code is None else code.when(failed, reason.value)
        )
        detail = F.when(failed, why) if detail is None else detail.when(failed, why)
    assert code is not None and detail is not None
    return code, detail


def earliest_copy(rows: DataFrame, key: str = "event_id") -> DataFrame:
    """One row per `key`: the copy that arrived first, then the lowest file key, then the lowest
    line. Every copy of an event carries the same bytes, so this only picks the lineage."""
    order = Window.partitionBy(key).orderBy(ARRIVAL_TS, SOURCE_FILE, SOURCE_LINE)
    ranked = rows.withColumn("_copy", F.row_number().over(order))
    return ranked.where(F.col("_copy") == 1).drop("_copy")


def known_zone_ids(spark: SparkSession) -> tuple[str, ...]:
    """The region timezone ids the JVM accepts. `to_utc_timestamp` throws on any other, so a
    zone is checked against these before it is used. One py4j call, not one per id."""
    zones = jvm(spark).java.time.ZoneId.getAvailableZoneIds()
    joined: str = jvm(spark).java.lang.String.join("\n", zones)
    return tuple(sorted(joined.split("\n")))


def flagged(items: Iterable[tuple[Column, Column]]) -> Column:
    """The labels of the (flag, label) pairs whose flag is true, as an array, empty when none is.
    A null flag counts as false."""
    marked = [F.when(F.coalesce(flag, F.lit(False)), label) for flag, label in items]
    if not marked:
        return F.array().cast("array<string>")
    return F.filter(F.array(*marked), lambda label: label.isNotNull())
