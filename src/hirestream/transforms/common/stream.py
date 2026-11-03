"""Helpers every stream-event silver job shares (SPEC §9.1, ADR-0019, ADR-0020).

A source's transform is these steps with its own columns and time parsing:
1. `read_contract_fields`: parse each line and read every contract field, cast to its type.
2. `shared_checks` plus the source's own time checks, combined by `first_reason` into one reason
   code per line.
3. `kept_events` (one row per event, the earliest copy) and `rejected_lines` (quarantine).

Also here: `earliest_copy`, `flagged`, and the timezones the JVM knows.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping
from typing import Final

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from hirestream.transforms.common.event_rules import SourceRules, contract_key_column, extract
from hirestream.transforms.common.lineage import (
    ARRIVAL_TS,
    RUN_ID,
    SOURCE_FILE,
    SOURCE_LINE,
    RunContext,
    with_run_lineage,
)
from hirestream.transforms.common.quarantine import PRECEDENCE, ReasonCode
from hirestream.transforms.common.session import jvm

PARSED: Final = "_json"  # the line as a VARIANT; null when it isn't a JSON object
CONTRACT_KEY: Final = "_key"  # the line's contract key, e.g. "interview_scheduled:v2"
CLOCK_SKEW_HOURS: Final = 1  # how far event_ts may run ahead of its object's arrival
Check = tuple[Column, Column]  # (failed, reason_detail)


def parse_lines(lines: DataFrame) -> DataFrame:
    """Add each line parsed as a VARIANT. `try_parse_json` returns null for invalid JSON,
    including duplicate keys; a valid line that isn't an object (`null`, `[1]`) is nulled here, so
    a null `_json` means exactly "not a JSON object" (MALFORMED_JSON)."""
    parsed = F.try_parse_json("value")
    is_object = F.col("value").rlike(r"^\s*\{")
    return lines.withColumn(PARSED, F.when(is_object, parsed))


def read_contract_fields(
    lines: DataFrame, rules: SourceRules, *, required: Collection[str]
) -> DataFrame:
    """Parse each line, then add its contract key and the value of every field any contract of the
    source has, cast to the contract's type: null if absent, JSON null, or not castable.

    Top-level fields and the `required` ones are read whatever the event's type and version say.
    Any other nested field is read only for the contracts that define it: a stray field from
    another type or version is drift, and arrives as null (ADR-0014 §5). Read values with
    `contract_field`.
    """
    parsed = parse_lines(lines)
    v = F.col(PARSED)
    key = contract_key_column(
        extract(v, rules.field("event_type")), extract(v, rules.field("schema_version"))
    )

    def read(path: str) -> Column:
        value = extract(v, rules.field(path))
        if "." in path and path not in required:
            return F.when(F.col(CONTRACT_KEY).isin(*rules.keys_with(path)), value)
        return value

    columns = [read(path).alias(_field_name(path)) for path in sorted(rules.paths)]
    return parsed.withColumn(CONTRACT_KEY, key).select("*", *columns)


def contract_field(path: str) -> Column:
    """The value `read_contract_fields` read for `path`, e.g. "payload.req_id"."""
    return F.col(_field_name(path))


def _field_name(path: str) -> str:
    return "_f_" + path.replace(".", "_")


def shown(path: str) -> Column:
    """`path=value` for a reason detail; `path=null` when there is no value."""
    value = F.coalesce(contract_field(path).cast("string"), F.lit("null"))
    return F.concat(F.lit(f"{path}="), value)


def shared_checks(
    rules: SourceRules, *, required: Iterable[str], event_ts: Column
) -> dict[ReasonCode, Check]:
    """The five checks every stream source makes the same way, on `read_contract_fields`' output.
    A source adds its own UNPARSEABLE_TIMESTAMP and UNRESOLVABLE_TIMEZONE checks, and passes its
    parsed event time as `event_ts`.

    - MALFORMED_JSON: the line isn't a JSON object.
    - MISSING_REQUIRED_FIELD: a `required` field is absent, null, or can't be cast to its type.
    - INVALID_ENUM: see `invalid_values`.
    - UNKNOWN_SCHEMA_VERSION: no contract for (event_type, schema_version).
    - OUT_OF_RANGE_TIMESTAMP: `event_ts` is more than CLOCK_SKEW_HOURS after its object arrived.
    """
    missing = flagged((contract_field(path).isNull(), F.lit(path)) for path in required)
    known = F.coalesce(F.col(CONTRACT_KEY).isin(*rules.events), F.lit(False))
    ahead = event_ts > F.col(ARRIVAL_TS) + F.expr(f"INTERVAL {CLOCK_SKEW_HOURS} HOURS")
    return {
        ReasonCode.MALFORMED_JSON: (F.col(PARSED).isNull(), F.lit("not a JSON object")),
        ReasonCode.MISSING_REQUIRED_FIELD: (
            F.size(missing) > 0,
            F.concat(F.lit("missing or invalid: "), F.array_join(missing, ", ")),
        ),
        ReasonCode.INVALID_ENUM: invalid_values(rules),
        ReasonCode.UNKNOWN_SCHEMA_VERSION: (
            ~known,
            F.concat(
                F.lit("no contract for "), shown("event_type"), F.lit(", "), shown("schema_version")
            ),
        ),
        ReasonCode.OUT_OF_RANGE_TIMESTAMP: (
            ahead,
            F.concat(
                F.lit(f"more than {CLOCK_SKEW_HOURS} h after the object arrived: "),
                shown("event_ts"),
            ),
        ),
    }


def invalid_values(rules: SourceRules) -> Check:
    """Values outside what a contract allows: the event type and source on every line, and each
    nested enum (payload, context) against its own contract's list, since lists differ between
    event types (scheduling's two `reason` lists)."""
    f = contract_field
    checks = [
        (~f("event_type").isin(*sorted(rules.event_types)), shown("event_type")),
        (~f("source").isin(*sorted(rules.field("source").allowed or ())), shown("source")),
    ]
    for path in sorted(p for p in rules.paths if "." in p):
        outside: Column | None = None
        for key, allowed in sorted(rules.allowed(path).items()):
            here, test = F.col(CONTRACT_KEY) == key, ~f(path).isin(*sorted(allowed))
            outside = F.when(here, test) if outside is None else outside.when(here, test)
        if outside is not None:
            checks.append((outside, shown(path)))
    bad = flagged(checks)
    return F.size(bad) > 0, F.array_join(bad, ", ")


def first_reason(checks: Mapping[ReasonCode, Check]) -> Check:
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


def kept_events(
    classified: DataFrame, run: RunContext, schema: StructType, *, event_ts: str, partition: str
) -> DataFrame:
    """The lines without a reason code as silver rows: one per event_id (the earliest copy), with
    the run's lineage and `partition`, the UTC date of `event_ts`, in `schema`'s order."""
    kept = classified.where(F.col("reason_code").isNull()).withColumns(
        {"schema_version": F.col("schema_version").cast("int"), partition: F.to_date(event_ts)}
    )
    return with_run_lineage(earliest_copy(kept), run).select(*schema.fieldNames())


def rejected_lines(
    classified: DataFrame, run: RunContext, *, event_ts: str, partition: str
) -> DataFrame:
    """The lines with a reason code, one row each, before any deduplication, so every bad copy is
    accounted for. `partition` is the UTC date of `event_ts`, null when it can't be read."""
    rejected = classified.where(F.col("reason_code").isNotNull())
    return rejected.select(
        F.col("value").alias("raw_line"),
        "reason_code",
        "reason_detail",
        SOURCE_FILE,
        SOURCE_LINE,
        ARRIVAL_TS,
        F.lit(run.run_id).alias(RUN_ID),
        F.to_date(event_ts).alias(partition),
    )


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
