"""silver.scheduling_events: scheduling-service bronze lines to one row per event (SPEC §9).

Each line is parsed, checked against the scheduling contracts, and either kept or quarantined with
one reason code (ADR-0014 §5, ADR-0019). Kept lines are unified across schema versions, their
start times resolved to UTC, and deduplicated on `event_id`. Everything here is a pure function of
the lines and the run: reading bronze and writing the tables is the caller's job (T2.2b).
"""

from __future__ import annotations

from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hirestream.transforms.common.event_rules import SourceRules
from hirestream.transforms.common.lineage import (
    ARRIVAL_TS,
    INGESTED_AT,
    RUN_ID,
    SOURCE_FILE,
    SOURCE_LINE,
    RunContext,
)
from hirestream.transforms.common.quarantine import (
    QUARANTINE_SCHEMA,
    ReasonCode,
    quarantine_table_schema,
)
from hirestream.transforms.common.stream import (
    CONTRACT_KEY,
    contract_field,
    first_reason,
    flagged,
    kept_events,
    known_zone_ids,
    read_contract_fields,
    rejected_lines,
    shared_checks,
    shown,
)
from hirestream.transforms.common.stream_silver import StreamSource

SOURCE: Final = "scheduling-service"
BRONZE: Final = "scheduling"  # the folder under bronze/ and quarantine/
TABLE: Final = "scheduling_events"
PARTITION: Final = "event_date"  # UTC date of event_ts (SPEC §9.1)

# ADR-0014 §5: Phase 2 quarantines on the envelope and the entity key. Other fields that are
# missing or can't be cast arrive as null, and DQ checks own them. A missing schema_version is
# UNKNOWN_SCHEMA_VERSION, as for the contracts.
REQUIRED: Final = (
    "event_id",
    "event_type",
    "source",
    "producer_version",
    "event_ts",
    "sent_ts",
    "payload.interview_id",
)
UTC_FORMAT: Final = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"  # event_ts, sent_ts, actual_* (SPEC §7.1)
OFFSET_FORMAT: Final = "yyyy-MM-dd'T'HH:mm:ssXXX"  # starts: local time with offset (SPEC §7.2)
NAIVE_FORMAT: Final = "yyyy-MM-dd'T'HH:mm:ss"  # starts from the 1.3.0 build (ADR-0011)
NAIVE_START: Final = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$"

# (silver column, contract field). Values keep the contract's type; times become UTC.
VALUES: Final = (
    ("event_id", "event_id"),
    ("event_type", "event_type"),
    ("schema_version", "schema_version"),
    ("producer_version", "producer_version"),
    ("interview_id", "payload.interview_id"),
    ("application_id", "payload.application_id"),
    ("req_id", "payload.req_id"),
    ("interview_type", "payload.interview_type"),
    ("loop_id", "payload.loop_id"),
    ("session_index", "payload.session_index"),
    ("duration_minutes", "payload.duration_minutes"),
    ("timezone", "payload.timezone"),
    ("interview_format", "payload.interview_format"),
    ("coordinator_id", "payload.coordinator_id"),
    ("reason", "payload.reason"),
    ("initiated_by", "payload.initiated_by"),
    ("no_show_party", "payload.no_show_party"),
    ("feedback_id", "payload.feedback_id"),
    ("recommendation", "payload.recommendation"),
    ("word_count", "payload.word_count"),
)
UTC_TIMES: Final = (
    ("event_ts", "event_ts"),
    ("sent_ts", "sent_ts"),
    ("actual_start_utc", "payload.actual_start"),
    ("actual_end_utc", "payload.actual_end"),
)
STARTS: Final = (
    ("scheduled_start_utc", "payload.scheduled_start"),
    ("previous_start_utc", "payload.previous_start"),
    ("new_start_utc", "payload.new_start"),
)
# v1 scheduled events name one interviewer and v2 lists a panel (SPEC §9.1 wraps v1's scalar in
# an array). Feedback events always name their one author.
INTERVIEWER: Final = "payload.interviewer_id"
INTERVIEWERS: Final = "payload.interviewer_ids"
SCHEDULED_V1: Final = "interview_scheduled:v1"
CHECKED_ONLY: Final = ("source",)  # validated, not stored: constant for the table

SCHEDULING_EVENTS_SCHEMA: Final = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_type", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("producer_version", StringType()),
        StructField("event_ts", TimestampType()),
        StructField("sent_ts", TimestampType()),
        StructField("interview_id", StringType()),
        StructField("application_id", StringType()),
        StructField("req_id", StringType()),
        StructField("interview_type", StringType()),
        StructField("loop_id", StringType()),
        StructField("session_index", LongType()),
        StructField("interviewer_ids", ArrayType(StringType())),
        StructField("interviewer_id", StringType()),
        StructField("scheduled_start_utc", TimestampType()),
        StructField("previous_start_utc", TimestampType()),
        StructField("new_start_utc", TimestampType()),
        StructField("start_tz_inferred", BooleanType()),
        StructField("duration_minutes", LongType()),
        StructField("timezone", StringType()),
        StructField("interview_format", StringType()),
        StructField("coordinator_id", StringType()),
        StructField("reason", StringType()),
        StructField("initiated_by", StringType()),
        StructField("no_show_party", StringType()),
        StructField("actual_start_utc", TimestampType()),
        StructField("actual_end_utc", TimestampType()),
        StructField("feedback_id", StringType()),
        StructField("recommendation", StringType()),
        StructField("word_count", LongType()),
        StructField(SOURCE_FILE, StringType()),
        StructField(SOURCE_LINE, IntegerType()),
        StructField(ARRIVAL_TS, TimestampType()),
        StructField(RUN_ID, StringType()),
        StructField(INGESTED_AT, TimestampType()),
        StructField(PARTITION, DateType()),
    ]
)
QUARANTINE_COLUMNS: Final = (*QUARANTINE_SCHEMA.fieldNames(), PARTITION)


def stream_source(spark: SparkSession) -> StreamSource:
    """This table as the stream runner sees it, with its contracts and zones loaded once."""
    rules, zones = SourceRules.load(SOURCE), known_zone_ids(spark)
    return StreamSource(
        name=BRONZE,
        table=TABLE,
        schema=SCHEDULING_EVENTS_SCHEMA,
        quarantine_schema=quarantine_table_schema(PARTITION),
        build=lambda lines, run: build(lines, rules, zones, run),
        partition=PARTITION,
    )


def build(
    lines: DataFrame, rules: SourceRules, zone_ids: tuple[str, ...], run: RunContext
) -> tuple[DataFrame, DataFrame]:
    """(silver rows, quarantine rows) for a batch of bronze lines from `Lake.read_lines`.

    Rows are deduplicated within the batch: one per `event_id`, the earliest copy. Quarantine has
    one row per rejected line, before deduplication, so every bad copy is accounted for.
    """
    classified = classify(lines, rules, zone_ids)
    return events(classified, run), quarantine(classified, run)


def classify(lines: DataFrame, rules: SourceRules, zone_ids: tuple[str, ...]) -> DataFrame:
    """Every line with its silver columns and its reason code (null when the line is kept)."""
    return _check(read_contract_fields(lines, rules, required=REQUIRED), rules, zone_ids)


def _check(fields: DataFrame, rules: SourceRules, zone_ids: tuple[str, ...]) -> DataFrame:
    f = contract_field
    key = F.col(CONTRACT_KEY)
    utc = {path: F.try_to_timestamp(f(path), F.lit(UTC_FORMAT)) for _, path in UTC_TIMES}
    zone = f("payload.timezone")
    resolvable = F.coalesce(zone.isin(*zone_ids), F.lit(False))
    usable_zone = F.when(resolvable, zone).otherwise(F.lit("UTC"))  # never a zone Spark rejects
    offset = {p: F.try_to_timestamp(f(p), F.lit(OFFSET_FORMAT)) for _, p in STARTS}
    naive = {p: offset[p].isNull() & f(p).rlike(NAIVE_START) for _, p in STARTS}
    wall_clock = {p: F.try_to_timestamp(f(p), F.lit(NAIVE_FORMAT)) for _, p in STARTS}
    # A naive start is read in its zone. to_utc_timestamp follows java.time: a time in a DST gap
    # moves forward, an ambiguous one takes the earlier offset (SPEC §9.1's policy).
    start_utc = {
        p: F.when(offset[p].isNotNull(), offset[p]).when(
            naive[p] & resolvable, F.to_utc_timestamp(wall_clock[p], usable_zone)
        )
        for _, p in STARTS
    }

    unparseable = flagged(
        [(f(p).isNotNull() & utc[p].isNull(), shown(p)) for _, p in UTC_TIMES]
        + [
            (
                f(p).isNotNull() & offset[p].isNull() & (~naive[p] | wall_clock[p].isNull()),
                shown(p),
            )
            for _, p in STARTS
        ]
    )
    unresolvable = flagged((naive[p] & ~resolvable, F.lit(p)) for _, p in STARTS)
    code, detail = first_reason(
        {
            **shared_checks(rules, required=REQUIRED, event_ts=utc["event_ts"]),
            ReasonCode.UNPARSEABLE_TIMESTAMP: (
                F.size(unparseable) > 0,
                F.array_join(unparseable, ", "),
            ),
            ReasonCode.UNRESOLVABLE_TIMEZONE: (
                F.size(unresolvable) > 0,
                F.concat(
                    F.lit("naive "),
                    F.array_join(unresolvable, ", "),
                    F.lit(" and "),
                    shown("payload.timezone"),
                ),
            ),
        }
    )
    inferred = F.lit(False)
    for _, p in STARTS:
        inferred = inferred | F.coalesce(naive[p] & resolvable, F.lit(False))
    one = f(INTERVIEWER)
    return fields.select(
        "value",
        SOURCE_FILE,
        SOURCE_LINE,
        ARRIVAL_TS,
        *(f(path).alias(column) for column, path in VALUES),
        *(utc[path].alias(column) for column, path in UTC_TIMES),
        *(start_utc[path].alias(column) for column, path in STARTS),
        inferred.alias("start_tz_inferred"),
        F.when(key == SCHEDULED_V1, F.when(one.isNotNull(), F.array(one)))
        .otherwise(f(INTERVIEWERS))
        .alias("interviewer_ids"),
        F.when(key != SCHEDULED_V1, one).alias("interviewer_id"),
        code.alias("reason_code"),
        detail.alias("reason_detail"),
    )


def events(classified: DataFrame, run: RunContext) -> DataFrame:
    """The kept lines as silver rows, one per event_id (the earliest copy), in schema order."""
    return kept_events(
        classified, run, SCHEDULING_EVENTS_SCHEMA, event_ts="event_ts", partition=PARTITION
    )


def quarantine(classified: DataFrame, run: RunContext) -> DataFrame:
    """The rejected lines, one row each. `event_date` is null when event_ts can't be read."""
    return rejected_lines(classified, run, event_ts="event_ts", partition=PARTITION)
