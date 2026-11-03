"""silver.scheduling_events: scheduling-service bronze lines to one row per event (SPEC §9).

Each line is parsed, checked against the scheduling contracts, and either kept or quarantined with
one reason code (ADR-0014 §5, ADR-0019). Kept lines are unified across schema versions, their
start times resolved to UTC, and deduplicated on `event_id`. Everything here is a pure function of
the lines and the run: reading bronze and writing the tables is the caller's job (T2.2b).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final

from pyspark.sql import Column, DataFrame, SparkSession
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

from hirestream.transforms.common.event_rules import SourceRules, contract_key_column, extract
from hirestream.transforms.common.lineage import (
    ARRIVAL_TS,
    INGESTED_AT,
    RUN_ID,
    SOURCE_FILE,
    SOURCE_LINE,
    RunContext,
    with_run_lineage,
)
from hirestream.transforms.common.quarantine import (
    QUARANTINE_SCHEMA,
    ReasonCode,
    quarantine_table_schema,
)
from hirestream.transforms.common.stream import (
    PARSED,
    earliest_copy,
    first_reason,
    flagged,
    known_zone_ids,
    parse_lines,
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
CLOCK_SKEW_HOURS: Final = 1  # how far event_ts may run ahead of its object's arrival

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
_KEY = "_key"  # the row's contract key, e.g. "interview_scheduled:v2"


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
    parsed = parse_lines(lines)
    v = F.col(PARSED)

    def read(path: str) -> Column:
        """The envelope and the entity key are read whatever the type and version say. Other
        payload fields only for the contracts that define them: a stray field is drift."""
        value = extract(v, rules.field(path))
        if path.startswith("payload.") and path not in REQUIRED:
            return F.when(F.col(_KEY).isin(*rules.keys_with(path)), value)
        return value

    paths = dict.fromkeys(
        [*REQUIRED, *(p for _, p in (*VALUES, *UTC_TIMES, *STARTS)), INTERVIEWER, INTERVIEWERS]
    )
    key = contract_key_column(
        extract(v, rules.field("event_type")), extract(v, rules.field("schema_version"))
    )
    fields = parsed.withColumn(_KEY, key).select(
        "*", *(read(path).alias(_field(path)) for path in paths)
    )
    return _check(fields, rules, zone_ids)


def _field(path: str) -> str:
    return "_f_" + path.replace(".", "_")


def _check(fields: DataFrame, rules: SourceRules, zone_ids: tuple[str, ...]) -> DataFrame:
    def f(path: str) -> Column:
        return F.col(_field(path))

    key = F.col(_KEY)
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

    def shown(path: str) -> Column:
        return F.concat(F.lit(f"{path}="), F.coalesce(f(path).cast("string"), F.lit("null")))

    missing = flagged((f(p).isNull(), F.lit(p)) for p in REQUIRED)
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
    ahead = utc["event_ts"] > F.col(ARRIVAL_TS) + F.expr(f"INTERVAL {CLOCK_SKEW_HOURS} HOURS")
    code, detail = first_reason(
        {
            ReasonCode.MALFORMED_JSON: (F.col(PARSED).isNull(), F.lit("not a JSON object")),
            ReasonCode.MISSING_REQUIRED_FIELD: (
                F.size(missing) > 0,
                F.concat(F.lit("missing or invalid: "), F.array_join(missing, ", ")),
            ),
            ReasonCode.INVALID_ENUM: _invalid(rules, f, shown),
            ReasonCode.UNKNOWN_SCHEMA_VERSION: (
                ~F.coalesce(key.isin(*rules.events), F.lit(False)),
                F.concat(
                    F.lit("no contract for "),
                    shown("event_type"),
                    F.lit(", "),
                    shown("schema_version"),
                ),
            ),
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
            ReasonCode.OUT_OF_RANGE_TIMESTAMP: (
                ahead,
                F.concat(
                    F.lit(f"more than {CLOCK_SKEW_HOURS} h after the object arrived: "),
                    shown("event_ts"),
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


def _invalid(
    rules: SourceRules, f: Callable[[str], Column], shown: Callable[[str], Column]
) -> tuple[Column, Column]:
    """Values outside what a contract allows: the event type and source on every line, and each
    payload enum against its own contract's list (the two `reason` lists differ)."""
    checks = [
        (~f("event_type").isin(*sorted(rules.event_types)), shown("event_type")),
        (~f("source").isin(*sorted(rules.field("source").allowed or ())), shown("source")),
    ]
    for path in sorted(p for p in rules.paths if p.startswith("payload.")):
        outside: Column | None = None
        for key, allowed in sorted(rules.allowed(path).items()):
            here, test = F.col(_KEY) == key, ~f(path).isin(*sorted(allowed))
            outside = F.when(here, test) if outside is None else outside.when(here, test)
        if outside is not None:
            checks.append((outside, shown(path)))
    bad = flagged(checks)
    return F.size(bad) > 0, F.array_join(bad, ", ")


def events(classified: DataFrame, run: RunContext) -> DataFrame:
    """The kept lines as silver rows, one per event_id (the earliest copy), in schema order."""
    kept = classified.where(F.col("reason_code").isNull()).withColumns(
        {"schema_version": F.col("schema_version").cast("int"), PARTITION: F.to_date("event_ts")}
    )
    rows = with_run_lineage(earliest_copy(kept), run)
    return rows.select(*SCHEDULING_EVENTS_SCHEMA.fieldNames())


def quarantine(classified: DataFrame, run: RunContext) -> DataFrame:
    """The rejected lines, one row each. `event_date` is null when event_ts can't be read."""
    rejected = classified.where(F.col("reason_code").isNotNull())
    return rejected.select(
        F.col("value").alias("raw_line"),
        "reason_code",
        "reason_detail",
        SOURCE_FILE,
        SOURCE_LINE,
        ARRIVAL_TS,
        F.lit(run.run_id).alias(RUN_ID),
        F.to_date("event_ts").alias(PARTITION),
    )
