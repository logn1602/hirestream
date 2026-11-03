"""silver.jobboard_events: job board bronze lines to one row per event (SPEC §9, ADR-0020).

The same steps as silver.scheduling_events (see `stream.py`), with the job board's own fields: the
`context` object is flattened into columns, and `event_ts` is epoch milliseconds (SPEC §7.1), not
an ISO string. There are no time zones to resolve. Everything here is a pure function of the lines
and the run; `stream_silver.run` reads bronze and writes the tables.
"""

from __future__ import annotations

from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
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
    contract_field,
    first_reason,
    flagged,
    kept_events,
    read_contract_fields,
    rejected_lines,
    shared_checks,
    shown,
)
from hirestream.transforms.common.stream_silver import StreamSource

SOURCE: Final = "jobboard-web"
BRONZE: Final = "jobboard"  # the folder under bronze/ and quarantine/
TABLE: Final = "jobboard_events"
PARTITION: Final = "event_date"  # UTC date of event_ts (SPEC §9.1)

# ADR-0014 §5: the envelope and the entity key, which is what chaos can drop. Any other field that
# is missing or can't be cast arrives as null, and DQ owns it: a renamed req_id reaches silver as
# null (COE-001).
REQUIRED: Final = (
    "event_id",
    "event_type",
    "source",
    "producer_version",
    "event_ts",
    "sent_ts",
    "context.session_id",
)
UTC_FORMAT: Final = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"  # sent_ts (SPEC §7.1)
LAST_MS: Final = 253_402_300_799_999  # 9999-12-31T23:59:59.999Z: Spark timestamps end there

# (silver column, contract field). Context fields are flattened; values keep the contract's type.
VALUES: Final = (
    ("event_id", "event_id"),
    ("event_type", "event_type"),
    ("schema_version", "schema_version"),
    ("producer_version", "producer_version"),
    ("session_id", "context.session_id"),
    ("visitor_id", "context.visitor_id"),
    ("employee_id", "context.employee_id"),
    ("user_agent", "context.user_agent"),
    ("referrer_type", "context.referrer_type"),
    ("device_type", "context.device_type"),  # v2 only: null for v1 (SPEC §9.1)
    ("page_type", "payload.page_type"),
    ("req_id", "payload.req_id"),
    ("query_text", "payload.query_text"),  # kept in silver only (SPEC §9.2)
    ("filter_location", "payload.filter_location"),
    ("filter_role_family", "payload.filter_role_family"),
    ("results_count", "payload.results_count"),
    ("position_in_results", "payload.position_in_results"),
    ("application_id", "payload.application_id"),
    ("candidate_id", "payload.candidate_id"),
)
TIMES: Final = (("event_ts_utc", "event_ts"), ("sent_ts", "sent_ts"))  # both become UTC timestamps
CHECKED_ONLY: Final = ("source",)  # validated, not stored: constant for the table

JOBBOARD_EVENTS_SCHEMA: Final = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_type", StringType()),
        StructField("schema_version", IntegerType()),
        StructField("producer_version", StringType()),
        StructField("event_ts_utc", TimestampType()),
        StructField("sent_ts", TimestampType()),
        StructField("session_id", StringType()),
        StructField("visitor_id", StringType()),
        StructField("employee_id", StringType()),
        StructField("user_agent", StringType()),
        StructField("referrer_type", StringType()),
        StructField("device_type", StringType()),
        StructField("page_type", StringType()),
        StructField("req_id", StringType()),
        StructField("query_text", StringType()),
        StructField("filter_location", StringType()),
        StructField("filter_role_family", StringType()),
        StructField("results_count", LongType()),
        StructField("position_in_results", LongType()),
        StructField("application_id", StringType()),
        StructField("candidate_id", StringType()),
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
    """This table as the stream runner sees it, with its contracts loaded once. `spark` is unused:
    every source has this signature, and the job board needs nothing from the JVM."""
    rules = SourceRules.load(SOURCE)
    return StreamSource(
        name=BRONZE,
        table=TABLE,
        schema=JOBBOARD_EVENTS_SCHEMA,
        quarantine_schema=quarantine_table_schema(PARTITION),
        build=lambda lines, run: build(lines, rules, run),
        partition=PARTITION,
    )


def build(lines: DataFrame, rules: SourceRules, run: RunContext) -> tuple[DataFrame, DataFrame]:
    """(silver rows, quarantine rows) for a batch of bronze lines from `Lake.read_lines`.

    Rows are deduplicated within the batch: one per `event_id`, the earliest copy. Quarantine has
    one row per rejected line, before deduplication, so every bad copy is accounted for.
    """
    classified = classify(lines, rules)
    return events(classified, run), quarantine(classified, run)


def classify(lines: DataFrame, rules: SourceRules) -> DataFrame:
    """Every line with its silver columns and its reason code (null when the line is kept).

    `event_ts` is read as a bigint like any integer field. Absent, null or not castable, it is a
    missing required field. Below the contracts' minimum (epoch seconds) or past the end of Spark's
    timestamp range, it is unparseable.
    """
    fields = read_contract_fields(lines, rules, required=REQUIRED)
    f = contract_field
    ms = f("event_ts")
    readable = (ms >= _epoch_floor(rules)) & (ms <= LAST_MS)
    # Nulled before converting: timestamp_millis throws on a long overflow, which would fail the
    # whole job over one line, and past year 9999 it leaves Spark's timestamp range.
    times = {
        "event_ts": F.timestamp_millis(F.when(readable, ms)),
        "sent_ts": F.try_to_timestamp(f("sent_ts"), F.lit(UTC_FORMAT)),
    }
    unparseable = flagged(
        [
            (ms.isNotNull() & ~readable, shown("event_ts")),
            (f("sent_ts").isNotNull() & times["sent_ts"].isNull(), shown("sent_ts")),
        ]
    )
    code, detail = first_reason(
        {
            **shared_checks(rules, required=REQUIRED, event_ts=times["event_ts"]),
            ReasonCode.UNPARSEABLE_TIMESTAMP: (
                F.size(unparseable) > 0,
                F.array_join(unparseable, ", "),
            ),
            # Job board events carry no time zones (SPEC §7.3), so none can be unresolvable.
            ReasonCode.UNRESOLVABLE_TIMEZONE: (F.lit(False), F.lit(None).cast("string")),
        }
    )
    return fields.select(
        "value",
        SOURCE_FILE,
        SOURCE_LINE,
        ARRIVAL_TS,
        *(f(path).alias(column) for column, path in VALUES),
        *(times[path].alias(column) for column, path in TIMES),
        code.alias("reason_code"),
        detail.alias("reason_detail"),
    )


def _epoch_floor(rules: SourceRules) -> int:
    """The contracts' minimum for event_ts, which rules out epoch seconds."""
    minimum = rules.field("event_ts").minimum
    if minimum is None:
        raise ValueError(f"the {rules.source} contracts give event_ts no minimum")
    return int(minimum)


def events(classified: DataFrame, run: RunContext) -> DataFrame:
    """The kept lines as silver rows, one per event_id (the earliest copy), in schema order."""
    return kept_events(
        classified, run, JOBBOARD_EVENTS_SCHEMA, event_ts="event_ts_utc", partition=PARTITION
    )


def quarantine(classified: DataFrame, run: RunContext) -> DataFrame:
    """The rejected lines, one row each. `event_date` is null when event_ts can't be read."""
    return rejected_lines(classified, run, event_ts="event_ts_utc", partition=PARTITION)
