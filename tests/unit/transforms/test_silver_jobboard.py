"""silver.jobboard_events on hand-made lines. Every case below runs through one `build`, in one
module-scoped batch (tests/unit/transforms/cases.py)."""

import json
from datetime import UTC, date, datetime
from typing import Any

import pytest
from pyspark.sql import SparkSession

from hirestream.contracts import load_contract
from hirestream.transforms.common.event_rules import SourceRules
from hirestream.transforms.common.lake import LINE_SCHEMA
from hirestream.transforms.common.lineage import RunContext
from hirestream.transforms.common.quarantine import PRECEDENCE, ReasonCode
from hirestream.transforms.silver_jobboard import (
    BRONZE,
    CHECKED_ONLY,
    JOBBOARD_EVENTS_SCHEMA,
    LAST_MS,
    QUARANTINE_COLUMNS,
    SOURCE,
    TIMES,
    VALUES,
    _epoch_floor,
    build,
)
from tests.unit.transforms.bronze import DROP, epoch_ms, jobboard_example
from tests.unit.transforms.cases import Cases, Line, Result, json_text, rejected, rows_of

RULES = SourceRules.load(SOURCE)
RUN = RunContext("run-1", datetime(2026, 1, 1, tzinfo=UTC))
CASES = Cases(BRONZE)
case = CASES.add
example = jobboard_example


@pytest.fixture(scope="module")
def results(spark: SparkSession) -> dict[str, Result]:
    return CASES.run(spark, lambda lines: build(lines, RULES, RUN))


# -- kept events --------------------------------------------------------------------------------

EXAMPLES = case(
    "examples",
    *(
        example(rules.event_type, rules.version, index)
        for rules in RULES.events.values()
        for index in range(len(load_contract(SOURCE, rules.event_type, rules.version)["examples"]))
    ),
)


def test_every_contract_example_becomes_one_row(results: dict[str, Result]) -> None:
    assert len(rows_of(results, EXAMPLES)) == len(CASES[EXAMPLES]) == 12


def test_rows_have_the_silver_schema(spark: SparkSession) -> None:
    built, quarantined = build(spark.createDataFrame([], LINE_SCHEMA), RULES, RUN)
    expected = [(f.name, f.dataType) for f in JOBBOARD_EVENTS_SCHEMA.fields]
    assert [(f.name, f.dataType) for f in built.schema.fields] == expected
    assert tuple(quarantined.columns) == QUARANTINE_COLUMNS


END_OF_FEBRUARY = datetime(2025, 2, 28, 23, 59, 59, 999_000, tzinfo=UTC)
SEARCH = example(
    "job_search", event_ts=epoch_ms(END_OF_FEBRUARY), sent_ts="2025-03-01T00:00:01.250Z"
)
INTERNAL_VIEW = example(
    "job_view",
    event_ts=epoch_ms(datetime(2025, 3, 1, tzinfo=UTC)),
    context__employee_id="E000123",
    context__referrer_type="internal_portal",
)
SUBMIT = example("apply_submit")
COLUMNS = case(
    "columns",
    (SEARCH, "yyyy=2025/part-3.jsonl.gz", 7, datetime(2025, 3, 1, 0, 5)),
    INTERNAL_VIEW,
    SUBMIT,
)


def test_epoch_milliseconds_become_utc_times_and_dates(results: dict[str, Result]) -> None:
    rows = rows_of(results, COLUMNS)
    search, view = rows[SEARCH["event_id"]], rows[INTERNAL_VIEW["event_id"]]
    assert (search.event_ts_utc, search.event_date) == (
        datetime(2025, 2, 28, 23, 59, 59, 999_000),  # the last millisecond of a UTC day
        date(2025, 2, 28),
    )
    assert (view.event_ts_utc, view.event_date) == (datetime(2025, 3, 1), date(2025, 3, 1))
    assert search.sent_ts == datetime(2025, 3, 1, 0, 0, 1, 250_000)


def test_context_and_payload_fields_become_columns(results: dict[str, Result]) -> None:
    rows = rows_of(results, COLUMNS)
    search, view, submit = (rows[e["event_id"]] for e in (SEARCH, INTERNAL_VIEW, SUBMIT))
    context, payload = SEARCH["context"], SEARCH["payload"]
    assert (search.session_id, search.visitor_id, search.employee_id) == (
        context["session_id"],
        context["visitor_id"],
        None,
    )
    assert (search.user_agent, search.referrer_type, search.device_type) == (
        context["user_agent"],
        "search_engine",
        "desktop",
    )
    assert (search.query_text, search.results_count, search.filter_role_family) == (
        payload["query_text"],
        payload["results_count"],
        payload["filter_role_family"],
    )
    assert (search.req_id, search.filter_location, search.application_id) == (None, None, None)
    assert (view.employee_id, view.referrer_type, view.req_id, view.position_in_results) == (
        "E000123",
        "internal_portal",
        INTERNAL_VIEW["payload"]["req_id"],
        INTERNAL_VIEW["payload"]["position_in_results"],
    )
    assert (submit.application_id, submit.candidate_id) == (
        SUBMIT["payload"]["application_id"],
        SUBMIT["payload"]["candidate_id"],
    )
    assert (search._source_file, search._source_line, search._arrival_ts, search._run_id) == (
        f"bronze/{BRONZE}/{COLUMNS}/yyyy=2025/part-3.jsonl.gz",
        7,
        datetime(2025, 3, 1, 0, 5),
        "run-1",
    )


V1 = example("page_view", 1)
V2 = example("page_view", 2, context__device_type="tablet")
V1_WITH_DEVICE = example("page_view", 1, context__device_type="mobile")
VERSIONS = case("versions", V1, V2, V1_WITH_DEVICE)


def test_only_v2_events_have_a_device_type(results: dict[str, Result]) -> None:
    rows = rows_of(results, VERSIONS)
    old, new = rows[V1["event_id"]], rows[V2["event_id"]]
    assert (old.schema_version, old.device_type) == (1, None)
    assert (new.schema_version, new.device_type) == (2, "tablet")
    assert rows[V1_WITH_DEVICE["event_id"]].device_type is None  # not a v1 field: drift


# -- quarantine ---------------------------------------------------------------------------------

AT = datetime(2025, 2, 22, 18, 31, 52, 48_000, tzinfo=UTC)
OVERFLOW = 2**63 - 1  # timestamp_millis throws on this one: it must not reach it


def soon(body: dict[str, Any], arrival: datetime) -> Line:
    return (body, "part-0.jsonl.gz", 1, arrival)


REASON_LINES: list[tuple[str, Line, ReasonCode, str]] = [
    ("truncated", '{"event_id": "x", "event_type": "job_v', ReasonCode.MALFORMED_JSON, "JSON"),
    ("not-an-object", '"job_view"', ReasonCode.MALFORMED_JSON, "JSON"),
    (
        "no-event-id",
        example("job_view", event_id=DROP),
        ReasonCode.MISSING_REQUIRED_FIELD,
        "missing or invalid: event_id",
    ),
    (
        "no-session",
        example("job_save", context__session_id=DROP),
        ReasonCode.MISSING_REQUIRED_FIELD,
        "missing or invalid: context.session_id",
    ),
    (
        "context-not-an-object",
        example("apply_start", context="web"),
        ReasonCode.MISSING_REQUIRED_FIELD,
        "context.session_id",
    ),
    (
        "null-event-ts",
        example("page_view", event_ts=None),
        ReasonCode.MISSING_REQUIRED_FIELD,
        "missing or invalid: event_ts",
    ),
    (
        "iso-event-ts",  # the scheduling spelling: it can't be cast to epoch milliseconds
        example("page_view", event_ts="2025-02-22T18:31:52.048Z"),
        ReasonCode.MISSING_REQUIRED_FIELD,
        "missing or invalid: event_ts",
    ),
    (
        "no-sent-ts",
        example("job_view", sent_ts=DROP),
        ReasonCode.MISSING_REQUIRED_FIELD,
        "missing or invalid: sent_ts",
    ),
    (
        "unknown-event-type",
        example("job_view", event_type="JOB_VIEW"),
        ReasonCode.INVALID_ENUM,
        "event_type=JOB_VIEW",
    ),
    (
        "upper-cased-referrer",  # what chaos does to a job board line (ADR-0012)
        example("job_search", context__referrer_type="SEARCH_ENGINE"),
        ReasonCode.INVALID_ENUM,
        "context.referrer_type=SEARCH_ENGINE",
    ),
    (
        "unknown-device",
        example("job_view", context__device_type="watch"),
        ReasonCode.INVALID_ENUM,
        "context.device_type=watch",
    ),
    (
        "unknown-page",
        example("page_view", payload__page_type="landing"),
        ReasonCode.INVALID_ENUM,
        "payload.page_type=landing",
    ),
    (
        "wrong-source",
        example("job_save", source="scheduling-service"),
        ReasonCode.INVALID_ENUM,
        "source=scheduling-service",
    ),
    (
        "unknown-version",
        example("apply_submit", schema_version=3),
        ReasonCode.UNKNOWN_SCHEMA_VERSION,
        "schema_version=3",
    ),
    (
        "no-version",
        example("apply_submit", schema_version=DROP),
        ReasonCode.UNKNOWN_SCHEMA_VERSION,
        "schema_version=null",
    ),
    (
        "epoch-seconds",
        example("job_view", event_ts=1_740_249_112),
        ReasonCode.UNPARSEABLE_TIMESTAMP,
        "event_ts=1740249112",
    ),
    (
        "overflowing-millis",
        example("job_view", event_ts=OVERFLOW),
        ReasonCode.UNPARSEABLE_TIMESTAMP,
        f"event_ts={OVERFLOW}",
    ),
    (
        "year-10000",
        example("job_view", event_ts=LAST_MS + 1),
        ReasonCode.UNPARSEABLE_TIMESTAMP,
        f"event_ts={LAST_MS + 1}",
    ),
    (
        "no-millis-sent-ts",
        example("job_save", sent_ts="2025-02-22T18:31:54Z"),
        ReasonCode.UNPARSEABLE_TIMESTAMP,
        "sent_ts=2025-02-22T18:31:54Z",
    ),
    (
        "from-the-future",
        soon(example("job_view", event_ts=epoch_ms(AT)), datetime(2025, 2, 22, 17, 31)),
        ReasonCode.OUT_OF_RANGE_TIMESTAMP,
        f"after the object arrived: event_ts={epoch_ms(AT)}",
    ),
]
REASONS = {
    case(f"reason-{name}", line): (code, detail) for name, line, code, detail in REASON_LINES
}


@pytest.mark.parametrize("name", REASONS)
def test_each_reason_code(results: dict[str, Result], name: str) -> None:
    code, detail = REASONS[name]
    line = rejected(results, name)
    assert line.reason_code == code.value
    assert detail in line.reason_detail
    assert line._run_id == "run-1" and line.raw_line == json_text(CASES[name][0])


def test_every_reason_code_but_the_time_zones_has_a_case() -> None:
    # Job board events carry no time zone, so none can be unresolvable.
    expected = set(ReasonCode) - {ReasonCode.UNRESOLVABLE_TIMEZONE}
    assert {code for code, _ in REASONS.values()} == expected


def test_the_epoch_floor_is_the_contracts_minimum() -> None:
    assert _epoch_floor(RULES) == 10**12
    bare = {"properties": {"event_ts": {"type": "integer"}}}
    with pytest.raises(ValueError, match="no minimum"):
        _epoch_floor(SourceRules.from_schemas("test", {("a", 1): bare}))


# Each line breaks two rules; the first in PRECEDENCE must win.
PAIRS = {
    case(f"pair-{i}", line): code
    for i, (line, code) in enumerate(
        [
            (
                example("job_view", context__session_id=DROP, context__referrer_type="X"),
                ReasonCode.MISSING_REQUIRED_FIELD,
            ),
            (
                example("job_view", event_type="job_viewed", schema_version=7),
                ReasonCode.INVALID_ENUM,
            ),
            (
                example("job_view", schema_version=9, event_ts=1_740_249_112),
                ReasonCode.UNKNOWN_SCHEMA_VERSION,
            ),
            (
                soon(
                    example("job_view", sent_ts="soon", event_ts=epoch_ms(AT)),
                    datetime(2025, 1, 1),
                ),
                ReasonCode.UNPARSEABLE_TIMESTAMP,
            ),
        ]
    )
}


@pytest.mark.parametrize("name", PAIRS)
def test_the_first_code_in_precedence_wins(results: dict[str, Result], name: str) -> None:
    assert rejected(results, name).reason_code == PAIRS[name].value


def test_pairs_cover_each_step_of_the_precedence() -> None:
    possible = [code for code in PRECEDENCE if code != ReasonCode.UNRESOLVABLE_TIMEZONE]
    assert set(PAIRS.values()) >= set(possible[1:-1])  # each code beats the next one


BAD_REFERRER = example(
    "job_view",
    event_ts=epoch_ms(datetime(2025, 3, 1, 8, tzinfo=UTC)),
    context__referrer_type="?",
)
PARTITIONS = case("partitions", "{not json", BAD_REFERRER, example("job_view", event_ts=OVERFLOW))


def test_quarantine_is_partitioned_by_event_date_when_it_can_be(
    results: dict[str, Result],
) -> None:
    rows, quarantined = results[PARTITIONS]
    by_reason = {line.reason_code: line for line in quarantined}
    assert rows == [] and list(quarantined[0].asDict()) == list(QUARANTINE_COLUMNS)
    assert by_reason["MALFORMED_JSON"].event_date is None
    assert by_reason["UNPARSEABLE_TIMESTAMP"].event_date is None
    assert by_reason["INVALID_ENUM"].event_date == date(2025, 3, 1)


# -- drift and leniency -------------------------------------------------------------------------

# COE-001's incident renames req_id without bumping schema_version (SPEC §6.8, §19).
RENAMED = example("job_view", payload__req_id=DROP, payload__requisition_id="R000777")
STRAY = example("job_view", payload__query_text="engineer", extra_top="drift")
DRIFT = case("drift", RENAMED, STRAY)


def test_a_renamed_req_id_reaches_silver_as_null(results: dict[str, Result]) -> None:
    rows = rows_of(results, DRIFT)
    assert rows[RENAMED["event_id"]].req_id is None
    assert rows[STRAY["event_id"]].query_text is None  # a job_search field, not a job_view one


DIGITS = example("job_search", event_ts=str(epoch_ms(AT)), payload__results_count="4")
UNREADABLE = example("job_search", payload__results_count="many")
ANONYMOUS = example("page_view", context__visitor_id=DROP, context__referrer_type=DROP)
LENIENT = case("lenient", DIGITS, UNREADABLE, ANONYMOUS)


def test_non_key_fields_that_cannot_be_read_arrive_as_null(results: dict[str, Result]) -> None:
    rows = rows_of(results, LENIENT)
    digits = rows[DIGITS["event_id"]]  # Spark's ANSI cast reads a string of digits
    assert (digits.event_ts_utc, digits.results_count) == (AT.replace(tzinfo=None), 4)
    assert rows[UNREADABLE["event_id"]].results_count is None
    anonymous = rows[ANONYMOUS["event_id"]]
    assert (anonymous.visitor_id, anonymous.referrer_type) == (None, None)


# -- duplicates ---------------------------------------------------------------------------------

TWICE = example("job_save")
EARLY, LATE = datetime(2030, 1, 1, 1), datetime(2030, 1, 1, 2)
COPIES = case(
    "copies",
    (TWICE, "b", 1, EARLY),
    (TWICE, "a", 2, LATE),
    (json.dumps(TWICE)[:30], "a", 1, EARLY),
)


def test_the_earliest_kept_copy_wins(results: dict[str, Result]) -> None:
    rows, quarantined = results[COPIES]
    assert [(row._source_file.rsplit("/", 1)[1], row._source_line) for row in rows] == [("b", 1)]
    assert [line.reason_code for line in quarantined] == ["MALFORMED_JSON"]


# -- coverage of the contracts ------------------------------------------------------------------


def test_every_contract_field_has_a_silver_column() -> None:
    mapped = dict((*VALUES, *TIMES))
    assert set(mapped.values()) | set(CHECKED_ONLY) == RULES.paths
    assert set(mapped) <= set(JOBBOARD_EVENTS_SCHEMA.fieldNames())
