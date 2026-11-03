"""silver.scheduling_events on hand-made lines. Every case below runs through one `build`, in one
module-scoped batch: each case's lines sit under their own file prefix, and its test reads them
back. One Spark run instead of one per test keeps this module fast."""

import json
from datetime import UTC, date, datetime
from typing import Any

import pytest
from pyspark.sql import Row, SparkSession

from hirestream.contracts import load_contract
from hirestream.transforms.common.event_rules import SourceRules
from hirestream.transforms.common.lake import LINE_SCHEMA
from hirestream.transforms.common.lineage import RunContext
from hirestream.transforms.common.quarantine import PRECEDENCE, ReasonCode
from hirestream.transforms.common.stream import known_zone_ids
from hirestream.transforms.silver_scheduling import (
    CHECKED_ONLY,
    INTERVIEWER,
    INTERVIEWERS,
    QUARANTINE_COLUMNS,
    SCHEDULING_EVENTS_SCHEMA,
    SOURCE,
    STARTS,
    UTC_TIMES,
    VALUES,
    build,
)
from tests.unit.transforms.bronze import DROP, example

RULES = SourceRules.load(SOURCE)
RUN = RunContext("run-1", datetime(2026, 1, 1, tzinfo=UTC))
ARRIVAL = datetime(2030, 1, 1)  # after every example's event_ts
Body = str | dict[str, Any]
Line = Body | tuple[Body, str, int, datetime]  # a body, or (body, file, line number, arrival)
CASES: dict[str, list[Line]] = {}


def case(name: str, *lines: Line) -> str:
    assert name not in CASES and "/" not in name
    CASES[name] = list(lines)
    return name


Result = tuple[list[Row], list[Row]]


@pytest.fixture(scope="module")
def results(spark: SparkSession) -> dict[str, Result]:
    data = []
    for name, lines in CASES.items():
        for n, line in enumerate(lines, start=1):
            body, file, number, arrived = (
                line if isinstance(line, tuple) else (line, "part-0.jsonl.gz", n, ARRIVAL)
            )
            text = body if isinstance(body, str) else json.dumps(body)
            data.append((text, f"bronze/scheduling/{name}/{file}", number, arrived))
    rows, quarantined = build(
        spark.createDataFrame(data, LINE_SCHEMA), RULES, known_zone_ids(spark), RUN
    )
    out: dict[str, Result] = {name: ([], []) for name in CASES}
    for row in rows.collect():
        out[row._source_file.split("/")[2]][0].append(row)
    for row in quarantined.collect():
        out[row._source_file.split("/")[2]][1].append(row)
    return out


def rows_of(results: dict[str, Result], name: str) -> dict[str, Row]:
    rows, quarantined = results[name]
    assert quarantined == [], quarantined
    return {row.event_id: row for row in rows}


def rejected(results: dict[str, Result], name: str) -> Row:
    rows, quarantined = results[name]
    assert rows == [] and len(quarantined) == 1, (rows, quarantined)
    return quarantined[0]


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
    assert len(rows_of(results, EXAMPLES)) == len(CASES[EXAMPLES]) == 16


def test_rows_have_the_silver_schema(spark: SparkSession) -> None:
    empty = spark.createDataFrame([], LINE_SCHEMA)
    built, quarantined = build(empty, RULES, known_zone_ids(spark), RUN)
    expected = [(f.name, f.dataType) for f in SCHEDULING_EVENTS_SCHEMA.fields]
    assert [(f.name, f.dataType) for f in built.schema.fields] == expected
    assert tuple(quarantined.columns) == QUARANTINE_COLUMNS


V1 = example("interview_scheduled", 1)
PANEL = example("interview_scheduled", 2, 1)
FEEDBACK = example("feedback_submitted", 2)
UNIFIED = case("unified", V1, PANEL, FEEDBACK)


def test_v1_and_v2_scheduled_events_share_one_shape(results: dict[str, Result]) -> None:
    rows = rows_of(results, UNIFIED)
    old, new, written = rows[V1["event_id"]], rows[PANEL["event_id"]], rows[FEEDBACK["event_id"]]
    assert old.interviewer_ids == [V1["payload"]["interviewer_id"]]  # v1's scalar, wrapped
    assert (old.interviewer_id, old.interview_format, old.schema_version) == (None, None, 1)
    assert new.interviewer_ids == PANEL["payload"]["interviewer_ids"]
    assert (len(new.interviewer_ids), new.interview_format, new.schema_version) == (
        2,
        "in_person",
        2,
    )
    assert written.interviewer_id == FEEDBACK["payload"]["interviewer_id"]
    assert written.interviewer_ids is None
    assert written.word_count == FEEDBACK["payload"]["word_count"]


LATE_EVENING = example("interview_scheduled", 2, event_ts="2025-02-14T23:30:00.000Z")
COMPLETED = example("interview_completed", 2)
COLUMNS = case(
    "columns", (LATE_EVENING, "yyyy=2025/part-9.jsonl.gz", 7, datetime(2025, 2, 15)), COMPLETED
)


def test_columns_carry_utc_times_and_lineage(results: dict[str, Result]) -> None:
    rows = rows_of(results, COLUMNS)
    row = rows[LATE_EVENING["event_id"]]
    assert row.scheduled_start_utc == datetime(2025, 2, 18, 7, 15)  # 12:45 +05:30
    assert row.start_tz_inferred is False
    assert (row.event_ts, row.event_date) == (datetime(2025, 2, 14, 23, 30), date(2025, 2, 14))
    assert row.sent_ts == datetime(2025, 2, 14, 15, 21, 3, 12_000)
    assert (row._source_file, row._source_line, row._arrival_ts) == (
        f"bronze/scheduling/{COLUMNS}/yyyy=2025/part-9.jsonl.gz",
        7,
        datetime(2025, 2, 15),
    )
    assert (row._run_id, row._ingested_at) == ("run-1", datetime(2026, 1, 1))
    done = rows[COMPLETED["event_id"]]
    assert done.actual_start_utc is not None and done.actual_end_utc > done.actual_start_utc


# -- quarantine ---------------------------------------------------------------------------------


def soon(body: dict[str, Any], arrival: datetime) -> Line:
    return (body, "part-0.jsonl.gz", 1, arrival)


NAIVE = "2025-02-18T12:45:00"
REASON_LINES: list[tuple[str, Line, ReasonCode, str]] = [
    ("truncated", '{"event_id": "x", "event_type": "inter', ReasonCode.MALFORMED_JSON, "JSON"),
    ("array", "[1, 2]", ReasonCode.MALFORMED_JSON, "JSON"),
    ("json-null", "null", ReasonCode.MALFORMED_JSON, "JSON"),
    ("duplicate-keys", '{"event_id": "a", "event_id": "b"}', ReasonCode.MALFORMED_JSON, "JSON"),
    (
        "no-event-id",
        example("interview_completed", event_id=DROP),
        ReasonCode.MISSING_REQUIRED_FIELD,
        "missing or invalid: event_id",
    ),
    (
        "no-entity-key",
        example("interview_no_show", payload__interview_id=DROP),
        ReasonCode.MISSING_REQUIRED_FIELD,
        "payload.interview_id",
    ),
    (
        "null-event-ts",
        example("feedback_updated", event_ts=None),
        ReasonCode.MISSING_REQUIRED_FIELD,
        "event_ts",
    ),
    (
        "payload-not-an-object",
        example("interview_cancelled", payload="cancelled"),
        ReasonCode.MISSING_REQUIRED_FIELD,
        "payload.interview_id",
    ),
    (
        "unknown-event-type",
        example("interview_completed", event_type="INTERVIEW_COMPLETED"),
        ReasonCode.INVALID_ENUM,
        "event_type=INTERVIEW_COMPLETED",
    ),
    (
        "upper-cased-enum",
        example("interview_cancelled", payload__reason="OTHER"),
        ReasonCode.INVALID_ENUM,
        "payload.reason=OTHER",
    ),
    (
        "another-types-enum",
        example("interview_rescheduled", payload__reason="position_filled"),
        ReasonCode.INVALID_ENUM,
        "payload.reason=position_filled",
    ),
    (
        "wrong-source",
        example("interview_no_show", source="jobboard-web"),
        ReasonCode.INVALID_ENUM,
        "source=jobboard-web",
    ),
    (
        "unknown-version",
        example("interview_completed", schema_version=3),
        ReasonCode.UNKNOWN_SCHEMA_VERSION,
        "schema_version=3",
    ),
    (
        "no-version",
        example("interview_completed", schema_version=DROP),
        ReasonCode.UNKNOWN_SCHEMA_VERSION,
        "schema_version=null",
    ),
    (
        "no-millis",
        example("interview_completed", event_ts="2025-02-14T15:21:02Z"),
        ReasonCode.UNPARSEABLE_TIMESTAMP,
        "event_ts=2025-02-14T15:21:02Z",
    ),
    (
        "bad-actual-start",
        example("interview_completed", payload__actual_start="soon"),
        ReasonCode.UNPARSEABLE_TIMESTAMP,
        "payload.actual_start=soon",
    ),
    (
        "bad-start",
        example("interview_scheduled", payload__scheduled_start="tomorrow"),
        ReasonCode.UNPARSEABLE_TIMESTAMP,
        "payload.scheduled_start=tomorrow",
    ),
    (
        "impossible-naive-start",
        example("interview_scheduled", payload__scheduled_start="2025-02-30T10:00:00"),
        ReasonCode.UNPARSEABLE_TIMESTAMP,
        "payload.scheduled_start=2025-02-30T10:00:00",
    ),
    (
        "naive-no-zone",
        example("interview_scheduled", payload__scheduled_start=NAIVE, payload__timezone=DROP),
        ReasonCode.UNRESOLVABLE_TIMEZONE,
        "naive payload.scheduled_start and payload.timezone=null",
    ),
    (
        "naive-unknown-zone",
        example("interview_rescheduled", payload__new_start=NAIVE, payload__timezone="Mars/Base"),
        ReasonCode.UNRESOLVABLE_TIMEZONE,
        "naive payload.new_start and payload.timezone=Mars/Base",
    ),
    (
        "from-the-future",
        soon(
            example("interview_completed", event_ts="2025-02-14T12:00:00.001Z"),
            datetime(2025, 2, 14, 11),
        ),
        ReasonCode.OUT_OF_RANGE_TIMESTAMP,
        "after the object arrived: event_ts=2025-02-14T12:00:00.001Z",
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


def json_text(line: Line) -> str:
    body = line[0] if isinstance(line, tuple) else line
    return body if isinstance(body, str) else json.dumps(body)


def test_every_reason_code_has_a_case() -> None:
    assert {code for code, _ in REASONS.values()} == set(ReasonCode)


# Each line breaks two rules; the first in PRECEDENCE must win.
PAIRS = {
    case(f"pair-{i}", line): code
    for i, (line, code) in enumerate(
        [
            (
                example(
                    "interview_no_show", payload__interview_id=DROP, payload__no_show_party="X"
                ),
                ReasonCode.MISSING_REQUIRED_FIELD,
            ),
            (
                example("interview_completed", event_type="interview_done", schema_version=7),
                ReasonCode.INVALID_ENUM,
            ),
            (
                example("interview_completed", schema_version=9, event_ts="yesterday"),
                ReasonCode.UNKNOWN_SCHEMA_VERSION,
            ),
            (
                example(
                    "interview_rescheduled",
                    payload__previous_start="?",
                    payload__new_start=NAIVE,
                    payload__timezone=DROP,
                ),
                ReasonCode.UNPARSEABLE_TIMESTAMP,
            ),
            (
                soon(
                    example(
                        "interview_scheduled",
                        payload__scheduled_start=NAIVE,
                        payload__timezone=DROP,
                    ),
                    datetime(2020, 1, 1),
                ),
                ReasonCode.UNRESOLVABLE_TIMEZONE,
            ),
            (
                example(
                    "interview_scheduled",
                    payload__interview_type="PANEL",
                    payload__scheduled_start=NAIVE,
                    payload__timezone=DROP,
                ),
                ReasonCode.INVALID_ENUM,
            ),
        ]
    )
}


@pytest.mark.parametrize("name", PAIRS)
def test_the_first_code_in_precedence_wins(results: dict[str, Result], name: str) -> None:
    assert rejected(results, name).reason_code == PAIRS[name].value


def test_pairs_cover_each_step_of_the_precedence() -> None:
    assert set(PAIRS.values()) >= set(PRECEDENCE[1:-1])  # each code beats the next one


BAD_ENUM = example("interview_cancelled", event_ts="2025-03-01T08:00:00.000Z", payload__reason="?")
PARTITIONS = case("partitions", "{not json", BAD_ENUM)


def test_quarantine_is_partitioned_by_event_date_when_it_can_be(
    results: dict[str, Result],
) -> None:
    rows, quarantined = results[PARTITIONS]
    by_reason = {line.reason_code: line for line in quarantined}
    assert rows == [] and list(quarantined[0].asDict()) == list(QUARANTINE_COLUMNS)
    assert by_reason["MALFORMED_JSON"].event_date is None
    assert by_reason["INVALID_ENUM"].event_date == date(2025, 3, 1)


# -- time zones ---------------------------------------------------------------------------------

DST = {
    case(f"dst-{i}", example("interview_scheduled", 1, payload__scheduled_start=naive,
                              payload__timezone=zone)): (zone, utc)
    for i, (naive, zone, utc) in enumerate([
        ("2025-03-09T02:30:00", "America/New_York", datetime(2025, 3, 9, 7, 30)),  # gap: later
        ("2025-11-02T01:30:00", "America/New_York", datetime(2025, 11, 2, 5, 30)),  # overlap: EDT
        ("2025-03-30T01:30:00", "Europe/London", datetime(2025, 3, 30, 1, 30)),  # gap: later
        ("2025-10-26T01:30:00", "Europe/London", datetime(2025, 10, 26, 0, 30)),  # overlap: BST
        ("2025-06-01T10:00:00", "Asia/Kolkata", datetime(2025, 6, 1, 4, 30)),
    ])
}  # fmt: skip


@pytest.mark.parametrize("name", DST)
def test_naive_starts_are_read_in_their_zone(results: dict[str, Result], name: str) -> None:
    zone, utc = DST[name]
    (row,) = rows_of(results, name).values()
    assert (row.scheduled_start_utc, row.start_tz_inferred, row.timezone) == (utc, True, zone)


LONDON = {"payload__new_start": "2025-04-23T09:00:00", "payload__timezone": "Europe/London"}
RESCHEDULED = case(
    "rescheduled",
    example("interview_rescheduled", 1, payload__previous_start="2025-04-22T10:30:00", **LONDON),
    example(
        "interview_rescheduled", 1, payload__previous_start="2025-04-22T10:30:00+01:00", **LONDON
    ),
)


def test_rescheduled_starts_resolve_independently(results: dict[str, Result]) -> None:
    for row in rows_of(results, RESCHEDULED).values():
        assert (row.previous_start_utc, row.new_start_utc, row.start_tz_inferred) == (
            datetime(2025, 4, 22, 9, 30),
            datetime(2025, 4, 23, 8, 0),
            True,
        )


# -- drift and leniency -------------------------------------------------------------------------

STRAY = example("interview_cancelled", payload__recommendation="hire", extra_top="drift")
DRIFT = case("drift", STRAY)


def test_unknown_and_stray_fields_are_ignored(results: dict[str, Result]) -> None:
    (row,) = rows_of(results, DRIFT).values()
    assert row.recommendation is None  # not a field of interview_cancelled
    assert row.reason == STRAY["payload"]["reason"]


UNREADABLE = example("interview_scheduled", payload__duration_minutes="long")
STRINGLY = example("interview_scheduled", payload__duration_minutes="45")
NO_REASON = example("interview_cancelled", payload__reason=DROP)
LENIENT = case("lenient", UNREADABLE, STRINGLY, NO_REASON)


def test_non_key_fields_that_cannot_be_read_arrive_as_null(results: dict[str, Result]) -> None:
    rows = rows_of(results, LENIENT)
    assert rows[UNREADABLE["event_id"]].duration_minutes is None
    assert rows[STRINGLY["event_id"]].duration_minutes == 45  # Spark's ANSI cast reads "45"
    assert rows[NO_REASON["event_id"]].reason is None


# -- duplicates ---------------------------------------------------------------------------------

TWICE = example("interview_completed")
EARLY, LATE = datetime(2030, 1, 1, 1), datetime(2030, 1, 1, 2)
COPIES = case(
    "copies",
    (TWICE, "b", 1, LATE),
    (TWICE, "c", 9, EARLY),
    (TWICE, "a", 5, EARLY),
    (TWICE, "a", 3, EARLY),
)


def test_the_earliest_copy_wins(results: dict[str, Result]) -> None:
    (row,) = rows_of(results, COPIES).values()
    assert (row._source_file, row._source_line, row._arrival_ts) == (
        f"bronze/scheduling/{COPIES}/a",
        3,
        EARLY,
    )


TWIN = example("interview_no_show")
TWINS = case("twins", json.dumps(TWIN)[:40], TWIN)


def test_a_broken_copy_is_quarantined_and_its_twin_kept(results: dict[str, Result]) -> None:
    rows, quarantined = results[TWINS]
    assert [row.event_id for row in rows] == [TWIN["event_id"]]
    assert [line.reason_code for line in quarantined] == ["MALFORMED_JSON"]


# -- coverage of the contracts ------------------------------------------------------------------


def test_every_contract_field_has_a_silver_column() -> None:
    mapped = {path for _, path in (*VALUES, *UTC_TIMES, *STARTS)} | {INTERVIEWER, INTERVIEWERS}
    assert mapped | set(CHECKED_ONLY) == RULES.paths
