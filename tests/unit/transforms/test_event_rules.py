from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from hirestream.transforms.common.event_rules import (
    FieldRule,
    SourceRules,
    contract_key,
    contract_key_column,
    extract,
)

SCHEDULING = SourceRules.load("scheduling-service")
ENVELOPE = ("event_id", "event_type", "schema_version", "source", "producer_version", "event_ts")


def test_every_scheduling_contract_is_loaded() -> None:
    assert SCHEDULING.event_types == {
        "interview_scheduled",
        "interview_rescheduled",
        "interview_cancelled",
        "interview_completed",
        "interview_no_show",
        "feedback_submitted",
        "feedback_updated",
    }
    assert set(SCHEDULING.events) == {
        contract_key(t, v) for t in SCHEDULING.event_types for v in (1, 2)
    }
    for rules in SCHEDULING.events.values():
        assert set(ENVELOPE) | {"sent_ts", "payload.interview_id"} <= set(rules.fields)


def test_fields_carry_type_nullability_allowed_values_and_minimum() -> None:
    assert SCHEDULING.field("payload.duration_minutes") == FieldRule(
        "payload.duration_minutes", "bigint", nullable=False, allowed=None, minimum=1
    )
    assert SCHEDULING.field("payload.interview_id").minimum is None
    assert SCHEDULING.field("payload.loop_id").nullable
    assert SCHEDULING.field("payload.interviewer_ids").spark_type == "array<string>"
    assert SCHEDULING.field("schema_version").spark_type == "bigint"
    assert SCHEDULING.events["interview_completed:v2"].fields["schema_version"].allowed == (
        frozenset({"2"})
    )
    assert SCHEDULING.field("source").allowed == frozenset({"scheduling-service"})


def test_enums_are_per_contract() -> None:
    reasons = SCHEDULING.allowed("payload.reason")
    assert set(reasons) == {
        "interview_cancelled:v1",
        "interview_cancelled:v2",
        "interview_rescheduled:v1",
        "interview_rescheduled:v2",
    }
    assert "position_filled" in reasons["interview_cancelled:v1"]
    assert "position_filled" not in reasons["interview_rescheduled:v1"]


def test_v2_changes_only_the_scheduled_payload() -> None:
    assert SCHEDULING.keys_with("payload.interviewer_ids") == ("interview_scheduled:v2",)
    assert SCHEDULING.keys_with("payload.interview_format") == ("interview_scheduled:v2",)
    scheduled_v1 = ("interview_scheduled:v1",)
    feedback = tuple(f"feedback_{kind}:v{v}" for kind in ("submitted", "updated") for v in (1, 2))
    assert set(SCHEDULING.keys_with("payload.interviewer_id")) == {*scheduled_v1, *feedback}


def test_job_board_contracts_load_with_their_context() -> None:
    jobboard = SourceRules.load("jobboard-web")
    assert jobboard.field("event_ts").spark_type == "bigint"  # epoch milliseconds (SPEC §7.1)
    assert jobboard.field("event_ts").minimum == 10**12  # which rules out epoch seconds
    assert jobboard.field("context.employee_id").nullable
    assert jobboard.keys_with("context.device_type") == tuple(
        key for key in jobboard.events if key.endswith(":v2")
    )


def schema(**payload: Any) -> dict[str, Any]:
    return {"properties": {"event_id": {"type": "string"}, "payload": {
        "type": "object", "properties": payload}}}  # fmt: skip


def test_a_field_must_have_one_type_and_minimum_across_contracts() -> None:
    schemas = {
        ("a", 1): schema(n={"type": "integer"}),
        ("b", 1): schema(n={"type": "string"}),
    }
    with pytest.raises(ValueError, match=r"'payload\.n' has different types"):
        SourceRules.from_schemas("test", schemas)
    bounds = {
        ("a", 1): schema(n={"type": "integer", "minimum": 0}),
        ("b", 1): schema(n={"type": "integer", "minimum": 1}),
    }
    with pytest.raises(ValueError, match=r"'payload\.n' has different minimums"):
        SourceRules.from_schemas("test", bounds)
    with pytest.raises(KeyError, match=r"no test contract has 'payload\.x'"):
        SourceRules.from_schemas("test", {("a", 1): schema()}).field("payload.x")
    with pytest.raises(ValueError, match="no contracts for source"):
        SourceRules.load("nobody")


def test_extract_reads_typed_values_and_null_for_the_rest(spark: SparkSession) -> None:
    lines = [
        '{"event_type":"a","schema_version":1,"payload":{"n":45,"tags":["x"],"s":"t"}}',
        '{"event_type":"a","schema_version":2,"payload":{"n":"45","tags":"x","s":null}}',
        '{"payload":{"n":"many"}}',
    ]
    df = spark.createDataFrame([(line,) for line in lines], "value string")
    parsed = F.try_parse_json("value")
    n = FieldRule("payload.n", "bigint", nullable=False, allowed=None)
    tags = FieldRule("payload.tags", "array<string>", nullable=False, allowed=None)
    s = FieldRule("payload.s", "string", nullable=True, allowed=None)
    event_type = FieldRule("event_type", "string", nullable=False, allowed=None)
    version = FieldRule("schema_version", "bigint", nullable=False, allowed=None)
    key = contract_key_column(extract(parsed, event_type), extract(parsed, version))
    rows = df.select(
        extract(parsed, n), extract(parsed, tags), extract(parsed, s), key.alias("key")
    ).collect()
    assert [tuple(r) for r in rows] == [
        (45, ["x"], "t", "a:v1"),
        (45, None, None, "a:v2"),  # ANSI cast: "45" reads as 45; a string isn't an array
        (None, None, None, None),
    ]
