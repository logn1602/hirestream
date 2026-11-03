from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from hirestream.transforms.common.event_rules import SourceRules
from hirestream.transforms.common.quarantine import PRECEDENCE, ReasonCode
from hirestream.transforms.common.stream import (
    contract_field,
    first_reason,
    flagged,
    invalid_values,
    known_zone_ids,
    read_contract_fields,
)


def test_first_reason_needs_a_check_for_every_code(spark: SparkSession) -> None:
    checks = {code: (F.lit(False), F.lit("")) for code in PRECEDENCE[1:]}  # F.lit needs a session
    with pytest.raises(ValueError, match="MALFORMED_JSON"):
        first_reason(checks)


def test_first_reason_follows_precedence_and_treats_null_as_passed(spark: SparkSession) -> None:
    failing = {ReasonCode.INVALID_ENUM, ReasonCode.OUT_OF_RANGE_TIMESTAMP}
    checks = {code: (F.lit(code in failing), F.lit(f"why {code.value}")) for code in PRECEDENCE} | {
        ReasonCode.MALFORMED_JSON: (F.lit(None).cast("boolean"), F.lit("never"))
    }
    code, detail = first_reason(checks)
    assert spark.range(1).select(code, detail).first() == ("INVALID_ENUM", "why INVALID_ENUM")
    passing = {reason: (F.lit(False), F.lit("x")) for reason in PRECEDENCE}
    assert spark.range(1).select(*first_reason(passing)).first() == (None, None)


def test_flagged_keeps_the_labels_of_true_flags(spark: SparkSession) -> None:
    labels = flagged(
        [(F.lit(True), F.lit("a")), (F.lit(None), F.lit("b")), (F.lit(True), F.lit("c"))]
    )
    assert spark.range(1).select(labels, flagged([])).first() == (["a", "c"], [])


def test_known_zone_ids_are_the_jvms_region_ids(spark: SparkSession) -> None:
    zones = known_zone_ids(spark)
    assert {"Asia/Kolkata", "Europe/London", "America/New_York", "UTC"} <= set(zones)
    assert "Mars/Base" not in zones and list(zones) == sorted(zones)


def contract(event_type: str, **payload: Any) -> dict[str, Any]:
    envelope = {
        "event_type": {"type": "string", "const": event_type},
        "schema_version": {"type": "integer", "const": 1},
        "source": {"type": "string", "const": "test"},
    }
    return {"properties": envelope | {"payload": {"type": "object", "properties": payload}}}


TEST_RULES = SourceRules.from_schemas(
    "test",
    {
        ("a", 1): contract("a", key={"type": "string"}, kind={"type": "string", "enum": ["x"]}),
        ("b", 1): contract("b", key={"type": "string"}, kind={"type": "string", "enum": ["y"]}),
        ("c", 1): contract("c", key={"type": "string"}, size={"type": "integer"}),
    },
)


def test_nested_fields_are_read_and_checked_per_contract(spark: SparkSession) -> None:
    lines = [
        '{"event_type":"a","schema_version":1,"source":"test","payload":{"key":"1","kind":"x"}}',
        '{"event_type":"b","schema_version":1,"source":"test","payload":{"key":"2","kind":"x"}}',
        '{"event_type":"c","schema_version":1,"source":"test","payload":{"key":"3","kind":"x"}}',
        '{"event_type":"d","schema_version":1,"source":"test","payload":{"key":"4","size":5}}',
    ]
    df = spark.createDataFrame([(line,) for line in lines], "value string")
    fields = read_contract_fields(df, TEST_RULES, required=("payload.key",))
    invalid, detail = invalid_values(TEST_RULES)
    got = fields.select(
        contract_field("payload.key"),
        contract_field("payload.kind"),
        contract_field("payload.size"),
        invalid,
        detail,
    ).collect()
    assert [tuple(row) for row in got] == [
        ("1", "x", None, False, ""),
        ("2", "x", None, True, "payload.kind=x"),  # b's own list holds only "y"
        ("3", None, None, False, ""),  # c has no `kind`: drift, not read and not checked
        ("4", None, None, True, "event_type=d"),  # the key is read for every line
    ]
