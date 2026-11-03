import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from hirestream.transforms.common.quarantine import PRECEDENCE, ReasonCode
from hirestream.transforms.common.stream import first_reason, flagged, known_zone_ids


def test_first_reason_needs_a_check_for_every_code() -> None:
    checks = {code: (F.lit(False), F.lit("")) for code in PRECEDENCE[1:]}
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
