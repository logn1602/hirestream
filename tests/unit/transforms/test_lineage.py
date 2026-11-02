from datetime import UTC, datetime, timedelta, timezone

import pytest
from pyspark.sql import DataFrame, SparkSession

from hirestream.generator.ground_truth import REASONS
from hirestream.transforms.common.lineage import (
    INGESTED_AT,
    RUN_ID,
    content_fingerprint,
    with_run_lineage,
)
from hirestream.transforms.common.quarantine import QUARANTINE_SCHEMA, ReasonCode

STARTED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
ROWS = [("a", 1), ("b", None), (None, 2)]


def frame(spark: SparkSession, rows: list[tuple[str | None, int | None]]) -> DataFrame:
    return spark.createDataFrame(rows, "k string, v int")


def test_run_lineage_stamps_every_row_with_the_run(spark: SparkSession) -> None:
    other_zone = STARTED.astimezone(timezone(timedelta(hours=-5)))  # the same instant
    df = with_run_lineage(frame(spark, ROWS), run_id="run-1", ingested_at=other_zone)
    assert df.columns == ["k", "v", RUN_ID, INGESTED_AT]
    assert {(r[RUN_ID], r[INGESTED_AT]) for r in df.collect()} == {
        ("run-1", datetime(2026, 1, 2, 3, 4, 5))
    }


@pytest.mark.parametrize(
    ("run_id", "ingested_at", "message"),
    [("", STARTED, "run_id"), ("run-1", STARTED.replace(tzinfo=None), "timezone-aware")],
)
def test_run_lineage_rejects_a_missing_run_or_a_naive_time(
    spark: SparkSession, run_id: str, ingested_at: datetime, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        with_run_lineage(frame(spark, ROWS), run_id=run_id, ingested_at=ingested_at)


def test_fingerprint_ignores_order_partitioning_and_the_run(spark: SparkSession) -> None:
    first = with_run_lineage(frame(spark, ROWS), run_id="run-1", ingested_at=STARTED)
    rerun = with_run_lineage(
        frame(spark, ROWS[::-1]).repartition(3), run_id="run-2", ingested_at=STARTED + timedelta(1)
    )
    assert content_fingerprint(first) == content_fingerprint(rerun)
    assert content_fingerprint(first, exclude=()) != content_fingerprint(rerun, exclude=())


@pytest.mark.parametrize(
    "rows",
    [
        [("a", 1), ("b", None), (None, 3)],  # a changed value
        [("a", 1), ("b", None), (None, 2), ("a", 1)],  # a duplicated row
        [("a", 1), (None, None), ("b", 2)],  # a null moved to another row
        [("a", 1), ("b", None)],  # a missing row
    ],
)
def test_fingerprint_sees_any_change_to_the_rows(
    spark: SparkSession, rows: list[tuple[str | None, int | None]]
) -> None:
    assert content_fingerprint(frame(spark, rows)) != content_fingerprint(frame(spark, ROWS))


def test_fingerprint_sees_a_type_change(spark: SparkSession) -> None:
    as_long = spark.createDataFrame([("a", 1)], "k string, v bigint")
    assert content_fingerprint(as_long) != content_fingerprint(frame(spark, [("a", 1)]))


def test_fingerprint_of_an_empty_frame_and_of_nothing(spark: SparkSession) -> None:
    assert content_fingerprint(frame(spark, [])) == content_fingerprint(frame(spark, []))
    with pytest.raises(ValueError, match="every column is excluded"):
        content_fingerprint(frame(spark, ROWS), exclude=("k", "v"))


def test_reason_codes_match_the_ground_truth() -> None:
    # The generator predicts quarantine counts per code; silver must use the same codes.
    assert tuple(ReasonCode) == REASONS
    assert QUARANTINE_SCHEMA.fieldNames()[:3] == ["raw_line", "reason_code", "reason_detail"]
