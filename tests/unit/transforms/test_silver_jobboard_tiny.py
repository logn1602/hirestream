"""silver.jobboard_events reconciled with a real tiny backfill (ADR-0015, ADR-0020).

The judges, as for scheduling:
- quarantine counts per reason and the number of events equal `ground_truth.json`;
- each quarantined line gets the verdict of the jsonschema-based reference classifier
  (tests/contract_checks.py);
- every silver column equals what plain Python makes of the raw line;
- each event is its earliest kept copy.

Asking the reference about every one of tiny's 178k job board lines takes about 26 s, so that
test is `slow`, like the generator's own every-line check of the ground truth. The fast tests
imply it: silver's codes for its quarantined lines match the reference, and its counts per code
match the ground truth's, which the generator's slow test shows are the reference's counts over
every line.
"""

import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import Row, SparkSession

from hirestream.generator.run import BackfillResult
from hirestream.transforms.common.event_rules import SourceRules
from hirestream.transforms.common.lake import Lake, LakePaths
from hirestream.transforms.common.lineage import RunContext
from hirestream.transforms.common.quarantine import ReasonCode
from hirestream.transforms.silver_jobboard import (
    BRONZE,
    SOURCE,
    VALUES,
    classify,
    events,
    quarantine,
)
from tests.contract_checks import Contracts, quarantine_reason
from tests.unit.transforms.tiny import EPOCH, Copy, TinyBronze, kept_copies, tiny_bronze, value

RULES = SourceRules.load(SOURCE)
RUN = RunContext("tiny", datetime(2026, 1, 1, tzinfo=UTC))


@dataclass
class Tiny:
    rows: list[Row]
    rejected: list[Row]
    bronze: TinyBronze


@pytest.fixture(scope="module")
def tiny(spark: SparkSession, backfill: BackfillResult) -> Iterator[Tiny]:
    bronze = tiny_bronze(backfill, BRONZE)
    lake = Lake(spark, LakePaths.from_root(bronze.root))
    classified = classify(lake.read_lines(bronze.keys), RULES).cache()
    yield Tiny(events(classified, RUN).collect(), quarantine(classified, RUN).collect(), bronze)
    classified.unpersist()


@pytest.fixture(scope="module")
def copies(tiny: Tiny) -> dict[str, list[Copy]]:
    return kept_copies(tiny.bronze, tiny.rejected)


def test_quarantine_and_event_counts_equal_the_ground_truth(tiny: Tiny) -> None:
    counts = {code.value: 0 for code in ReasonCode}
    for line in tiny.rejected:
        counts[line.reason_code] += 1
    truth = tiny.bronze.truth
    assert counts == truth["expected_quarantine"][SOURCE]
    assert len(tiny.rows) == truth["streams"][SOURCE]["expected_silver_events"]
    assert len({row.event_id for row in tiny.rows}) == len(tiny.rows)


def test_each_quarantined_line_gets_the_reference_verdict(tiny: Tiny, contracts: Contracts) -> None:
    for line in tiny.rejected:
        raw = tiny.bronze.lines[(line._source_file, line._source_line)]
        assert quarantine_reason(contracts, SOURCE, raw) == line.reason_code, raw
    found = {line.reason_code for line in tiny.rejected}
    assert found == {"MALFORMED_JSON", "MISSING_REQUIRED_FIELD", "INVALID_ENUM"}  # chaos's three


@pytest.mark.slow
def test_every_line_gets_the_reference_verdict(tiny: Tiny, contracts: Contracts) -> None:
    expected = {
        where: reason
        for where, raw in tiny.bronze.lines.items()
        if (reason := quarantine_reason(contracts, SOURCE, raw)) is not None
    }
    assert {(q._source_file, q._source_line): q.reason_code for q in tiny.rejected} == expected


def test_each_event_is_its_earliest_kept_copy(tiny: Tiny, copies: dict[str, list[Copy]]) -> None:
    assert {row.event_id for row in tiny.rows} == set(copies)
    assert sum(len(c) > 1 for c in copies.values()) > 0  # the batch has duplicates to drop
    for row in tiny.rows:
        assert (row._arrival_ts, row._source_file, row._source_line) == min(copies[row.event_id])


def test_every_column_equals_pythons_reading_of_the_line(
    tiny: Tiny, copies: dict[str, list[Copy]]
) -> None:
    for row in tiny.rows:
        _, key, n = min(copies[row.event_id])
        body = json.loads(tiny.bronze.lines[(key, n)])
        for column, path in VALUES:
            assert row[column] == value(body, path), (column, body)
        assert row.event_ts_utc == EPOCH + timedelta(milliseconds=body["event_ts"]), body
        sent = datetime.fromisoformat(body["sent_ts"].replace("Z", "+00:00"))
        assert row.sent_ts == sent.astimezone(UTC).replace(tzinfo=None), body
        assert row.event_date == row.event_ts_utc.date()


def test_both_versions_every_type_and_internal_visitors_are_present(tiny: Tiny) -> None:
    assert {row.event_type for row in tiny.rows} == RULES.event_types
    assert {row.schema_version for row in tiny.rows} == {1, 2}
    assert all((row.device_type is None) == (row.schema_version == 1) for row in tiny.rows)
    assert any(row.employee_id is not None for row in tiny.rows)
