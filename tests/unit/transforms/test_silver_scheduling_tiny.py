"""silver.scheduling_events reconciled with a real tiny backfill (ADR-0015, ADR-0019).

Three independent judges:
- every bronze line gets the verdict of the jsonschema-based classifier the generator's ground
  truth was checked with (tests/contract_checks.py);
- quarantine counts per reason and the number of events equal `ground_truth.json`;
- every silver column equals what plain Python (`json`, `zoneinfo`) makes of the raw line.
"""

import gzip
import json
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from pyspark.sql import Row, SparkSession

from hirestream.generator.run import BackfillResult
from hirestream.transforms.common.event_rules import SourceRules
from hirestream.transforms.common.lake import Lake, LakePaths
from hirestream.transforms.common.lineage import RunContext
from hirestream.transforms.common.quarantine import ReasonCode
from hirestream.transforms.common.stream import known_zone_ids
from hirestream.transforms.silver_scheduling import (
    BRONZE,
    SOURCE,
    STARTS,
    UTC_TIMES,
    VALUES,
    classify,
    events,
    quarantine,
)
from tests.contract_checks import Contracts, quarantine_reason

RUN = RunContext("tiny", datetime(2026, 1, 1, tzinfo=UTC))
EPOCH = datetime(1970, 1, 1)


@dataclass
class Tiny:
    rows: list[Row]
    rejected: list[Row]
    truth: dict[str, Any]
    lines: dict[tuple[str, int], bytes]  # (key, line number) -> raw bytes
    arrival: dict[str, datetime]  # key -> the object's mtime, as Spark reads it (ms)


@pytest.fixture(scope="module")
def tiny(spark: SparkSession, backfill: BackfillResult) -> Iterator[Tiny]:
    root = backfill.manifest_path.parents[2]  # <lake>/_runs/<run_id>/manifest.json
    keys = sorted(
        f.path for f in backfill.simulation.stream_files if f.path.startswith(f"bronze/{BRONZE}/")
    )
    lines: dict[tuple[str, int], bytes] = {}
    arrival: dict[str, datetime] = {}
    for key in keys:
        path = root / key
        raw = gzip.decompress(path.read_bytes()).split(b"\n")
        assert raw[-1] == b""  # every line ends in a newline
        lines |= {(key, n): line for n, line in enumerate(raw[:-1], start=1)}
        arrival[key] = EPOCH + timedelta(milliseconds=path.stat().st_mtime_ns // 1_000_000)
    lake = Lake(spark, LakePaths.from_root(root))
    classified = classify(
        lake.read_lines(keys), SourceRules.load(SOURCE), known_zone_ids(spark)
    ).cache()
    truth = json.loads(Path(backfill.ground_truth_path).read_text())
    yield Tiny(
        events(classified, RUN).collect(),
        quarantine(classified, RUN).collect(),
        truth,
        lines,
        arrival,
    )
    classified.unpersist()


def test_every_line_gets_the_reference_verdict(tiny: Tiny, contracts: Contracts) -> None:
    expected = {
        where: reason
        for where, line in tiny.lines.items()
        if (reason := quarantine_reason(contracts, SOURCE, line)) is not None
    }
    got = {(q._source_file, q._source_line): q.reason_code for q in tiny.rejected}
    assert got == expected
    assert sum(1 for q in tiny.rejected if q.reason_code == "UNRESOLVABLE_TIMEZONE") > 0


def test_quarantine_and_event_counts_equal_the_ground_truth(tiny: Tiny) -> None:
    counts = {code.value: 0 for code in ReasonCode}
    for line in tiny.rejected:
        counts[line.reason_code] += 1
    assert counts == tiny.truth["expected_quarantine"][SOURCE]
    assert len(tiny.rows) == tiny.truth["streams"][SOURCE]["expected_silver_events"]
    assert len({row.event_id for row in tiny.rows}) == len(tiny.rows)


def kept_copies(tiny: Tiny) -> dict[str, list[tuple[datetime, str, int]]]:
    rejected = {(q._source_file, q._source_line) for q in tiny.rejected}
    copies: dict[str, list[tuple[datetime, str, int]]] = {}
    for (key, n), line in tiny.lines.items():
        if (key, n) not in rejected:
            event_id = json.loads(line)["event_id"]
            copies.setdefault(event_id, []).append((tiny.arrival[key], key, n))
    return copies


def test_each_event_is_its_earliest_kept_copy(tiny: Tiny) -> None:
    copies = kept_copies(tiny)
    assert {row.event_id for row in tiny.rows} == set(copies)
    assert sum(len(c) > 1 for c in copies.values()) > 0  # the batch has duplicates to drop
    for row in tiny.rows:
        assert (row._arrival_ts, row._source_file, row._source_line) == min(copies[row.event_id])


def value(body: dict[str, Any], path: str) -> Any:
    node: Any = body
    for part in path.split("."):
        node = node.get(part) if isinstance(node, dict) else None
    return node


def utc(text: str | None, zone: str | None = None) -> datetime | None:
    """Python's reading of a time: offsets as given, naive times in their zone. zoneinfo's
    default fold=0 takes the earlier offset in an overlap and the pre-gap offset in a gap, which
    puts a gap time later: the same policy as SPEC §9.1."""
    if text is None:
        return None
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        assert zone is not None
        parsed = parsed.replace(tzinfo=ZoneInfo(zone))
    return parsed.astimezone(UTC).replace(tzinfo=None)


def test_every_column_equals_pythons_reading_of_the_line(tiny: Tiny) -> None:
    copies = kept_copies(tiny)
    inferred = 0
    for row in tiny.rows:
        _, key, n = min(copies[row.event_id])
        body = json.loads(tiny.lines[(key, n)])
        for column, path in VALUES:
            assert row[column] == value(body, path), (column, body)
        for column, path in UTC_TIMES:
            assert row[column] == utc(value(body, path)), (column, body)
        zone = value(body, "payload.timezone")
        naive = False
        for column, path in STARTS:
            text = value(body, path)
            naive |= text is not None and datetime.fromisoformat(text).tzinfo is None
            assert row[column] == utc(text, zone), (column, body)
        assert row.start_tz_inferred is naive
        inferred += naive
        one, panel = value(body, "payload.interviewer_id"), value(body, "payload.interviewer_ids")
        if body["event_type"] == "interview_scheduled":
            assert row.interviewer_ids == (panel if panel is not None else [one])
            assert row.interviewer_id is None
        else:
            assert (row.interviewer_ids, row.interviewer_id) == (None, one)
    bug = tiny.truth["streams"][SOURCE]["timezone_bug"]
    assert 0 < inferred <= sum(bug["naive_starts"].values()) - sum(bug["missing_timezone"].values())


def test_both_schema_versions_and_panels_are_present(tiny: Tiny) -> None:
    scheduled = [row for row in tiny.rows if row.event_type == "interview_scheduled"]
    assert {row.schema_version for row in scheduled} == {1, 2}
    assert all(len(row.interviewer_ids) == 1 for row in scheduled if row.schema_version == 1)
    assert any(len(row.interviewer_ids) == 2 for row in scheduled if row.schema_version == 2)
    assert all(row.interview_format is not None for row in scheduled if row.schema_version == 2)
