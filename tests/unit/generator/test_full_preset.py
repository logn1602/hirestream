"""SPEC §6.11 at `full` (T1.11): contracts on the lines a full backfill actually wrote.

Needs a full lake: `HIRESTREAM_FULL_LAKE=<lake root> uv run pytest -m full`, skipped otherwise.

- **Scheduling:** every line is classified, so the quarantine counts must equal the ground
  truth's exactly (ADR-0015 §8).
- **Job board:** about 20 M lines, so every 50th is checked. Each must be valid, or carry exactly
  the fault that chaos or the timezone bug explains. The sampled fault shares must match the
  ground truth's within sampling error, and every job-board contract must be met at least once.
"""

from __future__ import annotations

import gzip
import json
import math
import os
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from hirestream.generator.manifest import read_manifest
from hirestream.generator.sinks import SOURCE_DIRS
from tests.contract_checks import STARTS, VALID, Contracts, Signature, quarantine_reason

LAKE = os.environ.get("HIRESTREAM_FULL_LAKE")
pytestmark = [
    pytest.mark.full,
    pytest.mark.no_cover,
    pytest.mark.skipif(not LAKE, reason="HIRESTREAM_FULL_LAKE is not set"),
]
EVERY = 50  # job-board lines sampled: about 400k of 20 M
TIMEZONE_BUG = frozenset(
    {("pattern", f"/payload/{key}") for key in STARTS} | {("required", "/payload/timezone")}
)


@pytest.fixture(scope="module")
def run() -> tuple[Path, dict[str, Any]]:
    assert LAKE is not None
    lake = Path(LAKE)
    runs = sorted((lake / "_runs").iterdir())
    assert len(runs) == 1, runs  # a backfill never mixes two runs
    assert read_manifest(runs[0] / "manifest.json").preset == "full"
    truth: dict[str, Any] = json.loads((runs[0] / "ground_truth.json").read_text())
    return lake, truth


def _lines(lake: Path, source: str) -> Iterator[bytes]:
    for part in sorted((lake / "bronze" / SOURCE_DIRS[source]).rglob("*.jsonl.gz")):
        yield from gzip.decompress(part.read_bytes()).splitlines()


def _explained(source: str, signature: Signature) -> bool:
    """Chaos breaks a line in exactly one way (ADR-0012); the timezone bug breaks every start
    field's offset and sometimes drops `timezone` (ADR-0011). Nothing else may be wrong."""
    rest = signature - TIMEZONE_BUG if source == "scheduling-service" else signature
    return len(rest) <= 1 and all(keyword in ("required", "enum", "const") for keyword, _ in rest)


def test_every_scheduling_line_is_valid_or_explained(
    run: tuple[Path, dict[str, Any]], contracts: Contracts
) -> None:
    lake, truth = run
    source = "scheduling-service"
    reasons: Counter[str] = Counter()
    lines = 0
    for line in _lines(lake, source):
        lines += 1
        reason = quarantine_reason(contracts, source, line)
        if reason is not None:
            reasons[reason] += 1
        if reason != "MALFORMED_JSON":
            signature = contracts.signature(source, json.loads(line))
            assert _explained(source, signature), (signature, line[:300])
    assert lines == truth["streams"][source]["lines"]
    assert reasons == {k: v for k, v in truth["expected_quarantine"][source].items() if v}


def test_a_job_board_sample_is_valid_or_explained(
    run: tuple[Path, dict[str, Any]], contracts: Contracts
) -> None:
    lake, truth = run
    source = "jobboard-web"
    reasons: Counter[str] = Counter()
    met: set[tuple[str, int]] = set()
    sampled = lines = 0
    for i, line in enumerate(_lines(lake, source)):
        lines += 1
        if i % EVERY:
            continue
        sampled += 1
        try:
            body = json.loads(line)
        except ValueError:
            reasons["MALFORMED_JSON"] += 1
            continue
        signature = contracts.signature(source, body)
        if signature == VALID:
            met.add((body["event_type"], body["schema_version"]))
            continue
        assert _explained(source, signature), (signature, line[:300])
        reason = quarantine_reason(contracts, source, line)
        assert reason is not None, (signature, line[:300])
        reasons[reason] += 1
    assert lines == truth["streams"][source]["lines"]
    assert sampled == math.ceil(lines / EVERY)
    assert met == {(t, v) for s, t, v in contracts.schemas if s == source}
    expected = {k: v for k, v in truth["expected_quarantine"][source].items() if v}
    assert set(reasons) <= set(expected)
    for reason, n in expected.items():
        mean = n / lines * sampled  # a binomial count: allow 4 standard deviations
        assert abs(reasons[reason] - mean) <= 4 * math.sqrt(mean) + 1, (reason, reasons, mean)
