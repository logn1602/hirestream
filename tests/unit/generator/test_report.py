"""generation_report.md (SPEC §6.10, ADR-0015): complete, readable, and never hashed."""

import json
from typing import Any

from hirestream.generator import report
from hirestream.generator.calibration import Check
from hirestream.generator.run import BackfillResult


def _text(backfill: BackfillResult) -> str:
    return backfill.report_path.read_text()


def test_written_beside_the_manifest_and_left_out_of_its_hashes(backfill: BackfillResult) -> None:
    assert backfill.report_path == backfill.manifest_path.parent / report.REPORT
    assert report.REPORT not in json.dumps(backfill.manifest.model_dump(mode="json"))


def test_every_check_is_listed_with_its_result(backfill: BackfillResult) -> None:
    lines = _text(backfill).splitlines()
    for check in backfill.checks:
        row = next(line for line in lines if line.startswith(f"| {check.name} |"))
        assert row.endswith("| pass |" if check.passed else "| **warn** |"), row
    passed = sum(c.passed for c in backfill.checks)
    assert f"{passed} of {len(backfill.checks)} within target." in _text(backfill)


def test_sections_and_counts(backfill: BackfillResult) -> None:
    text = _text(backfill)
    assert [line for line in text.splitlines() if line.startswith("#")] == [
        "# Generation report: gt",
        "## Calibration",
        "## Hidden truths (SPEC §13.2)",
        "## Sources",
        "## Workforce",
        "## Chaos injected",
    ]
    sim = backfill.simulation
    for source, n in sim.chaos_truth.events.items():
        assert f"| {source} | {n:,} |" in text
    for (source, event_type), n in sim.chaos_truth.event_types.items():
        assert f"| {source} | {event_type} | {n:,} |" in text
    assert f"| applications | {len(sim.ats_snapshot.applications):,} | not loaded |" in text
    assert "| Preset | tiny |" in text
    assert "| scheduling-service | timezone bug: naive starts | 261 |" in text


def test_tables_are_well_formed(backfill: BackfillResult) -> None:
    """Every row has its header's column count, so the Markdown renders as a table."""
    columns = 0
    for line in _text(backfill).splitlines():
        if not line.startswith("|"):
            columns = 0
            continue
        cells = line.count("|") - 1
        columns = columns or cells
        assert cells == columns, line


def test_only_runtime_and_memory_vary(backfill: BackfillResult) -> None:
    truth: dict[str, Any] = json.loads(backfill.ground_truth_path.read_text())
    args = (backfill.manifest, backfill.simulation, truth, backfill.checks)
    fast = report.render(*args, runtime_s=1234.56, peak_rss=181 * 2**20)
    slow = report.render(*args, runtime_s=99.0, peak_rss=4 * 2**30)
    assert "| Runtime | 1,234.6 s |" in fast
    assert "| Peak RSS | 181 MiB (whole process) |" in fast
    changed = {a for a, b in zip(fast.splitlines(), slow.splitlines(), strict=True) if a != b}
    assert changed == {"| Runtime | 1,234.6 s |", "| Peak RSS | 181 MiB (whole process) |"}


def test_peak_rss_is_this_process() -> None:
    assert 10 * 2**20 < report.peak_rss_bytes() < 64 * 2**30  # bytes, not KiB


def test_values_and_bands_read_naturally() -> None:
    assert report._value(Check("stream_events", 1_234_567, 1_000_000, None)) == "1,234,567"
    assert report._band(Check("stream_events", 0, 0, None)) == ">= 0"
    assert report._band(Check("req_fill_rate", 0.4, 0.8, 0.92)) == "0.800 to 0.920"
    assert report._value(Check("median_time_to_fill_days", 58.0, 35, 60)) == "58.0"
    assert report._value(Check("ht4_monotonic", 1.0, 1, None)) == "yes"
    assert report._value(Check("ht4_monotonic", None, 1, None)) == "n/a"
    assert report._band(Check("ht4_monotonic", 0.0, 1, None)) == "declines"


def test_a_run_without_chaos_says_so() -> None:
    stream = {
        "events": 10, "duplicates": {}, "malformed": {}, "lag_bands": {"0": 10}, "lost": 0,
        "late_burst": 0, "renamed": 0, "unresolvable_timezone_lines": 0,
    }  # fmt: skip
    truth = {
        "streams": {"jobboard-web": stream},
        "expected_quarantine": {"jobboard-web": {"MALFORMED_JSON": 0}},
        "hris": {
            "missing_days": [], "renamed_days": [], "late_exports": 0, "deferred_changes": 0,
            "duplicate": None, "partial": {"day": "2025-01-02", "kept": 9, "dropped": 1},
        },
    }  # fmt: skip
    text = report._chaos(truth)
    assert text.count("| none |  |  |") == 2  # nothing injected, nothing to quarantine
    assert "- Missing days: none" in text
    assert "- Partial file: 2025-01-02, 9 rows kept, 1 dropped" in text
    assert "Duplicate row" not in text
