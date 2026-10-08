"""ground_truth.json (SPEC §6.10, ADR-0015): every section reconciles with what it summarizes."""

import gzip
import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any

import pytest

from hirestream.generator import ground_truth
from hirestream.generator.config import load_config
from hirestream.generator.run import BackfillResult, run_backfill
from tests.contract_checks import Contracts, quarantine_reason

SOURCES = {"jobboard-web": "jobboard", "scheduling-service": "scheduling"}


@pytest.fixture(scope="module")
def backfill(tmp_path_factory: pytest.TempPathFactory, base_config_path: Path) -> BackfillResult:
    lake = tmp_path_factory.mktemp("lake")
    return run_backfill(load_config(base_config_path, "tiny"), lake, run_id="gt")


@pytest.fixture(scope="module")
def truth(backfill: BackfillResult) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(backfill.ground_truth_path.read_text())
    return loaded


def _total(rows: list[dict[str, Any]]) -> int:
    return sum(int(row["count"]) for row in rows)


def test_written_beside_the_manifest_and_hashed_into_it(
    backfill: BackfillResult, truth: dict[str, Any]
) -> None:
    data = backfill.ground_truth_path.read_bytes()
    assert backfill.ground_truth_path.parent == backfill.manifest_path.parent
    assert backfill.manifest.ground_truth_sha256 == hashlib.sha256(data).hexdigest()
    assert "ground_truth_sha256" in backfill.manifest.deterministic_view()  # same seed, same bytes
    assert data == ground_truth.to_bytes(truth)  # canonical: sorted keys, stable floats
    assert truth["version"] == ground_truth.VERSION


def test_ats_counts_reconcile_with_the_final_ats_state(
    backfill: BackfillResult, truth: dict[str, Any]
) -> None:
    snapshot = backfill.simulation.ats_snapshot
    ats = truth["ats"]
    offers = Counter(o.status for o in snapshot.offers)
    assert _total(ats["offers_extended"]) == len(snapshot.offers)
    for status in ("accepted", "declined", "rescinded"):
        assert _total(ats[f"offers_{status}"]) == offers[status]
    assert ats["offers_pending"] == offers["extended"]
    assert _total(ats["hires"]) == sum(a.status == "hired" for a in snapshot.applications)
    assert ats["starts"] == ground_truth._rows(backfill.simulation.ats_truth.hires)
    assert sum(ats["applications"].values()) == len(snapshot.applications)
    for row in ats["offers_extended"]:  # months come from the stored UTC timestamps
        assert datetime.strptime(row["month"], "%Y-%m")


def test_months_follow_utc_not_the_local_business_day() -> None:
    seattle_evening = int(datetime(2025, 2, 1, 0, 30, tzinfo=UTC).timestamp() * 1000)
    assert ground_truth._month(seattle_evening) == "2025-02"  # 16:30 PST on Jan 31


def test_workforce_counts_reconcile_with_the_event_log(
    backfill: BackfillResult, truth: dict[str, Any]
) -> None:
    kinds = Counter(e.kind for e in backfill.simulation.events)
    workforce = truth["workforce"]
    assert sum(workforce["transfers_by_month"].values()) == kinds["transfer"]
    assert sum(workforce["promotions_by_month"].values()) == kinds["promotion"]
    start, end = backfill.simulation.headcount
    assert workforce["headcount"] == {"start": start, "end": end}


def test_feedback_latencies_and_ht1(backfill: BackfillResult, truth: dict[str, Any]) -> None:
    latency = truth["feedback_latency_hours"]
    groups = [latency[g] for g in ("all", "overloaded", "normal")]
    assert latency["all"]["n"] == len(backfill.simulation.feedback_latencies)
    assert latency["overloaded"]["n"] + latency["normal"]["n"] == latency["all"]["n"]
    for group in groups:
        quantiles = [group[f"p{q}"] for q in (50, 75, 90, 95, 99)]
        assert quantiles == sorted(quantiles)
    ht1 = truth["hidden_truths"]["ht1"]
    assert ht1["ratio"] == round(backfill.simulation.scheduling_summary["ht1_ratio"], 6)
    medians = latency["overloaded"]["p50"] / latency["normal"]["p50"]
    assert ht1["ratio"] == pytest.approx(medians, rel=1e-4)


def test_hidden_truths_are_measured_as_the_warehouse_will(truth: dict[str, Any]) -> None:
    hidden = truth["hidden_truths"]
    ht2 = hidden["ht2"]["by_channel"]
    assert hidden["ht2"]["ratio"] == pytest.approx(
        ht2["referral"]["pass_rate"] / ht2["career_site"]["pass_rate"], rel=1e-5
    )
    assert hidden["ht2"]["drawn_ratio"] is not None
    ht3 = hidden["ht3"]
    long_rate = ht3["applications"]["long"] / ht3["employee_days"]["long"]
    short_rate = ht3["applications"]["short"] / ht3["employee_days"]["short"]
    assert ht3["ratio"] == pytest.approx(long_rate / short_rate, rel=1e-5)
    buckets = hidden["ht4"]["buckets"]
    assert [b["bucket"] for b in buckets] == [b for b in ground_truth.BUCKETS if b in
                                                {x["bucket"] for x in buckets}]  # fmt: skip
    rates = [b["acceptance"] for b in buckets]
    assert hidden["ht4"]["monotonic"] == all(a >= b for a, b in pairwise(rates))


def test_stream_counts_and_expected_quarantine(
    backfill: BackfillResult, truth: dict[str, Any]
) -> None:
    chaos = backfill.simulation.chaos_truth
    for source, section in truth["streams"].items():
        if "events" not in section:
            continue
        assert sum(section["events_by_type"].values()) == section["events"]
        assert section["expected_silver_events"] == section["events"] - section["unusable"]
        reasons = truth["expected_quarantine"][source]
        assert sum(reasons.values()) == (
            sum(n for (s, _), n in chaos.malformed.items() if s == source)
            + chaos.unresolvable_timezone[source]
        )
    bug = truth["streams"]["scheduling-service"]["timezone_bug"]
    assert bug["naive_starts"] == dict(backfill.simulation.scheduling_truth.naive_starts)


def _bronze_reasons(lake: Path, folder: str, contracts: Contracts, source: str) -> Counter[str]:
    reasons: Counter[str] = Counter()
    for part in sorted((lake / "bronze" / folder).rglob("*.jsonl.gz")):
        for line in gzip.decompress(part.read_bytes()).splitlines():
            reasons[quarantine_reason(contracts, source, line) or "kept"] += 1
    return reasons


def _check_source(
    backfill: BackfillResult, truth: dict[str, Any], contracts: Contracts, source: str
) -> None:
    lake = backfill.manifest_path.parents[2]
    found = _bronze_reasons(lake, SOURCES[source], contracts, source)
    expected = {k: v for k, v in truth["expected_quarantine"][source].items() if v}
    assert {k: v for k, v in found.items() if k != "kept"} == expected
    assert sum(found.values()) == truth["streams"][source]["lines"]


@pytest.mark.no_cover
def test_expected_quarantine_matches_the_scheduling_lines(
    backfill: BackfillResult, truth: dict[str, Any], contracts: Contracts
) -> None:
    """Classify every bronze scheduling line with the contracts (ADR-0014 §5): the counts
    are exactly the ground truth's. The slow test does the job board too.
    """
    _check_source(backfill, truth, contracts, "scheduling-service")


@pytest.mark.slow
@pytest.mark.no_cover
def test_expected_quarantine_matches_every_bronze_line(
    backfill: BackfillResult, truth: dict[str, Any], contracts: Contracts
) -> None:
    _check_source(backfill, truth, contracts, "jobboard-web")


def test_hris_section_mirrors_the_export(backfill: BackfillResult, truth: dict[str, Any]) -> None:
    hris = backfill.simulation.hris_truth
    section = truth["hris"]
    assert section["missing_days"] == [d.isoformat() for d in hris.missing_days]
    assert section["deferred_changes"] == hris.deferred_changes
    assert hris.duplicate is not None
    assert section["duplicate"] == {
        "day": hris.duplicate[0].isoformat(),
        "employee_id": hris.duplicate[1],
    }
