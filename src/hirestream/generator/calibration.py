"""Calibration: a run's measured metrics against `calibration_targets` (SPEC §6.10, ADR-0015).

Each metric is measured the way the warehouse will compute it (SPEC §13), so a pass here means the
same thing on the WBR. Definitions are pinned in ADR-0015. A miss is a warning, not an error: tiny
runs are too small to calibrate, and SPEC §6.11 allows an ADR to explain a deliberate miss.
"""

from __future__ import annotations

import statistics
from collections import Counter
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from hirestream.generator.config import CHANNELS, GeneratorConfig

if TYPE_CHECKING:
    from hirestream.generator.simulation import SimulationResult

DAY_MS = 86_400_000
Band = tuple[float | None, float | None]  # inclusive; None is unbounded


@dataclass(frozen=True)
class Check:
    name: str
    value: float | None  # None when the run has nothing to measure, e.g. no decided offers
    low: float | None
    high: float | None

    @property
    def passed(self) -> bool:
        if self.value is None:
            return False
        above = self.low is None or self.value >= self.low
        return above and (self.high is None or self.value <= self.high)


def measure(
    config: GeneratorConfig, result: SimulationResult, hidden: dict[str, Any]
) -> list[Check]:
    """Every calibration target, the hidden-truth bands, and the preset's event floor. `hidden`
    is ground truth's `hidden_truths`, so the report and the file can't disagree."""
    values = metrics(result, hidden)
    return [Check(name, values[name], *band) for name, band in targets(config).items()]


def summary(checks: list[Check]) -> str:
    missed = [c.name for c in checks if not c.passed]
    head = f"{len(checks) - len(missed)}/{len(checks)} within target"
    return f"{head}; warn: {', '.join(missed)}" if missed else head


def targets(config: GeneratorConfig) -> dict[str, Band]:
    t = config.calibration_targets
    return {
        "req_fill_rate": t.req_fill_rate,
        "median_time_to_fill_days": t.median_time_to_fill_days,
        "median_time_to_hire_days": t.median_time_to_hire_days,
        "applications_per_hire": t.applications_per_hire,
        "offer_acceptance_rate": t.offer_acceptance_rate,
        "internal_fill_rate": t.internal_fill_rate,
        **{f"channel_mix.{c}": band for c, band in sorted(t.application_channel_mix.items())},
        "clickstream_bot_event_share": t.clickstream_bot_event_share,
        "feedback_within_48h_share": t.feedback_within_48h_share,
        "ht1_ratio": t.hidden_truth_bands.ht1,
        "ht2_ratio": t.hidden_truth_bands.ht2,
        "ht3_ratio": t.hidden_truth_bands.ht3,
        "ht4_monotonic": (1, None),  # 1 when acceptance never rises with days to offer
        "stream_events": (config.scale.target_min_total_events, None),
    }


def metrics(result: SimulationResult, hidden: dict[str, Any]) -> dict[str, float | None]:
    """Measured values, keyed like `targets`; definitions in ADR-0015."""
    snapshot = result.ats_snapshot
    reqs = [r for r in snapshot.requisitions if not r.is_evergreen]
    closed = Counter(r.close_reason for r in reqs)
    resolved = closed["filled"] + closed["expired"]  # cancelled or dissolved: nothing to fill
    to_fill = [(r.closed_on - r.opened_on).days for r in reqs if r.close_reason == "filled"
               and r.closed_on is not None]  # fmt: skip
    apps = {a.application_id: a for a in snapshot.applications}
    to_hire = [
        (o.decided_ms - apps[o.application_id].applied_ms) / DAY_MS
        for o in snapshot.offers
        if o.status == "accepted"
        and o.decided_ms is not None
        and apps[o.application_id].status == "hired"
    ]
    offers = Counter(o.status for o in snapshot.offers)
    decided = offers["accepted"] + offers["declined"]
    hired = sum(a.status == "hired" for a in snapshot.applications)
    starts = sum(result.ats_truth.hires.values())
    internal = sum(n for (_, _, is_internal), n in result.ats_truth.hires.items() if is_internal)
    channels = result.ats_truth.applications
    applications = sum(channels.values())
    board = result.jobboard_truth.events
    return {
        "req_fill_rate": _ratio(closed["filled"], resolved),
        "median_time_to_fill_days": float(statistics.median(to_fill)) if to_fill else None,
        "median_time_to_hire_days": statistics.median(to_hire) if to_hire else None,
        "applications_per_hire": _ratio(applications, hired),
        "offer_acceptance_rate": _ratio(offers["accepted"], decided),
        "internal_fill_rate": _ratio(internal, starts),
        **{f"channel_mix.{c}": _ratio(channels[c], applications) for c in sorted(CHANNELS)},
        "clickstream_bot_event_share": _ratio(board["bot"], sum(board.values())),
        "feedback_within_48h_share": _within_48h(result),
        "ht1_ratio": hidden["ht1"]["ratio"],
        "ht2_ratio": hidden["ht2"]["ratio"],
        "ht3_ratio": hidden["ht3"]["ratio"],
        "ht4_monotonic": float(hidden["ht4"]["monotonic"]) if hidden["ht4"]["buckets"] else None,
        "stream_events": float(sum(result.chaos_truth.events.values())),  # emitted, before chaos
    }


def _within_48h(result: SimulationResult) -> float | None:
    """Feedback submitted within 48 h of the interview's end, per expected feedback (completed
    interviews x interviewers, fct_interview's grain): late and missing both count against it."""
    expected = result.scheduling_truth.expected_feedback
    return result.scheduling_summary["within_48h"] if expected else None


def _ratio(numerator: float, denominator: float) -> float | None:
    return numerator / denominator if denominator else None
