"""Ground truth: what really happened in a run, before chaos (SPEC §6.10, ADR-0015).

Facts come from each engine's truth, recorded as they happened, or from the ATS's final state, the
same rows the warehouse will extract. ATS months are the UTC months of the stored timestamps, as the
warehouse will compute them, so SPEC §13.2's exact-match checks can hold. The file is byte-stable:
sorted keys, rounded floats, and nothing that differs between runs of the same seed.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np

from hirestream.generator.ats import HT4_BUCKETS, ht4_bucket
from hirestream.generator.config import GeneratorConfig
from hirestream.generator.manifest import write_atomic

if TYPE_CHECKING:
    from hirestream.generator.simulation import SimulationResult

GROUND_TRUTH = "ground_truth.json"
VERSION = 1
# SPEC §9.1's quarantine reasons; chaos injects four of them (ADR-0014 §5 maps kinds to reasons).
REASONS = (
    "MALFORMED_JSON", "MISSING_REQUIRED_FIELD", "INVALID_ENUM", "UNPARSEABLE_TIMESTAMP",
    "UNRESOLVABLE_TIMEZONE", "UNKNOWN_SCHEMA_VERSION", "OUT_OF_RANGE_TIMESTAMP",
)  # fmt: skip
MALFORMED_REASON = {
    "truncated_json": "MALFORMED_JSON",
    "missing_required_field": "MISSING_REQUIRED_FIELD",
    "invalid_enum": "INVALID_ENUM",
}
QUANTILES = (0.5, 0.75, 0.9, 0.95, 0.99)
BUCKETS = (*(label for _, label in HT4_BUCKETS), ">60")
FIRST_GATE = {"advanced": "advance", "not_selected": "reject", "candidate_withdrew": "withdraw"}


def build(config: GeneratorConfig, result: SimulationResult) -> dict[str, Any]:
    return {
        "version": VERSION,
        "window": {
            "sim_start": config.window.sim_start.isoformat(),
            "sim_end": config.window.sim_end.isoformat(),
        },
        "ats": _ats(result),
        "workforce": _workforce(config, result),
        "feedback_latency_hours": _latencies(result.feedback_latencies),
        "hidden_truths": hidden_truths(result),
        "streams": _streams(result),
        "expected_quarantine": expected_quarantine(result),
        "hris": _hris(result),
    }


def to_bytes(truth: dict[str, Any]) -> bytes:
    return (json.dumps(truth, indent=2, sort_keys=True, ensure_ascii=True) + "\n").encode()


def write(truth: dict[str, Any], run_dir: Path) -> tuple[Path, str]:
    """Write `ground_truth.json` atomically; returns its path and sha256."""
    data = to_bytes(truth)
    path = run_dir / GROUND_TRUTH
    write_atomic(path, data)
    return path, hashlib.sha256(data).hexdigest()


# ------------------------------------------------------------------ hidden truths (SPEC §13.2)


def hidden_truths(result: SimulationResult) -> dict[str, Any]:
    """HT1 to HT4 as realized: measured the way the warehouse can measure them."""
    return {"ht1": _ht1(result), "ht2": _ht2(result), "ht3": _ht3(result), "ht4": _ht4(result)}


def _ht1(result: SimulationResult) -> dict[str, Any]:
    overloaded = [h for h, over in result.feedback_latencies if over]
    normal = [h for h, over in result.feedback_latencies if not over]
    ratio = float(np.median(overloaded) / np.median(normal)) if overloaded and normal else None
    return {
        "metric": "median feedback latency, overloaded / normal",
        "ratio": _round(ratio),
        "overloaded_feedback": len(overloaded),
        "normal_feedback": len(normal),
    }


def _ht2(result: SimulationResult) -> dict[str, Any]:
    """First-gate decisions in the ATS history: advanced, rejected or withdrawn at `applied`.
    Closures (req filled or cancelled, the applicant left) are not screening decisions.
    """
    channel = {a.application_id: a.channel for a in result.ats_snapshot.applications}
    outcomes: Counter[tuple[str, str]] = Counter()
    for change in result.ats_snapshot.changes:
        if change.from_stage == "applied" and change.reason in FIRST_GATE:
            outcomes[(channel[change.application_id], FIRST_GATE[change.reason])] += 1
    rate: dict[str, float] = {}
    by_channel: dict[str, dict[str, Any]] = {}
    for name in sorted({c for c, _ in outcomes}):
        decided = sum(outcomes[(name, o)] for o in ("advance", "reject", "withdraw"))
        rate[name] = outcomes[(name, "advance")] / decided
        by_channel[name] = {
            "decided": decided,
            "advanced": outcomes[(name, "advance")],
            "pass_rate": _round(rate[name]),
        }
    referral, career = rate.get("referral"), rate.get("career_site")
    ratio = referral / career if referral is not None and career else None
    return {
        "metric": "first-gate pass rate, referral / career_site",
        "ratio": _round(ratio),
        "drawn_ratio": _round(_drawn_ht2(result)),  # as drawn at entry, before any censoring
        "by_channel": by_channel,
    }


def _drawn_ht2(result: SimulationResult) -> float | None:
    gate = result.ats_truth.first_gate
    rate = {}
    for name in ("referral", "career_site"):
        total = sum(gate[(name, o)] for o in ("advance", "withdraw", "reject"))
        rate[name] = gate[(name, "advance")] / total if total else None
    referral, career = rate["referral"], rate["career_site"]
    return referral / career if referral is not None and career else None


def _ht3(result: SimulationResult) -> dict[str, Any]:
    truth = result.jobboard_truth
    apps: Counter[str] = Counter()
    days: Counter[str] = Counter()
    for (_, group), n in truth.ht3_applications.items():
        apps[group] += n
    for (_, group), n in truth.ht3_employee_days.items():
        days[group] += n
    rate = {g: apps[g] / days[g] if days[g] else None for g in ("long", "short")}
    long_rate, short_rate = rate["long"], rate["short"]
    ratio = long_rate / short_rate if long_rate is not None and short_rate else None
    return {
        "metric": "internal applications per employee-day, long / short tenure in role",
        "ratio": _round(ratio),
        "applications": dict(sorted(apps.items())),
        "employee_days": dict(sorted(days.items())),
    }


def _ht4(result: SimulationResult) -> dict[str, Any]:
    decided: Counter[tuple[str, bool]] = Counter()
    for offer in result.ats_snapshot.offers:
        if offer.status in ("accepted", "declined"):
            decided[(ht4_bucket(offer.days_to_offer), offer.status == "accepted")] += 1
    buckets: list[dict[str, Any]] = []  # a list: JSON key sorting would scramble the order
    rates: list[float] = []  # in bucket order, empty buckets skipped
    for label in BUCKETS:
        n = decided[(label, True)] + decided[(label, False)]
        if n:
            rates.append(decided[(label, True)] / n)
            buckets.append({"bucket": label, "decided": n, "acceptance": _round(rates[-1])})
    return {
        "metric": "offer acceptance by days-to-offer bucket",
        "buckets": buckets,
        "monotonic": all(a >= b for a, b in pairwise(rates)),
    }


# ------------------------------------------------------------------ sections


def _ats(result: SimulationResult) -> dict[str, Any]:
    """Counts by month x channel x internal, from the final ATS state (SPEC §13.2)."""
    apps = {a.application_id: a for a in result.ats_snapshot.applications}
    tables: dict[str, Counter[tuple[str, str, bool]]] = {
        name: Counter() for name in ("offers_extended", "offers_accepted", "offers_declined",
                                     "offers_rescinded", "hires")
    }  # fmt: skip
    pending = 0
    for offer in result.ats_snapshot.offers:
        app = apps[offer.application_id]
        key = (app.channel, app.employee_id is not None)
        tables["offers_extended"][(_month(offer.extended_ms), *key)] += 1
        if offer.status == "extended":
            pending += 1  # still undecided when the window closed
        elif offer.decided_ms is not None:
            tables[f"offers_{offer.status}"][(_month(offer.decided_ms), *key)] += 1
        if app.status == "hired" and offer.start_date is not None:
            tables["hires"][(f"{offer.start_date:%Y-%m}", *key)] += 1
    out: dict[str, Any] = {name: _rows(counter) for name, counter in tables.items()}
    out["offers_pending"] = pending
    out["starts"] = _rows(result.ats_truth.hires)  # joined the workforce inside the window
    out["applications"] = dict(sorted(result.ats_truth.applications.items()))
    return out


def _workforce(config: GeneratorConfig, result: SimulationResult) -> dict[str, Any]:
    by_month: Counter[tuple[str, str]] = Counter(
        (f"{e.day:%Y-%m}", e.kind) for e in result.events if e.kind in ("transfer", "promotion")
    )
    start, end = result.headcount
    return {
        "transfers_by_month": _by_month(by_month, "transfer"),
        "promotions_by_month": _by_month(by_month, "promotion"),  # successions included
        "headcount": {"start": start, "end": end},
        "net_growth": _round((end - start) / start if start else None),
        "planned_growth_annual": config.workforce.growth_annual,
    }


def _latencies(latencies: list[tuple[float, bool]]) -> dict[str, Any]:
    groups = {
        "all": [h for h, _ in latencies],
        "overloaded": [h for h, over in latencies if over],
        "normal": [h for h, over in latencies if not over],
    }
    out = {}
    for name, hours in groups.items():
        entry: dict[str, Any] = {"n": len(hours)}
        if hours:
            entry["mean"] = _round(float(np.mean(hours)))
            values = np.quantile(hours, QUANTILES)
            entry.update(
                {
                    f"p{round(q * 100)}": _round(float(v))
                    for q, v in zip(QUANTILES, values, strict=True)
                }
            )
        out[name] = entry
    return out


def _streams(result: SimulationResult) -> dict[str, Any]:
    chaos = result.chaos_truth
    out = {}
    for source in sorted(chaos.events):
        events = chaos.events[source]
        out[source] = {
            "events": events,
            "events_by_type": {
                t: n for (s, t), n in sorted(chaos.event_types.items()) if s == source
            },
            "lines": chaos.lines[source],
            "duplicates": {k: n for (s, k), n in sorted(chaos.duplicates.items()) if s == source},
            "malformed": {k: n for (s, k), n in sorted(chaos.malformed.items()) if s == source},
            "lost": chaos.lost[source],
            "unusable": chaos.unusable[source],
            "expected_silver_events": events - chaos.unusable[source],
            "lag_bands": {
                str(b): n for (s, b), n in sorted(chaos.lag_bands.items()) if s == source
            },
            "late_burst": chaos.late_burst[source],
            "renamed": chaos.renamed[source],
            "unresolvable_timezone_lines": chaos.unresolvable_timezone[source],
        }
    truth = result.scheduling_truth
    out.setdefault("scheduling-service", {})["timezone_bug"] = {
        "naive_starts": dict(sorted(truth.naive_starts.items())),
        "missing_timezone": dict(sorted(truth.missing_timezone.items())),
    }
    return out


def expected_quarantine(result: SimulationResult) -> dict[str, dict[str, int]]:
    """Lines silver should quarantine, by source and SPEC §9.1 reason (ADR-0014 §5)."""
    chaos = result.chaos_truth
    out = {}
    for source in sorted(chaos.events):
        reasons = dict.fromkeys(REASONS, 0)
        for (s, kind), n in chaos.malformed.items():
            if s == source:
                reasons[MALFORMED_REASON[kind]] += n
        reasons["UNRESOLVABLE_TIMEZONE"] = chaos.unresolvable_timezone[source]
        out[source] = reasons
    return out


def _hris(result: SimulationResult) -> dict[str, Any]:
    truth = result.hris_truth
    return {
        "missing_days": [d.isoformat() for d in truth.missing_days],
        "renamed_days": [d.isoformat() for d in truth.renamed_days],
        "duplicate": (
            None
            if truth.duplicate is None
            else {"day": truth.duplicate[0].isoformat(), "employee_id": truth.duplicate[1]}
        ),
        "partial": (
            None
            if truth.partial is None
            else {
                "day": truth.partial[0].isoformat(),
                "kept": truth.partial[1],
                "dropped": truth.partial[2],
            }
        ),
        "deferred_changes": truth.deferred_changes,
        "late_exports": truth.late_exports,
    }


# ------------------------------------------------------------------ helpers


def _month(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m")


def _rows(counter: Counter[tuple[str, str, bool]]) -> list[dict[str, Any]]:
    return [
        {"month": month, "channel": channel, "internal": internal, "count": n}
        for (month, channel, internal), n in sorted(counter.items())
    ]


def _by_month(counter: Counter[tuple[str, str]], kind: str) -> dict[str, int]:
    return {month: n for (month, k), n in sorted(counter.items()) if k == kind}


def _round(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)
