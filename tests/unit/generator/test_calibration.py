"""Calibration (SPEC §6.10, ADR-0015): each target measured as the warehouse will compute it."""

from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from hirestream.generator import calibration
from hirestream.generator.calibration import Check
from hirestream.generator.config import CalibrationTargets, load_config
from hirestream.generator.run import BackfillResult
from hirestream.generator.simulation import SimulationResult

DAY_MS = 86_400_000
START = date(2025, 1, 1)
HIDDEN: dict[str, Any] = {
    "ht1": {"ratio": 2.0},
    "ht2": {"ratio": 1.8},
    "ht3": {"ratio": None},
    "ht4": {"monotonic": False, "buckets": [{"bucket": "<=30"}]},
}


@pytest.mark.parametrize(
    ("value", "low", "high", "passed"),
    [
        (0.5, 0.4, 0.6, True),
        (0.4, 0.4, 0.6, True),  # bands are inclusive
        (0.6, 0.4, 0.6, True),
        (0.39, 0.4, 0.6, False),
        (0.61, 0.4, 0.6, False),
        (2e6, 1e6, None, True),  # a floor
        (None, 0.4, 0.6, False),  # nothing to measure is a warning, never a pass
    ],
)
def test_bands_are_inclusive_and_nothing_measured_warns(
    value: float | None, low: float, high: float | None, passed: bool
) -> None:
    assert Check("metric", value, low, high).passed is passed


def test_every_target_and_band_is_checked(base_config_path: Path) -> None:
    config = load_config(base_config_path, "dev")
    names = list(calibration.targets(config))
    scalars = set(CalibrationTargets.model_fields) - {
        "application_channel_mix",
        "hidden_truth_bands",
    }
    mix = {f"channel_mix.{c}" for c in config.calibration_targets.application_channel_mix}
    hidden = {"ht1_ratio", "ht2_ratio", "ht3_ratio", "ht4_monotonic"}
    assert set(names) == scalars | mix | hidden | {"stream_events"}
    assert len(names) == 18
    assert calibration.targets(config)["stream_events"] == (1_000_000, None)  # the preset's floor


def _req(close: str | None, days: int | None, evergreen: bool = False) -> SimpleNamespace:
    closed = None if days is None else START + timedelta(days=days)
    return SimpleNamespace(
        is_evergreen=evergreen, close_reason=close, opened_on=START, closed_on=closed
    )


def _hire(app_id: str, applied_day: int, decided_day: int | None, app_status: str,
          offer_status: str) -> tuple[SimpleNamespace, SimpleNamespace]:  # fmt: skip
    app = SimpleNamespace(application_id=app_id, status=app_status, applied_ms=applied_day * DAY_MS)
    decided = None if decided_day is None else decided_day * DAY_MS
    offer = SimpleNamespace(application_id=app_id, status=offer_status, decided_ms=decided)
    return app, offer


def _result(**overrides: Any) -> SimulationResult:
    """Just the parts `metrics` reads, with answers worked out by hand."""
    pairs = [
        _hire("a1", 0, 30, "hired", "accepted"),
        _hire("a2", 10, 60, "hired", "accepted"),
        _hire("a3", 0, 5, "no_start", "accepted"),  # accepted, then never started
        _hire("a4", 0, 20, "offer_declined", "declined"),
        _hire("a5", 0, 25, "rejected", "rescinded"),
        _hire("a6", 0, None, "active", "extended"),  # undecided when the window closed
    ]
    snapshot = SimpleNamespace(
        requisitions=[
            _req("filled", 40),
            _req("filled", 60),
            _req("expired", 90),
            _req("cancelled", 10),
            _req("team_dissolved", 5),
            _req(None, None),
            _req("filled", 5, evergreen=True),
            _req("expired", 90, evergreen=True),
        ],
        applications=[app for app, _ in pairs],
        offers=[offer for _, offer in pairs],
    )
    fields: dict[str, Any] = {
        "ats_snapshot": snapshot,
        "ats_truth": SimpleNamespace(
            hires=Counter({("2025-02", "internal", True): 1, ("2025-02", "referral", False): 3}),
            applications=Counter(career_site=60, referral=20, sourced=10, agency=2, internal=8),
        ),
        "jobboard_truth": SimpleNamespace(events=Counter(human=90, bot=10)),
        "scheduling_truth": SimpleNamespace(expected_feedback=40),
        "scheduling_summary": {"within_48h": 0.75},
        "chaos_truth": SimpleNamespace(events=Counter({"jobboard-web": 100, "x": 20})),
    }
    return cast("SimulationResult", SimpleNamespace(**(fields | overrides)))


def test_each_metric_follows_its_definition() -> None:
    values = calibration.metrics(_result(), HIDDEN)
    assert values["req_fill_rate"] == 2 / 3  # evergreen, cancelled, dissolved and open left out
    assert values["median_time_to_fill_days"] == 50  # filled non-evergreen reqs only
    assert values["median_time_to_hire_days"] == 40  # hired applications: 30 and 50 days
    assert values["applications_per_hire"] == 100 / 2  # the no-start isn't a hire
    assert values["offer_acceptance_rate"] == 3 / 4  # rescinded and pending aren't decisions
    assert values["internal_fill_rate"] == 1 / 4  # of starts inside the window
    assert values["channel_mix.career_site"] == 0.6
    assert values["channel_mix.internal"] == 0.08
    assert values["clickstream_bot_event_share"] == 0.1
    assert values["feedback_within_48h_share"] == 0.75
    assert (values["ht1_ratio"], values["ht2_ratio"], values["ht3_ratio"]) == (2.0, 1.8, None)
    assert values["ht4_monotonic"] == 0.0
    assert values["stream_events"] == 120


def test_nothing_to_measure_is_none_not_zero() -> None:
    empty = SimpleNamespace(requisitions=[], applications=[], offers=[])
    values = calibration.metrics(
        _result(
            ats_snapshot=empty,
            ats_truth=SimpleNamespace(hires=Counter(), applications=Counter()),
            jobboard_truth=SimpleNamespace(events=Counter()),
            scheduling_truth=SimpleNamespace(expected_feedback=0),
            scheduling_summary={"within_48h": 0.0},
        ),
        HIDDEN | {"ht4": {"monotonic": True, "buckets": []}},
    )
    assert {name for name, value in values.items() if value is None} == {
        "req_fill_rate", "median_time_to_fill_days", "median_time_to_hire_days",
        "applications_per_hire", "offer_acceptance_rate", "internal_fill_rate",
        "channel_mix.agency", "channel_mix.career_site", "channel_mix.internal",
        "channel_mix.referral", "channel_mix.sourced", "clickstream_bot_event_share",
        "feedback_within_48h_share", "ht3_ratio", "ht4_monotonic",
    }  # fmt: skip


def test_the_run_is_measured_against_its_config(backfill: BackfillResult) -> None:
    checks = {c.name: c for c in backfill.checks}
    sim = backfill.simulation
    assert checks["stream_events"].value == sum(sim.chaos_truth.events.values())
    assert checks["stream_events"].low == 0  # tiny has no floor
    assert checks["feedback_within_48h_share"].value == sim.scheduling_summary["within_48h"]
    assert checks["ht1_ratio"].value == round(sim.scheduling_summary["ht1_ratio"], 6)
    assert (checks["req_fill_rate"].low, checks["req_fill_rate"].high) == (0.80, 0.92)


def test_summary_names_every_miss() -> None:
    checks = [Check("a", 1, 0, 2), Check("b", 3, 0, 2), Check("c", None, 0, 2)]
    assert calibration.summary(checks) == "1/3 within target; warn: b, c"
    assert calibration.summary(checks[:1]) == "1/1 within target"
