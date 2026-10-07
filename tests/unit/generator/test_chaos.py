import json
from collections import Counter
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import orjson
import pytest

from hirestream.generator.calendar import build_calendar
from hirestream.generator.chaos import DAY_MS, HOUR_MS, ChaosLayer, Delivery
from hirestream.generator.config import GeneratorConfig, load_config
from hirestream.generator.events import StreamEvent

WriteConfig = Callable[[dict[str, Any]], Path]
JOBBOARD, SCHEDULING = "jobboard-web", "scheduling-service"
DAY = date(2025, 2, 3)
DAY_START = int(datetime(2025, 2, 3, tzinfo=UTC).timestamp() * 1000)


class Collect:
    def __init__(self) -> None:
        self.deliveries: list[Delivery] = []

    def push(self, delivery: Delivery) -> None:
        self.deliveries.append(delivery)


def _jobboard(n: int, start_ms: int = DAY_START, step_ms: int = 7) -> list[StreamEvent]:
    events = []
    for i in range(n):
        ts = start_ms + i * step_ms
        body = {
            "event_id": f"e{i}",
            "event_type": "page_view",
            "schema_version": 1,
            "source": JOBBOARD,
            "producer_version": "3.2.0",
            "event_ts": ts,
            "sent_ts": "2025-02-03T00:00:00.000Z",
            "context": {"session_id": f"s{i}", "referrer_type": "direct"},
            "payload": {"page_type": "job_detail", "req_id": "R000001"},
        }
        events.append(StreamEvent(JOBBOARD, "page_view", ts, ts + 3, f"s{i}", body))
    return events


def _scheduling(
    n: int, start_ms: int = DAY_START, timezone: bool = True, step_ms: int = 7
) -> list[StreamEvent]:
    events = []
    for i in range(n):
        ts = start_ms + i * step_ms
        payload: dict[str, Any] = {
            "interview_id": f"I{i}",
            "interview_type": "onsite",
            "scheduled_start": "2025-02-10T10:00:00",
        }
        if timezone:
            payload["timezone"] = "Europe/London"
        body = {
            "event_id": f"e{i}",
            "event_type": "interview_scheduled",
            "schema_version": 1,
            "source": SCHEDULING,
            "event_ts": "2025-02-03T00:00:00.000Z",
            "payload": payload,
        }
        events.append(StreamEvent(SCHEDULING, "interview_scheduled", ts, ts + 3, f"I{i}", body))
    return events


def _layer(
    cfg: GeneratorConfig, incidents: Sequence[str] = (), seed: int = 1602
) -> tuple[ChaosLayer, Collect]:
    target = Collect()
    rng = np.random.default_rng(seed)
    return ChaosLayer(cfg, build_calendar(cfg, incidents), rng, target), target


@pytest.fixture(scope="module")
def cfg(base_config_path: Path) -> GeneratorConfig:
    return load_config(base_config_path, "tiny")


def _config(raw: dict[str, Any], write: WriteConfig, **streams: object) -> GeneratorConfig:
    raw["chaos"]["streams"].update(streams)
    return load_config(write(raw), "tiny")


def test_rates_follow_the_config(cfg: GeneratorConfig) -> None:
    layer, target = _layer(cfg)
    events = _jobboard(200_000)
    layer.write(events)
    truth, chaos = layer.truth, cfg.chaos.streams
    n = len(events)
    assert truth.events[JOBBOARD] == n and truth.lines[JOBBOARD] == len(target.deliveries)
    assert sum(truth.duplicates.values()) / n == pytest.approx(chaos.duplicate_rate, abs=0.002)
    malformed = sum(truth.malformed.values())
    assert malformed / len(target.deliveries) == pytest.approx(chaos.malformed_rate, abs=0.0004)
    kinds = Counter(kind for _, kind in truth.malformed.elements())
    assert set(kinds) == set(chaos.malformed_kinds) and min(kinds.values()) > malformed / 5
    shares = [truth.lag_bands[(JOBBOARD, b)] / n for b in range(len(chaos.delivery_lag))]
    for share, band in zip(shares, chaos.delivery_lag, strict=True):
        assert share == pytest.approx(band.share, rel=0.1)


def test_arrivals_and_duplicates(cfg: GeneratorConfig) -> None:
    layer, target = _layer(cfg)
    events = _jobboard(50_000)
    layer.write(events)
    sent = {e.body["event_id"]: e.sent_ts_ms for e in events}
    bands = cfg.chaos.streams.delivery_lag
    lo, hi = cfg.chaos.streams.duplicate_extra_lag_seconds
    copies: dict[str, list[Delivery]] = {}
    for d in target.deliveries:
        try:
            event_id = orjson.loads(d.line).get("event_id")
        except orjson.JSONDecodeError:
            continue
        if event_id is not None:
            copies.setdefault(event_id, []).append(d)
    for event_id, delivered in copies.items():
        first = delivered[0]
        lag = (first.arrival_ms - sent[event_id]) / 1000
        assert any(b.seconds[0] <= lag <= b.seconds[1] for b in bands)
        if len(delivered) == 2:
            second = delivered[1]
            assert lo <= (second.arrival_ms - first.arrival_ms) / 1000 <= hi
            assert first.partition_key == second.partition_key
    twins = [d for d in copies.values() if len(d) == 2]
    assert twins and sum(a.line == b.line for a, b in twins) / len(twins) > 0.99


def test_malformed_lines_are_broken_as_labelled(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    cfg = _config(raw_config, write_config, malformed_rate=1.0, duplicate_rate=0.0)
    layer, target = _layer(cfg)
    layer.write(_jobboard(3_000))
    layer.write(_scheduling(3_000))
    seen: Counter[str] = Counter()
    for d in target.deliveries:
        try:
            body = json.loads(d.line)
        except json.JSONDecodeError:
            seen["truncated_json"] += 1
            continue
        holder = body.get("context") if d.source == JOBBOARD else body.get("payload")
        key = "session_id" if d.source == JOBBOARD else "interview_id"
        complete = all(k in body for k in ("event_id", "event_type", "event_ts"))
        if not complete or key not in holder:
            seen["missing_required_field"] += 1
        else:
            enums = [body["event_type"], *(v for v in holder.values() if isinstance(v, str))]
            assert any(v.isupper() for v in enums)
            seen["invalid_enum"] += 1
    truth = layer.truth
    assert seen == Counter(kind for _, kind in truth.malformed.elements())
    assert sum(truth.lost.values()) == 6_000  # every copy of every event is broken
    assert sum(truth.unresolvable_timezone.values()) == 0  # broken lines quarantine first


def test_unresolvable_timezones_count_well_formed_copies(cfg: GeneratorConfig) -> None:
    layer, target = _layer(cfg)
    layer.write(_scheduling(20_000, timezone=False))
    truth = layer.truth
    well_formed = len(target.deliveries) - sum(truth.malformed.values())
    assert truth.unresolvable_timezone[SCHEDULING] == well_formed


def _first_copies(target: Collect) -> tuple[Counter[str], dict[str, int]]:
    """Copies per event id, and each event's first arrival (malformed lines skipped)."""
    copies: Counter[str] = Counter()
    first: dict[str, int] = {}
    for d in target.deliveries:
        try:
            event_id = orjson.loads(d.line)["event_id"]
        except (orjson.JSONDecodeError, KeyError):
            continue
        copies[event_id] += 1
        first.setdefault(event_id, d.arrival_ms)
    return copies, first


def test_duplicate_storm(cfg: GeneratorConfig) -> None:
    layer, target = _layer(cfg, ["duplicate_storm"])
    storm = layer._storm
    assert storm is not None and storm.source == JOBBOARD
    # 50k events from an hour before the 3-hour storm to an hour after it.
    layer.write(_jobboard(50_000, start_ms=storm.start_ms - HOUR_MS, step_ms=360))
    copies, first = _first_copies(target)
    hit = {e: storm.start_ms <= first[e] < storm.end_ms for e in copies}
    inside = [copies[e] == 2 for e in copies if hit[e]]
    outside = [copies[e] == 2 for e in copies if not hit[e]]
    assert sum(inside) / len(inside) == pytest.approx(0.30, abs=0.02)
    assert sum(outside) / len(outside) == pytest.approx(0.015, abs=0.005)
    assert layer.truth.duplicates[(JOBBOARD, "storm")] == pytest.approx(sum(inside), abs=10)


def test_silent_schema_break(cfg: GeneratorConfig) -> None:
    span = build_calendar(cfg, ["silent_schema_break"])["incidents.silent_schema_break"]
    start = int(datetime.combine(span.start, datetime.min.time(), UTC).timestamp() * 1000)
    layer, target = _layer(cfg, ["silent_schema_break"])
    before = _jobboard(1_000, start_ms=start - DAY_MS)
    during = _jobboard(1_000, start_ms=start + HOUR_MS)
    layer.write(before)
    layer.write(during)
    renamed = {"before": 0, "during": 0}
    for d in target.deliveries:
        try:
            body = orjson.loads(d.line)
        except orjson.JSONDecodeError:
            continue
        if "payload" in body and "requisition_id" in body["payload"]:
            assert "req_id" not in body["payload"] and body["schema_version"] == 1
            renamed["during" if body["event_ts"] >= start else "before"] += 1
    assert renamed["before"] == 0 and renamed["during"] > 990
    assert layer.truth.renamed[JOBBOARD] == 1_000


def test_late_burst(cfg: GeneratorConfig) -> None:
    layer, target = _layer(cfg, ["late_burst"])
    burst = layer._burst
    assert burst is not None and burst.source == SCHEDULING
    # 8k events from an hour before the 6-hour burst to an hour after it.
    events = _scheduling(8_000, start_ms=burst.start_ms - HOUR_MS, step_ms=3_600)
    layer.write(events)
    sent = {e.body["event_id"]: e.sent_ts_ms for e in events}
    _, first = _first_copies(target)
    inside = {e for e, ms in sent.items() if burst.start_ms <= ms < burst.end_ms}
    assert all(first[e] - sent[e] >= 3 * DAY_MS for e in inside if e in first)
    natural = [first[e] - sent[e] >= 3 * DAY_MS for e in first if e not in inside]
    assert sum(natural) / len(natural) < 0.01  # only the mixture's own 1-7 day tail
    assert layer.truth.late_burst[SCHEDULING] == len(inside) > 5_000


def test_incidents_change_only_what_they_touch(cfg: GeneratorConfig) -> None:
    span = build_calendar(cfg, ["silent_schema_break"])["incidents.silent_schema_break"]
    start = int(datetime.combine(span.start, datetime.min.time(), UTC).timestamp() * 1000)
    events = _jobboard(5_000, start_ms=start - HOUR_MS, step_ms=1_440)  # 2 h around midnight
    plain, plain_target = _layer(cfg)
    broken, broken_target = _layer(cfg, ["silent_schema_break", "late_burst", "duplicate_storm"])
    plain.write(events)
    broken.write(events)
    a, b = plain_target.deliveries, broken_target.deliveries
    assert [d.arrival_ms for d in a] == [d.arrival_ms for d in b]  # same draws, same arrivals
    differ = 0
    for x, y in zip(a, b, strict=True):
        if x.line != y.line:  # only events stamped on the break's day
            assert events[int(y.partition_key[1:])].event_ts_ms >= start
            differ += 1
    assert differ >= broken.truth.renamed[JOBBOARD] > 2_000


def test_only_the_silent_variant_is_modelled(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    raw_config["incidents"]["silent_schema_break"]["schema_version_unchanged"] = False
    cfg = load_config(write_config(raw_config), "tiny")
    with pytest.raises(ValueError, match="silent variant"):
        _layer(cfg)


def test_an_empty_batch_draws_nothing(cfg: GeneratorConfig) -> None:
    layer, target = _layer(cfg)
    layer.write([])
    assert not target.deliveries and layer.truth.summary()["lines"] == 0
