"""Stream chaos: what happens to events between the producers and bronze (SPEC §6.8, ADR-0012).

Producers hand finished events to `ChaosLayer.write`, their `EventSink`. For each event the layer
decides when it arrives (`sent_ts` plus a lag from the configured mixture), whether it is delivered
twice, and whether a delivered copy is malformed, and it applies the enabled incidents. Lines go to
the delivery queue, which hands them to the sinks in arrival order.

Every draw comes from the `chaos` stream, and the same number of draws is made per event whatever
is enabled. Nothing flows back into the simulation, and turning an incident on changes only what
that incident touches.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, NamedTuple, Protocol

import numpy as np
import orjson

from hirestream.generator.calendar import CalendarEvent
from hirestream.generator.config import GeneratorConfig, StreamSource
from hirestream.generator.events import StreamEvent

HOUR_MS = 3_600_000
DAY_MS = 86_400_000
SOURCES: dict[StreamSource, str] = {"scheduling": "scheduling-service", "jobboard": "jobboard-web"}
# What a `missing_required_field` line can lose: three envelope fields, or the entity key.
_ENVELOPE: tuple[tuple[str, ...], ...] = (("event_id",), ("event_type",), ("event_ts",))
REQUIRED: dict[str, tuple[tuple[str, ...], ...]] = {
    "scheduling-service": (*_ENVELOPE, ("payload", "interview_id")),
    "jobboard-web": (*_ENVELOPE, ("context", "session_id")),
}
SCHEDULING_ENUMS = (
    "interview_type", "reason", "initiated_by", "no_show_party", "recommendation",
    "interview_format",
)  # fmt: skip
STARTS_EVENTS = frozenset({"interview_scheduled", "interview_rescheduled"})
_DRAWS = 10  # uniforms per event, fixed so enabling an incident never reshuffles the rest


class Delivery(NamedTuple):
    """One line as it reaches a sink: arrival time, source, partition key, JSON (no newline)."""

    arrival_ms: int
    source: str
    partition_key: str
    line: bytes


class DeliveryTarget(Protocol):
    def push(self, delivery: Delivery) -> None: ...


@dataclass
class ChaosTruth:
    """What chaos did, counted as it happened, for T1.10's ground truth (SPEC §6.10)."""

    events: Counter[str] = field(default_factory=Counter)  # producer events in, by source
    lines: Counter[str] = field(default_factory=Counter)  # lines delivered, duplicates included
    duplicates: Counter[tuple[str, str]] = field(default_factory=Counter)  # (source, regular|storm)
    malformed: Counter[tuple[str, str]] = field(default_factory=Counter)  # (source, kind)
    lost: Counter[str] = field(default_factory=Counter)  # events whose every copy is malformed
    lag_bands: Counter[tuple[str, int]] = field(
        default_factory=Counter
    )  # (source, band), originals
    late_burst: Counter[str] = field(default_factory=Counter)  # events delayed by the incident
    renamed: Counter[str] = field(default_factory=Counter)  # silent_schema_break
    unresolvable_timezone: Counter[str] = field(default_factory=Counter)  # well-formed copies

    def summary(self) -> dict[str, int]:
        late = sum(n for (_, band), n in self.lag_bands.items() if band > 0)
        return {
            "events": sum(self.events.values()),
            "lines": sum(self.lines.values()),
            "duplicates": sum(self.duplicates.values()),
            "storm_duplicates": sum(n for (_, k), n in self.duplicates.items() if k == "storm"),
            "malformed": sum(self.malformed.values()),
            "lost": sum(self.lost.values()),
            "late": late,  # arrived in a band beyond the first (an hour or more after sending)
            "late_burst": sum(self.late_burst.values()),
            "renamed": sum(self.renamed.values()),
            "unresolvable_timezone": sum(self.unresolvable_timezone.values()),
        }


@dataclass(frozen=True, slots=True)
class _Window:
    source: str
    start_ms: int
    end_ms: int  # exclusive

    def mask(self, ms: np.ndarray[Any, np.dtype[np.int64]]) -> np.ndarray[Any, np.dtype[np.bool_]]:
        return (ms >= self.start_ms) & (ms < self.end_ms)


class ChaosLayer:
    def __init__(
        self,
        config: GeneratorConfig,
        calendar: Mapping[str, CalendarEvent],
        rng: np.random.Generator,
        target: DeliveryTarget,
    ) -> None:
        self.truth = ChaosTruth()
        self._cfg = config.chaos.streams
        self._rng = rng
        self._target = target
        bands = self._cfg.delivery_lag
        self._band_cum = np.cumsum([b.share for b in bands])
        self._band_lo = np.array([b.seconds[0] * 1000 for b in bands], dtype=np.int64)
        self._band_span = np.array(
            [(b.seconds[1] - b.seconds[0]) * 1000 + 1 for b in bands], dtype=np.int64
        )
        lo, hi = self._cfg.duplicate_extra_lag_seconds
        self._dup_lo, self._dup_span = lo * 1000, (hi - lo) * 1000 + 1
        self._kinds = list(self._cfg.malformed_kinds)
        inc = config.incidents
        # Hour-long incidents start at an hour drawn here, always, in this order (calendar.py).
        storm_hour = int(rng.integers(0, 24 - inc.duplicate_storm.hours + 1))
        burst_hour = int(rng.integers(0, 24 - inc.late_burst.hours + 1))
        self._storm = self._window(calendar, "duplicate_storm", inc.duplicate_storm.source,
                                   storm_hour, inc.duplicate_storm.hours)  # fmt: skip
        self._storm_rate = inc.duplicate_storm.duplicate_rate
        self._burst = self._window(calendar, "late_burst", inc.late_burst.source,
                                   burst_hour, inc.late_burst.hours)  # fmt: skip
        self._burst_ms = inc.late_burst.delay_days * DAY_MS
        brk = inc.silent_schema_break
        if not brk.schema_version_unchanged:
            raise ValueError("silent_schema_break models the silent variant only")
        span = calendar.get("incidents.silent_schema_break")
        self._break = (
            None
            if span is None
            else (
                SOURCES[brk.source],
                span.start,
                span.end,
                brk.rename_field.from_,
                brk.rename_field.to,
            )
        )

    def write(self, events: Sequence[StreamEvent]) -> None:
        """The producers' sink: deliver a day's events from one source, through chaos."""
        n = len(events)
        if not n:
            return
        u = self._rng.random((_DRAWS, n))
        sent = np.fromiter((e.sent_ts_ms for e in events), dtype=np.int64, count=n)
        band = np.minimum(
            np.searchsorted(self._band_cum, u[0], side="right"), len(self._band_cum) - 1
        )
        arrival = sent + self._band_lo[band] + (u[1] * self._band_span[band]).astype(np.int64)
        source = events[0].source
        burst = self._mask(self._burst, source, sent)
        arrival = arrival + burst * self._burst_ms
        storm = self._mask(self._storm, source, arrival)
        rate = np.where(storm, self._storm_rate, self._cfg.duplicate_rate)
        dup = u[2] < rate
        dup_arrival = arrival + self._dup_lo + (u[3] * self._dup_span).astype(np.int64)
        broken = u[4] < self._cfg.malformed_rate
        dup_broken = dup & (u[5] < self._cfg.malformed_rate)
        kinds = len(self._kinds)
        kind = (u[6] * kinds).astype(np.int64)
        dup_kind = (u[7] * kinds).astype(np.int64)
        truth = self.truth
        truth.events[source] += n
        for b, count in enumerate(np.bincount(band, minlength=len(self._band_cum))):
            if count:
                truth.lag_bands[(source, b)] += int(count)
        truth.late_burst[source] += int(burst.sum())
        # Per-event work in plain Python lists: numpy scalar access costs more than the JSON.
        arrival_l, dup_arrival_l = arrival.tolist(), dup_arrival.tolist()
        special = (dup | broken).tolist()
        dup_l, broken_l, dup_broken_l, storm_l = (
            dup.tolist(),
            broken.tolist(),
            dup_broken.tolist(),
            storm.tolist(),
        )
        kind_l, dup_kind_l = kind.tolist(), dup_kind.tolist()
        detail_l, dup_detail_l = u[8].tolist(), u[9].tolist()
        push, dumps = self._target.push, _dumps
        scheduling = source == "scheduling-service"
        brk = self._break if self._break is not None and self._break[0] == source else None
        renamed = unresolvable = 0
        for i, event in enumerate(events):
            body = event.body
            if brk is not None and self._breaks(event):
                body, hit = _renamed(body, brk[3], brk[4])
                renamed += hit
            line = dumps(body)
            missing_tz = (
                scheduling
                and event.event_type in STARTS_EVENTS
                and "timezone" not in body["payload"]
            )
            if not special[i]:  # ~98% of events: one well-formed copy
                push(Delivery(arrival_l[i], source, event.partition_key, line))
                unresolvable += missing_tz
                continue
            copies = [(arrival_l[i], broken_l[i], kind_l[i], detail_l[i])]
            if dup_l[i]:
                copies.append((dup_arrival_l[i], dup_broken_l[i], dup_kind_l[i], dup_detail_l[i]))
                truth.duplicates[(source, "storm" if storm_l[i] else "regular")] += 1
            bad = 0
            for at, is_broken, k, detail in copies:
                if is_broken:
                    name = self._kinds[k]
                    out = self._corrupt(source, body, line, name, detail)
                    truth.malformed[(source, name)] += 1
                    bad += 1
                else:
                    out = line
                    unresolvable += missing_tz
                push(Delivery(at, source, event.partition_key, out))
            truth.lines[source] += len(copies) - 1  # the one copy is counted below
            truth.lost[source] += bad == len(copies)
        truth.lines[source] += n
        truth.renamed[source] += renamed
        truth.unresolvable_timezone[source] += unresolvable

    # ------------------------------------------------------------------ incidents

    @staticmethod
    def _window(
        calendar: Mapping[str, CalendarEvent],
        name: str,
        source: StreamSource,
        hour: int,
        hours: int,
    ) -> _Window | None:
        span = calendar.get(f"incidents.{name}")
        if span is None:
            return None
        start = _midnight_ms(span.start) + hour * HOUR_MS
        return _Window(SOURCES[source], start, start + hours * HOUR_MS)

    @staticmethod
    def _mask(
        window: _Window | None, source: str, ms: np.ndarray[Any, np.dtype[np.int64]]
    ) -> np.ndarray[Any, np.dtype[np.bool_]]:
        if window is None or window.source != source:
            return np.zeros(len(ms), dtype=bool)
        return window.mask(ms)

    def _breaks(self, event: StreamEvent) -> bool:
        assert self._break is not None
        source, start, end, _, _ = self._break
        if event.source != source:
            return False
        day = datetime.fromtimestamp(event.event_ts_ms / 1000, UTC).date()
        return start <= day <= end

    # ------------------------------------------------------------------ malformed lines

    def _corrupt(
        self, source: str, body: dict[str, Any], line: bytes, kind: str, u: float
    ) -> bytes:
        if kind == "truncated_json":  # any strict prefix of a JSON object is invalid JSON
            return line[: 1 + int(u * (len(line) - 1))]
        if kind == "missing_required_field":
            paths = REQUIRED[source]
            return _dumps(_without(body, paths[int(u * len(paths))]))
        return _dumps(_bad_enum(source, body))


def _dumps(obj: dict[str, Any]) -> bytes:
    """orjson's bytes keep its 4 KiB output buffer; a line waits a day in the queue, so copy it
    to its own size (9x less memory for a ~450-byte line).
    """
    return memoryview(orjson.dumps(obj)).tobytes()


def _renamed(body: dict[str, Any], old: str, new: str) -> tuple[dict[str, Any], bool]:
    """The body with payload `old` renamed `new`, in place in the key order."""
    payload = body.get("payload")
    if not isinstance(payload, dict) or old not in payload:
        return body, False
    return {**body, "payload": {(new if k == old else k): v for k, v in payload.items()}}, True


def _without(body: dict[str, Any], path: tuple[str, ...]) -> dict[str, Any]:
    if len(path) == 1:
        return {k: v for k, v in body.items() if k != path[0]}
    parent, key = path
    return {**body, parent: {k: v for k, v in body[parent].items() if k != key}}


def _bad_enum(source: str, body: dict[str, Any]) -> dict[str, Any]:
    """An enum the event carries, upper-cased: a value outside every contract (all are lower)."""
    if source == "jobboard-web":
        context = body["context"]
        return {**body, "context": {**context, "referrer_type": context["referrer_type"].upper()}}
    payload = body["payload"]
    for key in SCHEDULING_ENUMS:
        if isinstance(payload.get(key), str):
            return {**body, "payload": {**payload, key: payload[key].upper()}}
    return {**body, "event_type": body["event_type"].upper()}  # interview_completed has none


def _midnight_ms(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp() * 1000)
