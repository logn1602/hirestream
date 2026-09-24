"""Stream events as producers emit them, and the sinks they flow into (SPEC §6.9, ADR-0007 §5).

A producer (the job board, later the scheduling service) builds one `StreamEvent` per event, with
the JSON envelope from SPEC §7.1 in `body`. Until T1.8 adds the delivery queue, chaos, and the
file and Kinesis sinks, events go to a counting sink (the CLI) or a collecting sink (tests).
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol


@dataclass(slots=True)
class StreamEvent:
    """One event before delivery chaos: what happened, when, and the envelope to serialise."""

    source: str  # "jobboard-web" or "scheduling-service"
    event_type: str
    event_ts_ms: int  # when it truly happened (epoch milliseconds, UTC)
    sent_ts_ms: int  # when the producer sent it
    partition_key: str  # session_id or interview_id: keeps per-entity order (SPEC §6.9)
    body: dict[str, Any]


class EventSink(Protocol):
    def write(self, events: Sequence[StreamEvent]) -> None: ...


class CountingSink:
    """Counts events per (source, event_type) and keeps nothing else, so memory stays flat."""

    def __init__(self) -> None:
        self.counts: Counter[tuple[str, str]] = Counter()

    def write(self, events: Sequence[StreamEvent]) -> None:
        self.counts.update((event.source, event.event_type) for event in events)

    @property
    def total(self) -> int:
        return sum(self.counts.values())


class CollectingSink:
    """Keeps every event. Only for tests at tiny scale."""

    def __init__(self) -> None:
        self.events: list[StreamEvent] = []

    def write(self, events: Sequence[StreamEvent]) -> None:
        self.events.extend(events)


_DAY_MS = 86_400_000
_DATE_PREFIX: dict[int, str] = {}


def iso_utc_ms(epoch_ms: int) -> str:
    """Epoch milliseconds as ISO-8601 UTC with a `Z`, e.g. 2025-01-01T10:15:30.123Z.

    Hot path (one call per event): the date part is cached per day and the time is integer
    arithmetic, about 3x faster than going through `datetime.isoformat`.
    """
    day, rem = divmod(epoch_ms, _DAY_MS)
    prefix = _DATE_PREFIX.get(day)
    if prefix is None:
        prefix = _DATE_PREFIX[day] = datetime.fromtimestamp(day * 86_400, UTC).strftime("%Y-%m-%d")
    seconds, millis = divmod(rem, 1000)
    minutes, sec = divmod(seconds, 60)
    hours, minute = divmod(minutes, 60)
    return f"{prefix}T{hours:02d}:{minute:02d}:{sec:02d}.{millis:03d}Z"


def uuid4_str(high: int, low: int) -> str:
    """The UUIDv4 string for 128 random bits, identical to str(uuid.UUID(int=..., version=4))."""
    high = (high & 0xFFFFFFFFFFFF0FFF) | 0x0000000000004000  # version 4
    low = (low & 0x3FFFFFFFFFFFFFFF) | 0x8000000000000000  # RFC 4122 variant
    h = f"{high:016x}{low:016x}"
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"
