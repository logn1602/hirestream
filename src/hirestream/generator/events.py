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


def iso_utc_ms(epoch_ms: int) -> str:
    """Epoch milliseconds as ISO-8601 UTC with a `Z`, e.g. 2025-01-01T10:15:30.123Z."""
    stamp = datetime.fromtimestamp(epoch_ms / 1000, UTC).isoformat(timespec="milliseconds")
    return stamp.replace("+00:00", "Z")
