"""The delivery queue between the chaos layer and the stream sinks (SPEC §6.9, ADR-0012).

Lines wait here until the simulation clock passes their arrival time. After simulating day D, the
simulation flushes with D's own UTC midnight as the clock: one day behind, because the next day's
events can be stamped hours before their UTC midnight. Every line that has arrived by then goes to
the sinks in arrival order; only the last day's lines and the lagged ones stay in memory.

It behaves as SPEC §6.9's min-heap on arrival time, but appends and sorts each flush's lines
instead: a stable sort keeps push order for equal arrivals, and it is several times cheaper in
Python than a heap push per line.
"""

from __future__ import annotations

from collections.abc import Sequence
from operator import attrgetter
from typing import Protocol

from hirestream.generator.chaos import Delivery

_ARRIVAL = attrgetter("arrival_ms")


class LineSink(Protocol):
    def write(self, deliveries: Sequence[Delivery]) -> None: ...
    def close(self) -> None: ...


class DeliveryQueue:
    def __init__(self, sink: LineSink) -> None:
        self._sink = sink
        self._pending: list[Delivery] = []  # in push order

    def __len__(self) -> int:
        return len(self._pending)

    def push(self, delivery: Delivery) -> None:
        self._pending.append(delivery)

    def flush_until(self, clock_ms: int) -> None:
        """Hand the sinks everything that has arrived before `clock_ms`, in arrival order."""
        ready = [d for d in self._pending if d.arrival_ms < clock_ms]
        if not ready:
            return
        self._pending = [d for d in self._pending if d.arrival_ms >= clock_ms]
        ready.sort(key=_ARRIVAL)  # stable: equal arrivals keep push order
        self._sink.write(ready)

    def close(self) -> None:
        """End of the run: deliver the lag tail (up to a week past the window), then close."""
        if self._pending:
            self.flush_until(max(d.arrival_ms for d in self._pending) + 1)
        self._sink.close()
