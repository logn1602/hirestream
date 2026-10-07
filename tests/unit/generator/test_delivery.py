from collections.abc import Sequence

from hirestream.generator.chaos import Delivery
from hirestream.generator.delivery import DeliveryQueue


class Collect:
    def __init__(self) -> None:
        self.batches: list[list[Delivery]] = []
        self.closed = False

    def write(self, deliveries: Sequence[Delivery]) -> None:
        self.batches.append(list(deliveries))

    def close(self) -> None:
        self.closed = True


def _d(arrival: int, tag: str) -> Delivery:
    return Delivery(arrival, "jobboard-web", tag, tag.encode())


def test_flush_hands_over_arrivals_before_the_clock_in_order() -> None:
    sink = Collect()
    queue = DeliveryQueue(sink)
    for d in (_d(30, "c"), _d(10, "a"), _d(20, "b1"), _d(20, "b2"), _d(99, "late")):
        queue.push(d)
    queue.flush_until(30)  # the clock is exclusive
    assert [d.partition_key for d in sink.batches[0]] == ["a", "b1", "b2"]  # ties: push order
    assert len(queue) == 2
    queue.flush_until(30)
    assert len(sink.batches) == 1  # nothing new: no empty batch


def test_close_delivers_the_tail_then_closes() -> None:
    sink = Collect()
    queue = DeliveryQueue(sink)
    queue.push(_d(5, "a"))
    queue.push(_d(10**12, "a week later"))
    queue.flush_until(6)
    queue.close()
    assert [d.partition_key for d in sink.batches[1]] == ["a week later"]
    assert sink.closed and len(queue) == 0


def test_closing_an_empty_queue_just_closes() -> None:
    sink = Collect()
    DeliveryQueue(sink).close()
    assert sink.closed and not sink.batches
