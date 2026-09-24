from datetime import date

import numpy as np

from hirestream.generator.candidates import CandidateRegistry
from hirestream.generator.events import CollectingSink, CountingSink, StreamEvent, iso_utc_ms


def _event(event_type: str, ts: int = 0) -> StreamEvent:
    return StreamEvent("jobboard-web", event_type, ts, ts, "s-1", {"event_type": event_type})


def test_counting_sink_counts_and_keeps_nothing() -> None:
    sink = CountingSink()
    sink.write([_event("job_view"), _event("job_view"), _event("apply_start")])
    sink.write([])
    assert sink.counts == {("jobboard-web", "job_view"): 2, ("jobboard-web", "apply_start"): 1}
    assert sink.total == 3 and not hasattr(sink, "events")


def test_collecting_sink_keeps_order() -> None:
    sink = CollectingSink()
    sink.write([_event("a", 1), _event("b", 2)])
    sink.write([_event("c", 3)])
    assert [e.event_type for e in sink.events] == ["a", "b", "c"]


def test_iso_utc_ms() -> None:
    assert iso_utc_ms(1_735_726_530_123) == "2025-01-01T10:15:30.123Z"
    assert iso_utc_ms(0) == "1970-01-01T00:00:00.000Z"


def test_a_visitor_keeps_their_candidate() -> None:
    registry = CandidateRegistry(reapply_probability=0.0)
    rng = np.random.default_rng(1)
    first = registry.external(rng, visitor=7, city="Seattle", day=date(2025, 1, 2))
    assert registry.external(rng, visitor=7, city="Boston", day=date(2025, 2, 1)) == first
    other = registry.external(rng, visitor=8, city="Boston", day=date(2025, 2, 1))
    assert other != first
    assert (first, other) == ("C00000001", "C00000002")
    assert registry.candidates[first].location_city == "Seattle"


def test_new_visitors_sometimes_reuse_a_candidate() -> None:
    registry = CandidateRegistry(reapply_probability=0.5)
    rng = np.random.default_rng(2)
    ids = [
        registry.external(rng, visitor=v, city="Austin", day=date(2025, 1, 1)) for v in range(2000)
    ]
    reused = 2000 - len(set(ids))
    assert 850 < reused < 1150  # about half of the visitors after the first


def test_internal_candidates_are_one_per_employee() -> None:
    registry = CandidateRegistry(reapply_probability=1.0)
    a = registry.internal("E000010", "London", date(2025, 1, 5))
    assert registry.internal("E000010", "London", date(2025, 3, 1)) == a
    assert registry.internal("E000011", "London", date(2025, 3, 1)) != a
    assert registry.candidates[a].is_internal and registry.candidates[a].employee_id == "E000010"


def test_application_ids_are_sequential() -> None:
    registry = CandidateRegistry(reapply_probability=0.0)
    assert [registry.new_application_id() for _ in range(3)] == [
        "A000000001",
        "A000000002",
        "A000000003",
    ]
