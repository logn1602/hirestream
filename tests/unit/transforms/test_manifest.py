from datetime import UTC, date, datetime, timedelta

import pytest

from hirestream.transforms.common.lake import FileInfo
from hirestream.transforms.common.manifest import RunRecord, hour_of, window_hours

AT = datetime(2025, 3, 2, tzinfo=UTC)


def test_the_window_is_the_whole_hours_before_as_of() -> None:
    hours = window_hours(AT, 48)
    assert len(hours) == 48
    assert (hours[0], hours[-1]) == (AT - timedelta(hours=48), AT - timedelta(hours=1))
    late = window_hours(
        AT + timedelta(minutes=30), 2
    )  # 22:30 .. 00:30 -> the 23:00 and 00:00 hours
    assert late == [AT - timedelta(hours=1), AT]


@pytest.mark.parametrize(
    ("as_of", "hours", "message"),
    [(datetime(2025, 3, 2), 48, "timezone-aware"), (AT, 0, "at least one hour")],
)
def test_the_window_needs_an_aware_time_and_an_hour(
    as_of: datetime, hours: int, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        window_hours(as_of, hours)


def test_hour_of_reads_the_prefix_and_rejects_other_layouts() -> None:
    key = "bronze/scheduling/yyyy=2025/mm=03/dd=01/hh=23/part-00000-abc.jsonl.gz"
    assert hour_of(key, "scheduling") == datetime(2025, 3, 1, 23, tzinfo=UTC)
    for bad, source in [
        ("bronze/scheduling/stray.jsonl.gz", "scheduling"),
        ("bronze/scheduling/yyyy=2025/mm=03/dd=01/part-0.jsonl.gz", "scheduling"),
        (key, "jobboard"),
    ]:
        with pytest.raises(ValueError, match="unexpected bronze key"):
            hour_of(bad, source)


def test_a_run_record_round_trips_through_json() -> None:
    record = RunRecord(
        run_id="r1",
        source="scheduling",
        mode="incremental",
        as_of=AT,
        objects=(FileInfo("bronze/scheduling/k", 10, 1_740_873_600_000),),
        counts={"lines": 3, "quarantined": {"MALFORMED_JSON": 1}},
        touched=(date(2025, 3, 1),),
    )
    data = record.to_json()
    assert data["as_of"] == "2025-03-02T00:00:00Z"
    assert data["touched"] == ["2025-03-01"]
    assert RunRecord.from_json(data) == record
