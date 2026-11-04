"""The ATS extract's tables and commit records (ADR-0021): no database, no Spark."""

from datetime import UTC, datetime, timedelta

import pytest

from hirestream.generator.ats_sink import COLUMNS
from hirestream.ingest.ats_extract import AtsExtractError, _effective_mode, _filter
from hirestream.transforms.common.ats import (
    TABLES,
    TABLES_BY_NAME,
    ExtractRecord,
    TableExtract,
    bronze_key,
    check_monotonic,
    current_baseline,
    record_key,
)

T0 = datetime(2026, 11, 3, 14, 25, 1, 123456, tzinfo=UTC)
IDS = {table.name: 100 + i for i, table in enumerate(TABLES)}


def _record(
    at: datetime, mode: str = "incremental", wm: int = 10, ts_wm: datetime = T0
) -> ExtractRecord:
    tables = {
        t.name: TableExtract(
            rows=1,
            watermark_from=None,
            watermark=wm if t.append_only else ts_wm,
            key=bronze_key(t.name, at),
        )
        for t in TABLES
    }
    return ExtractRecord(at, "r", mode, IDS, tables)  # type: ignore[arg-type]


def test_tables_match_the_source_ddl_columns() -> None:
    assert {t.name: t.columns for t in TABLES} == COLUMNS
    assert all(t.key == t.columns[0] for t in TABLES)  # the primary key comes first (SPEC §7.4)
    assert [t.name for t in TABLES if t.append_only] == ["application_stage_changes"]


def test_keys() -> None:
    assert bronze_key("offers", T0) == "bronze/ats/offers/extract_ts=20261103T142501Z"
    assert record_key(T0) == "state/ats_watermarks/20261103T142501Z.json"


def test_record_round_trips_through_json() -> None:
    record = _record(T0, mode="full")
    data = record.to_json()
    assert data["extract_ts"] == "2026-11-03T14:25:01.123456Z"
    assert data["tables"]["application_stage_changes"]["watermark"] == 10
    assert data["tables"]["offers"]["watermark"] == "2026-11-03T14:25:01.123456Z"
    assert ExtractRecord.from_json(data) == record
    with pytest.raises(ValueError, match="version"):
        ExtractRecord.from_json({**data, "version": 99})
    with pytest.raises(ValueError, match="mode"):
        ExtractRecord.from_json({**data, "mode": "partial"})


def test_monotonic_history() -> None:
    first, later = _record(T0, "full"), _record(T0 + timedelta(hours=1), wm=12)
    check_monotonic([first, later])
    with pytest.raises(ValueError, match="is not after"):
        check_monotonic([later, first])
    with pytest.raises(ValueError, match="application_stage_changes's watermark moved back"):
        check_monotonic([first, _record(T0 + timedelta(hours=1), wm=9)])
    with pytest.raises(ValueError, match="candidates's watermark moved back"):
        check_monotonic([first, _record(T0 + timedelta(hours=1), ts_wm=T0 - timedelta(seconds=1))])
    # A full extract is a new baseline: its watermarks may be anywhere.
    check_monotonic([first, _record(T0 + timedelta(hours=1), "full", wm=1)])


def test_current_baseline_starts_at_the_latest_full_extract() -> None:
    a, b = _record(T0, "full"), _record(T0 + timedelta(hours=1))
    c, d = _record(T0 + timedelta(hours=2), "full"), _record(T0 + timedelta(hours=3))
    assert current_baseline([a, b, c, d]) == [c, d]
    assert current_baseline([a, b]) == [a, b]
    assert current_baseline([]) == []


def test_effective_mode() -> None:
    previous = _record(T0)
    later = T0 + timedelta(seconds=1)
    assert _effective_mode("incremental", None, IDS, T0) == "full"  # the first extract
    assert _effective_mode("incremental", previous, IDS, later) == "incremental"
    with pytest.raises(AtsExtractError, match="at least a second apart"):
        _effective_mode("incremental", previous, IDS, T0 + timedelta(microseconds=10))
    reloaded = {**IDS, "offers": 999}
    with pytest.raises(AtsExtractError, match="--full"):
        _effective_mode("incremental", previous, reloaded, later)
    assert _effective_mode("full", previous, reloaded, later) == "full"


def test_filter_overlaps_updated_at_by_an_hour_and_sends_naive_utc() -> None:
    _, params = _filter(TABLES_BY_NAME["offers"], T0)
    assert params == (datetime(2026, 11, 3, 13, 25, 1, 123456),)  # naive: a `timestamp`
    _, params = _filter(TABLES_BY_NAME["application_stage_changes"], 41)
    assert params == (41,)  # change_id: no overlap (SPEC §8)
    assert _filter(TABLES_BY_NAME["offers"], None)[1] == ()
