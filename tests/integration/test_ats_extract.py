"""`hirestream ingest ats` against a real Postgres (ADR-0021): watermarks, overlap, change_id,
the commit record, a reloaded source, and values surviving COPY → CSV → Parquet exactly."""

import time
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import psycopg
import pytest
from pyspark.sql import Row

from hirestream.generator.ats_sink import DDL
from hirestream.ingest.ats_extract import AtsExtractError, committed_records, extract
from hirestream.transforms.common.ats import TABLES, TABLES_BY_NAME
from hirestream.transforms.common.lake import Lake
from tests.pg import integration

pytestmark = [pytest.mark.integration, integration]

WM = datetime(2025, 3, 1, 12, 0, 0, 654321)  # the latest updated_at in the seed data


def _seed(dsn: str) -> None:
    """A small source: the DDL plus rows that exercise every type, null, '' and CSV quoting."""
    with psycopg.connect(dsn) as conn:
        conn.execute(DDL.read_text())
        conn.execute(
            "INSERT INTO candidates VALUES"
            " ('c1', 'Ann', 'O''Neil, \"Jr\"', 'ann@x.example', NULL, 'Seattle', 'US', false,"
            "  NULL, '2025-01-01 00:00:00', '2025-02-01 08:00:00.000001'),"
            " ('c2', 'Bo', 'Li', 'bo@x.example', '', 'London', 'GB', true, 'E1',"
            "  '2025-01-02 00:00:00', %(wm)s)",
            {"wm": WM},
        )
        conn.execute(
            "INSERT INTO requisitions VALUES ('r1', E'Engineer\\nII', 'eng', 'L4', 'org', 'team',"
            " 'Seattle', 2, 'm1', NULL, 'open', false, true, '2025-01-01 09:00:00', NULL,"
            " '2025-01-01 09:00:00', '2025-02-15 10:00:00')"
        )
        conn.execute(
            "INSERT INTO applications VALUES"
            " ('a1', 'c1', 'r1', 'career_site', '2025-01-03 10:00:00', 'offer', 'active', NULL,"
            "  %(wm)s),"
            " ('a2', 'c2', 'r1', 'internal', '2025-01-04 10:00:00', 'applied', 'active', '',"
            "  '2025-01-04 10:00:00')",
            {"wm": WM},
        )
        conn.execute(
            "INSERT INTO offers VALUES ('o1', 'a1', '2025-02-28 10:00:00', 'accepted',"
            " '2025-03-01 11:30:00', '2025-04-01', '2025-03-01 11:30:00')"
        )
        conn.execute(
            "INSERT INTO application_stage_changes (application_id, from_stage, to_stage,"
            " from_status, to_status, reason, changed_at, changed_by) VALUES"
            " ('a1', NULL, 'applied', NULL, 'active', 'applied', '2025-01-03 10:00:00', 'system'),"
            " ('a1', 'applied', 'offer', 'active', 'active', 'advance', '2025-02-28 10:00:00', 'r')"
        )


@pytest.fixture
def source(fresh_db: str) -> Iterator[str]:
    _seed(fresh_db)
    yield fresh_db


def _rows(lake: Lake, key: str | None, table: str) -> list[Row]:
    assert key is not None
    df = lake.spark.read.schema(TABLES_BY_NAME[table].schema).parquet(lake.paths.uri(key))
    return df.orderBy(TABLES_BY_NAME[table].key).collect()


def _next_second() -> None:
    """Extracts are named by the second they start in (SPEC §8's extract_ts)."""
    time.sleep(1.05)


def test_first_extract_pulls_everything_exactly(source: str, lake: Lake) -> None:
    result = extract(lake, source, "run-1")
    record = result.record
    assert record.mode == "full"  # no earlier extract: a baseline
    assert {n: e.rows for n, e in record.tables.items()} == {
        "candidates": 2, "requisitions": 1, "applications": 2, "offers": 1,
        "application_stage_changes": 2,
    }  # fmt: skip
    assert record.tables["candidates"].watermark == WM.replace(tzinfo=UTC)
    assert record.tables["application_stage_changes"].watermark == 2
    assert all(e.watermark_from is None for e in record.tables.values())
    assert committed_records(lake) == [record]
    assert lake.read_json(result.key) == record.to_json()

    c1, c2 = _rows(lake, record.tables["candidates"].key, "candidates")
    assert c1.last_name == 'O\'Neil, "Jr"' and c1.phone is None and c1.is_internal is False
    assert c1.updated_at == datetime(2025, 2, 1, 8, 0, 0, 1)  # microseconds survive
    assert c2.phone == "" and c2.employee_id == "E1"  # '' is not NULL
    (r1,) = _rows(lake, record.tables["requisitions"].key, "requisitions")
    assert r1.title == "Engineer\nII" and r1.headcount == 2 and r1.recruiter_id is None
    a1, a2 = _rows(lake, record.tables["applications"].key, "applications")
    assert a1.status_reason is None and a2.status_reason == ""
    (o1,) = _rows(lake, record.tables["offers"].key, "offers")
    assert o1.start_date == date(2025, 4, 1)
    changes = _rows(
        lake, record.tables["application_stage_changes"].key, "application_stage_changes"
    )
    assert [c.change_id for c in changes] == [1, 2] and changes[0].from_stage is None


def test_incremental_extracts_follow_the_watermarks(source: str, lake: Lake) -> None:
    first = extract(lake, source, "run-1").record
    _next_second()
    quiet = extract(lake, source, "run-2").record
    assert quiet.mode == "incremental"
    # Rows updated within the hour before each watermark are read again (the overlap); silver
    # drops them as duplicates. Stage changes use change_id, so nothing repeats there.
    assert {n: e.rows for n, e in quiet.tables.items()} == {
        "candidates": 1, "requisitions": 1, "applications": 1, "offers": 1,
        "application_stage_changes": 0,
    }  # fmt: skip
    assert quiet.tables["application_stage_changes"].key is None  # nothing landed
    for name, entry in quiet.tables.items():
        assert entry.watermark_from == first.tables[name].watermark == entry.watermark

    with psycopg.connect(source) as conn:
        conn.execute(
            "UPDATE applications SET status = 'hired', updated_at = '2025-03-02 09:00:00'"
            " WHERE application_id = 'a1'"
        )
        conn.execute(
            "INSERT INTO application_stage_changes (application_id, from_stage, to_stage,"
            " from_status, to_status, reason, changed_at, changed_by) VALUES ('a1', 'offer',"
            " 'offer', 'active', 'hired', 'hired', '2025-03-02 09:00:00', 'system')"
        )
    _next_second()
    moved = extract(lake, source, "run-3").record
    apps = _rows(lake, moved.tables["applications"].key, "applications")
    assert [(a.application_id, a.status) for a in apps] == [("a1", "hired")]
    assert moved.tables["applications"].watermark == datetime(2025, 3, 2, 9, tzinfo=UTC)
    changes = moved.tables["application_stage_changes"]
    assert (changes.rows, changes.watermark_from, changes.watermark) == (1, 2, 3)
    assert moved.tables["candidates"].watermark == first.tables["candidates"].watermark
    assert [r.run_id for r in committed_records(lake)] == ["run-1", "run-2", "run-3"]


def test_a_crash_before_the_commit_moves_no_watermark(
    source: str, lake: Lake, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = extract(lake, source, "run-1").record
    with psycopg.connect(source) as conn:
        conn.execute("UPDATE offers SET updated_at = '2025-03-05 00:00:00'")
    _next_second()

    def crash(key: str, value: object) -> None:
        raise OSError("disk gone")

    with monkeypatch.context() as patch:
        patch.setattr(lake, "write_new_json", crash)
        with pytest.raises(OSError, match="disk gone"):
            extract(lake, source, "run-2")
    assert committed_records(lake) == [first]  # its bronze directories are orphans nobody reads
    _next_second()
    retry = extract(lake, source, "run-2").record
    assert retry.tables["offers"].watermark_from == first.tables["offers"].watermark
    assert retry.tables["offers"].rows == 1


def test_a_reloaded_source_needs_a_full_extract(source: str, lake: Lake) -> None:
    extract(lake, source, "run-1")
    with psycopg.connect(source) as conn:  # what `generate backfill --overwrite` does
        conn.execute("DROP TABLE " + ", ".join(t.name for t in reversed(TABLES)) + " CASCADE")
    _seed(source)
    _next_second()
    with pytest.raises(AtsExtractError, match="--full"):
        extract(lake, source, "run-2")
    full = extract(lake, source, "run-2", mode="full").record
    assert full.mode == "full" and full.tables["candidates"].rows == 2


def test_cli_ingest_ats(
    source: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lake: Lake
) -> None:
    from typer.testing import CliRunner

    from hirestream.cli import app

    monkeypatch.setenv("HIRESTREAM_ATS_DSN", source)
    monkeypatch.setenv("HIRESTREAM_LAKE_ROOT", str(tmp_path / "cli-lake"))
    result = CliRunner().invoke(app, ["ingest", "ats", "--run-id", "cli"])
    assert result.exit_code == 0, result.output
    assert "run_id=cli mode=full" in result.output
    assert "application_stage_changes: 2 rows (everything); watermark 2" in result.output
    time.sleep(1.05)
    again = CliRunner().invoke(app, ["ingest", "ats", "--run-id", "cli-2"])
    assert again.exit_code == 0, again.output
    assert "mode=incremental" in again.output
    assert "application_stage_changes: 0 rows (change_id after 2); watermark 2" in again.output
