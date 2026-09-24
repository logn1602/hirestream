import hashlib
import os
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from hirestream.generator.calendar import CalendarEvent
from hirestream.generator.config import Window
from hirestream.generator.manifest import (
    MANIFEST_NAME,
    RunManifest,
    file_entry,
    git_state,
    read_manifest,
    write_manifest,
)


def _manifest(run_id: str = "r1", **overrides: object) -> RunManifest:
    fields: dict[str, object] = {
        "run_id": run_id,
        "created_at": datetime(2026, 9, 24, 1, 2, 3, tzinfo=UTC),
        "preset": "tiny",
        "seed": 1602,
        "incidents": [],
        "config_hash": "0" * 64,
        "git_commit": "abc123",
        "git_dirty": False,
        "window": Window(sim_start=date(2025, 1, 1), sim_end=date(2025, 3, 31)),
        "calendar": {
            "workforce.reorg": CalendarEvent(start=date(2025, 3, 8), end=date(2025, 3, 8))
        },
    }
    return RunManifest.model_validate({**fields, **overrides})


def test_file_entry_hashes_and_sizes(tmp_path: Path) -> None:
    data = b"line one\nline two\n" * 100_000  # spans several read chunks
    path = tmp_path / "bronze" / "jobboard" / "part-0.jsonl.gz"
    path.parent.mkdir(parents=True)
    path.write_bytes(data)
    entry = file_entry(path, tmp_path, records=200_000)
    assert entry.path == "bronze/jobboard/part-0.jsonl.gz"
    assert entry.sha256 == hashlib.sha256(data).hexdigest()
    assert entry.bytes == len(data)
    assert entry.records == 200_000


def test_write_then_read_round_trips(tmp_path: Path) -> None:
    original = _manifest()
    path = write_manifest(original, tmp_path / "_runs" / "r1")
    assert path.name == MANIFEST_NAME
    assert read_manifest(path) == original
    assert list(path.parent.iterdir()) == [path]  # no temp file left behind


def test_failed_write_leaves_no_partial_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(src: str, dst: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError, match="disk full"):
        write_manifest(_manifest(), tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_deterministic_view_ignores_identity_fields() -> None:
    a = _manifest("r1")
    b = _manifest(
        "r2",
        created_at=datetime(2027, 1, 1, tzinfo=UTC),
        git_commit="def456",
        git_dirty=True,
    )
    assert a.deterministic_view() == b.deterministic_view()
    assert a.deterministic_view() != _manifest(seed=7).deterministic_view()


def test_unknown_manifest_field_is_rejected(tmp_path: Path) -> None:
    path = write_manifest(_manifest(), tmp_path)
    path.write_text(path.read_text().replace('"seed"', '"surprise": 1, "seed"'))
    with pytest.raises(ValueError, match="surprise"):
        read_manifest(path)


def test_git_state_in_a_repo(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(tmp_path),
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@example.com",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "init",
        ],
        check=True,
    )
    commit, dirty = git_state(tmp_path)
    assert commit is not None and len(commit) == 40
    assert dirty is False
    (tmp_path / "new.txt").write_text("x")
    assert git_state(tmp_path) == (commit, True)


def test_git_state_outside_a_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path.parent))
    assert git_state(tmp_path) == (None, None)
