"""Run manifest: `<lake>/_runs/<run_id>/manifest.json` (SPEC §6.10).

The manifest records what produced a run (preset, seed, config hash, git commit) and what it
produced (per-file sha256 and counts). Its identity fields (run_id, created_at, git state)
differ between runs by design; `deterministic_view()` is the part two runs with the same
code, preset, and seed must agree on.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from hirestream.generator.calendar import CalendarEvent
from hirestream.generator.config import Window

MANIFEST_NAME = "manifest.json"
_IDENTITY_FIELDS = {"run_id", "created_at", "git_commit", "git_dirty"}
_CHUNK = 1 << 20


class FileEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str  # POSIX path relative to the lake root
    sha256: str
    bytes: int
    records: int | None = None


class RunManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest_version: Literal[1] = 1
    run_id: str
    created_at: datetime
    preset: str
    seed: int
    incidents: list[str]
    config_hash: str
    git_commit: str | None
    git_dirty: bool | None
    window: Window
    calendar: dict[str, CalendarEvent]
    files: list[FileEntry] = Field(default_factory=list)

    def deterministic_view(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude=_IDENTITY_FIELDS)


def file_entry(path: Path, lake_root: Path, records: int | None = None) -> FileEntry:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(_CHUNK):
            digest.update(chunk)
    return FileEntry(
        path=path.relative_to(lake_root).as_posix(),
        sha256=digest.hexdigest(),
        bytes=path.stat().st_size,
        records=records,
    )


def git_state(cwd: Path | None = None) -> tuple[str | None, bool | None]:
    """(commit, dirty) of the working tree, or (None, None) outside a git checkout."""
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=cwd, capture_output=True, text=True, check=True
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"], cwd=cwd, capture_output=True, text=True, check=True
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        return None, None
    return commit, bool(status.strip())


def write_manifest(manifest: RunManifest, run_dir: Path) -> Path:
    """Write atomically: a crash never leaves a half-written manifest behind."""
    run_dir.mkdir(parents=True, exist_ok=True)
    target = run_dir / MANIFEST_NAME
    fd, tmp = tempfile.mkstemp(dir=run_dir, prefix=".manifest-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as fh:
            fh.write(manifest.model_dump_json(indent=2) + "\n")
        os.replace(tmp, target)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return target


def read_manifest(path: Path) -> RunManifest:
    return RunManifest.model_validate_json(path.read_text())
