import re
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from hirestream.cli import app
from hirestream.generator.manifest import read_manifest
from hirestream.generator.run import make_run_id

REPO = Path(__file__).parents[2]
CONFIG = REPO / "config" / "generator" / "base.yaml"
runner = CliRunner()
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    """Strip colour codes (rich colours output under CI) and rich's line wrapping."""
    return " ".join(ANSI.sub("", text).split())


def _backfill(lake: Path, *args: str) -> tuple[int, str]:
    result = runner.invoke(
        app,
        ["generate", "backfill", "--config", str(CONFIG), "--lake-root", str(lake), *args],
    )
    return result.exit_code, result.output


def test_backfill_tiny_writes_a_manifest(tmp_path: Path) -> None:
    code, out = _backfill(tmp_path, "--preset", "tiny", "--run-id", "t1")
    assert code == 0, out
    assert "run_id=t1 preset=tiny seed=1602" in out
    assert "world: 3 orgs, 7 teams, 300 employees (41 managers, 2 on leave)" in out
    assert "workforce: 11 terminations, 1 leave start, 8 promotions" in out
    assert "hris: 89 files, 26,701 rows" in out  # requisitions draw from their own stream
    assert "requisitions: 7 open at go-live, 9 opened (8 backfill, 1 growth), 0 filled" in out
    assert "jobboard: 251,460 events (9.8% bots) in 32,948 sessions; 2,741 applications" in out
    manifest = read_manifest(tmp_path / "_runs" / "t1" / "manifest.json")
    assert manifest.preset == "tiny"
    assert manifest.window.n_days == 90
    assert len(manifest.files) == 89  # 90 days minus the missing snapshot
    assert all((tmp_path / f.path).exists() for f in manifest.files)
    assert "chaos.scheduling_tz_bug" in manifest.calendar


def test_seed_and_incidents_are_recorded(tmp_path: Path) -> None:
    code, out = _backfill(
        tmp_path,
        "--preset", "tiny", "--seed", "42", "--run-id", "t2",
        "--incident", "late_burst", "--incident", "duplicate_storm", "--incident", "late_burst",
    )  # fmt: skip
    assert code == 0, out
    manifest = read_manifest(tmp_path / "_runs" / "t2" / "manifest.json")
    assert manifest.seed == 42
    assert manifest.incidents == ["duplicate_storm", "late_burst"]
    assert "incidents.late_burst" in manifest.calendar


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--preset", "huge"], "choose from ['dev', 'full', 'tiny']"),
        (["--preset", "tiny", "--incident", "meteor"], "unknown ['meteor']"),
        (["--preset", "tiny", "--seed", "-1"], "--seed"),
    ],
)
def test_bad_arguments_exit_2(tmp_path: Path, args: list[str], message: str) -> None:
    code, out = _backfill(tmp_path, *args)
    assert code == 2
    assert message in _plain(out)
    assert not (tmp_path / "_runs").exists()


def test_unbuildable_world_exits_2_and_touches_nothing(tmp_path: Path) -> None:
    stale = _stale_file(tmp_path)
    raw = yaml.safe_load(CONFIG.read_text())
    raw["presets"]["tiny"]["scale"]["initial_headcount"] = 20  # teams too small for a manager
    config = tmp_path / "small.yaml"
    config.write_text(yaml.safe_dump(raw))
    result = runner.invoke(
        app,
        ["generate", "backfill", "--config", str(config), "--lake-root", str(tmp_path),
         "--preset", "tiny", "--overwrite"],
    )  # fmt: skip
    assert result.exit_code == 2
    assert "can't have a manager" in _plain(result.output)
    assert not (tmp_path / "_runs").exists()
    assert stale.exists()  # the config is checked before --overwrite deletes anything


def test_same_seed_runs_write_identical_files(tmp_path: Path) -> None:
    """SPEC §6.11: two tiny runs with the same seed produce identical file hashes."""
    for run_id in ("a", "b"):
        assert _backfill(tmp_path / run_id, "--preset", "tiny", "--run-id", run_id)[0] == 0
    a = read_manifest(tmp_path / "a" / "_runs" / "a" / "manifest.json")
    b = read_manifest(tmp_path / "b" / "_runs" / "b" / "manifest.json")
    assert a.run_id != b.run_id
    assert a.files and a.deterministic_view() == b.deterministic_view()


def _stale_file(lake: Path) -> Path:
    stale = lake / "bronze" / "hris" / "snapshot_date=1999-01-01" / "employees_19990101.csv.gz"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"old")
    return stale


def test_refuses_to_mix_runs_without_overwrite(tmp_path: Path) -> None:
    stale = _stale_file(tmp_path)
    code, out = _backfill(tmp_path, "--preset", "tiny")
    assert code == 2
    assert "already holds generated data; pass --overwrite" in _plain(out)
    assert stale.exists() and not (tmp_path / "_runs").exists()


def test_overwrite_replaces_generated_data(tmp_path: Path) -> None:
    stale = _stale_file(tmp_path)
    code, out = _backfill(tmp_path, "--preset", "tiny", "--overwrite", "--run-id", "o")
    assert code == 0, out
    assert not stale.exists()
    assert len(read_manifest(tmp_path / "_runs" / "o" / "manifest.json").files) == 89


def test_lake_root_defaults_to_env_var(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIRESTREAM_LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.chdir(REPO)  # default --config is relative to the repo root
    result = runner.invoke(app, ["generate", "backfill", "--preset", "tiny", "--run-id", "e"])
    assert result.exit_code == 0, result.output
    assert (tmp_path / "lake" / "_runs" / "e" / "manifest.json").exists()


def test_live_tail_is_not_implemented_yet() -> None:
    result = runner.invoke(app, ["generate", "live-tail"])
    assert result.exit_code == 1
    assert "T3.4" in result.output


def test_make_run_id() -> None:
    now = datetime(2026, 9, 24, 1, 2, 3, tzinfo=UTC)
    assert make_run_id("dev", 1602, now) == "20260924T010203Z-dev-s1602"
