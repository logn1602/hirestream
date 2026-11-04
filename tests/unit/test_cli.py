import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from pyspark.sql import SparkSession
from typer.testing import CliRunner

from hirestream import cli
from hirestream.cli import app
from hirestream.config import PipelineConfig, SparkConfig
from hirestream.generator.calibration import Check
from hirestream.generator.manifest import read_manifest
from hirestream.generator.run import SeedChecks, make_run_id
from hirestream.transforms.common.lake import Lake, LakePaths
from tests.pii import TEST_SALT
from tests.unit.transforms.ats_bronze import candidate, land
from tests.unit.transforms.bronze import event_at, put_object, view_at

REPO = Path(__file__).parents[2]
CONFIG = REPO / "config" / "generator" / "base.yaml"


@pytest.fixture(scope="module")
def quiet_config(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """base.yaml with almost no job-board traffic, for runs that only check CLI behaviour."""
    raw = yaml.safe_load(CONFIG.read_text())
    raw["jobboard"]["external"]["base_daily_views_per_open_req"] = 0.001
    raw["jobboard"]["internal"]["p_employee_browses_per_day"] = 0.0
    path = tmp_path_factory.mktemp("cfg") / "config" / "generator" / "base.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(yaml.safe_dump(raw))
    return path


runner = CliRunner()
ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _plain(text: str) -> str:
    """Strip colour codes (rich colours output under CI) and rich's line wrapping."""
    return " ".join(ANSI.sub("", text).split())


def _backfill(
    lake: Path, *args: str, config: Path = CONFIG, ats_db: bool = False
) -> tuple[int, str]:
    """Run backfill; the ATS Postgres load is skipped unless a test asks for it."""
    flags = [] if ats_db else ["--skip-ats-db"]
    result = runner.invoke(
        app,
        ["generate", "backfill", "--config", str(config), "--lake-root", str(lake), *flags, *args],
    )
    return result.exit_code, result.output


def test_backfill_tiny_writes_a_manifest(tmp_path: Path) -> None:
    code, out = _backfill(tmp_path, "--preset", "tiny", "--run-id", "t1")
    assert code == 0, out
    assert "run_id=t1 preset=tiny seed=1602" in out
    assert "world: 3 orgs, 7 teams, 300 employees (41 managers, 2 on leave)" in out
    assert "workforce: 12 terminations, 2 leave starts, 7 promotions" in out
    assert ", 6 hires, 2 transfers" in out  # the ATS's hires join the workforce (and shift draws)
    assert "requisitions: 7 open at go-live, 13 opened (10 backfill, 3 growth), 6 filled" in out
    assert "jobboard: 175,375 events (13.2% bots) in 21,166 sessions; 3,064 applications" in out
    assert "ats: 4,154 applications (2,990 career site, 74 internal, 1,090 referral" in out
    assert "; 55 offers; 8 hires (2 internal); 1 no-starts" in out
    assert "scheduling: 1,568 interviews (185 panels), 1,202 completed, 195 cancelled" in out
    assert "timezone bug: 266 naive starts, 4 without a timezone" in out
    assert "bronze: 3,603 stream files, 182,641 lines from 179,925 events (2,716 duplicates" in out
    assert "hris: 89 files, 26,768 rows" in out
    assert "calibration: 12/18 within target; warn: req_fill_rate, offer_acceptance_rate" in out
    run_dir = tmp_path / "_runs" / "t1"
    for name in ("manifest", "ground_truth"):
        assert f"{name}={run_dir / name}.json" in out
    assert f"report={run_dir / 'generation_report.md'}" in out
    manifest = read_manifest(run_dir / "manifest.json")
    assert manifest.preset == "tiny"
    assert manifest.window.n_days == 90
    hris = [f for f in manifest.files if f.path.startswith("bronze/hris/")]
    assert len(hris) == 89  # 90 days minus the missing snapshot
    assert len(manifest.files) == 89 + 3_603  # plus the stream parts
    assert all((tmp_path / f.path).exists() for f in manifest.files)
    assert "chaos.scheduling_tz_bug" in manifest.calendar


def test_seed_and_incidents_are_recorded(tmp_path: Path, quiet_config: Path) -> None:
    code, out = _backfill(
        tmp_path,
        "--preset", "tiny", "--seed", "42", "--run-id", "t2",
        "--incident", "late_burst", "--incident", "duplicate_storm", "--incident", "late_burst",
        config=quiet_config,
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
         "--preset", "tiny", "--overwrite", "--skip-ats-db"],
    )  # fmt: skip
    assert result.exit_code == 2
    assert "can't have a manager" in _plain(result.output)
    assert not (tmp_path / "_runs").exists()
    assert stale.exists()  # the config is checked before --overwrite deletes anything


def test_same_seed_runs_write_identical_files(tmp_path: Path, quiet_config: Path) -> None:
    """SPEC §6.11: two tiny runs with the same seed produce identical file hashes."""
    for run_id in ("a", "b"):
        code, _ = _backfill(
            tmp_path / run_id, "--preset", "tiny", "--run-id", run_id, config=quiet_config
        )
        assert code == 0
    a = read_manifest(tmp_path / "a" / "_runs" / "a" / "manifest.json")
    b = read_manifest(tmp_path / "b" / "_runs" / "b" / "manifest.json")
    assert a.run_id != b.run_id
    assert a.files and a.deterministic_view() == b.deterministic_view()  # ground truth included
    reports = [
        (tmp_path / r / "_runs" / r / "generation_report.md").read_text().splitlines()
        for r in ("a", "b")
    ]
    varying = ("# Generation report:", "| Created |", "| Runtime |", "| Peak RSS |")
    assert [line for line in reports[0] if not line.startswith(varying)] == [
        line for line in reports[1] if not line.startswith(varying)
    ]  # the report is unhashed only because of its run identity, runtime and memory


def _stale_file(lake: Path, prefix: str = "hris/snapshot_date=1999-01-01") -> Path:
    stale = lake / "bronze" / prefix / "old.gz"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"old")
    return stale


@pytest.mark.parametrize("prefix", ["scheduling/yyyy=1999", "jobboard/yyyy=1999"])
def test_stream_folders_are_generated_data_too(tmp_path: Path, prefix: str) -> None:
    stale = _stale_file(tmp_path, prefix)
    code, out = _backfill(tmp_path, "--preset", "tiny")
    assert code == 2 and "pass --overwrite" in _plain(out) and stale.exists()


def test_refuses_to_mix_runs_without_overwrite(tmp_path: Path) -> None:
    stale = _stale_file(tmp_path)
    code, out = _backfill(tmp_path, "--preset", "tiny")
    assert code == 2
    assert "already holds generated data; pass --overwrite" in _plain(out)
    assert stale.exists() and not (tmp_path / "_runs").exists()


def test_overwrite_replaces_generated_data(tmp_path: Path, quiet_config: Path) -> None:
    stale = _stale_file(tmp_path)
    stream_stale = _stale_file(tmp_path, "jobboard/yyyy=1999")
    code, out = _backfill(
        tmp_path, "--preset", "tiny", "--overwrite", "--run-id", "o", config=quiet_config
    )
    assert code == 0, out
    assert not stale.exists() and not stream_stale.exists()
    files = read_manifest(tmp_path / "_runs" / "o" / "manifest.json").files
    assert len([f for f in files if f.path.startswith("bronze/hris/")]) == 89


def test_lake_root_defaults_to_env_var(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, quiet_config: Path
) -> None:
    monkeypatch.setenv("HIRESTREAM_LAKE_ROOT", str(tmp_path / "lake"))
    monkeypatch.chdir(quiet_config.parents[2])  # default --config is config/generator/base.yaml
    result = runner.invoke(
        app, ["generate", "backfill", "--preset", "tiny", "--run-id", "e", "--skip-ats-db"]
    )
    assert result.exit_code == 0, result.output
    assert (tmp_path / "lake" / "_runs" / "e" / "manifest.json").exists()


def test_live_tail_is_not_implemented_yet() -> None:
    result = runner.invoke(app, ["generate", "live-tail"])
    assert result.exit_code == 1
    assert "T3.4" in result.output


def test_make_run_id() -> None:
    now = datetime(2026, 9, 24, 1, 2, 3, tzinfo=UTC)
    assert make_run_id("dev", 1602, now) == "20260924T010203Z-dev-s1602"


def _no_ats_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("HIRESTREAM_ATS_DSN", "ATS_DB_NAME", "ATS_DB_USER", "ATS_DB_PASSWORD"):
        monkeypatch.delenv(key, raising=False)


def test_backfill_needs_an_ats_db_or_skip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _no_ats_env(monkeypatch)
    code, out = _backfill(tmp_path, "--preset", "tiny", ats_db=True)
    assert code == 2
    assert "no ats-db configured" in _plain(out) and not (tmp_path / "_runs").exists()


def test_unreachable_ats_db_fails_before_simulating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _no_ats_env(monkeypatch)
    monkeypatch.setenv(
        "HIRESTREAM_ATS_DSN", "host=127.0.0.1 port=1 dbname=ats user=x password=hunter2"
    )
    code, out = _backfill(tmp_path, "--preset", "tiny", ats_db=True)
    assert code == 2
    text = _plain(out)
    assert "cannot reach the ats-db at 127.0.0.1:1/ats" in text
    assert "hunter2" not in text  # never print credentials
    assert not (tmp_path / "_runs").exists() and not (tmp_path / "bronze").exists()


def test_calibrate_runs_each_seed_in_a_throwaway_lake(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, quiet_config: Path
) -> None:
    scratch = tmp_path / "tmp"
    scratch.mkdir()
    monkeypatch.setattr(tempfile, "tempdir", str(scratch))
    monkeypatch.chdir(tmp_path)
    args = ["--preset", "tiny", "--seed", "3", "--seed", "4", "--seed", "3"]
    result = runner.invoke(app, ["generate", "calibrate", "--config", str(quiet_config), *args])
    assert result.exit_code == 0, result.output
    seeds = [line.split(":")[0] for line in result.output.splitlines() if line.startswith("seed ")]
    assert seeds == ["seed 3", "seed 4"]  # in order, each once
    assert "## Calibration sweep: tiny, 2 seeds" in result.output
    assert "| Metric | Target | s3 | s4 | Min | Max | Result |" in result.output
    assert not any(scratch.iterdir())  # every throwaway lake is gone
    assert not (tmp_path / "data").exists()  # and data/lake was never touched


def test_calibrate_defaults_to_five_seeds_from_meta_seed(monkeypatch: pytest.MonkeyPatch) -> None:
    ran: list[int] = []

    def fake(config: object, seeds: list[int], **_: object) -> list[SeedChecks]:
        ran.extend(seeds)
        return [SeedChecks(s, [Check("req_fill_rate", 0.85, 0.8, 0.92)], 1.0) for s in seeds]

    monkeypatch.setattr(cli, "run_calibration", fake)
    result = runner.invoke(
        app, ["generate", "calibrate", "--config", str(CONFIG), "--preset", "dev"]
    )
    assert result.exit_code == 0, result.output
    assert ran == [1602, 1603, 1604, 1605, 1606]
    assert "1 of 1 within target on every seed." in result.output


def test_calibrate_rejects_an_unknown_preset() -> None:
    result = runner.invoke(app, ["generate", "calibrate", "--config", str(CONFIG), "--preset", "x"])
    assert result.exit_code == 2
    assert "choose from ['dev', 'full', 'tiny']" in _plain(result.output)


@pytest.fixture
def silver_lake(spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Lake:
    """A lake with one scheduling and one job board object, and a pipeline config that points at
    it and sizes Spark like the shared test session, which the command joins instead of starting
    its own."""
    config = PipelineConfig(
        env="local",
        lake_root=str(tmp_path),
        dq_checks=Path("config/dq/checks.yaml"),
        spark=SparkConfig(master="local[2]", shuffle_partitions=2),
    )
    monkeypatch.setattr("hirestream.config.load_pipeline_config", lambda: config)
    lake = Lake(spark, LakePaths.from_root(tmp_path))
    hour = datetime(2025, 3, 1, 10, tzinfo=UTC)
    put_object(lake, hour, [event_at(hour), '{"broken'])
    put_object(lake, hour, [view_at(hour), view_at(hour), view_at(hour)], source="jobboard")
    return lake


SILVER = ["run", "silver", "--source", "scheduling", "--as-of", "2025-03-02T00:00:00Z"]


def test_run_silver_builds_scheduling_and_skips_a_finished_run(silver_lake: Lake) -> None:
    result = runner.invoke(app, [*SILVER, "--mode", "bulk", "--run-id", "cli-1"])
    assert result.exit_code == 0, result.output
    out = _plain(result.output)
    assert "run_id=cli-1 source=scheduling mode=bulk as_of=2025-03-02T00:00:00Z" in out
    assert "lines: 2 · written: 1 · duplicates: 0 · quarantined: 1 (MALFORMED_JSON 1)" in out
    assert "touched: 1 event_date partition (2025-03-01 .. 2025-03-01)" in out
    again = _plain(runner.invoke(app, [*SILVER, "--mode", "bulk", "--run-id", "cli-1"]).output)
    assert "(already finished: nothing to do)" in again
    assert silver_lake.exists("state/touched_partitions/cli-1/scheduling.json")


def test_run_silver_names_its_own_runs(silver_lake: Lake) -> None:
    result = runner.invoke(app, [*SILVER, "--mode", "incremental"])
    assert result.exit_code == 0, result.output
    run_id = _plain(result.output).split()[0].removeprefix("run_id=")
    assert re.fullmatch(r"[0-9]{8}T[0-9]{6}Z-silver-scheduling-[0-9a-f]{4}", run_id)
    assert "objects: 1 · lines: 2 · written: 1" in _plain(result.output)  # inside the 48 h


def test_run_silver_builds_the_job_board(silver_lake: Lake) -> None:
    args = ["run", "silver", "--source", "jobboard", "--as-of", "2025-03-02T00:00:00Z"]
    result = runner.invoke(app, [*args, "--mode", "bulk", "--run-id", "cli-2"])
    assert result.exit_code == 0, result.output
    out = _plain(result.output)
    assert "run_id=cli-2 source=jobboard mode=bulk as_of=2025-03-02T00:00:00Z" in out
    assert "lines: 3 · written: 3 · duplicates: 0 · quarantined: 0" in out
    assert silver_lake.exists("silver/jobboard_events")
    assert silver_lake.exists("state/touched_partitions/cli-2/jobboard.json")


ATS = ["run", "silver", "--source", "ats", "--as-of", "2025-03-03T00:00:00Z"]
EXTRACTED = datetime(2025, 3, 2, tzinfo=UTC)


def test_run_silver_builds_the_ats_tables(
    silver_lake: Lake, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HIRESTREAM_PII_SALT", TEST_SALT)
    land(
        silver_lake,
        EXTRACTED,
        {"candidates": [candidate("c1", "ann@x.example", datetime(2025, 3, 1))]},
        mode="full",
    )
    result = runner.invoke(app, [*ATS, "--run-id", "ats-1"])
    assert result.exit_code == 0, result.output
    out = _plain(result.output)
    assert "run_id=ats-1 source=ats mode=incremental as_of=2025-03-03T00:00:00Z" in out
    assert "baseline 20250302T000000Z: 1 extract (1 new, a new baseline); rebuilt" in out
    assert "ats_candidates: 1 row (1 read, 0 superseded)" in out
    assert "ats_offers: 0 rows (0 read, 0 superseded)" in out
    assert "touched: 1 date (2025-03-01 .. 2025-03-01)" in out
    quiet = _plain(runner.invoke(app, [*ATS, "--run-id", "ats-2"]).output)
    assert "(0 new); nothing new: tables unchanged" in quiet and "touched: 0 dates" in quiet
    again = _plain(runner.invoke(app, [*ATS, "--run-id", "ats-1"]).output)
    assert "(already finished: nothing to do)" in again
    assert silver_lake.exists("state/touched_partitions/ats-1/ats.json")


@pytest.mark.parametrize(
    ("salt", "message"),
    [(None, "HIRESTREAM_PII_SALT is not set"), ("change-me", "placeholder")],
)
def test_run_silver_ats_needs_a_real_salt(
    silver_lake: Lake, monkeypatch: pytest.MonkeyPatch, salt: str | None, message: str
) -> None:
    if salt is None:
        monkeypatch.delenv("HIRESTREAM_PII_SALT", raising=False)
    else:
        monkeypatch.setenv("HIRESTREAM_PII_SALT", salt)
    result = runner.invoke(app, ATS)
    assert result.exit_code == 2 and message in _plain(result.output)
    assert not silver_lake.exists("state/pii")  # refused before anything ran


def test_run_silver_ats_without_an_extract(
    silver_lake: Lake, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HIRESTREAM_PII_SALT", TEST_SALT)
    result = runner.invoke(app, ATS)
    assert result.exit_code == 1
    assert "run `hirestream ingest ats` first" in _plain(result.output)


def test_run_silver_for_later_sources_is_not_implemented_yet() -> None:
    result = runner.invoke(app, ["run", "silver", "--source", "hris"])
    assert result.exit_code == 1 and "not implemented yet (T2.5)" in _plain(result.output)


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["--source", "nope"], "choose from scheduling"),
        (["--source", "scheduling", "--mode", "sideways"], "choose from incremental, bulk"),
        (["--source", "scheduling", "--as-of", "yesterday"], "not an ISO-8601 time"),
    ],
)
def test_run_silver_rejects_bad_arguments(args: list[str], message: str) -> None:
    result = runner.invoke(app, ["run", "silver", *args])
    assert result.exit_code == 2 and message in _plain(result.output)
