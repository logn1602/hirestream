"""Generator entry point shared by the CLI and (later) Airflow."""

from __future__ import annotations

import shutil
import tempfile
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from hirestream.generator import calibration, ground_truth, report
from hirestream.generator.ats_sink import PostgresSink, RequisitionClock
from hirestream.generator.calendar import build_calendar
from hirestream.generator.calibration import Check
from hirestream.generator.config import GeneratorConfig, config_hash
from hirestream.generator.errors import OutputExistsError
from hirestream.generator.hris import HRIS_PREFIX
from hirestream.generator.manifest import RunManifest, git_state, write_manifest
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.simulation import SimulationResult, simulate
from hirestream.generator.sinks import STREAM_PREFIXES
from hirestream.generator.world import build_world

RUNS_DIR = "_runs"
# Source data a backfill owns; it is replaced as a whole, never mixed across runs (ADR-0005 §7).
GENERATED_PREFIXES: tuple[Path, ...] = (HRIS_PREFIX, *STREAM_PREFIXES)


@dataclass(frozen=True)
class BackfillResult:
    manifest: RunManifest
    manifest_path: Path
    world_summary: str  # the world on sim_start; the simulation advances it in place
    simulation: SimulationResult
    ground_truth_path: Path
    report_path: Path  # generation_report.md, not hashed: it records runtime and memory
    checks: list[Check]  # calibration against `calibration_targets`; a miss is a warning


@dataclass(frozen=True)
class SeedChecks:
    seed: int
    checks: list[Check]
    runtime_s: float


def make_run_id(preset: str, seed: int, now: datetime | None = None) -> str:
    stamp = (now or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{preset}-s{seed}"


def run_backfill(
    config: GeneratorConfig,
    lake_root: Path,
    *,
    seed: int | None = None,
    incidents: Sequence[str] = (),
    run_id: str | None = None,
    overwrite: bool = False,
    ats_dsn: str | None = None,
) -> BackfillResult:
    """Resolve the run, simulate the window, load the ATS (unless `ats_dsn` is None), and write
    the ground truth, the manifest and the report. Every check that can refuse runs before
    anything is deleted or simulated."""
    started = time.perf_counter()
    seed = config.meta.seed if seed is None else seed
    incidents = sorted(set(incidents))
    calendar = build_calendar(config, incidents)
    plan = SeedPlan(seed)
    world = build_world(config, plan)  # before touching the lake: a bad config changes nothing
    world_summary = world.summary()
    sink = PostgresSink(ats_dsn) if ats_dsn else None
    if sink is not None:
        sink.check()
        sink.ensure_writable(overwrite)
    _prepare_outputs(lake_root, overwrite)
    now = datetime.now(UTC)
    run_id = run_id or make_run_id(config.preset, seed, now)
    commit, dirty = git_state()

    result = simulate(config, plan, calendar, world, lake_root)
    ats_tables = {}
    if sink is not None:
        zones = {loc.city: ZoneInfo(loc.tz) for loc in config.org_model.locations}
        clock = RequisitionClock(seed, zones, config.scheduling.business_hours_local)
        ats_tables = sink.load(result.ats_snapshot, clock, overwrite)
    run_dir = lake_root / RUNS_DIR / run_id
    truth = ground_truth.build(config, result)
    truth_path, truth_sha = ground_truth.write(truth, run_dir)
    manifest = RunManifest(
        run_id=run_id,
        created_at=now,
        preset=config.preset,
        seed=seed,
        incidents=incidents,
        config_hash=config_hash(config),
        git_commit=commit,
        git_dirty=dirty,
        window=config.window,
        calendar=calendar,
        files=[*result.files, *result.stream_files],
        ats_tables=ats_tables,
        ground_truth_sha256=truth_sha,
    )
    path = write_manifest(manifest, run_dir)
    checks = calibration.measure(config, result, truth["hidden_truths"])
    text = report.render(
        manifest,
        result,
        truth,
        checks,
        runtime_s=time.perf_counter() - started,
        peak_rss=report.peak_rss_bytes(),
    )
    report_path = report.write(text, run_dir)
    return BackfillResult(manifest, path, world_summary, result, truth_path, report_path, checks)


def run_calibration(
    config: GeneratorConfig,
    seeds: Sequence[int],
    *,
    scratch: Path | None = None,
    on_seed: Callable[[SeedChecks], None] | None = None,
) -> list[SeedChecks]:
    """Backfill each seed into a throwaway lake (under `scratch`, else the system temp dir) and
    keep only its calibration checks (ADR-0016). Seeds run one at a time, so a run's memory is
    freed before the next starts. The ATS is never loaded into Postgres."""
    results = []
    for seed in seeds:
        started = time.perf_counter()
        with tempfile.TemporaryDirectory(prefix="calibrate-", dir=scratch) as lake:
            run = run_backfill(config, Path(lake), seed=seed, run_id=f"calibrate-s{seed}")
        result = SeedChecks(seed, run.checks, time.perf_counter() - started)
        if on_seed is not None:
            on_seed(result)
        results.append(result)
    return results


def _prepare_outputs(lake_root: Path, overwrite: bool) -> None:
    occupied = [
        lake_root / prefix
        for prefix in GENERATED_PREFIXES
        if (lake_root / prefix).is_dir() and any((lake_root / prefix).iterdir())
    ]
    if occupied and not overwrite:
        where = ", ".join(str(path) for path in occupied)
        raise OutputExistsError(f"{where} already holds generated data; pass --overwrite")
    for path in occupied:
        shutil.rmtree(path)
