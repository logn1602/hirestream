"""Generator entry point shared by the CLI and (later) Airflow."""

from __future__ import annotations

import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from hirestream.generator.ats_sink import PostgresSink, RequisitionClock
from hirestream.generator.calendar import build_calendar
from hirestream.generator.config import GeneratorConfig, config_hash
from hirestream.generator.errors import OutputExistsError
from hirestream.generator.hris import HRIS_PREFIX
from hirestream.generator.manifest import RunManifest, git_state, write_manifest
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.simulation import SimulationResult, simulate
from hirestream.generator.world import build_world

RUNS_DIR = "_runs"
# Source data a backfill owns; it is replaced as a whole, never mixed across runs (ADR-0005 §7).
GENERATED_PREFIXES: tuple[Path, ...] = (HRIS_PREFIX,)


@dataclass(frozen=True)
class BackfillResult:
    manifest: RunManifest
    manifest_path: Path
    world_summary: str  # the world on sim_start; the simulation advances it in place
    simulation: SimulationResult


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
    the manifest. Every check that can refuse runs before anything is deleted or simulated."""
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
        files=result.files,
        ats_tables=ats_tables,
    )
    path = write_manifest(manifest, lake_root / RUNS_DIR / run_id)
    return BackfillResult(manifest, path, world_summary, result)


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
