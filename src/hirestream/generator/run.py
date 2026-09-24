"""Generator entry point shared by the CLI and (later) Airflow."""

from __future__ import annotations

import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from hirestream.generator.calendar import build_calendar
from hirestream.generator.config import GeneratorConfig, config_hash
from hirestream.generator.hris import HRIS_PREFIX
from hirestream.generator.manifest import RunManifest, git_state, write_manifest
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.simulation import SimulationResult, simulate
from hirestream.generator.world import build_world

RUNS_DIR = "_runs"
# Source data a backfill owns; it is replaced as a whole, never mixed across runs (ADR-0005 §7).
GENERATED_PREFIXES: tuple[Path, ...] = (HRIS_PREFIX,)


class OutputExistsError(RuntimeError):
    """The lake already holds generated source data and overwriting was not requested."""


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
) -> BackfillResult:
    """Resolve the run (seed, calendar), simulate the window, and write the manifest."""
    seed = config.meta.seed if seed is None else seed
    incidents = sorted(set(incidents))
    calendar = build_calendar(config, incidents)
    plan = SeedPlan(seed)
    world = build_world(config, plan)  # before touching the lake: a bad config changes nothing
    world_summary = world.summary()
    _prepare_outputs(lake_root, overwrite)
    now = datetime.now(UTC)
    run_id = run_id or make_run_id(config.preset, seed, now)
    commit, dirty = git_state()

    result = simulate(config, plan, calendar, world, lake_root)
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
