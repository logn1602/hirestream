"""Generator entry point shared by the CLI and (later) Airflow."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from hirestream.generator.calendar import build_calendar
from hirestream.generator.config import GeneratorConfig, config_hash
from hirestream.generator.manifest import RunManifest, git_state, write_manifest
from hirestream.generator.seeds import SeedPlan
from hirestream.generator.world import World, build_world

RUNS_DIR = "_runs"


@dataclass(frozen=True)
class BackfillResult:
    manifest: RunManifest
    manifest_path: Path
    world: World


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
) -> BackfillResult:
    """Resolve the run (seed, calendar), generate, and write the manifest."""
    seed = config.meta.seed if seed is None else seed
    incidents = sorted(set(incidents))
    calendar = build_calendar(config, incidents)
    plan = SeedPlan(seed)
    world = build_world(config, plan)  # before any output: a bad config writes nothing
    now = datetime.now(UTC)
    run_id = run_id or make_run_id(config.preset, seed, now)
    commit, dirty = git_state()

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
        files=[],
    )
    path = write_manifest(manifest, lake_root / RUNS_DIR / run_id)
    return BackfillResult(manifest=manifest, manifest_path=path, world=world)
