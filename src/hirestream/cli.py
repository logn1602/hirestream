"""`hirestream` command line (SPEC §5.3). Commands stay thin; logic lives in the package."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated

import typer

from hirestream.generator.config import INCIDENT_NAMES, load_config, preset_names
from hirestream.generator.run import OutputExistsError, run_backfill
from hirestream.generator.workforce import EventKind
from hirestream.generator.world import WorldBuildError

DEFAULT_CONFIG = Path("config/generator/base.yaml")
DEFAULT_LAKE_ROOT = Path("data/lake")

app = typer.Typer(no_args_is_help=True, help="HireStream data platform.")
generate_app = typer.Typer(no_args_is_help=True, help="Simulate Halcyon's source systems.")
app.add_typer(generate_app, name="generate")


@app.callback()
def main() -> None:
    """HireStream data platform."""


def resolve_lake_root(option: Path | None) -> Path:
    if option is not None:
        return option
    return Path(os.environ.get("HIRESTREAM_LAKE_ROOT", DEFAULT_LAKE_ROOT))


@generate_app.command("backfill")
def backfill(
    preset: Annotated[str, typer.Option(help="Preset from the config: tiny, dev, or full.")],
    seed: Annotated[int | None, typer.Option(min=0, help="Overrides meta.seed.")] = None,
    incident: Annotated[
        list[str] | None,
        typer.Option(help=f"Enable an incident (repeatable): {', '.join(INCIDENT_NAMES)}."),
    ] = None,
    config: Annotated[
        Path, typer.Option(exists=True, dir_okay=False, help="Generator config.")
    ] = DEFAULT_CONFIG,
    lake_root: Annotated[
        Path | None, typer.Option(help="Defaults to $HIRESTREAM_LAKE_ROOT, then ./data/lake.")
    ] = None,
    run_id: Annotated[str | None, typer.Option(help="Override the generated run id.")] = None,
    overwrite: Annotated[
        bool, typer.Option("--overwrite", help="Replace generated source data already in the lake.")
    ] = False,
) -> None:
    """Generate the whole simulation window in one pass."""
    if preset not in (names := preset_names(config)):
        raise typer.BadParameter(f"choose from {names}", param_hint="--preset")
    incidents = incident or []
    if unknown := sorted(set(incidents) - set(INCIDENT_NAMES)):
        raise typer.BadParameter(
            f"unknown {unknown}; choose from {list(INCIDENT_NAMES)}", param_hint="--incident"
        )

    try:
        result = run_backfill(
            load_config(config, preset),
            resolve_lake_root(lake_root),
            seed=seed,
            incidents=incidents,
            run_id=run_id,
            overwrite=overwrite,
        )
    except WorldBuildError as exc:
        raise typer.BadParameter(str(exc), param_hint="--config") from exc
    except OutputExistsError as exc:
        raise typer.BadParameter(str(exc), param_hint="--lake-root") from exc
    manifest, sim = result.manifest, result.simulation
    typer.echo(f"run_id={manifest.run_id} preset={manifest.preset} seed={manifest.seed}")
    typer.echo(f"world: {result.world_summary}")
    typer.echo(f"workforce: {_counts(sim.event_counts)}")
    r = sim.req_summary
    typer.echo(
        f"requisitions: {r['go_live']:,} open at go-live, {r['opened']:,} opened "
        f"({r['backfill']:,} backfill, {r['growth']:,} growth), {r['filled']:,} filled, "
        f"{r['cancelled']:,} cancelled, {r['expired']:,} expired, {r['open_at_end']:,} open at end"
    )
    rows = sum(entry.records or 0 for entry in sim.files)
    typer.echo(f"hris: {len(sim.files)} files, {rows:,} rows")
    typer.echo(f"manifest={result.manifest_path}")


def _counts(counts: Mapping[EventKind, int]) -> str:
    """{"leave_start": 2, "promotion": 1} -> "2 leave starts, 1 promotion"."""
    parts = [f"{n} {kind.replace('_', ' ')}{'' if n == 1 else 's'}" for kind, n in counts.items()]
    return ", ".join(parts) or "no changes"


@generate_app.command("live-tail")
def live_tail() -> None:
    """Advance the simulation in real time (T3.4)."""
    typer.echo("generate live-tail: not implemented yet (T3.4)", err=True)
    raise typer.Exit(code=1)
