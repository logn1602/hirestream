"""`hirestream` command line (SPEC §5.3). Commands stay thin; logic lives in the package."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated

import typer

from hirestream.generator.config import INCIDENT_NAMES, load_config, preset_names
from hirestream.generator.run import run_backfill

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
) -> None:
    """Generate the whole simulation window in one pass."""
    if preset not in (names := preset_names(config)):
        raise typer.BadParameter(f"choose from {names}", param_hint="--preset")
    incidents = incident or []
    if unknown := sorted(set(incidents) - set(INCIDENT_NAMES)):
        raise typer.BadParameter(
            f"unknown {unknown}; choose from {list(INCIDENT_NAMES)}", param_hint="--incident"
        )

    manifest, path = run_backfill(
        load_config(config, preset),
        resolve_lake_root(lake_root),
        seed=seed,
        incidents=incidents,
        run_id=run_id,
    )
    typer.echo(f"run_id={manifest.run_id} preset={manifest.preset} seed={manifest.seed}")
    typer.echo(f"manifest={path}")


@generate_app.command("live-tail")
def live_tail() -> None:
    """Advance the simulation in real time (T3.4)."""
    typer.echo("generate live-tail: not implemented yet (T3.4)", err=True)
    raise typer.Exit(code=1)
