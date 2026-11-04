"""`hirestream` command line (SPEC §5.3). Commands stay thin; logic lives in the package."""

from __future__ import annotations

import os
import secrets
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, cast

import typer

from hirestream.generator import calibration, report
from hirestream.generator.ats_db import AtsDbError, describe, resolve_ats_dsn
from hirestream.generator.config import INCIDENT_NAMES, load_config, preset_names
from hirestream.generator.errors import OutputExistsError
from hirestream.generator.run import SeedChecks, run_backfill, run_calibration
from hirestream.generator.workforce import EventKind
from hirestream.generator.world import WorldBuildError

DEFAULT_CONFIG = Path("config/generator/base.yaml")
DEFAULT_LAKE_ROOT = Path("data/lake")

app = typer.Typer(no_args_is_help=True, help="HireStream data platform.")
generate_app = typer.Typer(no_args_is_help=True, help="Simulate Halcyon's source systems.")
app.add_typer(generate_app, name="generate")
run_app = typer.Typer(no_args_is_help=True, help="Run pipeline jobs.")
app.add_typer(run_app, name="run")
ingest_app = typer.Typer(no_args_is_help=True, help="Land source data in bronze.")
app.add_typer(ingest_app, name="ingest")
SILVER_SOURCES = ("scheduling", "jobboard")  # the stream sources (SPEC §9.2)
SILVER_LATER = {"ats": "T2.4b", "hris": "T2.5"}
SILVER_MODES = ("incremental", "bulk")


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
    skip_ats_db: Annotated[
        bool, typer.Option("--skip-ats-db", help="Don't load the ATS into Postgres (ats-db).")
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

    ats_dsn = None if skip_ats_db else resolve_ats_dsn()
    if not skip_ats_db and ats_dsn is None:
        raise typer.BadParameter(
            "no ats-db configured: set ATS_DB_* in .env (cp .env.example .env) and run `make up`,"
            " or pass --skip-ats-db",
            param_hint="--skip-ats-db",
        )
    try:
        result = run_backfill(
            load_config(config, preset),
            resolve_lake_root(lake_root),
            seed=seed,
            incidents=incidents,
            run_id=run_id,
            overwrite=overwrite,
            ats_dsn=ats_dsn,
        )
    except WorldBuildError as exc:
        raise typer.BadParameter(str(exc), param_hint="--config") from exc
    except OutputExistsError as exc:
        raise typer.BadParameter(str(exc), param_hint="--overwrite") from exc
    except AtsDbError as exc:
        raise typer.BadParameter(str(exc), param_hint="--skip-ats-db") from exc
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
    j = sim.jobboard_summary
    bot_share = j["bot_events"] / j["events"] if j["events"] else 0.0
    typer.echo(
        f"jobboard: {j['events']:,} events ({bot_share:.1%} bots) in {j['sessions']:,} sessions; "
        f"{j['career_site'] + j['internal']:,} applications "
        f"({j['career_site']:,} career site, {j['internal']:,} internal)"
    )
    a = sim.ats_summary
    typer.echo(
        f"ats: {a['applications']:,} applications ({a['career_site']:,} career site, "
        f"{a['internal']:,} internal, {a['direct']:,} referral/sourced/agency); "
        f"{a['offers']:,} offers; {a['hires']:,} hires ({a['hires_internal']:,} internal); "
        f"{a['no_starts']:,} no-starts"
    )
    sc = sim.scheduling_summary
    typer.echo(
        f"scheduling: {int(sc['interviews']):,} interviews ({int(sc['panels']):,} panels), "
        f"{int(sc['completed']):,} completed, "
        f"{int(sc['cancelled']):,} cancelled, {int(sc['no_shows']):,} no-shows, "
        f"{int(sc['reschedules']):,} reschedules; {int(sc['feedback']):,} feedback "
        f"({sc['within_48h']:.0%} of completed within 48 h; HT1 ratio {sc['ht1_ratio']:.2f} "
        f"from {int(sc['overloaded_feedback']):,} overloaded); timezone bug: "
        f"{int(sc['naive_starts']):,} naive starts, {int(sc['unresolvable_timezone']):,} "
        "without a timezone"
    )
    c = sim.chaos_summary
    incident_note = (
        f"; incidents: {c['storm_duplicates']:,} storm duplicates, {c['renamed']:,} renamed, "
        f"{c['late_burst']:,} held back"
        if c["storm_duplicates"] or c["renamed"] or c["late_burst"]
        else ""
    )
    typer.echo(
        f"bronze: {len(sim.stream_files):,} stream files, {c['lines']:,} lines from "
        f"{c['events']:,} events ({c['duplicates']:,} duplicates, {c['malformed']:,} malformed, "
        f"{c['late']:,} over an hour late){incident_note}"
    )
    rows = sum(entry.records or 0 for entry in sim.files)
    if ats_dsn is None:
        typer.echo("ats-db: skipped (--skip-ats-db)")
    else:
        t = {name: entry.rows for name, entry in manifest.ats_tables.items()}
        typer.echo(
            f"ats-db: {len(t)} tables loaded into {describe(ats_dsn)} ({t['candidates']:,} "
            f"candidates, {t['requisitions']:,} requisitions, {t['applications']:,} applications, "
            f"{t['offers']:,} offers, {t['application_stage_changes']:,} stage changes)"
        )
    typer.echo(f"hris: {len(sim.files)} files, {rows:,} rows")
    typer.echo(f"calibration: {calibration.summary(result.checks)}")
    typer.echo(f"manifest={result.manifest_path}")
    typer.echo(f"ground_truth={result.ground_truth_path}")
    typer.echo(f"report={result.report_path}")


SWEEP_SEEDS = 5  # ADR-0016: meta.seed and the next four, unless --seed is given


@generate_app.command("calibrate")
def calibrate(
    preset: Annotated[str, typer.Option(help="Preset from the config: tiny, dev, or full.")],
    seed: Annotated[
        list[int] | None,
        typer.Option(
            min=0,
            help=f"Seed to run (repeatable). Default: meta.seed and the next {SWEEP_SEEDS - 1}.",
        ),
    ] = None,
    config: Annotated[
        Path, typer.Option(exists=True, dir_okay=False, help="Generator config.")
    ] = DEFAULT_CONFIG,
) -> None:
    """Check calibration over several seeds. Each backfill goes to a throwaway lake, so this
    never touches data/lake or the ATS database."""
    if preset not in (names := preset_names(config)):
        raise typer.BadParameter(f"choose from {names}", param_hint="--preset")
    resolved = load_config(config, preset)
    seeds = (
        list(dict.fromkeys(seed)) if seed else [resolved.meta.seed + i for i in range(SWEEP_SEEDS)]
    )

    def progress(result: SeedChecks) -> None:
        line = calibration.summary(result.checks)
        typer.echo(f"seed {result.seed}: {line} ({result.runtime_s:,.0f} s)")

    try:
        results = run_calibration(resolved, seeds, on_seed=progress)
    except WorldBuildError as exc:
        raise typer.BadParameter(str(exc), param_hint="--config") from exc
    typer.echo("")
    typer.echo(report.render_sweep(preset, results))


def _counts(counts: Mapping[EventKind, int]) -> str:
    """{"leave_start": 2, "promotion": 1} -> "2 leave starts, 1 promotion"."""
    parts = [f"{n} {kind.replace('_', ' ')}{'' if n == 1 else 's'}" for kind, n in counts.items()]
    return ", ".join(parts) or "no changes"


@generate_app.command("live-tail")
def live_tail() -> None:
    """Advance the simulation in real time (T3.4)."""
    typer.echo("generate live-tail: not implemented yet (T3.4)", err=True)
    raise typer.Exit(code=1)


@ingest_app.command("ats")
def ingest_ats(
    full: Annotated[
        bool,
        typer.Option("--full", help="Ignore the watermarks and pull everything: a new baseline."),
    ] = False,
    run_id: Annotated[
        str | None, typer.Option(help="Default: <UTC time>-ingest-ats-<4 hex>.")
    ] = None,
) -> None:
    """Extract the ATS database into bronze by watermark (SPEC §8). Needs the ats-db (`make up`)."""
    dsn = resolve_ats_dsn()
    if dsn is None:
        raise typer.BadParameter(
            "no ats-db configured: set ATS_DB_* in .env (cp .env.example .env) and run `make up`",
            param_hint="HIRESTREAM_ATS_DSN",
        )
    # Spark is imported here, not at the top, so `generate` never pays for it.
    from hirestream.config import PipelineConfigError, load_pipeline_config
    from hirestream.ingest.ats_extract import AtsExtractError, extract
    from hirestream.transforms.common.ats import OVERLAP_HOURS
    from hirestream.transforms.common.lake import Lake
    from hirestream.transforms.common.session import build_spark_session

    try:
        config = load_pipeline_config()
    except PipelineConfigError as exc:
        raise typer.BadParameter(str(exc), param_hint="config/pipeline") from exc
    started = datetime.now(UTC)
    rid = run_id or f"{started:%Y%m%dT%H%M%SZ}-ingest-ats-{secrets.token_hex(2)}"
    spark = build_spark_session(config.spark_settings(app_name="ingest-ats"))
    try:
        result = extract(
            Lake(spark, config.lake_paths()), dsn, rid, mode="full" if full else "incremental"
        )
    except (AtsDbError, AtsExtractError, ValueError) as exc:
        typer.echo(f"ingest ats: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    record = result.record
    typer.echo(
        f"run_id={record.run_id} mode={record.mode} extract_ts={_iso(record.extract_ts)} "
        f"from {describe(dsn)}"
    )
    for name, entry in record.tables.items():
        if entry.watermark_from is None:
            since = "everything"
        elif isinstance(entry.watermark_from, datetime):
            since = f"updated after {_wm(entry.watermark_from)} - {OVERLAP_HOURS} h overlap"
        else:
            since = f"change_id after {entry.watermark_from}"
        rows = f"{entry.rows:,} row{'' if entry.rows == 1 else 's'}"
        typer.echo(f"{name}: {rows} ({since}); watermark {_wm(entry.watermark)}")
    typer.echo(f"record={result.key}")


def _wm(value: object) -> str:
    return _iso(value) if isinstance(value, datetime) else str(value)


@run_app.command("silver")
def run_silver(
    source: Annotated[
        str, typer.Option(help="scheduling or jobboard (ats and hris arrive in T2.4b, T2.5).")
    ],
    mode: Annotated[
        str,
        typer.Option(help="incremental: new objects in the trailing window; bulk: rebuild all."),
    ] = "incremental",
    as_of: Annotated[
        str | None,
        typer.Option(help="The UTC time the run is for, ISO-8601. Default: now, to the hour."),
    ] = None,
    run_id: Annotated[
        str | None, typer.Option(help="Default: <UTC time>-silver-<source>-<4 hex>.")
    ] = None,
) -> None:
    """Build a silver table from bronze (SPEC §9). Settings come from config/pipeline/."""
    if source in SILVER_LATER:
        typer.echo(
            f"run silver --source {source}: not implemented yet ({SILVER_LATER[source]})", err=True
        )
        raise typer.Exit(code=1)
    if source not in SILVER_SOURCES:
        raise typer.BadParameter(
            f"choose from {', '.join((*SILVER_SOURCES, *SILVER_LATER))}", param_hint="--source"
        )
    if mode not in SILVER_MODES:
        raise typer.BadParameter(f"choose from {', '.join(SILVER_MODES)}", param_hint="--mode")
    moment = _as_of(as_of)
    # Spark is imported here, not at the top, so `generate` never pays for it.
    from hirestream.config import PipelineConfigError, load_pipeline_config
    from hirestream.transforms import silver_jobboard, silver_scheduling
    from hirestream.transforms.common import stream_silver
    from hirestream.transforms.common.lake import Lake
    from hirestream.transforms.common.lineage import RunContext
    from hirestream.transforms.common.manifest import BronzeChangedError, Mode
    from hirestream.transforms.common.session import build_spark_session

    try:
        config = load_pipeline_config()
    except PipelineConfigError as exc:
        raise typer.BadParameter(str(exc), param_hint="config/pipeline") from exc
    started = datetime.now(UTC)
    context = RunContext(
        run_id or f"{started:%Y%m%dT%H%M%SZ}-silver-{source}-{secrets.token_hex(2)}", started
    )
    spark = build_spark_session(config.spark_settings(app_name=f"silver-{source}"))
    lake = Lake(spark, config.lake_paths())
    sources = {
        "scheduling": silver_scheduling.stream_source,
        "jobboard": silver_jobboard.stream_source,
    }
    try:
        result = stream_silver.run(
            lake,
            sources[source](spark),
            context,
            mode=cast("Mode", mode),  # checked against SILVER_MODES above
            as_of=moment,
            window=config.listing_window_hours,
        )
    except (BronzeChangedError, ValueError) as exc:
        typer.echo(f"run silver --source {source}: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    record = result.record
    note = " (already finished: nothing to do)" if result.skipped else ""
    head = f"run_id={record.run_id} source={source} mode={record.mode}"
    typer.echo(f"{head} as_of={_iso(record.as_of)}{note}")
    typer.echo(_counts_line(record.counts))
    touched = record.touched
    span = f" ({touched[0]} .. {touched[-1]})" if touched else ""
    typer.echo(
        f"touched: {len(touched)} event_date partition{'' if len(touched) == 1 else 's'}{span}"
    )


def _as_of(text: str | None) -> datetime:
    if text is None:
        return datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise typer.BadParameter(f"not an ISO-8601 time: {text!r}", param_hint="--as-of") from exc
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _counts_line(counts: Mapping[str, Any]) -> str:
    quarantined = counts["quarantined"]
    reasons = ", ".join(f"{code} {n:,}" for code, n in quarantined.items())
    return (
        f"objects: {counts['objects']:,} · lines: {counts['lines']:,} · written: "
        f"{counts['written']:,} · duplicates: {counts['duplicates']:,} · quarantined: "
        f"{sum(quarantined.values()):,}{f' ({reasons})' if reasons else ''}"
    )
