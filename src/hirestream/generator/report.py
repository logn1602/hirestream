"""generation_report.md: one run, for a person to read (SPEC §6.10, ADR-0015).

Written after the manifest and never hashed, because runtime and peak memory differ between runs
of the same seed. Everything else comes from the manifest, `ground_truth.json` and the
calibration checks, so the report never disagrees with them.

`render_sweep` formats the same checks across several seeds (`generate calibrate`, ADR-0016).
"""

from __future__ import annotations

import resource
import sys
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from hirestream.generator.calibration import Check
from hirestream.generator.manifest import RunManifest, write_atomic

if TYPE_CHECKING:
    from hirestream.generator.run import SeedChecks
    from hirestream.generator.simulation import SimulationResult

REPORT = "generation_report.md"
HIDDEN_TRUTHS = ("ht1", "ht2", "ht3")
COUNTS = frozenset({"stream_events"})  # whole numbers, shown as such


def peak_rss_bytes() -> int:
    """This process's peak resident set so far. `ru_maxrss` is KiB on Linux and bytes on macOS."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def write(text: str, run_dir: Path) -> Path:
    path = run_dir / REPORT
    write_atomic(path, text.encode())
    return path


def render(
    manifest: RunManifest,
    result: SimulationResult,
    truth: dict[str, Any],
    checks: Sequence[Check],
    *,
    runtime_s: float,
    peak_rss: int,
) -> str:
    sections = [
        f"# Generation report: {manifest.run_id}",
        _identity(manifest, runtime_s, peak_rss),
        _calibration(checks),
        _hidden_truths(truth, {c.name: c for c in checks}),
        _sources(manifest, result, truth),
        _workforce(result, truth),
        _chaos(truth),
    ]
    return "\n\n".join(sections) + "\n"


def render_sweep(preset: str, results: Sequence[SeedChecks]) -> str:
    """Every calibration target on every seed, with its range across them (ADR-0016)."""
    names = [c.name for c in results[0].checks]
    if any([c.name for c in r.checks] != names for r in results):
        raise ValueError("every seed must be measured against the same targets")
    rows, within = [], 0
    for i, name in enumerate(names):
        per_seed = [r.checks[i] for r in results]
        values = [c.value for c in per_seed if c.value is not None]
        band = per_seed[0].low, per_seed[0].high
        low = Check(name, min(values) if values else None, *band)
        high = Check(name, max(values) if values else None, *band)
        missed = sum(not c.passed for c in per_seed)
        within += not missed
        result = f"**warn** ({missed} of {len(per_seed)})" if missed else "pass"
        rows.append(
            (name, _band(per_seed[0]), *map(_value, per_seed), _value(low), _value(high), result)
        )
    seeds = [f"s{r.seed}" for r in results]
    header = ("Metric", "Target", *seeds, "Min", "Max", "Result")
    return "\n\n".join(
        [
            f"## Calibration sweep: {preset}, {len(results)} seeds",
            f"{within} of {len(names)} within target on every seed.",
            _table(header, rows, align="ll" + "r" * (len(seeds) + 2) + "l"),
        ]
    )


# ------------------------------------------------------------------ sections


def _identity(manifest: RunManifest, runtime_s: float, peak_rss: int) -> str:
    window = manifest.window
    commit = manifest.git_commit or "unknown"
    rows = [
        ("Preset", manifest.preset),
        ("Seed", str(manifest.seed)),
        ("Window", f"{window.sim_start} to {window.sim_end}"),
        ("Incidents", ", ".join(manifest.incidents) or "none"),
        ("Config hash", f"`{manifest.config_hash}`"),
        ("Git commit", f"`{commit}`" + (" (dirty)" if manifest.git_dirty else "")),
        ("Created", manifest.created_at.isoformat()),
        ("Ground truth sha256", f"`{manifest.ground_truth_sha256}`"),
        ("Runtime", f"{runtime_s:,.1f} s"),
        ("Peak RSS", f"{peak_rss / 2**20:,.0f} MiB (whole process)"),
    ]
    return _table(("Run", ""), rows)


def _calibration(checks: Sequence[Check]) -> str:
    passed = sum(c.passed for c in checks)
    rows = [(c.name, _value(c), _band(c), "pass" if c.passed else "**warn**") for c in checks]
    return "\n\n".join(
        [
            "## Calibration",
            f"{passed} of {len(checks)} within target. A miss is a warning: SPEC §6.11 wants "
            "dev and full within target, or an ADR that explains the miss. Definitions: ADR-0015.",
            _table(("Metric", "Measured", "Target", "Result"), rows, align="lrll"),
        ]
    )


def _hidden_truths(truth: dict[str, Any], checks: dict[str, Check]) -> str:
    hidden = truth["hidden_truths"]
    rows = []
    for name in HIDDEN_TRUTHS:
        check = checks[f"{name}_ratio"]
        rows.append((name.upper(), hidden[name]["metric"], _value(check), _band(check)))
    ht4 = hidden["ht4"]
    rows.append(
        ("HT4", ht4["metric"], "declines" if ht4["monotonic"] else "**does not decline**", "")
    )
    buckets = [(b["bucket"], f"{b['decided']:,}", f"{b['acceptance']:.3f}") for b in ht4["buckets"]]
    drawn = hidden["ht2"]["drawn_ratio"]
    return "\n\n".join(
        [
            "## Hidden truths (SPEC §13.2)",
            _table(("", "Metric", "Realized", "Band"), rows, align="llrl"),
            f"HT2 as drawn when applications arrived: {_number(drawn)}. The realized ratio "
            "counts only the first-gate decisions the ATS recorded before the window closed.",
            "HT4, offer acceptance by days to offer:",
            _table(("Days to offer", "Decided offers", "Acceptance"), buckets, align="lrr"),
        ]
    )


def _sources(manifest: RunManifest, result: SimulationResult, truth: dict[str, Any]) -> str:
    streams = {s: v for s, v in truth["streams"].items() if "events" in v}
    totals = [
        (
            source,
            f"{v['events']:,}",
            f"{v['lines']:,}",
            f"{sum(v['duplicates'].values()):,}",
            f"{sum(v['malformed'].values()):,}",
            f"{v['unusable']:,}",
            f"{v['expected_silver_events']:,}",
        )
        for source, v in streams.items()
    ]
    by_type = [
        (source, event_type, f"{n:,}")
        for source, v in streams.items()
        for event_type, n in v["events_by_type"].items()
    ]
    snapshot = result.ats_snapshot
    loaded = {name: entry.rows for name, entry in manifest.ats_tables.items()}
    ats = [
        ("candidates", len(snapshot.candidates)),
        ("requisitions", len(snapshot.requisitions)),
        ("applications", len(snapshot.applications)),
        ("offers", len(snapshot.offers)),
        ("application_stage_changes", len(snapshot.changes)),
    ]
    ats_rows = [
        (name, f"{n:,}", f"{loaded[name]:,}" if name in loaded else "not loaded") for name, n in ats
    ]
    hris_rows = sum(entry.records or 0 for entry in result.files)
    return "\n\n".join(
        [
            "## Sources",
            "Stream events are what the producers emitted. Lines are what reached bronze, "
            "duplicates and malformed copies included. Expected in silver = events minus those "
            "with no usable copy.",
            _table(
                ("Source", "Events", "Lines", "Duplicates", "Malformed", "Unusable", "In silver"),
                totals,
                align="lrrrrrr",
            ),
            _table(("Source", "Event type", "Events"), by_type, align="llr"),
            f"Bronze stream files: {len(result.stream_files):,}. HRIS: {len(result.files):,} "
            f"snapshot files, {hris_rows:,} rows.",
            "ATS final state:",
            _table(("Table", "Rows", "ats-db"), ats_rows, align="lrr"),
        ]
    )


def _workforce(result: SimulationResult, truth: dict[str, Any]) -> str:
    workforce = truth["workforce"]
    start, end = workforce["headcount"]["start"], workforce["headcount"]["end"]
    lines = [
        "## Workforce",
        f"Headcount {start:,} on the first day, {end:,} after the last "
        f"({_percent(workforce['net_growth'])}). Plan: "
        f"{workforce['planned_growth_annual']:.1%} a year.",
    ]
    if result.growth_plans:
        last = result.growth_plans[-1]
        seats = sum(p.seats for p in result.growth_plans)
        lines.append(
            f"Monthly headcount plans: {len(result.growth_plans):,}; growth seats they planned: "
            f"{seats:,}. The last ({last.day}) aimed for {last.target:,.0f} employees by the "
            "following month."
        )
    events = [(kind.replace("_", " "), f"{n:,}") for kind, n in result.event_counts.items()]
    lines.append(_table(("Change", "Count"), events, align="lr"))
    return "\n\n".join(lines)


def _chaos(truth: dict[str, Any]) -> str:
    rows = []
    for source, v in truth["streams"].items():
        if "events" in v:
            for kind, n in v["duplicates"].items():
                rows.append((source, f"duplicate ({kind})", n))
            for kind, n in v["malformed"].items():
                rows.append((source, f"malformed ({kind})", n))
            late = sum(n for band, n in v["lag_bands"].items() if band != "0")
            rows += [
                (source, "lost (every copy malformed)", v["lost"]),
                (source, "an hour or more late", late),
                (source, "held back (late burst)", v["late_burst"]),
                (source, "renamed field (silent schema break)", v["renamed"]),
                (source, "unresolvable timezone lines", v["unresolvable_timezone_lines"]),
            ]
        if "timezone_bug" in v:
            bug = v["timezone_bug"]
            rows.append((source, "timezone bug: naive starts", sum(bug["naive_starts"].values())))
    quarantine = [
        (source, reason, n)
        for source, reasons in truth["expected_quarantine"].items()
        for reason, n in reasons.items()
        if n
    ]
    hris = truth["hris"]
    hris_lines = [
        f"- Missing days: {_days(hris['missing_days'])}",
        f"- Renamed files: {_days(hris['renamed_days'])}",
        f"- Late exports: {hris['late_exports']:,}; changes deferred to a later file: "
        f"{hris['deferred_changes']:,}",
    ]
    if hris["duplicate"]:
        dup = hris["duplicate"]
        hris_lines.append(f"- Duplicate row: {dup['employee_id']} on {dup['day']}")
    if hris["partial"]:
        part = hris["partial"]
        hris_lines.append(
            f"- Partial file: {part['day']}, {part['kept']:,} rows kept, "
            f"{part['dropped']:,} dropped"
        )
    injected = [(s, k, f"{n:,}") for s, k, n in rows if n] or [("none", "", "")]
    return "\n\n".join(
        [
            "## Chaos injected",
            _table(("Source", "Kind", "Count"), injected, align="llr"),
            "Expected quarantine in silver (ADR-0014 §5):",
            _table(
                ("Source", "Reason", "Lines"),
                [(s, r, f"{n:,}") for s, r, n in quarantine] or [("none", "", "")],
                align="llr",
            ),
            "HRIS:",
            "\n".join(hris_lines),
        ]
    )


# ------------------------------------------------------------------ formatting


def _table(header: Sequence[str], rows: Iterable[Sequence[str]], align: str = "") -> str:
    marks = {"l": "---", "r": "--:"}
    align = align or "l" * len(header)
    lines = [
        "| " + " | ".join(header) + " |",
        "|" + "|".join(marks[a] for a in align) + "|",
        *("| " + " | ".join(row) + " |" for row in rows),
    ]
    return "\n".join(lines)


def _value(check: Check) -> str:
    if check.name == "ht4_monotonic" and check.value is not None:
        return "yes" if check.value else "no"
    return _number(check.value, whole=check.name in COUNTS)


def _band(check: Check) -> str:
    if check.name == "ht4_monotonic":
        return "declines"
    low = _number(check.low, whole=check.name in COUNTS)
    return f">= {low}" if check.high is None else f"{low} to {_number(check.high)}"


def _number(value: float | None, *, whole: bool = False) -> str:
    if value is None:
        return "n/a"
    if whole or abs(value) >= 1000:
        return f"{value:,.0f}"
    return f"{value:.1f}" if abs(value) >= 10 else f"{value:.3f}"


def _percent(value: float | None) -> str:
    return "n/a" if value is None else f"{value:+.1%}"


def _days(days: list[str]) -> str:
    return ", ".join(days) if days else "none"
