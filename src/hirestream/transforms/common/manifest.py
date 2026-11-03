"""The bronze manifest: which objects silver has processed, and which are new (SPEC §8, ADR-0019).

Each run commits one record, `state/bronze_manifest/<source>/<run_id>.json`, after its outputs are
written. An object is new until a record lists it. Firehose can deliver an object into an hour
prefix after that hour has passed, so incremental runs look at a trailing window of prefixes and
skip what a record already lists, instead of trusting a prefix to be complete.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Final, Literal

from hirestream.transforms.common.lake import FileInfo, Lake

Mode = Literal["incremental", "bulk"]
MODES: Final = ("incremental", "bulk")
_HOUR_KEY = re.compile(
    r"^bronze/(?P<source>[^/]+)/yyyy=(?P<y>[0-9]{4})/mm=(?P<m>[0-9]{2})/dd=(?P<d>[0-9]{2})"
    r"/hh=(?P<h>[0-9]{2})/[^/]+$"
)


class BronzeChangedError(RuntimeError):
    """An object silver already processed has a different size or mtime now. Bronze is
    append-only, so something rewrote it (usually `make generate`): rebuild with --mode bulk."""


@dataclass(frozen=True)
class RunRecord:
    """What a run committed: the objects it processed, its counts, and the partitions it touched."""

    run_id: str
    source: str  # the bronze folder, e.g. "scheduling"
    mode: Mode
    as_of: datetime
    objects: tuple[FileInfo, ...]
    counts: Mapping[str, Any] = field(default_factory=dict)
    touched: tuple[date, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source": self.source,
            "mode": self.mode,
            "as_of": self.as_of.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "objects": [
                {"key": f.key, "size": f.size, "mtime_ms": f.mtime_ms} for f in self.objects
            ],
            "counts": dict(self.counts),
            "touched": [d.isoformat() for d in self.touched],
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> RunRecord:
        return cls(
            run_id=data["run_id"],
            source=data["source"],
            mode=data["mode"],
            as_of=datetime.fromisoformat(data["as_of"].replace("Z", "+00:00")),
            objects=tuple(FileInfo(o["key"], o["size"], o["mtime_ms"]) for o in data["objects"]),
            counts=data["counts"],
            touched=tuple(date.fromisoformat(d) for d in data["touched"]),
        )


def record_key(lake: Lake, source: str, run_id: str) -> str:
    return lake.paths.state("bronze_manifest", source, f"{run_id}.json")


def records(lake: Lake, source: str) -> list[RunRecord]:
    """Every committed record of `source`, read in one Spark job rather than one call per file."""
    files = lake.list_files(lake.paths.state("bronze_manifest", source))
    if not files:
        return []
    # wholetext as text()'s own argument: text() passes its default (False) over any .option().
    texts = lake.spark.read.text([lake.paths.uri(f.key) for f in files], wholetext=True)
    found = [RunRecord.from_json(json.loads(row.value)) for row in texts.collect()]
    return sorted(found, key=lambda record: record.run_id)


def processed(lake: Lake, source: str) -> dict[str, FileInfo]:
    """Every object a committed record lists, by key."""
    return {f.key: f for record in records(lake, source) for f in record.objects}


def window_hours(as_of: datetime, hours: int) -> list[datetime]:
    """The hour prefixes an incremental run lists: every whole hour h with
    as_of - hours <= h < as_of."""
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError(f"as_of must be timezone-aware, got {as_of!r}")
    if hours < 1:
        raise ValueError(f"the listing window must be at least one hour, got {hours}")
    end = as_of.astimezone(UTC)
    start = end - timedelta(hours=hours)
    first = start.replace(minute=0, second=0, microsecond=0)
    if first < start:
        first += timedelta(hours=1)
    return [
        first + timedelta(hours=n) for n in range(hours + 1) if first + timedelta(hours=n) < end
    ]


def hour_of(key: str, source: str) -> datetime:
    """The UTC hour of a bronze object's prefix; a key outside the layout is an error."""
    match = _HOUR_KEY.match(key)
    if match is None or match["source"] != source:
        raise ValueError(
            f"unexpected bronze key {key!r}: want bronze/{source}/yyyy=/mm=/dd=/hh=/<object>"
        )
    return datetime(int(match["y"]), int(match["m"]), int(match["d"]), int(match["h"]), tzinfo=UTC)


def discover(
    lake: Lake, source: str, *, mode: Mode, as_of: datetime, window: int
) -> list[FileInfo]:
    """The objects a run should process, sorted by key.

    Bulk lists all of the source's bronze and ignores the manifest, because a bulk run rebuilds
    everything. Incremental lists the day folders around the trailing `window` hours, keeps the
    objects whose hour prefix falls inside it, and drops those a record already lists.
    """
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}, got {mode!r}")
    if mode == "bulk":
        listed = lake.list_files(lake.paths.bronze(source))
        for f in listed:
            hour_of(f.key, source)
        return listed
    hours = set(window_hours(as_of, window))
    days = sorted({f"{lake.paths.bronze(source)}/{h:yyyy=%Y/mm=%m/dd=%d}" for h in hours})
    listed = [f for f in lake.list_files(days) if hour_of(f.key, source) in hours]
    seen = processed(lake, source)
    changed = [f.key for f in listed if f.key in seen and seen[f.key] != f]
    if changed:
        raise BronzeChangedError(
            f"{len(changed)} processed bronze object(s) changed since, e.g. {changed[0]!r}: "
            "bronze is append-only; rebuild with --mode bulk"
        )
    return [f for f in listed if f.key not in seen]
