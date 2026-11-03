"""A real tiny backfill's stream bronze, read with plain Python, for the silver reconciliation
tests (ADR-0015, ADR-0019): every line by (object key, line number), each object's arrival as
Spark reads it, and the ground truth."""

from __future__ import annotations

import gzip
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pyspark.sql import Row

from hirestream.generator.run import BackfillResult

EPOCH = datetime(1970, 1, 1)
Copy = tuple[datetime, str, int]  # (arrival, object key, line number): what dedupe orders by


@dataclass(frozen=True)
class TinyBronze:
    root: Path  # the lake
    keys: list[str]  # the folder's objects, sorted
    lines: dict[tuple[str, int], bytes]  # (key, line number) -> the raw line
    arrival: dict[str, datetime]  # key -> the object's mtime, to the millisecond
    truth: dict[str, Any]  # ground_truth.json


def tiny_bronze(backfill: BackfillResult, folder: str) -> TinyBronze:
    """Every object the backfill wrote under bronze/<folder>/."""
    root = backfill.manifest_path.parents[2]  # <lake>/_runs/<run_id>/manifest.json
    prefix = f"bronze/{folder}/"
    keys = sorted(f.path for f in backfill.simulation.stream_files if f.path.startswith(prefix))
    lines: dict[tuple[str, int], bytes] = {}
    arrival: dict[str, datetime] = {}
    for key in keys:
        path = root / key
        raw = gzip.decompress(path.read_bytes()).split(b"\n")
        assert raw[-1] == b""  # every line ends in a newline
        lines |= {(key, n): line for n, line in enumerate(raw[:-1], start=1)}
        arrival[key] = EPOCH + timedelta(milliseconds=path.stat().st_mtime_ns // 1_000_000)
    truth = json.loads(Path(backfill.ground_truth_path).read_text())
    return TinyBronze(root, keys, lines, arrival, truth)


def kept_copies(bronze: TinyBronze, rejected: Iterable[Row]) -> dict[str, list[Copy]]:
    """Every copy of every event among the lines silver didn't quarantine, by event_id."""
    dropped = {(q._source_file, q._source_line) for q in rejected}
    copies: dict[str, list[Copy]] = {}
    for (key, n), line in bronze.lines.items():
        if (key, n) not in dropped:
            event_id = json.loads(line)["event_id"]
            copies.setdefault(event_id, []).append((bronze.arrival[key], key, n))
    return copies


def value(body: dict[str, Any], path: str) -> Any:
    """A dotted field of a decoded event; None when any part is missing."""
    node: Any = body
    for part in path.split("."):
        node = node.get(part) if isinstance(node, dict) else None
    return node
