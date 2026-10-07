"""Stream sinks: Firehose-style files in bronze (SPEC §6.9, §8, ADR-0012).

`FileSink` buckets lines by arrival hour (UTC), the way Firehose buckets by arrival:
`bronze/<source>/yyyy=YYYY/mm=MM/dd=DD/hh=HH/part-<n>-<uuid>.jsonl.gz`, rolling every
`stream_file_max_events` lines. A line that turns up for an hour already written goes into a new
part in that hour's folder, like a late Firehose object. Names come from a hash of the seed and the
part, and gzip carries no timestamp, so the same seed writes the same bytes to the same paths. Each
file's mtime is set to its latest arrival: deterministic, and a per-object arrival time for silver,
like an S3 object's `LastModified`.
"""

from __future__ import annotations

import gzip
import hashlib
import os
import uuid
from collections import Counter
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

from hirestream.generator.chaos import HOUR_MS, Delivery
from hirestream.generator.manifest import FileEntry

BRONZE = Path("bronze")
SOURCE_DIRS = {"scheduling-service": "scheduling", "jobboard-web": "jobboard"}
STREAM_PREFIXES: tuple[Path, ...] = tuple(BRONZE / name for name in SOURCE_DIRS.values())
GZIP_LEVEL = 1  # 2.5x faster than level 6 for files ~20% larger; streams are most bytes (ADR-0012)


class FileSink:
    def __init__(self, lake_root: Path, max_events: int, seed: int) -> None:
        self.files: list[FileEntry] = []
        self._root = lake_root
        self._max = max_events
        self._seed = seed
        self._parts: Counter[tuple[str, int]] = Counter()  # (source, hour) -> parts written

    def write(self, deliveries: Sequence[Delivery]) -> None:
        """Write lines, already in arrival order, into their arrival hours' folders."""
        groups: dict[tuple[str, int], list[Delivery]] = {}
        for d in deliveries:
            groups.setdefault((d.source, d.arrival_ms // HOUR_MS), []).append(d)
        for (source, hour), items in groups.items():
            for start in range(0, len(items), self._max):
                self._write_part(source, hour, items[start : start + self._max])

    def close(self) -> None:
        pass  # every part is complete when written

    def _write_part(self, source: str, hour: int, items: Sequence[Delivery]) -> None:
        n = self._parts[(source, hour)]
        self._parts[(source, hour)] += 1
        at = datetime.fromtimestamp(hour * 3600, UTC)
        name = f"part-{n:05d}-{self._uuid(source, hour, n)}.jsonl.gz"
        relative = (
            BRONZE / SOURCE_DIRS[source]
            / f"yyyy={at:%Y}" / f"mm={at:%m}" / f"dd={at:%d}" / f"hh={at:%H}" / name
        )  # fmt: skip
        data = gzip.compress(
            b"\n".join(d.line for d in items) + b"\n", compresslevel=GZIP_LEVEL, mtime=0
        )
        path = self._root / relative
        path.parent.mkdir(parents=True, exist_ok=True)  # each part is almost always a new hour
        _write_atomic(path, data)
        last_ns = items[-1].arrival_ms * 1_000_000  # lines arrive in order
        os.utime(path, ns=(last_ns, last_ns))
        self.files.append(
            FileEntry(
                path=relative.as_posix(),
                sha256=hashlib.sha256(data).hexdigest(),
                bytes=len(data),
                records=len(items),
            )
        )

    def _uuid(self, source: str, hour: int, n: int) -> str:
        """A UUIDv4-shaped name derived from the seed: no random stream is consumed."""
        digest = hashlib.sha256(f"{self._seed}:{source}:{hour}:{n}".encode()).digest()
        return str(uuid.UUID(bytes=digest[:16], version=4))


def _write_atomic(path: Path, data: bytes) -> None:
    """Write beside the target, then rename: a reader never sees half a part. The temp name is
    fixed (the part name is unique), which spares `mkstemp`'s retries on a slow disk.
    """
    tmp = path.with_name(f".{path.name}.tmp")
    try:
        tmp.write_bytes(data)
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
