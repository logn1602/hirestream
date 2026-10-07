import gzip
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from hirestream.generator.chaos import HOUR_MS, Delivery
from hirestream.generator.sinks import FileSink

T0 = int(datetime(2025, 2, 3, 9, tzinfo=UTC).timestamp() * 1000)  # 09:00 UTC


def _d(arrival: int, n: int, source: str = "jobboard-web") -> Delivery:
    return Delivery(arrival, source, f"s{n}", json.dumps({"n": n}).encode())


def _lines(path: Path) -> list[bytes]:
    return gzip.decompress(path.read_bytes()).splitlines()


def test_files_follow_arrival_hours_and_roll(tmp_path: Path) -> None:
    sink = FileSink(tmp_path, max_events=3, seed=1602)
    sink.write([_d(T0 + i * 60_000, i) for i in range(5)] + [_d(T0 + HOUR_MS, 5)])
    paths = [Path(f.path) for f in sink.files]
    assert [p.parent.as_posix() for p in paths] == [
        "bronze/jobboard/yyyy=2025/mm=02/dd=03/hh=09",
        "bronze/jobboard/yyyy=2025/mm=02/dd=03/hh=09",
        "bronze/jobboard/yyyy=2025/mm=02/dd=03/hh=10",
    ]
    assert [p.name[:10] for p in paths] == ["part-00000", "part-00001", "part-00000"]
    assert [f.records for f in sink.files] == [3, 2, 1]
    assert _lines(tmp_path / paths[0]) == [b'{"n": 0}', b'{"n": 1}', b'{"n": 2}']
    for entry in sink.files:
        data = (tmp_path / entry.path).read_bytes()
        assert entry.sha256 == hashlib.sha256(data).hexdigest() and entry.bytes == len(data)


def test_a_late_line_gets_a_new_part_in_its_hour(tmp_path: Path) -> None:
    sink = FileSink(tmp_path, max_events=100, seed=1602)
    sink.write([_d(T0, 0)])
    sink.write([_d(T0 + 5, 1)])  # the same hour, after it was written: like a late object
    assert [Path(f.path).name[:10] for f in sink.files] == ["part-00000", "part-00001"]


def test_names_bytes_and_mtimes_are_deterministic(tmp_path: Path) -> None:
    batch = [_d(T0 + i, i, "scheduling-service") for i in range(10)]
    a, b, c = (FileSink(tmp_path / x, 100, seed) for x, seed in (("a", 1), ("b", 1), ("c", 2)))
    for sink in (a, b, c):
        sink.write(batch)
    assert a.files == b.files  # same paths, hashes, sizes
    assert a.files[0].path != c.files[0].path  # the name carries the seed
    path = tmp_path / "a" / a.files[0].path
    assert path.parent.as_posix().endswith("bronze/scheduling/yyyy=2025/mm=02/dd=03/hh=09")
    assert os.stat(path).st_mtime_ns == (T0 + 9) * 1_000_000  # the latest arrival
