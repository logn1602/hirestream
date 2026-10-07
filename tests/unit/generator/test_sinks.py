import gzip
import hashlib
import json
import os
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from hirestream.generator.chaos import HOUR_MS, Delivery
from hirestream.generator.config import load_config
from hirestream.generator.run import BackfillResult, run_backfill
from hirestream.generator.sinks import FileSink

WriteConfig = Callable[[dict[str, Any]], Path]
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


@pytest.fixture(scope="module")
def backfill(tmp_path_factory: pytest.TempPathFactory, base_config_path: Path) -> BackfillResult:
    lake = tmp_path_factory.mktemp("lake")
    return run_backfill(load_config(base_config_path, "tiny"), lake, run_id="t")


def _streams(result: BackfillResult) -> list[Any]:
    return [f for f in result.manifest.files if not f.path.startswith("bronze/hris/")]


def test_a_backfill_lands_every_line_in_bronze(backfill: BackfillResult) -> None:
    lake = backfill.manifest_path.parents[2]
    files = _streams(backfill)
    on_disk = {p.relative_to(lake).as_posix() for p in (lake / "bronze").rglob("*.jsonl.gz")}
    assert on_disk == {f.path for f in files}
    chaos = backfill.simulation.chaos_summary
    assert sum(f.records or 0 for f in files) == chaos["lines"]
    sources: Counter[str] = Counter()
    for entry in files[::25]:  # a sample: every line was sent before its file's hour ended
        path = lake / entry.path
        hour_end = (
            datetime.fromtimestamp(path.stat().st_mtime, UTC)
            .replace(minute=0, second=0, microsecond=0)
            .timestamp()
            * 1000
            + HOUR_MS
        )
        for line in _lines(path):
            try:
                body = json.loads(line)
            except json.JSONDecodeError:
                continue
            sent = datetime.fromisoformat(body["sent_ts"].replace("Z", "+00:00")).timestamp()
            assert sent * 1000 < hour_end
            sources[body.get("source", "?")] += 1
    assert set(sources) >= {"jobboard-web", "scheduling-service"}


def test_chaos_never_touches_the_simulation(
    backfill: BackfillResult,
    tmp_path: Path,
    raw_config: dict[str, Any],
    write_config: WriteConfig,
) -> None:
    raw_config["chaos"]["streams"].update(duplicate_rate=0.5, malformed_rate=0.05)
    raw_config["chaos"]["streams"]["delivery_lag"] = [{"share": 1.0, "seconds": [0, 60]}]
    other = run_backfill(load_config(write_config(raw_config), "tiny"), tmp_path, run_id="o")
    hris = [f for f in backfill.manifest.files if f.path.startswith("bronze/hris/")]
    assert hris == [f for f in other.manifest.files if f.path.startswith("bronze/hris/")]
    a, b = backfill.simulation, other.simulation
    assert a.ats_snapshot == b.ats_snapshot and a.events == b.events
    assert a.scheduling_summary == b.scheduling_summary
    assert a.jobboard_summary == b.jobboard_summary
    assert b.chaos_summary["duplicates"] > 10 * a.chaos_summary["duplicates"]
