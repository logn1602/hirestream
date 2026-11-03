"""Hand-made bronze lines for a silver transform's unit tests, run as one batch.

Each case's lines sit under their own file prefix, `bronze/<folder>/<case>/...`, so a single
`build` over every case serves a whole test module, and each test reads its case back by name. One
Spark run instead of one per test keeps a module fast.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, Row, SparkSession

from hirestream.transforms.common.lake import LINE_SCHEMA

ARRIVAL = datetime(2030, 1, 1)  # after every example's event_ts
Body = str | dict[str, Any]
Line = Body | tuple[Body, str, int, datetime]  # a body, or (body, file, line number, arrival)
Result = tuple[list[Row], list[Row]]  # (silver rows, quarantine rows)
Build = Callable[[DataFrame], tuple[DataFrame, DataFrame]]


class Cases(dict[str, list[Line]]):
    """Named cases, each a list of lines, for one bronze folder."""

    def __init__(self, folder: str) -> None:
        super().__init__()
        self.folder = folder

    def add(self, name: str, *lines: Line) -> str:
        assert name not in self and "/" not in name
        self[name] = list(lines)
        return name

    def run(self, spark: SparkSession, build: Build) -> dict[str, Result]:
        """Every case's lines through one `build`; the results split back by case."""
        data = []
        for name, lines in self.items():
            for n, line in enumerate(lines, start=1):
                body, file, number, arrived = (
                    line if isinstance(line, tuple) else (line, "part-0.jsonl.gz", n, ARRIVAL)
                )
                key = f"bronze/{self.folder}/{name}/{file}"
                data.append((json_text(body), key, number, arrived))
        rows, quarantined = build(spark.createDataFrame(data, LINE_SCHEMA))
        out: dict[str, Result] = {name: ([], []) for name in self}
        for row in rows.collect():
            out[row._source_file.split("/")[2]][0].append(row)
        for row in quarantined.collect():
            out[row._source_file.split("/")[2]][1].append(row)
        return out


def json_text(line: Line) -> str:
    body = line[0] if isinstance(line, tuple) else line
    return body if isinstance(body, str) else json.dumps(body)


def rows_of(results: dict[str, Result], name: str) -> dict[str, Row]:
    """A case's silver rows by event_id; the case must have quarantined nothing."""
    rows, quarantined = results[name]
    assert quarantined == [], quarantined
    return {row.event_id: row for row in rows}


def rejected(results: dict[str, Result], name: str) -> Row:
    """A case's one quarantined line; the case must have kept nothing."""
    rows, quarantined = results[name]
    assert rows == [] and len(quarantined) == 1, (rows, quarantined)
    return quarantined[0]
