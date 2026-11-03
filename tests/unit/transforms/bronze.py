"""Hand-made scheduling bronze for silver tests: contract examples as events, written as
Firehose-style gzip objects with a chosen mtime (the object's arrival, ADR-0012)."""

import copy
import gzip
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from hirestream.contracts import load_contract
from hirestream.transforms.common.lake import Lake

SOURCE = "scheduling-service"
DROP = object()  # a change that removes the field


def example(kind: str, version: int = 2, index: int = 0, /, **changes: Any) -> dict[str, Any]:
    """A contract example with a fresh event_id and changes; `payload__reason` reaches into the
    payload, and DROP removes a field."""
    body: dict[str, Any] = copy.deepcopy(load_contract(SOURCE, kind, version)["examples"][index])
    body["event_id"] = str(uuid.uuid4())
    for dotted, value in changes.items():
        *parents, name = dotted.split("__")
        node = body
        for parent in parents:
            node = node[parent]
        if value is DROP:
            del node[name]
        else:
            node[name] = value
    return body


def iso_ms(at: datetime) -> str:
    """ISO-8601 UTC with milliseconds and a Z, as the contracts spell event_ts."""
    return at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{at.microsecond // 1000:03d}Z"


def event_at(at: datetime, kind: str = "interview_completed", **changes: Any) -> dict[str, Any]:
    """An event that happened (and was sent) at `at`."""
    return example(kind, 2, 0, event_ts=iso_ms(at), sent_ts=iso_ms(at), **changes)


def put_object(
    lake: Lake,
    hour: datetime,
    lines: list[dict[str, Any] | str],
    *,
    part: int = 0,
    arrived: datetime | None = None,
    source: str = "scheduling",
) -> str:
    """Write a bronze object into `hour`'s prefix; its mtime (arrival) defaults to the end of
    the hour. Returns its key."""
    key = (
        f"bronze/{source}/yyyy={hour:%Y}/mm={hour:%m}/dd={hour:%d}/hh={hour:%H}"
        f"/part-{part:05d}.jsonl.gz"
    )
    path = Path(lake.paths.uri(key).removeprefix("file:"))
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "".join((line if isinstance(line, str) else json.dumps(line)) + "\n" for line in lines)
    path.write_bytes(gzip.compress(text.encode(), mtime=0))
    at = arrived or hour + timedelta(minutes=59)
    ns = int(at.timestamp() * 1_000_000_000)
    os.utime(path, ns=(ns, ns))
    return key
