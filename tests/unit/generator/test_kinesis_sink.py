"""KinesisSink against moto's Kinesis (SPEC §6.9, ADR-0013). Nothing here can reach AWS: moto
intercepts every call, and the fixture swaps in fake credentials and drops any profile.
"""

import random
from collections.abc import Callable, Iterator, Sequence
from typing import TYPE_CHECKING, Any, cast

import boto3
import pytest
from moto import mock_aws

from hirestream.generator.chaos import Delivery
from hirestream.generator.errors import StreamDeliveryError
from hirestream.generator.sinks import PUT_RECORDS_MAX_BYTES, KinesisSink

if TYPE_CHECKING:
    from mypy_boto3_kinesis import KinesisClient
    from mypy_boto3_kinesis.type_defs import (
        PutRecordsOutputTypeDef,
        PutRecordsRequestEntryTypeDef,
    )

STREAMS = {"scheduling-service": "interview-events", "jobboard-web": "jobboard-events"}


@pytest.fixture
def kinesis(monkeypatch: pytest.MonkeyPatch) -> Iterator["KinesisClient"]:
    for key in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_SESSION_TOKEN"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        client = boto3.client("kinesis", region_name="us-east-1")
        for name in STREAMS.values():
            client.create_stream(StreamName=name, ShardCount=1)
        yield client


def _read(client: "KinesisClient", stream: str) -> list[tuple[str, bytes]]:
    shard = client.list_shards(StreamName=stream)["Shards"][0]["ShardId"]
    iterator = client.get_shard_iterator(
        StreamName=stream, ShardId=shard, ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]
    records: list[tuple[str, bytes]] = []
    while True:
        response = client.get_records(ShardIterator=iterator, Limit=10_000)
        if not response["Records"]:
            return records
        records += [(r["PartitionKey"], bytes(r["Data"])) for r in response["Records"]]
        iterator = response["NextShardIterator"]


def _deliveries(n: int, source: str = "jobboard-web", size: int = 0) -> list[Delivery]:
    pad = "x" * size
    return [
        Delivery(i, source, f"s{i % 7}", f'{{"n":{i},"pad":"{pad}"}}'.encode()) for i in range(n)
    ]


class Flaky:
    """moto's client, except chosen records fail the way throttled ones do."""

    def __init__(self, client: "KinesisClient", fails: Callable[[int, bytes], bool]) -> None:
        self.client, self.fails = client, fails
        self.calls: list[list[bytes]] = []

    def put_records(
        self, *, Records: Sequence["PutRecordsRequestEntryTypeDef"], StreamName: str
    ) -> "PutRecordsOutputTypeDef":
        attempt = len(self.calls) + 1
        self.calls.append([bytes(cast(bytes, r["Data"])) for r in Records])
        decisions = [self.fails(attempt, bytes(cast(bytes, r["Data"]))) for r in Records]
        ok = [r for r, bad in zip(Records, decisions, strict=True) if not bad]
        passed = iter(
            self.client.put_records(Records=ok, StreamName=StreamName)["Records"] if ok else []
        )
        throttled = {
            "ErrorCode": "ProvisionedThroughputExceededException",
            "ErrorMessage": "Rate exceeded for shard",
        }
        results = [throttled if bad else next(passed) for bad in decisions]
        return cast(
            "PutRecordsOutputTypeDef",
            {"FailedRecordCount": sum(decisions), "Records": results},
        )


def _sink(client: Any, **kwargs: Any) -> tuple[KinesisSink, list[float]]:
    slept: list[float] = []
    sink = KinesisSink(client, STREAMS, sleep=slept.append, jitter=random.Random(7), **kwargs)
    return sink, slept


def test_records_land_in_their_source_stream(kinesis: "KinesisClient") -> None:
    sink, slept = _sink(kinesis)
    jobs, interviews = _deliveries(1_234), _deliveries(40, "scheduling-service")
    sink.write([*jobs, *interviews])
    sink.close()
    assert _read(kinesis, "jobboard-events") == [(d.partition_key, d.line) for d in jobs]
    assert _read(kinesis, "interview-events") == [(d.partition_key, d.line) for d in interviews]
    assert sink.records_sent == 1_274 and sink.records_retried == 0 and not slept


def test_batches_respect_the_record_and_byte_limits(kinesis: "KinesisClient") -> None:
    flaky = Flaky(kinesis, lambda attempt, data: False)
    sink, _ = _sink(flaky)
    sink.write(_deliveries(1_234))
    assert [len(c) for c in flaky.calls] == [500, 500, 234]
    flaky.calls.clear()
    sink.write(_deliveries(600, size=20_000))  # 12 MB: the 5 MiB cap splits it, not the count
    sizes = [sum(len(d) + 2 for d in call) for call in flaky.calls]  # + the 2-byte keys
    assert [len(c) for c in flaky.calls] == [261, 261, 78]
    assert all(size <= PUT_RECORDS_MAX_BYTES for size in sizes)
    capped = Flaky(kinesis, lambda attempt, data: False)
    small, _ = _sink(capped, batch_max=100)  # config's put_records_batch_max
    small.write(_deliveries(250))
    assert [len(c) for c in capped.calls] == [100, 100, 50]


def test_only_failed_records_are_retried_with_backoff(kinesis: "KinesisClient") -> None:
    rng = random.Random(3)
    unlucky = {d.line for d in _deliveries(1_000) if rng.random() < 0.3}
    flaky = Flaky(kinesis, lambda attempt, data: attempt == 1 and data in unlucky)
    sink, slept = _sink(flaky)
    batch = _deliveries(1_000)
    sink.write(batch[:500])
    sink.write(batch[500:])
    first, retry = flaky.calls[0], flaky.calls[1]
    assert set(retry) == {d for d in first if d in unlucky}  # nothing that succeeded is resent
    landed = _read(kinesis, "jobboard-events")
    assert sorted(data for _, data in landed) == sorted(d.line for d in batch)  # once each
    assert sink.records_retried == len(retry) and len(slept) == 1
    assert 0 <= slept[0] <= 0.1  # full jitter on the first backoff: uniform(0, base)


def test_it_gives_up_after_max_retries(kinesis: "KinesisClient") -> None:
    poison = _deliveries(3)[1].line
    flaky = Flaky(kinesis, lambda attempt, data: data == poison)
    sink, slept = _sink(flaky, max_retries=4, base_delay_s=0.5, max_delay_s=2.0)
    with pytest.raises(
        StreamDeliveryError, match="1 of 3 records to jobboard-events still failing"
    ):
        sink.write(_deliveries(3))
    assert len(flaky.calls) == 5 and [len(c) for c in flaky.calls[1:]] == [1, 1, 1, 1]
    caps = [0.5, 1.0, 2.0, 2.0]  # base x 2^attempt, capped at max_delay
    assert len(slept) == 4 and all(0 <= s <= cap for s, cap in zip(slept, caps, strict=True))
    assert sink.records_sent == 2


def test_bad_input_is_refused_before_sending(kinesis: "KinesisClient") -> None:
    flaky = Flaky(kinesis, lambda attempt, data: False)
    sink, _ = _sink(flaky)
    with pytest.raises(ValueError, match="no Kinesis stream configured for 'mystery'"):
        sink.write([Delivery(0, "mystery", "k", b"{}")])
    with pytest.raises(ValueError, match="over Kinesis's 1 MiB limit"):
        sink.write(_deliveries(1, size=1024 * 1024))
    with pytest.raises(ValueError, match=r"batch_max must be 1\.\.500"):
        KinesisSink(kinesis, STREAMS, batch_max=501)
    sink.write([])
    assert not flaky.calls
