# ADR-0013: Kinesis sink: batching, retries, and ordering

- **Status:** Accepted
- **Date:** 2026-10-07
- **Task:** T1.8 (T1.8b)
- **Deviates from:** nothing in the spec. It fills in what §6.9 leaves open.

## Context
SPEC §6.9 says the `KinesisSink`:
- sends `PutRecords` batches of at most 500 records and 5 MiB
- keys records by `interview_id` (scheduling) or `session_id` (job board), keeping each entity's
  order within a shard
- retries only the failed records, with exponential backoff and jitter
- is unit-tested with moto, including partial failures

Elsewhere:
- **§16:** the CDK stack creates `hirestream-interview-events` and `hirestream-jobboard-events`
  (provisioned, 1 shard, 24 h retention).
- **ADR-0003 G3:** real Kinesis and Firehose behaviour is first exercised in T4.6.
- **ADR-0012:** the queue hands sinks lines in arrival order, behind a `LineSink` interface.

The spec does not say:
- the per-record size limit, or what happens when a record breaks it
- the backoff parameters
- when the sink gives up
- what happens to ordering when only some records fail
- how tests can fail part of a batch, which moto doesn't do on its own
- where boto3 is needed

## Decision

### 1. Interface
- **The same `LineSink` as `FileSink`:** `write(deliveries)` and `close()`.
- **Inputs:** a client with `put_records`, and a source → stream-name map. Stream names come from
  the CDK outputs at runtime (Phase 4), never from config.
- **No runtime boto3 import in the generator's sink module:** whoever builds the client (live-tail
  in T3.4, the cloud commands in Phase 4) imports boto3, which is now a runtime dependency.

### 2. Batching
- **Order:** records go out in arrival order, per stream.
- **When a batch closes:** at `batch_max` records (`output.kinesis.put_records_batch_max`, at most
  500), or when the next record would take it past 5 MiB, counting data plus partition key the way
  AWS counts.
- **Oversized records:** a record over 1 MiB is refused with a `ValueError` before anything is sent.
  Lines are about 0.5 KB, so this only guards against bugs.

### 3. Retries
- **Only failed records:** only records whose result has an `ErrorCode` are sent again, in their
  original order. Retrying the whole batch would duplicate every record that succeeded.
- **Backoff ("full jitter"):** the delay before retry k (from 0) is uniform(0, min(5 s, 0.1 s ×
  2^k)), so producers that fail together don't retry together.
- **Giving up:** after `max_retries` (`output.kinesis.max_retries`, 5) the sink raises
  `StreamDeliveryError` with the failed count and error codes.
- **Request-level errors:** network failures and throttling of the call itself are left to
  botocore's own retry policy. A partial `PutRecords` failure is an HTTP 200, so botocore never
  retries it, which is why the sink does.

### 4. Ordering
Kinesis orders a shard's records by arrival, and the partition key puts an entity's records on one
shard. A record retried after a partial failure arrives after any later record of the same entity
that succeeded in that batch, so an entity's order can break exactly then. That's accepted:
- **Consumers don't depend on arrival order:** silver deduplicates on `event_id` and orders by
  `event_ts` (§9.1), the same disorder bronze's chaos already produces.
- **Strict order is costly:** one `PutRecord` per event with `SequenceNumberForOrdering`, or
  holding back later records of a failed entity, costs throughput or a good deal of state.

### 5. Tests
- **moto 5 (`mock_aws`):** provides the streams.
- **A wrapper fails chosen records** with `ProvisionedThroughputExceededException`, since moto can't
  fail part of a batch.
- **Nothing reaches AWS:** the fixture sets fake credentials and drops any `AWS_PROFILE`, so no test
  can reach a real account.
- **What they check:**
  - delivery to the right stream, in order
  - both batch limits, with a byte-limit case that can't pass on the count limit
  - that nothing successful is resent
  - backoff bounds and the give-up path
  - refused input

### 6. Dependencies
- **Runtime:** boto3 1.43.109.
- **Dev:** moto[kinesis] 5.2.3 and boto3-stubs[kinesis] 1.43.108, for strict mypy.

## Alternatives considered
- **Let botocore retry partial failures.** Impossible: the call succeeds, so botocore sees nothing to
  retry.
- **Retry the whole batch.** Rejected. It duplicates every record that succeeded.
- **Backoff without jitter.** Rejected. Producers that fail together retry together, and the shard
  throttles them again.
- **LocalStack.** Rejected in ADR-0003 as another moving part with partial emulation.

## Consequences
- **T3.4:** wires `generate live-tail --sink kinesis`. The client comes from the `hirestream`
  profile, stream names from CDK outputs, and `batch_max` and `max_retries` from config. Chaos still
  applies upstream (duplicates, malformed lines); arrival times become Kinesis's own.
- **T4.6:** sends real traffic through Kinesis and Firehose, and checks the parity gap G3.

## References
- `docs/SPEC.md` §6.9, §9.1, §16
- ADR-0003 (G3), ADR-0012 (the delivery queue and `LineSink`)
