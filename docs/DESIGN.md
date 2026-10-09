# DESIGN — HireStream

Narrative design: what the system does, how it is built, and why each alternative lost.
Requirements live in `SPEC.md`; individual decisions in `decisions/`. Finalised in T6.2.

## Problem and scope
<!-- Halcyon's recruiting and mobility questions; what is out of scope (SPEC §0–§1). -->

## Architecture
<!-- Diagram (SPEC §2), data flow source → bronze → silver → gold → warehouse → dashboards. -->

## Local and cloud modes
Local vs cloud differences and how each is contained: [ADR-0003](decisions/0003-local-cloud-parity-gaps.md).

## Key decisions
| Decision | Choice | Record |
|---|---|---|
| How decisions are recorded | ADRs | [ADR-0001](decisions/0001-record-architecture-decisions.md) |
| Spark runtime | EMR Serverless `emr-spark-8.1.0`, Spark 4.1.1 | [ADR-0002](decisions/0002-emr-serverless-release-and-runtime-pins.md) |
| Initial workforce | Steady state of the configured dynamics; 5–9 span tree; L8 leaders, L6+ managers | [ADR-0004](decisions/0004-initial-workforce-and-org-structure.md) |
| Workforce dynamics and HRIS export | One hazard per employee per day; heir/skip-level succession; every HRIS field change dated; end-of-day gzip CSV snapshots with late exports | [ADR-0005](decisions/0005-workforce-dynamics-and-hris-export.md) |
| Requisitions | Monthly headcount plan for growth; go-live pipeline; Pareto popularity capped at 200 (α 2.0 since ADR-0016: max ≈ 100× mean) | [ADR-0006](decisions/0006-requisitions.md) |
| Job-board traffic | Sessions sized from expected views; external, internal (HT3) and bot sessions; UUIDv4 ids from the seed; counting sink until T1.8 | [ADR-0007](decisions/0007-job-board-traffic.md) |
| ATS engine | Outcome drawn at stage entry (HT2); offers only while a seat is free (HT4); hires join the workforce; no-starts give seats back; ATS goes live empty | [ADR-0008](decisions/0008-ats-engine-and-hires.md) |
| ATS source database | Strict vendor schema (PK/FK/CHECK); atomic COPY load; payload sha256 in the manifest; fail fast before simulating | [ADR-0009](decisions/0009-ats-source-database.md) |
| Scheduling engine | Event-driven interviews; a trained 10% interview; compounding over-cap penalty; slowness spread over popularity; HT1 labelled by final weekly load | [ADR-0010](decisions/0010-scheduling-engine.md) |
| Scheduling schema v2 and timezone bug | Versions by event time (1.2.4 → 1.3.0 → 1.3.1 → 2.0.0); panels count and give feedback per panelist; one format per loop; the bug has its own random stream, so it changes bytes, never reality | [ADR-0011](decisions/0011-scheduling-schema-v2-and-timezone-bug.md) |
| Stream chaos, delivery, and bronze files | Chaos per delivered copy with fixed draws from its own stream (never touches the simulation); queue flushed one day behind; Firehose-style hourly parts with deterministic names, mtime = latest arrival | [ADR-0012](decisions/0012-stream-chaos-delivery-and-files.md) |
| Kinesis sink | PutRecords within 500 records / 5 MiB; resend only failed records with full-jitter backoff, then fail loudly; per-entity order may break only on a partial failure (silver orders by event time) | [ADR-0013](decisions/0013-kinesis-sink.md) |
| Event contracts | 26 strict, self-contained JSON Schema 2020-12 files (closed objects, consts, enums, portable patterns); read with stdlib, validated in tests; the timezone-bug build must fail exactly where the bug is | [ADR-0014](decisions/0014-event-contracts.md) |
| Ground truth, calibration and the generation report | Truth from engine counters and the ATS's final state, never from bronze; UTC months; targets measured as the warehouse's metrics will be; ground truth hashed into the manifest, the report (runtime, memory) not; a miss warns | [ADR-0015](decisions/0015-ground-truth-and-generation-report.md) |
| Calibration tuning at dev | Five-seed sweep (`generate calibrate`); regular reqs get applications early and a funnel that needs fewer; evergreen ×7 so its hiring stays put; a faster pipeline with a realistic tail; HT4 at dev an explained miss (a power problem) | [ADR-0016](decisions/0016-calibration-tuning-at-dev.md) |
| Full-preset acceptance | Measured, not tuned: 22.9 M events, 18 of 18 targets, 1.3 GiB, 1,417 s by the process's clock (wall time counted VM stalls and sleep); contracts checked on the bytes in bronze; two full runs byte-identical | [ADR-0017](decisions/0017-full-preset-acceptance.md) |

## Alternatives considered (T6.2)
- Kinesis vs MSK vs self-managed Kafka
- Micro-batch vs Structured Streaming
- Redshift vs Athena + Iceberg
- CDK vs Terraform
- Custom DQ vs Deequ / Glue Data Quality
- Airflow vs Step Functions
- Snapshot-based SCD2 vs CDC

## Limitations and next steps
