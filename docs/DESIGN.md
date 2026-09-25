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
| Requisitions | Monthly headcount plan for growth; go-live pipeline; Pareto popularity capped at 200 (max ≈ 51× mean) | [ADR-0006](decisions/0006-requisitions.md) |
| Job-board traffic | Sessions sized from expected views; external, internal (HT3) and bot sessions; UUIDv4 ids from the seed; counting sink until T1.8 | [ADR-0007](decisions/0007-job-board-traffic.md) |
| ATS engine | Outcome drawn at stage entry (HT2); offers only while a seat is free (HT4); hires join the workforce; no-starts give seats back; ATS goes live empty | [ADR-0008](decisions/0008-ats-engine-and-hires.md) |
| ATS source database | Strict vendor schema (PK/FK/CHECK); atomic COPY load; payload sha256 in the manifest; fail fast before simulating | [ADR-0009](decisions/0009-ats-source-database.md) |

## Alternatives considered (T6.2)
- Kinesis vs MSK vs self-managed Kafka
- Micro-batch vs Structured Streaming
- Redshift vs Athena + Iceberg
- CDK vs Terraform
- Custom DQ vs Deequ / Glue Data Quality
- Airflow vs Step Functions
- Snapshot-based SCD2 vs CDC

## Limitations and next steps
