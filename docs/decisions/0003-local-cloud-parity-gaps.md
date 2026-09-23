# ADR-0003: Local/cloud parity gaps

- **Status:** Accepted
- **Date:** 2026-09-23
- **Task:** T0.4

## Context
HireStream runs in two modes (§2.1). `local` is free and runs on every PR. `cloud` is billable and
runs only in bounded demo sessions (T4.6, T5.3). The promise is "jobs are identical in both modes;
only IO endpoints and the Spark launcher differ." The promise is only honest if every place where the
modes *do* differ is written down, together with how we contain the risk. Otherwise a bug that only
appears in the cloud is found in the one session we pay for.

## Decision
Keep one code path. The only switches are `HIRESTREAM_ENV` and `config/pipeline/{local,cloud}.yaml`.
Accept the gaps below. Each gap is **contained** (by a design rule) and **covered** (by a test or check).

| # | Concern | Local | Cloud | Risk | Containment and coverage |
|---|---|---|---|---|---|
| G1 | Spark build | Upstream Apache Spark 4.1.1 (PyPI) | `4.1.1-amzn-0` (Amazon patches, EMR runtime optimisations) | Different plans or performance; rare behavioural differences | Same upstream version and ANSI default (ADR-0002, `test_versions.py`). Correctness is judged on outputs (DQ, ground-truth checks), never on plans. Performance claims come from the environment where they were measured (T5.4) |
| G2 | Filesystem | Local POSIX dirs under `./data/lake` | S3 through S3A (EMR 8.x removed EMRFS) | S3 has no atomic rename; listings are consistent but slow; partial writes are visible to `ls` | All IO goes through the lake IO layer (T2.1). Writes use partition overwrite (dynamic) or write to a staging prefix and then publish. Nothing relies on rename-as-commit. Silver discovers inputs from the bronze manifest, not from "the hour directory is complete" |
| G3 | Stream transport | `FileSink` writes Firehose-style gzip JSONL straight into `bronze/` | Kinesis Data Streams → Firehose → S3 | Firehose adds buffering delay, late objects in past hour prefixes, error-prefix objects, and at-least-once duplicates. Local writes are prompt and exact | `FileSink` copies the Firehose key layout and file format. Chaos (§6.8) injects duplicates and late arrivals locally. Silver dedupes and uses the manifest's trailing window (§8). `_firehose_errors/` exists only in the cloud and is counted by a DQ check. `KinesisSink` is tested with moto, including partial `PutRecords` failures. Real Firehose behaviour is first exercised in T4.6 |
| G4 | ATS source location | Container `ats-db` → extract → local bronze | The same local container → extract → S3 bronze | None on the extract path; only the upload differs | Intentional hybrid ("on-prem source → cloud lake"). Same extractor, different lake root |
| G5 | Warehouse engine | Postgres 16 (`warehouse-db`) | Redshift Serverless via the Data API | SQL dialect (MERGE semantics, `COPY` sources, types, `ANALYZE`), no FK enforcement on Redshift, different planner | Separate DDL per engine (`sql/ddl/{postgres,redshift}/`). Metrics SQL is portable and tested on Postgres in CI. Load verification (row count + column checksum, X-04) runs in both modes. Redshift-only SQL runs only in bounded cloud sessions (T4.4, T5.3) |
| G6 | Load path | pyarrow → psycopg `COPY FROM STDIN` | Redshift `COPY FROM 's3://…' FORMAT AS PARQUET` | Type coercion differs (timestamps, decimals) | Gold types are chosen to map cleanly to both. The X-04 checksum catches coercion drift |
| G7 | Alerts | Console (+ optional Slack webhook) | SNS email | Alert delivery itself is untested locally | Alert sinks sit behind one interface (T2.11). Local tests assert the payload. SNS delivery is checked once in T4.6 |
| G8 | Dashboards | Metabase on `warehouse-db` | Metabase stays local; Redshift is shown through Query Editor v2 screenshots | Dashboards never read Redshift | Accepted. Dashboards prove the metrics layer, and Redshift proves tuning (T5.3). Saves running a public Redshift endpoint |
| G9 | Compute capacity | `local[*]` on a laptop / CI runner | EMR Serverless, default account quota **16 concurrent vCPUs** in a new account | The spec's planned cap (32 vCPU / 128 GB, §16) exceeds the default quota, so jobs above it queue or fail | T4.2 sets the application's maximum capacity at or below the account quota. It does not request a quota increase unless Shubh approves |
| G10 | Retention | Local bronze kept until deleted | S3 lifecycle expires `bronze/` and `quarantine/` after 30 days | A cloud backfill older than 30 days can't be replayed from bronze | Cloud sessions regenerate from the seeded generator instead of relying on retained bronze |
| G11 | Identity and secrets | `.env` credentials for local containers | IAM roles (EMR execution role, Redshift COPY role), SSO profile, GitHub OIDC | IAM permission bugs are invisible locally | `cdk synth` in CI (T4.1). Least-privilege roles are reviewed in T4.1 and exercised in T4.6 |
| G12 | Orchestration target | Airflow tasks run the CLI with local Spark | The same Airflow; tasks call `hirestream cloud …` and poll EMR / the Data API | Async job polling, timeouts, and retries only exist in the cloud | `hirestream cloud` wraps polling with timeouts. moto/stubbed unit tests cover state transitions (T4.3, T4.4) |

## Alternatives considered
- **Run everything in the cloud.** Rejected. It costs money on every PR and slows the feedback loop
  from seconds to minutes. The guardrails in `CLAUDE.md` require bounded, approved cloud sessions.
- **LocalStack for Kinesis, Firehose, S3, and Redshift.** Rejected. It is another moving part whose
  Firehose and Redshift emulation is partial, so it would add gaps of its own. moto covers the
  client-side contracts we own (`PutRecords` batching and retries). Real service behaviour is
  checked in the cloud demo.
- **Use Redshift-compatible SQL locally (e.g. an emulator).** Rejected. There is no faithful local
  Redshift. Keeping portable SQL plus per-engine DDL makes the dialect difference explicit and
  testable instead of hidden.

## Consequences
- Some behaviour is first seen in T4.6: real Firehose delivery timing, S3A commit performance, SNS,
  and IAM. T4.6 checks each gap G1–G12 (added to its PROGRESS entry), and any surprise gets an entry in `docs/NOTES.md`.
- New gaps found later are added by a new ADR that supersedes this one, keeping the table complete.
- `docs/DESIGN.md` links this table instead of restating it.
