# ADR-0018: Spark foundation: one session, lake IO through Hadoop, run-scoped lineage

- **Status:** Accepted
- **Date:** 2026-11-02
- **Task:** T2.1
- **Deviates from:** nothing in the spec. It fills in what §5, §8 and §9.1 leave open, and it
  settles how `_ingested_at` (§9.1) coexists with "re-running a job produces identical outputs"
  (CLAUDE.md).

## Context
Every silver and gold job (T2.2–T2.10) needs a SparkSession, paths, a way to list bronze, small
state files (§8's bronze manifest, §9.1's touched partitions), partition-overwrite writes, and
lineage columns. If each job did these its own way, ADR-0003's promise ("jobs are identical in both
modes; only IO endpoints and the Spark launcher differ") would depend on review. G2 also promises
that all IO goes through one layer that never relies on rename-as-commit.

Probing pyspark 4.1.1 on this machine (WSL, `America/New_York`) before writing anything found
three traps:
1. **Collected timestamps follow the Python process's zone.** With `spark.sql.session.timeZone=UTC`,
   `timestamp'2026-01-01 00:00:00'` collects as `2025-12-31 19:00` locally but `00:00` on CI and
   EMR, which run in UTC. Python UDF workers convert the same way.
2. **Workers start `python3` from PATH,** not the driver's interpreter. Run outside `uv run`, local
   Spark started the system Python 3.12 against a 3.11 driver and failed with
   `PYTHON_VERSION_MISMATCH`. PySpark reads only the `PYSPARK_PYTHON` environment variable for
   this, not a Spark conf.
3. **`_metadata.file_path` is URL-encoded** (`my%20lake`) while Hadoop listings are not
   (`my lake`). A manifest built from listings and lineage taken from `_metadata` would disagree
   for any root that needs encoding.

Also checked: Spark 4.1.1 still writes Parquet timestamps as INT96 by default. A local Hadoop
`rename` onto an existing file returns false rather than replacing it. `getOrCreate()` on a live
session applies only modifiable settings.

## Decision

### 1. One session factory (`transforms/common/session.py`)
- **Jobs never build a session.** They take one as an argument. `build_spark_session(settings)`
  starts it, or joins the running one on EMR, and then verifies it.
- **Pipeline settings, identical in both modes:** `PIPELINE_CONFS`, applied in both modes and
  verified at startup. A drifted session raises `SparkSessionError` before any job runs. They cannot
  be overridden from config.

  | Setting | Value | Why |
  |---|---|---|
  | `spark.sql.session.timeZone` | `UTC` | All timestamps are UTC after bronze |
  | `spark.sql.ansi.enabled` | `true` | Spark 4's default (ADR-0002), now explicit |
  | `spark.sql.sources.partitionOverwriteMode` | `dynamic` | §9.1 partition rewrites |
  | `spark.sql.parquet.compression.codec` | `snappy` | §9.1 |
  | `spark.sql.parquet.outputTimestampType` | `TIMESTAMP_MICROS` | INT96 is deprecated, needs calendar rebasing, and reads as nanoseconds in pyarrow. Micros is Spark's own precision and maps cleanly to Postgres and Redshift (G6) |

- **Launch settings:** `LAUNCH_CONFS` (`-Duser.timezone=UTC` for driver and executors,
  `spark.executorEnv.TZ=UTC` for Python workers) only work before a JVM starts. They go on the
  builder locally and on `spark-submit` for EMR (T4.3). Verification checks the driver JVM's
  default zone, so a submit that forgets them fails at startup.
- **What differs by mode:** only sizing. Shuffle partitions come from config. `master`, driver
  memory and the UI apply locally only.
- **Process settings:** the factory pins the Python process to UTC (`TZ` + `time.tzset()`). Locally
  it also defaults `PYSPARK_PYTHON` to `sys.executable`. The shared test fixture is built by the
  factory, and the root `conftest.py` pins the whole test run to UTC, so a laptop runs the tests in
  the same zone as CI.

### 2. Lake IO through Hadoop's FileSystem API (`transforms/common/lake.py`)
- **Paths:** `LakePaths` is pure path arithmetic under one root URI: `file:/abs/path` locally,
  `s3a://bucket[/prefix]` in the cloud.
- **Keys, not URIs:** a key is a path relative to the root. `_source_file` and state files record
  keys, so outputs don't depend on where the lake lives, and local and cloud outputs can be
  compared row for row.
- **Filesystem calls:** `Lake` makes listings, existence checks and small state files through
  Hadoop's FileSystem in the Spark JVM. That is the client Spark itself uses for `file:` and S3A,
  and it needs nothing extra on EMR.
  - Since T2.2b (ADR-0019), listings go through Spark's `binaryFile` source, which uses the same
    client inside one job. A py4j call per file had made listing 1,378 files take 11–78 s.
- **Listings:** they hide what Spark's file index hides: names below the prefix that start with `.`,
  or with `_` and contain no `=`. Temp files, `_SUCCESS`, checksum files, `_runs/` and
  `_firehose_errors/` are never inputs unless asked for by name.
- **State files are write-once,** like bronze. Each is written to a hidden temp file and renamed
  into place. Rename never replaces a file, so a second writer gets `FileExistsError` and the first
  file survives. This relies only on the atomicity of single-file writes, never on directory
  renames, which is consistent with G2. Per-run state uses per-run files
  (`state/touched_partitions/<run_id>.json`).
- **Reading lines:** `read_lines(keys)` returns raw lines (gzip by extension) with `_source_file`
  (the key) and `_arrival_ts` (the object's mtime: file mtime locally, `LastModified` on S3,
  ADR-0012).
  - The URL-encoded `_metadata.file_path` is decoded, with `+` kept literal as it is in a URI path.
  - A file outside the root raises rather than producing a wrong key.
- **Writing partitions:** `write_partitioned` sets `partitionOverwriteMode=dynamic` on the write
  itself, not only on the session. It refuses an empty `partition_by`, which would replace the whole
  table. It writes one file per partition, sorted, so a re-run rewrites the same layout.
- **Reading tables:** `read_parquet(key, schema)` returns an empty frame when nothing has been
  written yet, so a job's first run takes the same code path as every later run.

### 3. Run-scoped lineage (`transforms/common/lineage.py`)
- **Two kinds of lineage:**
  - `_source_file` and `_arrival_ts` describe the input, so they are a function of it.
  - `_run_id` and `_ingested_at` name the run. The caller passes both in. `ingested_at` is the
    run's start time, chosen once, and must be timezone-aware. A transform never calls
    `current_timestamp()`.
- **Idempotency:** it means identical outputs apart from the run-scoped columns.
  `content_fingerprint(df)` hashes the schema and the rows as a multiset, ignoring order and
  partitioning, and excludes `_run_id` and `_ingested_at` by default. T2.2 onward and T3.2's rerun
  test compare with it.

### 4. Quarantine basics and the import guard
- **Quarantine:** `ReasonCode` and `QUARANTINE_SCHEMA` hold §9.1's codes and columns. A test keeps
  `ReasonCode` identical to the generator's ground-truth list. Partitioning and reason precedence
  arrive with T2.2.
- **Import guard:** `tests/unit/test_spark_imports.py` enforces CLAUDE.md's rule that Spark code
  imports only the stdlib, pyspark and `requirements-spark.txt` (created empty). It follows
  first-party imports transitively, so `hirestream.contracts` and package `__init__` files are
  checked too.

## Alternatives considered
- **boto3 (or fsspec) for state files and listings.** Rejected. It adds a dependency to ship to
  EMR, and it opens a second IO path beside Spark's own, which is exactly the local/cloud split G2
  warns about.
- **`pathlib` locally and boto3 in the cloud.** Rejected for the same reason, worse: the code path
  CI tests would not be the one the cloud runs.
- **`_ingested_at = current_timestamp()`.** Rejected. Every re-run would differ, so "re-running
  produces identical outputs" would be false by construction.
- **Dropping `_ingested_at`.** Rejected. §9.1 requires it, and "when did this row land" is
  operationally useful.
- **Deriving `_ingested_at` from the inputs** (e.g. the latest arrival). Rejected. It would
  duplicate `_arrival_ts` and stop meaning "when the run happened".
- **Absolute URIs in `_source_file`.** Rejected. The same input would have different lineage in
  every checkout and every bucket, so bulk/incremental equality and local/cloud comparisons would
  need special cases.
- **Restricting lake paths to URI-safe characters** instead of decoding. Rejected. It is an
  arbitrary rule that would surface as a confusing error on someone's machine. Decoding is one
  expression, and a mismatch raises.
- **Letting each job set its own Spark confs.** Rejected. Parity would then depend on every author
  remembering every setting.

## Consequences
- **Job shape:** jobs take a `Lake` (and its session) and a run context. Tests get the same through
  the `spark` and `lake` fixtures. The session takes about 10 s to start, once per test run.
- **T4.3 (EMR submit):**
  - Pass `launch_confs()` at submit, and check whether EMR's default `extraJavaOptions` must be
    kept (appended to, not replaced).
  - Confirm whether EMR 8 maps `s3://` to S3A or needs `s3a://` roots.
  - `hirestream/__init__.py` reads its version from installed package metadata, which a
    `--py-files` zip doesn't have: ship a wheel, or make that lookup tolerant.
- **Big partitions:** one file per partition suits this data, where a day is far below 1 GB. If a
  partition ever grows past that, `write_partitioned` needs a size-based split (T5).
- **Spark Connect:** `Lake` and the verification use the classic session's private JVM handles
  (`_jvm`, `_jsc`). EMR Serverless runs classic Spark. A move to Spark Connect would need a
  different IO path.

## References
- Apache Spark 4.1 configuration and SQL reference: `spark.sql.session.timeZone`,
  `spark.sql.parquet.outputTimestampType`, `partitionOverwriteMode`, file metadata column
  `_metadata`. Checked against pyspark 4.1.1 behaviour in this repo on 2026-11-02.
- Hadoop FileSystem specification (rename semantics; object stores and S3A).
