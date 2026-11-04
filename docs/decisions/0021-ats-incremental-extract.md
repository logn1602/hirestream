# ADR-0021: The ATS incremental extract: one snapshot, COPY to Parquet, per-extract watermarks

- **Status:** Accepted
- **Date:** 2026-11-04
- **Task:** T2.4 (T2.4a: the extract; T2.4b: ATS silver)
- **Deviates from:** SPEC §8's single `state/ats_watermarks.json`. It becomes one write-once
  record per extract under `state/ats_watermarks/` (§4). Everything else fills in what §8 leaves
  open.

## Context
SPEC §8 fixes the extract's rules:
- mutable tables are pulled with `updated_at > watermark − 1 h`
- `application_stage_changes` is pulled with `change_id > last_max_change_id`
- watermarks advance only after the extract is durably written
- output goes to `bronze/ats/<table>/extract_ts=YYYYMMDDTHHMMSSZ/*.parquet`
- hard deletes are a known limitation

SPEC §7.4 says the source stores UTC in `timestamp without time zone`. ADR-0009 says
`generate backfill` drops and recreates the tables.

What was open:
- **Consistency:** whether the five tables are read at one moment or one after another.
- **Export path:** how rows get from Postgres to Parquet without a new dependency.
- **The watermark:** where its value comes from, and what happens when a table has no new rows.
- **Watermark state:** what "advance only after the extract is durably written" means, given that
  `Lake` writes state once and never overwrites it (ADR-0018).
- **A reloaded source:** after `generate backfill --overwrite`, the old watermarks describe a
  database that no longer exists.
- **Out-of-order commits:** whether `bigserial` ids can commit out of order.

## Decision

### 1. Two PRs
- **T2.4a:** the extract, its commit records, `hirestream ingest ats`.
- **T2.4b:** the five ATS silver tables and `run silver --source ats`.

### 2. One snapshot for all five tables
- **How:** the extract reads every table in one `REPEATABLE READ, READ ONLY` transaction. The
  tables then agree with each other: an application's candidate and requisition are in the same
  snapshot, which is what S-ATS-02 (FK integrity) will check.
- **`extract_ts`:** the transaction's start, `transaction_timestamp()`, in UTC. It names the
  extract's directories and its record, at second resolution, so two extracts must be at least a
  second apart. A second extract within the same second is refused, not merged.
- **Cost:** none for a source this size. A long read-only transaction on a busy OLTP database
  holds back vacuum, which is the trade-off to watch on a real vendor database.

### 3. COPY → CSV → Spark → Parquet
- **Export:** each table is exported with `COPY (SELECT …) TO STDOUT WITH (FORMAT csv, NULL '\N',
  FORCE_QUOTE *)` into a local temporary file. Spark reads that file with the table's fixed schema
  (`hirestream.transforms.common.ats.TABLES`) and writes one Parquet file per table.
- **Exact types:**
  - Timestamps are formatted by Postgres as `YYYY-MM-DDTHH24:MI:SS.US` (fixed width,
    microseconds) and parsed by Spark in the session's UTC. No Python `datetime` touches the data,
    so the process's local zone can't shift a value.
  - Booleans go out as `'true'`/`'false'` (`::text`), not COPY's `t`/`f`, which Spark doesn't read.
  - Dates go out as `YYYY-MM-DD`.
- **`FORCE_QUOTE *`:** with a NULL marker other than `''`, COPY writes an empty string unquoted,
  and Spark reads an unquoted empty field as null. Quoting every non-null value keeps `''` and
  NULL apart. The integration test caught this (NOTES, 2026-11-04).
- **`mode=FAILFAST`:** a value that doesn't fit its column stops the extract. The source is typed
  and constrained, so that would be our bug, not bad data.
- **Count check:** each table's landed Parquet row count must equal `count(*)` over the same
  filter in the same snapshot.

### 4. Watermarks come from the data
- **Value:** a table's new watermark is the largest `updated_at` (or `change_id`) among the rows
  the extract pulled. If it pulled none, the watermark stays where it was. The clock never sets a
  watermark, so the source's clock and the extractor's never have to agree.
- **Overlap:** the lower bound for `updated_at` is `watermark − 1 h`. Rows updated in the hour
  before the watermark are pulled again on every run. They are duplicates by design, and silver
  resolves them by primary key and `updated_at` (T2.4b). On tiny, an extract with no source
  changes re-reads 20 candidates, 1 requisition, 3 applications and 1 offer.
- **The parameter is sent naive:** psycopg sends a naive `datetime` as `timestamp`. An aware one
  would be a `timestamptz`, and Postgres would compare it through the session's `TimeZone`.

### 5. A record per extract is the commit point
- **Where:** `state/ats_watermarks/<extract_ts>.json`, write-once:
  - the extract's mode
  - the run id
  - each table's Postgres oid
  - each table's row count, previous watermark, new watermark, and bronze directory (null when it
    pulled no rows; no empty Parquet is written)
- **Order:** the record is written after every table's Parquet has landed and been counted. A
  crash before it leaves bronze directories that no record names. Nothing reads them, and the
  retry pulls from the same watermarks into a new `extract_ts`.
- **Current watermark:** the latest record's.
- **History:** the records are the full history. `check_monotonic` runs on every extract before
  committing, which is S-ATS-04 on the committed history:
  - extract times strictly increase
  - within a baseline, no watermark moves back
- **Why not one mutable `ats_watermarks.json`:**
  - it needs an overwrite that `Lake` deliberately doesn't have
  - a torn overwrite on S3 is a lost watermark
  - silver would still need a list of which extract directories are complete, which is what the
    records are

### 6. A reloaded source needs `--full`
- **Detection:** each record stores the oid of every source table. `generate backfill` drops and
  recreates the tables (ADR-0009), which gives them new oids.
- **Refusal:** an incremental extract against different oids refuses and names the fix. Its
  watermarks belong to the old data, and the new data's `updated_at` values can be lower, so it
  would silently miss rows.
- **`--full`:** ignores the watermarks, pulls everything, and records `mode: full`. A full extract
  is a new baseline, and silver (T2.4b) is built from the latest full extract and the incrementals
  after it. The first extract into an empty lake is always full.

### 7. `change_id` stays strict
- **The gap:** a `bigserial` id is taken when the row is inserted, not when its transaction
  commits. If transaction A takes id 100 and transaction B takes 101 and commits first, an extract
  between the two commits records 101 as the watermark, and 100 is never pulled.
- **Kept:** `change_id > last_max`, as SPEC §8 says, with this as a known limitation next to
  hard deletes. Nothing here produces it: the backfill loads in one transaction, and live-tail
  (T3.4) has a single writer.
- **The fix if it mattered:** re-read a window below the watermark (for example the last 1,000
  ids) and let silver dedupe on `change_id`, which it does anyway. Or, robustly, CDC (§23).

### 8. Where it runs
- **Local and cloud:** the extract runs beside the ATS (the Airflow host), with a local Spark
  session that writes to the lake root, an `s3a://` bucket in the cloud (SPEC §2.1). It never
  runs on EMR, because the source isn't reachable from there.
- **Packaging:** psycopg is a dependency of the package, not of `requirements-spark.txt`. The
  extract lives in `hirestream.ingest`, outside the Spark import guard. Only the table schemas and
  records it shares with silver live in `transforms/common/ats.py`.
- **Parity gap:** a local Spark writing to S3 needs the s3a connector and credentials on the
  Airflow host. T4.3 owns that (ADR-0003).

## Results (T2.4a)
| Preset | Rows | First extract (wall, incl. Spark start) | Second extract, no source changes |
|---|---|---|---|
| tiny | 17,725 | 15.5 s | 25 overlap rows re-read |
| full | 2,390,992 | 43 s | 304 overlap rows re-read, 20 s |

Row counts equal the generator's `ats_tables` counts exactly at both presets. Most of tiny's time
is Spark starting. At full, the bronze Parquet is about 52 MB, and the incremental extract right
after a regenerate refused as §6 says until `--full` was given. That was the first real use of the
guard. Speed was T2.4a's main risk, and COPY is fast enough that the JDBC and pyarrow fallbacks
aren't needed.

## Alternatives considered
- **Spark JDBC (`spark.read.jdbc`).** Rejected for now. It needs the Postgres JDBC driver as a JAR
  (a new dependency, downloaded at runtime or vendored), and its partitioned reads open one
  connection per partition, which loses the single snapshot unless they share an exported
  snapshot (`pg_export_snapshot`). It is the better tool for tables far larger than these.
- **psycopg rows → `spark.createDataFrame`.** Rejected. Every value becomes a Python object and is
  pickled into the JVM. Naive `datetime`s convert through the process's local zone. Memory grows
  with the chunk.
- **pyarrow.** Rejected. It is a new dependency for what COPY and Spark already do.
- **Bronze as the CSV COPY writes.** Rejected. SPEC §8 says Parquet, and typed columns in bronze
  mean silver never re-parses text from a typed source.
- **Watermark = the extract's clock time.** Rejected. A row committed late, with an `updated_at`
  before the clock, would be skipped. The data's own maximum plus the overlap is what the spec's
  "in-flight transactions" rule needs.
- **An overlap on `change_id`.** Deferred (§7).

## Consequences
- **Silver must be idempotent under overlap:** the same row arrives in several extracts. T2.4b
  merges by primary key, latest `updated_at`, then latest `extract_ts`.
- **Bronze grows on every run:** overlap rows land again each run, and a crashed extract leaves
  orphan directories. In S3 bronze expires after 30 days (§8).
- **`make generate` doesn't clear `bronze/ats` or the records.** The next incremental extract
  refuses (§6), and the RUNBOOK says to run `ingest ats --full`.

## References
- `docs/SPEC.md` §2.1, §7.4, §8, §12.2 (S-ATS-02, S-ATS-04), §23
- ADR-0003 (parity gaps), ADR-0009 (the source database), ADR-0018 (write-once state, `Lake`),
  ADR-0019 (the manifest as a commit point)
