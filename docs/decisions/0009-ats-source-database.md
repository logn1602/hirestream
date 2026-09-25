# ADR-0009: The ATS source database: schema, load, and determinism

- **Status:** Accepted
- **Date:** 2026-09-25
- **Task:** T1.6 (T1.6b)
- **Deviates from:** nothing in the spec. It fills in how SPEC §6.6's "bulk-loads the final state …
  with `COPY`" works.

## Context
ADR-0008 built the ATS engine in memory. SPEC §7.4 fixes the vendor schema (five tables, UTC in
`timestamp without time zone`, indexes on `updated_at`), and §6.6 says backfill bulk-loads the final
state and the full stage-change history with `COPY`. Still to decide:
- how backfill finds the database, and what happens when it's missing
- what a re-run does to existing data
- how strict the schema is
- where requisitions' times of day come from (they only track dates, ADR-0006)
- how determinism covers database contents, which aren't lake files

## Decision

### 1. Connecting
- **Where it comes from:** `HIRESTREAM_ATS_DSN`, or a DSN built from the `ATS_DB_*` keys in `.env`
  (host `127.0.0.1`, port `ATS_DB_PORT`). `make generate` loads `.env` with `uv run --env-file`.
- **Fail fast:** backfill connects **before** deleting anything or simulating.
- **Opting out:** `--skip-ats-db` skips the load (quick runs, tests).
- **Errors:** they name `host:port/dbname` only, never the user or password.

### 2. Re-running
As with HRIS (ADR-0005 §7), backfill refuses if `requisitions` already holds rows, unless
`--overwrite` is given. The load drops the five tables, runs `sql/ats_source/001_schema.sql`, and
copies every table **in one transaction**. The replacement is atomic: if any row is rejected, the
previous data stays.

### 3. A strict schema
- **Keys:** primary keys, plus foreign keys (applications → candidates and requisitions; offers →
  applications, unique per application; stage changes → applications).
- **Value checks:** CHECK constraints on every status, stage and channel.
- **Consistency checks:**
  - internal candidates have an `employee_id`
  - filled and cancelled reqs have `closed_at`
  - req timestamps are ordered
- **Why strict:** a generator bug fails at load time instead of reaching silver.
- **`change_id`:** `bigserial`, loaded with the generator's ids. The sequence is then moved past the
  maximum, so T3.4's live-tail inserts continue it.

### 4. Times of day for requisitions
`opened_at`, `closed_at` and `updated_at` get a time in business hours (`business_hours_local`) in
the req's city, derived from sha256(seed, req_id, field), then ordered so updated ≥ closed ≥
opened. `created_at` = `opened_at`. No random stream is consumed, so adding the sink changes no
generator output.

### 5. Determinism covers the database
The sink formats each table's rows as CSV itself: NULL is an empty unquoted field; timestamps are
`YYYY-MM-DD HH:MM:SS.mmm`, naive UTC; booleans are `true`/`false`. It streams them to `COPY` in
20k-row chunks and hashes the exact bytes. The manifest's `ats_tables` records `{rows, sha256}` per
table, and `deterministic_view()` includes it, so the same seed must produce the same database
contents.

### 6. CI
The `test` job runs a `postgres:16.15` service container with `HIRESTREAM_TEST_ATS_DSN` set.
Integration tests (marker `integration`) create and drop their own database, and skip when the
variable is unset. No new required check.

## Alternatives considered
- **Silently skip the load when no database is configured.** Rejected. A run would look complete
  without its ATS.
- **Let psycopg format the rows (`write_row`).** Rejected. The bytes sent would be the driver's, so
  there would be nothing stable to hash.
- **No foreign keys, like a loosely coupled extract.** Rejected for the source itself. Real ATS
  vendors enforce them, and S-ATS-02 checks the extract, not the source.
- **Random times of day from the `ats` stream.** Rejected. Changing the ATS's draws would then shift
  every requisition timestamp.
- **An addendum to ADR-0008.** Rejected. ADR-0001 makes merged ADRs immutable except their status.

## Consequences
- The DDL is read from the repo (`sql/ats_source/`). Packaging it into the wheel for the Airflow
  image is T3.1's job.
- Load time at `full` (millions of stage changes) is measured in T1.11. Memory stays flat because
  rows are generated lazily and chunked.
- T2.4 can extract by `updated_at` and `change_id` straight away.

## References
- `docs/SPEC.md` §5.1, §6.6, §7.4, §8 (ATS extract), §12.2 (S-ATS-01..04)
