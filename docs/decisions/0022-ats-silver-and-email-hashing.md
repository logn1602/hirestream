# ADR-0022: ATS silver rebuilt from the baseline, and email hashes as HMAC with one salt per lake

- **Status:** Accepted
- **Date:** 2026-11-04
- **Task:** T2.4b
- **Deviates from:** SPEC §9.1's `email_hash = sha2(concat(salt, lower(trim(email))), 256)`. It
  becomes HMAC-SHA256 over a normalization that strips all edge whitespace (§6, §7). Everything
  else fills in what §9.1 and §9.2 leave open.

## Context
SPEC §9.2 asks for `silver.ats_candidates`, `ats_requisitions`, `ats_applications` and
`ats_offers` as one row per primary key, the latest `updated_at` across extracts, and
`silver.ats_stage_changes` as one row per `change_id`, append-only. §9.1 adds lineage, the
`change_date` partition, and the PII rule. ADR-0021 lands one Parquet directory per table per
extract, names the complete ones in a write-once record, and defines the current baseline: the
latest full extract and the incrementals after it.

What was open:
- **Merge or rebuild:** whether each run merges new extracts into the existing tables, or
  rebuilds them from the baseline.
- **Ties:** the 1 h overlap re-reads rows, so the same key often arrives with the same
  `updated_at` in two extracts.
- **Bad rows:** whether ATS silver quarantines anything.
- **Lineage:** the stream tables' `_arrival_ts` is an object's mtime. An extract has a better
  time: its snapshot's.
- **What gold hears:** §9.1's touched partitions are dates of rewritten partitions, and four of
  the five tables aren't partitioned.
- **The salt:** where it comes from, what makes it unusable, and what happens if it changes.

Checked on 2026-11-04 against PySpark 4.1.1:
- Spark has no HMAC function (`spark.catalog.listFunctions()` lists none).
- HMAC-SHA256 built from `sha2`, `concat` and `unhex` matches Python's `hmac` exactly.
- Spark's `trim` strips only spaces: `trim('\ta@b.com\n')` is unchanged, while Python's
  `strip()` removes both.

## Decision

### 1. Every run rebuilds all five tables from the current baseline
- **How:** `transforms/silver_ats.py` reads each table's directories in the baseline (named by
  the committed records, never by listing), keeps one row per key, and replaces the whole table
  (`Lake.write_table`, a static overwrite).
- **Incremental runs** rebuild only when the baseline has an extract no earlier record of the same
  baseline built from, or a table is missing. Otherwise they commit a record that says nothing
  changed. **Bulk** deletes the tables and records first, then rebuilds.
- **A new baseline** (a `--full` extract after `make generate` reloads the source) rebuilds from
  that extract alone, so rows the reloaded source no longer has leave silver.

### 2. Which copy wins
- **Mutable tables:** the latest `updated_at`, a tie going to the later extract. Most ties are
  overlap copies of the same row. If the source changed a row without moving `updated_at`, the
  later copy is the truth.
- **Stage changes:** one row per `change_id`, the first extract's copy, as stream silver keeps the
  earliest copy of an event. Partitioned by `change_date`, the UTC date of `changed_at`.

### 3. No quarantine
The source enforces types, enums, keys and foreign keys, and the extract fails on any value that
doesn't fit its column. What can still go wrong is integrity across extracts (a row missed by the
overlap, say). That is DQ's to report (S-ATS-01 to -03, T2.11): dropping the row would hide it.

### 4. Lineage, counts and what gold hears
- **Lineage:** `_source_file` is the bronze Parquet file, `_arrival_ts` its extract's time to
  the second, as the extract's directory names it, plus `_run_id` and `_ingested_at`. Extracts
  are at least a second apart (ADR-0021 §2), so the second orders them.
- **Bronze is checked against its records:** each table's rows read must equal the sum of the
  records' row counts, or the run stops. Bronze is append-only, so a difference means something
  changed it.
- **The record**, `state/bronze_manifest/ats/<run_id>.json`, is the commit point. It holds the
  baseline, its extracts and the new ones, whether it rebuilt, the salt's fingerprint, and per
  table `read = written + superseded`, which X-01 (T2.11) reconciles.
- **Touched file**, `state/touched_partitions/<run_id>/ats.json`: per table, the UTC dates of the
  rows that came from new extracts (`updated_at`, or `changed_at` for stage changes), and
  `baseline_changed`. Overlap rows make it report a few dates too many, which only costs gold a
  rebuild. A changed baseline means rows may have vanished, so gold rebuilds what it took from ATS
  silver.

### 5. Reading bronze: per-file facts stay per file
Which extract a row came from is a fact about its file. The first version computed it per row:
each row's file path decoded into a lake key, a regular expression for its directory, a join on
that to find the extract, and a cache of the result. At full that cost 25-78 s per large table,
about seven times the Parquet scan (NOTES, 2026-11-04). Now:
- **`extract_ts` is a partition column:** the bronze layout is already Hive-style
  (`extract_ts=…/`), so `Lake.read_parquet_partitions` reads the baseline's directories with
  `extract_ts` filled per file, and the file name from `_metadata`. Only directories the records
  name are read; a record whose directory isn't `bronze_key(...)` stops the run.
- **Ties break on the id,** whose fixed-width text order is its time order. New rows are an `isin`
  on the new ids. Lineage columns are built only on the rows that win.
- **No cache:** the summary (row count and touched dates) reads two columns, so reading bronze
  twice is cheaper than caching wide rows in a 1 GB driver heap.
- **Rows written are counted by the write itself** (an observed metric of its result tasks), not
  by reading the table back: 548 partition footers took 25-34 s on this VM.

### 6. Email hashes are HMAC-SHA256
- **What:** `email_hash = HMAC-SHA256(salt, normalized email)` in lowercase hex
  (`transforms/common/pii.py`). Spark has no HMAC, so `hmac_sha256` builds it from `sha2`,
  `concat` and `unhex`, with the key's pads (RFC 2104) computed in Python. Tests: RFC 4231 cases
  2 and 6, and Python's `hmac` for keys of 1 to 200 bytes.
- **Why not SPEC's `sha2(concat(salt, email))`:** that is a secret-prefix hash. Its known
  weakness, length extension, can only produce hashes of strings that aren't real emails, so it
  would have been safe here. HMAC is the standard keyed hash and costs nothing more. The choice
  had to be made now: changing it later means rehashing every email in every lake.
- **Still personal data:** whoever holds the salt can hash a known email and find that person, so
  `email_hash` is pseudonymized, not anonymized.

### 7. One normalization
Strip leading and trailing whitespace (Java's `[\s\p{Z}]`: ASCII whitespace and every Unicode
space and separator, so a pasted non-breaking space too), then lowercase. Inner whitespace is
kept. A null or blank email has no hash: two blank emails must never look like one person. This
is the only implementation; HRIS silver (T2.5) uses it. `tests/pii.py` holds a plain-Python
reference for checking rows, and pinned hex values in `test_pii.py` keep the two from drifting
together.

### 8. The salt
- **Source:** `HIRESTREAM_PII_SALT`, read on the CLI side (`config.load_pii_key`) before Spark
  starts. It is a secret, so it lives in the environment, never in config files or arguments.
- **Refused:** unset or empty; `change-me`, the `.env.example` placeholder, which is public; under
  32 characters, a tripwire for obvious mistakes rather than a measure of randomness. Generate it
  with `secrets.token_hex(32)`.
- **Never shown:** `PiiKey` has no repr of its value, no message includes it, and records carry
  only its fingerprint: the first 16 hex characters of an HMAC of a fixed label. Checking a
  guessed salt against the fingerprint is no easier than against any known (email, hash) pair.
- **One salt per lake, forever:** a changed salt doesn't fail; it gives everyone a new hash and
  silently breaks joins with what is already hashed. The first run that hashes writes the
  fingerprint to `state/pii/salt.json`, write-once, and every later run must match it. Rotating
  is a deliberate RUNBOOK procedure.

## Results (T2.4b)
At full: 2,390,992 rows in three extracts, on a scratch lake holding T2.4a's two full-preset
extracts plus a third, quiet one.

| Phase | First version (per-row) | Now |
|---|--:|--:|
| Read and summarize stage changes (1.37 M rows) | 78.5 s | 4.5 s |
| Read and summarize applications (546 k) | 57.4 s | 2.2 s |
| Read and summarize candidates (460 k) | 24.8 s | 3.2 s |
| Bulk rebuild end to end, two rounds back to back, alternating | 248.5 s, 185.3 s | 208.1 s, 167.1 s |

- **Correct:** written rows equal the generator's counts exactly, and `read = written +
  superseded` in every table. Each quiet extract adds 304 overlap rows, all superseded.
- **Touched:** the first run reports all 548 dates (a new baseline); after one quiet extract,
  2 dates.
- **Nothing new:** about 21 s, most of it Spark starting.
- **What's left is writing:** stage changes land in 548 `change_date` partitions of about 2,500
  rows. A synthetic write of the same shape takes 51 s on this VM (30 s as one file), and the
  VM swaps, so wall times vary by a quarter between rounds.
- **Size:** silver at full is about 80 MB: candidates 33 MB, stage changes 33 MB, applications
  14 MB.

## Alternatives considered
- **Merge new extracts into the existing silver tables.** Rejected. Parquet has no row-level MERGE,
  so an unpartitioned table is rewritten either way; a merge saves only re-reading older extracts,
  and it would read the table it overwrites, which needs a staging copy and a swap. Iceberg's
  MERGE (SPEC §23) is the real fix at a larger scale.
- **Quarantine rows that break a foreign key.** Rejected (§3): it would hide the integrity
  problem S-ATS-02 exists to report.
- **The earliest copy wins for mutable tables too.** Rejected: it would keep a stale version of a
  row the source changed without moving `updated_at`.
- **SPEC's `sha2(concat(...))`, or Spark's `trim`.** Rejected (§6, §7).
- **Read each extract's directory separately and union them, with its time as a literal.** No
  per-row work either, but one scan per extract: a month of hourly extracts would be a
  700-branch plan. The partition column gives the same constants from one scan.
- **The salt as a CLI argument.** Rejected: it would show in `ps`, in Airflow task logs and, in the
  cloud, in EMR job details.

## Consequences
- **Gold (T2.6, T2.8):** reads the five tables, and `baseline_changed` in the touched file means
  rebuild what came from ATS silver.
- **HRIS silver (T2.5):** hashes with `PiiKey.email_hash` and registers the same salt.
- **DQ (T2.11):** S-ATS-01 to -03 run on these tables; X-01 reconciles each record's counts.
- **Cost:** every rebuild reads the whole baseline. Overlap rows add up between full extracts, so
  a periodic `--full` extract resets it.
- **Known gaps:** the HMAC pads are literals in the query plan, so they can show in `explain`
  output and Spark event logs, and they are as secret as the salt; the local Spark UI is off.
  In the cloud (T4.x) the salt comes from AWS Secrets Manager at runtime, and event logs need the
  same care. A leaked salt can only be rotated by rehashing from raw emails, which only bronze
  keeps, and bronze in S3 expires after 30 days.

## References
- `docs/SPEC.md` §5.1, §9.1, §9.2, §10.5, §12.2, §23
- ADR-0019 (records, touched files, bulk), ADR-0021 (the extract and its baseline)
- RFC 2104 (HMAC), RFC 4231 (HMAC-SHA256 test vectors)
