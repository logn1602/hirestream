# ADR-0019: Stream silver: rules from the contracts, one reason per line, and the bronze manifest

- **Status:** Accepted
- **Date:** 2026-11-02
- **Task:** T2.2 (T2.2a: the scheduling transform; T2.2b: the manifest and incremental runs)
- **Deviates from:** SPEC §9.1's touched-partitions path. It becomes one file per source inside a
  run directory (§9). Everything else fills in what §8, §9.1 and ADR-0014 §5 leave open.

## Context
The spec and earlier ADRs fix most of silver:
- **SPEC §9.1:** the rules.
  - Read lines as text and detect malformed JSON explicitly.
  - Parse against the contract, with ANSI mode on and `try_*` parsing.
  - Quarantine with seven reason codes.
  - Dedupe on `event_id`, keeping the earliest `_arrival_ts`, tie-broken on `_source_file`.
  - Read naive starts in their zone, with a DST policy.
  - Unify v1 and v2.
- **SPEC §9.2:** the table's columns.
- **ADR-0014 §5:** the order of five of the seven codes, and the quarantine set: the envelope and
  the entity key.
- **ADR-0011:** the producer versions and the naive-start bug.
- **ADR-0012:** duplicates, per-copy malformation, and each object's mtime as its arrival time.
- **ADR-0015:** expected quarantine counts per reason.

What they leave open:
- **The other two codes:** where `UNPARSEABLE_TIMESTAMP` and `OUT_OF_RANGE_TIMESTAMP` sit in the
  order, and what they mean.
- **Non-key fields:** what happens to one that is missing or has the wrong type.
- **Unknown dates:** which quarantine partition holds a line without a usable `event_ts`.
- **Ties:** how to order copies of an event that share an object.
- **State:** the layout of the manifest and touched-partitions files, what bulk mode does, and how a
  retried run behaves.

Probing Spark 4.1.1 on 2026-11-02 found the following:

| Question | Result | Consequence |
|---|---|---|
| Invalid JSON | `try_parse_json` returns null for it, and for duplicate keys | Both are MALFORMED_JSON |
| Valid JSON that isn't an object | `null`, `[1]` and `"x"` parse | Silver needs its own object check |
| DST | `to_utc_timestamp` uses java.time: a gap moves later, an overlap takes the earlier offset | That is §9.1's policy exactly |
| An unknown zone | `to_utc_timestamp` throws `INVALID_TIMEZONE` | One bad zone would fail the whole job |
| Format strictness | `try_to_timestamp` with `ss.SSS'Z'` rejects a missing fraction and invalid dates, without throwing | Contract formats can be enforced |
| Same-partition rewrite | Reading partitions and dynamically overwriting them in one job works | The merge needs no staging copy |

## Decision

### 1. Two PRs
- **T2.2a:** the transform, a pure function of a batch of lines and the run.
- **T2.2b:** the manifest, incremental and bulk runs, the writes, and `hirestream run silver`.

### 2. The rules come from the contracts
- **`SourceRules`:** flattens a source's JSON Schemas into one rule per field and contract
  (`event_type`, version): path, Spark type, nullability, enum or const.
  - JSON `integer` becomes `bigint`.
  - A field with two types across contracts fails at load.
- **Enums per contract:** they are kept per contract because they differ. `position_filled` is a
  reason to cancel but not to reschedule.
- **Column mapping:** renames, the v1 wrap and the `_utc` columns are silver's own code. A test
  requires every contract field to map to a column (only `source` is checked and not stored).

### 3. One reason per line
- **How:** every rule is checked on every line, and the first failure in `PRECEDENCE` wins.

  | Order | Code | Fails when |
  |---|---|---|
  | 1 | `MALFORMED_JSON` | not JSON, duplicate keys, or not an object |
  | 2 | `MISSING_REQUIRED_FIELD` | an envelope field (`event_id`, `event_type`, `source`, `producer_version`, `event_ts`, `sent_ts`) or `payload.interview_id` is missing, null, or can't be cast |
  | 3 | `INVALID_ENUM` | the event type, the source, or a payload enum is outside its contract |
  | 4 | `UNKNOWN_SCHEMA_VERSION` | no contract for (`event_type`, `schema_version`), including a missing version |
  | 5 | `UNPARSEABLE_TIMESTAMP` | a present time is not in its contract format |
  | 6 | `UNRESOLVABLE_TIMEZONE` | a naive start has no zone, or one the JVM doesn't know |
  | 7 | `OUT_OF_RANGE_TIMESTAMP` | `event_ts` is more than 1 h after its object arrived |

- **Placement of the two open codes:**
  - A time that can't be parsed comes before a zone that can't be found: the zone only matters
    once the time parses.
  - The plausibility check comes last, because it needs everything else to parse.
- **What out of range means:** an event can't be delivered before it happens; 1 h allows for clock
  skew. Ranges that depend on the simulation window stay in DQ (S-SCH-04). The generator never
  produces this, so it is zero by construction, as ADR-0015 expects.
- **Payload fields are read only for the contracts that define them,** so a stray field from
  another event type is drift and is ignored (ADR-0014 §5).
- **Lines with several faults:** silver checks every rule at once. The jsonschema-based reference
  classifier stops early in places (an unknown event type hides everything else), so it can pick a
  different, still valid code. Chaos breaks one thing per line, and the tiny reconciliation requires
  agreement on every generated line.

### 4. What silver keeps
- **Non-key fields:** a field outside the quarantine set that is missing or can't be cast arrives as
  null (ADR-0014 §5), and DQ owns it. Enums and times still quarantine, because they have codes.
- **Casts:** values are cast to the contract's type with Spark's ANSI rules, so `"45"` reads as 45
  and `45.5` as 45. The jsonschema reference keeps wrongly typed values too, so the two agree.

### 5. Time
- **UTC fields** (`event_ts`, `sent_ts`, `actual_*`): `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`, exactly as the
  contracts say.
- **Starts:**
  - With an offset, it is used as given (`XXX`; `Z` is accepted).
  - Naive (the 1.3.0 build), it is read in `payload.timezone` with `to_utc_timestamp`, and
    `start_tz_inferred` is set. A start is detected as naive by its format, never by producer
    version.
- **Zones:** checked against the JVM's region ids first, fetched in one py4j call, so an unknown
  zone quarantines one line instead of failing the job.
- **`event_date`:** the UTC date of `event_ts`.

### 6. Unification
- **`interviewer_ids`:** v1 scheduled events wrap their one `interviewer_id`, and v2 lists its
  panel.
- **`interviewer_id`:** only feedback events fill it.
- **`interview_format`:** null for v1.
- **Also stored:** `sent_ts`, for delivery lag.
- **Schema:** `SCHEDULING_EVENTS_SCHEMA` fixes the column order and types.

### 7. Duplicates
- **Within a batch:** the earliest copy wins, ordered by (`_arrival_ts`, `_source_file`,
  `_source_line`).
- **Across batches (T2.2b):** the spec's anti-join applies: a row already in silver wins. Objects
  arrive, and are processed, in mtime order, so the first copy processed is the earliest one, and
  incremental runs agree with a bulk run. T2.2b tests this.

### 8. Lineage gains `_source_line`
- **What:** `Lake.read_lines` numbers each line within its object, from 1. The order is the read
  order inside each split, sorted by split start, so it stays correct for an uncompressed file
  Spark splits.
- **Why:** it completes the tie-break, and it identifies a quarantined line, so a retried run can't
  quarantine it twice.

### 9. Quarantine and state (T2.2b)
- **Quarantine:** `quarantine/<source>/event_date=…/` with `raw_line, reason_code, reason_detail,
  _source_file, _source_line, _arrival_ts, _run_id`. A line whose `event_ts` can't be read goes in
  the null partition; silver doesn't invent a date for it.
- **Manifest:** write-once `state/bronze_manifest/<source>/<run_id>.json`. It lists the objects the
  run processed (key, size, mtime) and its counts. It is written last, so it is the run's commit
  point. A run whose manifest exists does nothing when retried.
- **Touched partitions:** `state/touched_partitions/<run_id>/<source>.json`, so two silver jobs in
  one pipeline run never write the same write-once file. The content is §9.1's: the source and the
  `event_date`s it rewrote.
  - It records the event dates of the batch's deduplicated rows, even where every row turned out to
    be a duplicate.
  - Reporting too many partitions only costs gold a rebuild; reporting too few would lose a change.
  - A retry after a crash reports the same set.
- **Incremental mode:** lists the trailing 48 h of hour prefixes before the run's logical time and
  skips objects already in the manifest.
- **Bulk mode:** rebuilds the source. It deletes the source's silver table, quarantine and manifest,
  then processes all of its bronze. That is what is needed after `make generate` replaces bronze.

## Results (T2.2a, tiny)
- **Lines:** 4,624 bronze scheduling lines in 1,378 objects became 4,541 events, the ground truth's
  `expected_silver_events`.
- **Quarantine:** 9 lines, exactly the ground truth's counts per reason: 4 unresolvable time zones,
  2 malformed, 2 missing fields, 1 invalid enum.
- **Reference agreement:** every line's code equals the jsonschema reference classifier's.
- **Columns:** every column of every event equals plain Python's reading of the raw line.
- **Naive starts:** 262 resolved and flagged, the 266 naive starts minus the 4 without a zone.
- **Panels:** 185, from v2's 1,265 interviewer ids over 1,080 scheduled events.

## Alternatives considered
- **`from_json` with a struct schema.** Rejected. Detecting a type mismatch depends on its
  partial-results behaviour, and it can't tell "not an object" from "an empty object". VARIANT
  parses once and extracts each field with `try_variant_get`.
- **Hard-coding enums and types in silver.** Rejected. SPEC §7 makes the contracts the source, and
  copies drift.
- **Stopping at the first failed check,** as the reference does. Rejected. Order would then depend
  on how the code is written. Checking everything and choosing by an explicit precedence makes the
  order a reviewable constant.
- **Quarantining every contract violation,** including non-key fields. Rejected by ADR-0014 §5:
  COE-001 needs the renamed `req_id` to reach silver as null.
- **Partitioning quarantine by arrival date.** Rejected. §9.1 names `event_date`, and a null
  partition is honest about the lines that have none.
- **One `<run_id>.json` for touched partitions.** Rejected. Parallel silver jobs would have to
  read, modify and write one write-once file.
- **Union-and-rededupe across batches** (the earliest copy wins globally). Rejected. It equals the
  anti-join whenever objects are processed in arrival order, which they are, and it would rewrite
  existing rows' lineage on retries.
- **Bulk mode as "process everything, merge as usual".** Rejected. After bronze is regenerated, old
  silver rows and manifest entries describe objects that no longer exist.

## Consequences
- **T2.3:** the job board reuses `SourceRules`, `stream.py` and the quarantine shape, adding its
  `context` fields and epoch-millisecond `event_ts`.
- **T2.2b:** adds the manifest, the writes, the CLI, crash-retry and incremental-vs-bulk tests, and
  the listing fix below.
- **T2.11:** X-01 reconciles each run's manifest counts (lines = kept + quarantined + duplicates).
  S-SCH-02 reads the quarantine by `_run_id`.
- **T4.3:** `contracts/` must ship with the job, as ADR-0014 already says.
- **Known cost:** Spark spends about 11 ms per small gzip object on this VM, so reading tiny's 1,378
  objects takes about 15 s. Firehose writes far fewer, larger objects.
- **Found while planning T2.2b:** `Lake.list_files` makes about six py4j calls per file, which took
  11–78 s for 1,378 files here. Listing through Spark's `binaryFile` source takes about 1 s, without
  reading contents. T2.2b switches to it.

## References
- `docs/SPEC.md` §7, §8, §9.1, §9.2, §12.2, §20
- ADR-0011, ADR-0012, ADR-0014 §5, ADR-0015, ADR-0018
- Spark 4.1 SQL functions: `try_parse_json`, `try_variant_get`, `try_to_timestamp`,
  `to_utc_timestamp`
