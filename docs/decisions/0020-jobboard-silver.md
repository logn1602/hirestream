# ADR-0020: Job board silver: epoch milliseconds, a flattened context, and shared checks

- **Status:** Accepted
- **Date:** 2026-11-03
- **Task:** T2.3
- **Deviates from:** nothing. It applies ADR-0019 to the second stream source and fills in what
  SPEC §7.1 and §9.2 leave open for the job board.

## Context
ADR-0019 fixed how a stream source becomes silver: rules come from the contracts, every rule is
checked, one code wins by precedence, earliest copy wins, and the manifest drives incremental runs.
`silver.jobboard_events` follows all of it. Its events differ from scheduling's in three ways:
- **`event_ts` is epoch milliseconds** (SPEC §7.1), an integer, while `sent_ts` is an ISO string.
- **A `context` object** (session, visitor, employee, user agent, referrer; v2 adds
  `device_type`) sits beside the payload, and SPEC §9.2 says to flatten it.
- **No time zones.** Nothing is local time, so `UNRESOLVABLE_TIMEZONE` can't happen.

What was open:
- **What counts as an unreadable `event_ts`** when it is an integer rather than a string.
- **The entity key** in the quarantine set (ADR-0014 §5 says "the envelope and the entity key").
- **How much of `silver_scheduling.py` to share.**

Probing Spark 4.1.1 with ANSI on, on 2026-11-03:

| Input to `timestamp_millis` | Result | Consequence |
|---|---|---|
| `253402300799999` (9999-12-31T23:59:59.999Z) | a timestamp | The last readable value |
| `9223372036854775807` | throws `ArithmeticException: long overflow` | One bad line would fail the whole job, so values are range-checked first |
| `1740823200` (epoch **seconds**) | 1970-01-21, silently | A unit mistake looks valid, so a floor is needed |

## Decision

### 1. Shared checks move to `stream.py`
- The contract reading and the five checks both sources need (malformed JSON, missing required,
  invalid enum, unknown version, out-of-range `event_ts`) move out of `silver_scheduling.py`, in
  their own refactor commit with scheduling's tests unchanged.
- They now cover any nested path, not only `payload.*`, so `context.*` is read and checked per
  contract like the payload.
- Field rules carry the contract's `minimum`. Each source keeps only what is its own: the column
  mapping and its timestamp parsing.

### 2. `event_ts`
- Read as `bigint` like every JSON integer (ADR-0019 §2).
- **Missing, null or not castable:** `MISSING_REQUIRED_FIELD`, as for scheduling.
- **Below the contracts' `minimum` (10^12, 2001-09-09) or above 9999-12-31T23:59:59.999Z:**
  `UNPARSEABLE_TIMESTAMP`. The floor is read from the contracts, not hard-coded, and loading fails
  if they lose it. Below it is almost certainly seconds; above it Spark can't represent the time.
- The value is nulled before `timestamp_millis` sees it, so no line can throw.
- **More than 1 h after its object arrived:** `OUT_OF_RANGE_TIMESTAMP`, shared with scheduling.
- `sent_ts` uses scheduling's strict ISO format.

### 3. Columns
- Silver keeps one column per contract field, named by the field, with `context.` dropped
  (`session_id`, `visitor_id`, `employee_id`, `user_agent`, `referrer_type`, `device_type`).
  A test requires every contract field to map to a column; `source` is checked and not stored.
- `device_type` is null for v1 (SPEC §9.1). `query_text` stays (SPEC §9.2: silver only).
- `event_ts_utc` and `event_date` (its UTC date) as for scheduling.

### 4. The quarantine set
- The envelope plus `context.session_id`, the job board's entity key: sessions are what gold's bot
  rules and the funnel group by. It is what `chaos.REQUIRED` drops for the job board.
- `payload.req_id` is not in it. A renamed `req_id` reaches silver as null, so COE-001 stays
  reproducible (ADR-0014 §5); DQ owns it.
- `UNRESOLVABLE_TIMEZONE` is always false. The code stays in the precedence so both sources share
  one quarantine schema and one reason list.

### 5. Bot traffic stays
Silver records what was sent. Flagging bots needs per-visitor windows over days, which is gold's
job (`fct_jobboard_event.is_suspected_bot`, T2.10).

### 6. Tests
- **Unit:** each contract example becomes a row; each reason code has a case; each step of the
  precedence has a pair of faults; the floor, the overflow and seconds; null non-key fields; the
  earliest copy. One Spark job serves a whole module (`tests/unit/transforms/cases.py`), which
  scheduling's tests now use too.
- **Runner and CLI:** the job board runs through the same `stream_silver.run`; incremental equals
  bulk; `hirestream run silver --source jobboard`.
- **Tiny reconciliation** (shared helpers in `tests/unit/transforms/tiny.py`): counts per code and
  events equal the ground truth, each quarantined line gets the jsonschema reference's verdict
  (every line, `slow`), every column equals Python's reading of the earliest copy.

## Consequences
- Tiny bulk: 2,225 objects, 178,017 lines → 175,202 events, 2,635 duplicates, 180 quarantined
  (62 `INVALID_ENUM`, 52 `MALFORMED_JSON`, 66 `MISSING_REQUIRED_FIELD`), equal to the ground truth;
  193 s on this VM (WSL2), JVM start included.
- A third stream source would add only its columns and its timestamp parsing.
- **Rejected:** inferring the unit from the value's size (seconds × 1000). A producer sending the
  wrong unit is a contract break that should be seen in quarantine, not silently repaired.
- **Rejected:** keeping `context` as a struct column. SPEC §9.2 asks for it flattened, and gold reads
  single columns.
