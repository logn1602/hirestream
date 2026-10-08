# ADR-0014: Event contracts: strict schemas, what fails on purpose, and how they change

- **Status:** Accepted
- **Date:** 2026-10-07
- **Task:** T1.9
- **Deviates from:** SPEC §6.11, as a clarification. "Every emitted event validates against its
  contract" holds for every correct producer build. The timezone-bug build's start fields are
  validated too, and must fail exactly as §4 says. The ADR also defines §7's "contract change".

## Context
SPEC §7 puts a JSON Schema per stream event type and version in
`contracts/<source>/<event_type>.v<N>.json`, "the single source for generator validation tests and
for silver parsing expectations", and requires a version bump and an ADR for contract changes. §6.11
wants every emitted event to validate (all of them at `tiny`, a sample at `full`), and §20 lists
contract tests.

What the spec leaves open, or makes awkward:
- **The timezone bug.** It (ADR-0011) emits v1 events whose start times have no offset, sometimes
  without `timezone`, against §7.2's "with offset". ADR-0011 left T1.9 to decide whether contracts
  describe the bug or validation leaves it out.
- **Who can read the files.** Silver (§9.1) must parse against the contracts in Spark, which can't
  import jsonschema (CLAUDE.md: stdlib, pyspark and `requirements-spark.txt` only; jsonschema needs
  the compiled `rpds-py`).
- **COE-001 (§19)** needs Phase 2 silver to accept the job board's renamed `requisition_id` as a
  valid event with `req_id = null`. A strict contract rejects it.
- **Dialect:** which JSON Schema features, and how strict.

## Decision

### 1. Files
- **Layout:** 26 files, `contracts/scheduling/` (7 types) and `contracts/jobboard/` (6 types), each
  in v1 and v2. The folder names are the bronze folders'.
  - Every type has a v2 file because every v2 event carries `schema_version: 2`.
  - Only `interview_scheduled`'s payload and the job board's `context` (`device_type`) change.
- **Dialect:** JSON Schema 2020-12, `$id` = `urn:hirestream:contract:<folder>:<type>:v<N>`.
- **Self-contained:** no `$ref` or `$defs`, so silver reads one file with stdlib `json` and needs no
  resolver.
- **Allowed keywords:** `type properties required additionalProperties enum const pattern minimum
  minLength items minItems uniqueItems allOf if then`, plus annotations (`title`, `description`,
  `$comment`, `examples`). A test enforces the list, along with:
  - ASCII only, no duplicate keys
  - canonical `json.dumps(indent=2)` formatting
  - anchored patterns
- **Hand-maintained:** the files were bootstrapped once with a throwaway script; the JSON is the
  source.
- **Loader:** `hirestream.contracts` (stdlib only, tested) finds and reads them, for tests now and
  silver later. Validation lives in the tests (`tests/contract_checks.py`).

### 2. Strict: what the producer promises
- **Closed objects:** every object has `additionalProperties: false`, and every key the producer
  sends is `required`. Nullable keys are typed `[T, "null"]`.
- **Fixed values:** `event_type`, `source` and `schema_version` are `const`. Enums are lower-case
  `enum` lists, per event type (the two `reason` sets differ).
- **`pattern`, never `format`:** `format` is only an annotation unless a checker is enabled.
  Patterns use `[0-9]` and `[.]`, with no backslashes and no `(?…)` groups, so they mean the same in
  Python, JavaScript and Java/Spark. In Python and Java, `$` also matches before a final newline;
  producers never send one.
- **Integers are strict:** tests reject JSON `1.0` as an integer, as Spark's `LongType` would.
  2020-12 itself accepts `1.0`.
- **Only the SPEC and invariants:**
  - **Encoded:** SPEC §7 and invariants. Counts ≥ 0; positions, indexes and durations ≥ 1; distinct
    panelists; epoch milliseconds ≥ 10^12 (catches seconds); phone screens have null `loop_id` and
    `session_index`.
  - **Not encoded:** what the simulation decides (panel size, durations, business hours, cities,
    role families, time zones). That's behaviour, tested elsewhere.
  - **How the phone-screen rule is written:** two `if/then` rules (no `else`), each with `required`
    inside the `if`, so an upper-cased `interview_type` is one `enum` error rather than three.

### 3. Which contract an event meets
An event meets the contract of its own `source`, `event_type` and `schema_version`, never one chosen
by date. The job board switches version by simulated day, so events near the switch carry either
version.
- **Missing `event_type`:** a `required` violation.
- **Unknown `event_type`** (e.g. chaos's upper-cased `INTERVIEW_COMPLETED`): an `enum` violation.
- **Unknown version:** an unknown-version violation.
- **Signatures:** violations are reported as (keyword, JSON pointer) sets. `required` and
  `additionalProperties` point at the missing or unexpected key.

### 4. The timezone-bug build fails on purpose
The contracts don't describe the bug, and tests don't skip it (decided with Shubh).
- **Correct builds** (scheduling 1.2.4, 1.3.1, 2.0.0; job board 3.2.0, 3.3.0) must have no
  violations.
- **The 1.3.0 build's start events** must have exactly:
  - `interview_scheduled`: `pattern` on `scheduled_start`
  - `interview_rescheduled`: `pattern` on `previous_start` and `new_start`
  - either: `required` on `timezone` when it was dropped

  Counts must equal the scheduler's truth (`naive_starts`, `missing_timezone`). Its other event
  types are clean.
- **Why:** this proves the contract sees the fault. Describing the bug would loosen v1 for every
  producer forever, so a future release that dropped offsets would pass silently. Skipping the build
  would never show the contract catching it.
- **Silver's repair isn't contract:** §9.1's repair (read a naive start in its `timezone`, flag
  `start_tz_inferred`) is a consumer policy, not part of the contract.

### 5. From violations to silver's reason codes (guidance for T2.2/T2.3)
When a line has several violations, the first match wins:

| Violations | Reason |
|---|---|
| unparseable JSON | `MALFORMED_JSON` |
| `required`, on anything but `timezone` | `MISSING_REQUIRED_FIELD` |
| `enum` / `const` | `INVALID_ENUM` |
| unknown version | `UNKNOWN_SCHEMA_VERSION` |
| naive start + `required` `timezone` | `UNRESOLVABLE_TIMEZONE` |

- **A naive start with its `timezone` present** is repaired (`start_tz_inferred`), not quarantined.
- **Unknown fields (`additionalProperties`) are drift.** Phase 2 ignores them, and T5.5's drift
  check reports them.
- **The quarantine set is silver's policy.** Phase 2 quarantines on the envelope and the entity key
  (what `chaos.REQUIRED` drops), not on `payload.req_id`.
- **COE-001 stays reproducible:** the renamed event reaches silver as `req_id = null`, as §19
  requires.

### 6. Tests
- **The files** (`tests/unit/test_contracts.py`):
  - dialect and formatting
  - names, `$id`s and `const`s
  - envelopes identical within a source and version
  - v2 changes exactly SPEC's
  - examples validate; both phone-screen branches covered
  - every `chaos.REQUIRED` path required; chaos's enum fields carry `enum`
  - the loader is stdlib-only
  - one mutation per fault, each reported exactly, including a naive start and the silent schema
    break's rename
- **Producers:**
  - **Scheduling:** every event at tiny, in the default suite, with the §4 expectations and truth
    counts, every contract met by a real event, and every declared enum value really sent (a
    two-way check that catches typos).
  - **Job board:** a sample in the default suite (every 10th event plus all saves and applications).
  - **Every event both producers emit at tiny** (§6.11 literally): `test_producer_contracts.py`,
    marked `slow`, with a counting `ValidatingSink`.
  - jsonschema runs about 3× slower under coverage tracing; validation tests are marked `no_cover`.
- **Chaos:**
  - **How:** real events and timezone-bug variants go through the chaos layer with every line broken.
  - **Each line** shows exactly one new violation of its recorded kind, so silver can give it one
    reason code.
  - **Totals:** counts equal `ChaosTruth`.
  - **Not here:** an incident-level `silent_schema_break` test is T5.5's, so it doesn't pre-empt
    COE-001's "producer contract test" action item.

### 7. Changing a contract
- **Producers own `schema_version`.** A change to what a version accepts arrives as a new version
  file and an ADR.
- **A released file's validation keywords are frozen**, because bronze can be replayed.
- **Free:** `description`, `examples` and formatting.
- **Corrections:** if a file misdescribed what producers actually sent, fix it with an ADR and no
  version bump.

### 8. Dependency
jsonschema 4.26.0 and types-jsonschema 4.26.0.20261006, dev only. Nothing validates at runtime.

## Results
- **At tiny, default config:** every one of about 259k events (254,814 job board, about 4,064
  scheduling) meets its contract, except the 261 timezone-bug start events. Those 226 scheduled and
  35 rescheduled events break exactly as §4 says, 4 of them also without a timezone, matching the
  CLI's "timezone bug: 261 naive starts, 4 without a timezone".
- **The slow test:** 56 s at a flat 84 MB.
- **Chaos:** all 1,200 malformed lines in the chaos test show exactly one new violation.

## Alternatives considered
- **A contract that describes the bug** (offset and `timezone` optional). Rejected (§4).
- **Skipping 1.3.0 in validation.** Rejected (§4).
- **A separate 1.3.0 contract.** Rejected. Contracts are keyed by `schema_version`, and the bug never
  bumped it.
- **Generating the JSON from Python.** Rejected. Python would become the real source, against SPEC
  §7, and all 26 outputs would be committed anyway.
- **`$ref`/`$defs`.** Rejected. Every consumer would need a resolver; consistency tests control the
  duplication instead.
- **`format: date-time`.** Rejected. It is annotation-only by default and accepts any offset or
  precision.
- **pydantic models as the contract.** Rejected. Spark can't import them.
- **Validating Python dicts.** Rejected. Tuples, NaN and numpy floats behave differently from the
  JSON on the wire, so events are validated after an orjson round trip.
- **Exhaustive job-board validation in the default suite.** Rejected. It adds about 30 s, or about
  100 s under coverage.

## Consequences
- **T1.10:** can classify bronze lines with these signatures to cross-check expected quarantine
  counts by reason.
- **T1.11:** owns "sampled at `full`", reusing `ValidatingSink`.
- **T2.2/T2.3:** silver takes field lists, types, nullability and enums from the files, applies §5's
  reason precedence, and tests the patterns under Spark's `rlike`.
- **T3.1/T4.3:** must ship `contracts/` with the code. It sits at the repo root, outside the wheel.
- **T5.5:** adds the drift check, S-JB-03, and the incident-level contract test.

## References
- `docs/SPEC.md` §6.8, §6.11, §7, §9.1, §19, §20
- ADR-0007, ADR-0010, ADR-0011, ADR-0012
- `contracts/README.md`
