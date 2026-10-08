# Event contracts

One JSON Schema (Draft 2020-12) per stream event type and schema version. They are the single source
for producer validation tests and for silver's parsing expectations (SPEC §7). ADR-0014 records the
rules below and why.

```
contracts/
  scheduling/<event_type>.v1.json  …v2.json   # scheduling-service: 7 event types
  jobboard/<event_type>.v1.json    …v2.json   # jobboard-web: 6 event types
```

Read them with `hirestream.contracts` (standard library only; silver can use it too). Tests validate
with `jsonschema` (dev dependency only).

## Rules
- **Self-contained.** No `$ref` or `$defs`: one file describes a whole event, so a consumer needs
  only a JSON parser.
- **Strict: what the producer promises.**
  - Every object is closed (`additionalProperties: false`), and every key the producer sends is
    `required`.
  - Nullable keys are typed `[type, "null"]`.
  - `event_type`, `source` and `schema_version` are `const`.
- **`pattern`, never `format`.**
  - `format` is only an annotation unless a checker is enabled.
  - Patterns are anchored and use only `[0-9]`-style classes and `[.]`: no backslashes and no
    `(?…)` groups, so they mean the same thing in Python, JavaScript, and Java/Spark.
  - In Python and Java, `$` also matches before a final newline; producers never send one.
- **Nothing the simulation decides.** Panel size, durations, business hours and config-driven values
  (cities, role families, time zones) are behaviour, tested elsewhere. Contracts hold SPEC §7 and
  invariants: counts ≥ 0, positions and indexes ≥ 1, distinct panelists.
- **Formatting.** ASCII only, `json.dumps(indent=2)` with a trailing newline. Each file carries at
  least one real event in `examples`; `interview_scheduled` has a phone screen and an onsite
  session.

The timezone-bug build (producer 1.3.0, ADR-0011) breaks the v1 contracts on purpose. Tests expect
its start fields to fail `pattern` (and `required` when `timezone` is dropped), and nothing else.

## Changing a contract
- **Producers own `schema_version`.** A change to what a version accepts arrives as a new version
  file, together with an ADR.
- **A released file's validation keywords are frozen.** Bronze can be replayed, so an old event must
  keep its old meaning.
- **Free:** `description`, `examples` and formatting.
- **Corrections:** if a file misdescribed what producers actually sent, fix it with an ADR and no
  version bump.

## Checks
- `uv run pytest tests/unit/test_contracts.py`: dialect, consistency, examples, mutations.
- Producer tests validate real events: all scheduling events and a sample of job-board events in the
  default suite.
- `uv run pytest -m slow tests/unit/generator/test_producer_contracts.py`: every event at `tiny`
  (SPEC §6.11).
