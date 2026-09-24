# TALKING_POINTS — HireStream

Decisions, trade-offs, numbers, and likely interview questions, by phase. Each entry: decision,
rejected alternative, and a question with a strong answer. Finalised in T6.3.

## Phase 0 — Bootstrap

### Pinning local Spark to the EMR release (ADR-0002)
- **Decision:** `emr-spark-8.1.0` (Spark 4.1.1, JDK 17, Python 3.11, ARM64). Local `pyspark==4.1.1`
  pinned exactly. `hirestream.versions` holds the pins; `tests/unit/test_versions.py` fails on drift.
- **Rejected:** the spec's `emr-spark-8.0.0`. It had the same runtimes, but an older Spark and
  shorter support, and it was written before 8.1.0 (LTS, supported to 2029) existed. Also rejected:
  `emr-7.14.0`, because Spark 3.5 has ANSI mode off by default, which would be a silent semantic
  difference.
- **Q: "Why pin pyspark exactly instead of `>=4.1`?"** A: Local runs are how I prove correctness
  before paying for EMR. A different minor version can change query plans, defaults such as ANSI
  mode and timestamp parsing, and Parquet writer behaviour. Then a green local run proves nothing
  about the cloud. The exact pin plus a test makes version drift a CI failure instead of a
  surprise in production.

### Naming parity gaps instead of claiming parity (ADR-0003)
- **Decision:** one code path. Twelve named gaps (Spark build, S3A vs POSIX, Firehose vs FileSink,
  Postgres vs Redshift, quota, …), each with its containment and the test that covers it.
- **Q: "How do you know your local pipeline behaves like production?"** A: I don't assume it. I
  list every difference and design around the dangerous ones. For example, nothing relies on
  rename-as-commit, because S3 has no atomic rename. Silver reads from a manifest instead of
  trusting that an hour prefix is complete, because Firehose delivers late. Correctness is checked
  on outputs (DQ and ground truth), which are engine-independent. What can't be tested locally
  goes on the cloud-demo checklist.

### Protecting main as a solo developer (T0.6)
- **Decision:** a repository ruleset, kept in `.github/rulesets/main.json`: PR required with 0
  approvals, four required checks pinned to the GitHub Actions app, merge-only, no force push,
  deletion, or bypass.
- **Rejected:** classic branch protection. Rulesets can be layered, can be exported as JSON, and can
  run in `evaluate` mode first. Also rejected: requiring 1 approval, because a solo owner can't
  approve their own PR, so it would force an admin bypass on every merge and teach the habit of
  bypassing.
- **Q: "What's the point of branch protection with no reviewers?"** A: It turns "CI must pass" and
  "history is never rewritten" from habits into guarantees, even for admins, since there are no
  bypass actors. Pinning checks to the Actions integration ID means another app can't satisfy a
  required check by posting a status with the same name.

## Phase 1 — Generator

### Deterministic generation with name-keyed random streams (T1.1)
- **Decision:** one `SeedSequence(seed, spawn_key=(sha256(name)[:4],))` per subsystem (world,
  workforce, requisitions, jobboard, ats, scheduling, chaos), PCG64 pinned, and golden first-draw
  values in a test.
- **Rejected:** `SeedSequence(seed).spawn(7)` by position. It works until someone adds or reorders a
  subsystem; then every stream after it shifts and every dataset silently changes. Also rejected: one
  shared generator, where any change in how many numbers one subsystem draws reshuffles all the rest.
- **Q: "How do you make a simulation reproducible and still easy to change?"** A: Give each
  subsystem an independent stream derived from the run seed and the subsystem's name. Changing how
  the job board consumes randomness then leaves the ATS output byte-identical, so a diff in
  downstream metrics points at the subsystem that changed. Pin the bit generator and keep golden
  values in a test, so a library upgrade that changes streams fails CI instead of quietly changing
  the data. Record the seed, config hash, and git commit in a run manifest.

### Validating simulation parameters at load time (T1.1)
- **Decision:** strict, frozen pydantic models over `base.yaml`: unknown keys rejected, shares must
  sum to 1, `p_advance + p_withdraw ≤ 1`, presets limited to `window` and `scale`.
- **Q: "Why so strict for a config file?"** A: A typo such as `attrition_anual` would otherwise be
  ignored, and the default would show up weeks later as a calibration miss. Failing at load time
  costs one line of error output.

## Phase 2 — Batch pipeline MVP

## Phase 3 — Orchestration and incremental processing

## Phase 4 — AWS

## Phase 5 — Tuning and COE

## Leadership Principles story map (T6.3)
| Leadership Principle | Story | Evidence |
|---|---|---|
| Customer Obsession | | |
| Ownership | | |
| Dive Deep | | |
| Insist on the Highest Standards | | |
| Frugality | | |
| Bias for Action | | |
| Deliver Results | | |
