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

### Starting the simulation in steady state (T1.2, ADR-0004)
- **Decision:** draw the initial workforce from the distribution the configured dynamics would
  produce after running for a long time. Tenure is piecewise exponential from growth and attrition
  (with the first-year multiplier). Time in role is the time since the last role change, from the
  promotion, lateral-move and location-change rates. Leave on day 1 is rate × mean duration. One
  new parameter, `company_founded`, caps tenure.
- **Numbers (full):** 16% in their first year, median tenure 3.9 years, 64% at or beyond HT3's
  548 days in role, 3,647 managers (14.6%).
- **Rejected:** hire dates uniform since founding, which gives about 4% first-year employees, so
  first-year attrition climbs for the whole simulation. Also rejected: separate tenure parameters,
  which would drift from the workforce rates.
- **Q: "Why does the starting state of a simulation matter?"** A: If day 1 isn't in steady state,
  every metric has a warm-up trend: attrition rising, promotions ramping, leave climbing from zero.
  On a WBR 6-12 chart that looks like a real business trend. Deriving the start from the same rates
  the simulation runs on removes it, and tuning a rate later reshapes the start consistently.

### Building an org chart with constraints (T1.2)
- **Decision:** a top-down split. A manager with more than 9 people below takes k reports in 5–9,
  j of them managers, and splits the rest evenly. The rule hi ≥ 2·lo − 1 is validated so a split
  always exists. Levels are assigned after the tree is built: L8 to org leaders, L7 preferred for
  managers of managers, L6 for first-line managers, and the remaining levels shuffled across ICs.
- **Q: "How did you test it?"** A: With invariants rather than examples. Exact headcount; every
  manager L6+ with 5–9 reports; every management chain ends at an org leader with no cycles; level,
  role and location counts exactly equal to the rounded shares. Plus a golden fingerprint that fails
  if a Faker or numpy upgrade silently changes the people.

### Workforce dynamics with invariants (T1.3, ADR-0005)
- **Decision:** a day-stepped engine with one draw per hazard per employee per day, and at most
  one hazard firing. When a manager leaves, their reports move to the next manager up. When a
  team manager or org leader leaves, the highest-level report takes over, promoted to L6 or L8 if
  needed. Lateral moves and manager changes apply only to ICs, so nothing cascades.
- **Numbers (full, 546 days):** 4,247 terminations, 2,521 promotions and 6,921 manager changes, in
  about 1 s. Tests check the org invariants on **every simulated day**: one L8 leader per org, L6+
  managers, reporting lines only to people still employed, no cycles.
- **Q: "How do you keep an org chart valid while people leave every day?"** A: Decide the
  succession rules up front, then test invariants rather than outcomes. I run the engine under
  heavy churn and in a deliberately tiny one-team org, and assert the rules after every day. That
  is how I found which edge cases needed rules: an org leader nobody can succeed, and a team whose
  last member leaves.

### An HRIS export that behaves like a real one (T1.3, ADR-0005)
- **Decision:** the export keeps its own *published* view of each employee, separate from the
  true state. 10% of changes appear 1–14 days late, carrying their true `job_effective_date`.
  Every change to an HRIS field sets that date, so SCD2 can place each version on the right day
  even across a missing file. Gzip uses `mtime=0` and no embedded file name, so the same data
  gives the same bytes and the manifest hashes are a real determinism check.
- **Numbers:** full = 545 files, 12.8M rows, 378 MB, 84 s, 114 MB peak memory. Each employee's
  CSV line is cached and rebuilt only when that employee changes.
- **Q: "What's the difference between event time and processing time in your HR data?"**
  A: `job_effective_date` is when a change took effect. The snapshot date is when HR's export first
  showed it. A late-entered promotion appears in the file for day E+k with an effective date of
  day E. The SCD2 build honours the effective date as long as it's after the current version
  starts. The generator records both, so the pipeline can be checked against the truth.

### Closing the loop: ATS hires become employees (T1.6, ADR-0008)
- **Decision:** each stage's outcome is drawn when an application enters it, so the outcome and
  its timing stay consistent, and HT2 is simply a multiplier at the first gate. Offers only go out
  while a req has a free seat (open seats minus undecided offers). An accepted offer takes the
  seat. On the start date the candidate becomes an employee, or an internal transfer that can
  open a backfill.
- **Numbers (dev):** 26,118 applications → 524 offers → 377 hires (82 internal), in 37 s. HT2's
  referral / career-site first-gate ratio is 1.8, inside its [1.5, 2.1] band.
- **Q: "How do you stop a simulated req from filling more seats than it has?"** A: Reserve seats at
  offer time, not at acceptance: free seats = open seats − undecided offers. An accepted offer takes
  a seat, and a no-start gives it back. A test checks that no req ever records more hires than its
  headcount.

### Loading a source database you can trust (T1.6b, ADR-0009)
- **Decision:** a strict vendor schema (PKs, FKs, CHECKs), loaded with `COPY` in one transaction.
  A rejected row rolls the whole replacement back, so `--overwrite` is atomic. Rows are formatted
  as CSV by our code and the exact bytes are hashed into the manifest, so the determinism check
  covers the database, not just lake files. The connection is checked before anything is deleted
  or simulated.
- **Q: "Why put constraints on synthetic data?"** A: They're free tests. A generator bug, like an
  offer whose application doesn't exist, fails at load time with a named constraint, instead of
  surfacing weeks later as an S-ATS-02 failure in silver. Real ATS vendors enforce them too.
- **Q: "How do you make a database load reproducible?"** A: Don't let the driver choose the bytes.
  Format rows yourself (NULLs, timestamp precision, booleans), hash what you send, and record the
  hash. Two runs with the same seed must produce the same per-table sha256.

### Making a hidden effect measurable: HT1 (T1.7a, ADR-0010)
- **Decision:** interview stages are now event-driven. Interviews are booked, moved, cancelled,
  replaced, missed or completed, and the ATS decides only when the feedback is in (or the 7-day
  cap passes), so slow feedback really slows hiring.
- **What it took to make HT1 measurable:**
  - only a trained 10% of employees interview
  - the over-cap penalty compounds
  - chronic slowness is spread evenly over popularity (blocked randomization)
  - the truth labels feedback by final weekly load, exactly as the warehouse will
- **Q: "Your effect is a ×2 multiplier; why did you measure 4.5?"** A: Overload barely happened. At
  dev, 1% of feedback came from 5 overloaded interviewers, and most of them happened to be
  chronically slow, so the ratio measured slowness. The fix is structural: concentrate interviewing
  on a trained pool, so overload spreads over dozens of people, and don't let luck decide whether
  the busiest people are slow.
- **Q: "Isn't stratifying the slow interviewers cheating?"** A: Every interviewer still has the same
  8% chance of being slow. Stratification only removes chance imbalance between popularity bands,
  as blocked randomization does in a clinical trial. The effect being measured doesn't change; the
  seed-to-seed noise does.
- **Q: "How do you know the generator and the warehouse agree on 'overloaded'?"** A: A test rebuilds
  every interviewer's weekly load from the emitted events alone (final start, not cancelled, ISO
  week of the UTC date) and asserts it equals the generator's own counter, interview for interview.

### Schema evolution and a producer bug, on purpose (T1.7b, ADR-0011)
- **Decision:**
  - **Versions:** follow each event's time. `1.2.4`, then the buggy `1.3.0` for 14 days, the
    `1.3.1` hotfix, and `2.0.0` from schema v2. The bump is major because `interviewer_id` becomes
    the array `interviewer_ids`.
  - **Panels:** a panel counts for both panelists, and each writes their own feedback, which
    matches the exploded `fct_interview` grain.
  - **The bug:** it writes start times without their offset, and 2% of those events drop the
    timezone.
- **Q: "How do you inject a bug without changing what you're simulating?"** A: Give the bug its own
  random stream, and make its formatting a pure function of the event's time. Then prove it: a test
  moves the bug window and raises its drop rate from 2% to 50%. Every interview, stage change and
  workforce event stays identical; only bytes differ, and converting the starts back to UTC makes
  the events identical too.
- **Q: "Why bump to 2.0.0 for one field?"** A: A scalar becoming an array breaks every consumer
  that reads `interviewer_id`. That's a breaking change, so it's a major version. Silver unifies
  them as `array<string>`, wrapping v1's scalar.

### Chaos that tests the pipeline but never the simulation (T1.8a, ADR-0012)
- **Decision:**
  - **Chaos:** applied per delivered copy (duplicates, a lag mixture, three malformed kinds, three
    incidents), with a fixed ten draws per event from its own stream.
  - **Delivery:** a queue flushed one day behind the simulation.
  - **Files:** Firehose-style hourly parts with deterministic names. Each file's mtime is its
    latest arrival.
- **Numbers (dev):** 1.83M events became 1.85M lines in 16.5k parts (258 MB): 1.5% duplicates, 0.1%
  malformed, 3% more than an hour late. Every simulation number is unchanged.
- **Q: "Why a fixed number of random draws per event?"** A: If I drew a malformed kind only when a
  line is malformed, turning on an incident would change how many draws happen and reshuffle every
  later decision. With fixed draws, a run with an incident differs from one without only where the
  incident acts. A test checks exactly that, which makes incident drills and the COE comparable to
  the baseline.
- **Q: "How did you find the memory problem?"** A: Peak RSS rose 100 MB, but the queue was small.
  tracemalloc showed 4 KiB allocations at one line, `orjson.dumps`: the library returns bytes that
  keep their output buffer. Copying each line to its own size cut memory 9× per line.

### Retrying a partial PutRecords failure (T1.8b, ADR-0013)
- **Decision:** batches within 500 records and 5 MiB. Only failed records are resent, with
  full-jitter exponential backoff, and after five retries the sink fails loudly. Tests use moto,
  plus a wrapper that throttles chosen records, because moto can't fail part of a batch.
- **Q: "Why not just let the SDK retry?"** A: A partial `PutRecords` failure is an HTTP 200 with
  `FailedRecordCount > 0`, so the SDK sees success. You have to read each result, resend only the
  failed records (resending the batch duplicates the rest), and back off with jitter so throttled
  producers don't retry in lockstep.
- **Q: "Does that keep per-entity order?"** A: Not quite. A retried record lands after later
  records of the same key that succeeded. Strict order needs `PutRecord` with
  `SequenceNumberForOrdering` per event, which costs throughput. Silver orders by `event_ts` and
  dedupes on `event_id`, so arrival order was never the contract.

### Contracts that catch the faults you inject (T1.9, ADR-0014)
- **Decision:**
  - 26 strict JSON Schemas, one per event type and version: closed objects, every sent key
    required, `const` versions, lower-case enums, backslash-free patterns that mean the same in
    Python and Java.
  - The known producer bug (1.3.0's offset-less start times) isn't written into the contract.
    The tests require it to fail exactly where the bug is, with counts matching the truth.
- **Q: "Why not make the contract accept the buggy timestamps?"** A: Then the contract is loose
  for every producer, forever: a future release that drops offsets would pass. Keeping the promise
  strict and treating the repair (read the naive time in its zone) as a consumer policy keeps the
  contract a detector. The test proves it detects the bug we know about.
- **Q: "How do you test that a schema actually catches things?"** A: Mutation tests, one fault each,
  must give exactly one expected violation. Chaos then breaks real events in three ways, and every
  broken line must show exactly one new violation of its recorded kind. Plus a two-way enum check:
  every value a contract declares must really be sent, which catches typos that silently widen a
  contract.

### Ground truth that can grade the pipeline (T1.10a, ADR-0015)
- **Decision:**
  - **`ground_truth.json`:** every run writes what really happened, taken from the engines' own
    counters and the ATS's final state, before chaos.
  - **Grading:** the warehouse will be checked against it: ATS counts exactly, stream counts within
    the injected loss, HT1–HT4 within bands.
  - **Comparable by construction:** each calibration target is defined the way the warehouse
    metric computes it, with UTC months, so an exact match is achievable.
  - **Measure first:** T1.10a moves no simulated number; T1.10b tunes, with an ADR.
- **Q: "How do you know your pipeline is right if the data is synthetic?"** A: Because I know the
  answer. The generator records the true counts before it injects faults, and the pipeline has to
  recover them: exactly for the ATS, within the known loss for streams. I also pre-computed how
  many lines silver must quarantine, per reason. A test classifies every bronze line with the
  contracts and gets exactly those counts.
- **Q: "Why not compute the expected counts from the bronze files?"** A: Then I'd be grading the
  pipeline with its own input, and a bug in reading the files would be on both sides of the
  comparison. Truth has to be recorded upstream of the faults.
- **Q: "Why split measuring from tuning?"** A: The first measurement found the req fill rate far off
  (0.40 against 0.80–0.92). Fixing it changes every number the simulation produces. Mixed with a
  new reporting layer, nobody could review either. First a ruler you trust, then adjust what it
  measures.
- **A finding worth telling:** at tiny, HT2 came out at 2.11 against a configured 1.8. My first
  theory blamed the measurement. Recording the ratio as drawn showed 2.14 at the draw itself:
  sampling noise, since four other seeds draw 1.73–1.85. Measure at the source before theorising.

### Calibrating against targets that pull against each other (T1.10b, ADR-0016)
- **Decision:**
  - **Diagnosis first:** the req fill rate was 0.40 against 0.80–0.92. Regular reqs got too few
    applications, the funnel needed about 60 per hire, and the two evergreen reqs absorbed any
    gain in conversion.
  - **Thirteen values, four problems:**
    - applications early in a posting's life (less popularity skew, more views and applies, slower
      decay)
    - a funnel that needs fewer (later-stage pass rates)
    - evergreen ×25 → ×7, so its hiring stays put
    - a faster pipeline with a realistic tail
  - **Evidence:** `make calibrate` checks five seeds. 17 of 18 targets pass on every seed.
- **Rejected:**
  - **More traffic alone:** applications per hire would pass 150, and full would produce 60 M+
    events.
  - **Sourcing that ignores popularity:** a spec change, and sourced applications would reach
    about 30% of the mix.
  - **Wider bands:** they hide the problem.
- **Q: "How do you calibrate 18 targets without overfitting one seed?"** A: I diagnosed before I
  tuned. Listing the expired reqs showed many with 50–90 applications and no hire, which pointed
  at the funnel, not the traffic. I worked out what the targets jointly require: to fill in under
  60 days, a typical req needs about 0.05 eventual hires a day when it's posted. Then I compared
  candidates over five seeds, because one parameter change reshuffles the random path and moves a
  single seed by ±0.04. The evidence is a command anyone can rerun.
- **Q: "You shipped with a check failing. Why?"** A: HT4's strict monotonic check fails about half
  the time at dev on correct data. Acceptance decay starts at day 30, so the first two buckets
  differ by about 2–3 points, while dev's sample gives about ±3 points of noise. I showed that
  with a power calculation, kept the check strict, and documented the dev miss, as the spec
  allows. At full, with 12× the offers, a chance failure has odds of about 0.3%. Loosening the
  check would have made it pass, and also made it unable to tell a decline from no effect.
- **Q: "Why change thirteen parameters?"** A: There were four distinct problems, and two of the
  changes only compensate for others. The direct channels scale with the apply rate so the
  channel mix holds, and evergreen popularity drops so its hiring stays where it was. Each one has
  a row in the ADR with its before and after.
- **A finding worth telling:** tuning surfaced a real bug. A fill scheduled same-day closures into
  a list that had already been read, so 1 early applicant in 6 stayed active on a filled req.
  T1.7a had the same class of bug in the scheduler. This time I audited every day-keyed queue.

### Measuring performance on a machine you don't control (T1.11, ADR-0017)
- **Decision:**
  - **Measure first:** full met every target with no change. That's 22.9 M events, 18 of 18
    targets, 1.3 GiB, and 824–1,417 s by the process's clock, against 45 min and 4 GB. The ATS
    loads 2.39 M rows into Postgres in 51 s.
  - **Rank work by counts:** deterministic counts, not timings, because the VM's speed varied
    2–3× between identical runs.
  - **Prove output by hashes:** three full runs wrote identical hashes for all 26,859 files.
  - **Check contracts on bronze:** every scheduling line, and 1 job-board line in 50.
- **Rejected:**
  - **Optimizing anyway** (GC tuning, gzip level): no need, and no way to measure a 10% gain
    here.
  - **Reporting wall time:** it counted a night's sleep.
- **Q: "Three identical runs took 14, 24 and 42 minutes. How do you trust any number?"** A: I name
  the clock. Wall time included VM stalls and the laptop sleeping, so I report the process's
  monotonic clock and CPU time. Even CPU time was inflated: the quiet run needed 752 CPU-seconds
  for work a busy run billed as 1,341. I compare work with counts that don't depend on the machine. When
  run 1 slowed right after the reorg, I didn't optimize the reorg. I reran it: the slow days
  moved, and the profiled call counts matched seasonality. Correctness comes from byte-identical
  outputs, not from timings.
- **Q: "Why didn't you optimize the generator?"** A: The profile shows the time is spread over
  event construction, serialization, gzip and GC, with no single hot spot. The targets were met
  with about 2× headroom, and every speed-up risks changing the bytes the calibration rests on. I
  wrote down the levers and the rule for using them: any change must leave dev's manifest hashes
  identical.
- **Q: "How do you validate 23 million events?"** A: I read back what was written. Every
  scheduling line is classified, and the counts per quarantine reason must equal the ground truth
  exactly. For the job board I sample every 50th line: each must be valid or carry exactly the
  fault that was injected, at the injected rate, and every contract has to show up. That checks
  the bytes consumers will read, in 11 minutes, without a second 25-minute simulation.

### Privacy by design in synthetic data (T1.6)
- Candidate phone numbers come only from ranges reserved for fiction (US 555-01xx, UK Ofcom's
  07700 900xxx; elsewhere an unassignable leading 0), and emails use `.example` domains. Even
  synthetic PII can't reach a real person.

### Generating a clickstream in vectors (T1.5, ADR-0007)
- **Decision:** each day, expected views per req = base × popularity × season × weekday ×
  posting-age decay. Sessions ~ Poisson(Σλ / mean session size), and views are assigned to reqs in
  proportion to λ. Each req then averages exactly its expected views while views still group into
  realistic sessions. Everything random for the day is drawn in vectors; the per-event loop only
  assembles envelopes.
- **Numbers:** dev = 1.93M events in 24 s at 87 MB peak memory (events stream through a sink, never
  held). Bot share 12.8%, inside the 5–15% target.
- **Q: "How do you make synthetic web traffic realistic enough to test bot rules?"** A: Bots come
  from a small pool; 25% send known bot user agents and 75% spoof browsers. They crawl 30–300 jobs
  1–8 s apart. Anything over 60 views breaks gold's "60 in 10 minutes" rule, while a spoofed bot
  with 30–60 views should get through. So the pipeline's detection rate can be measured against the
  generator's truth instead of assumed.

### Profiling before optimising (T1.5)
- A tiny run spent most of its time on envelope details, not simulation: `datetime.isoformat`,
  `uuid.UUID`, and numpy scalar indexing inside Python loops. Swapping those for a cached-date
  formatter, bit-masked UUID strings, and `.tolist()` gave about 2× with byte-identical output. I
  proved it with a sha256 over all 251,460 event bodies, before and after.
- **Q: "What's a common numpy performance trap?"** A: Indexing numpy arrays element by element in
  Python. Each access boxes a numpy scalar. Vectorise, or convert to lists once before the loop.

### Capping a heavy-tailed distribution (T1.4, ADR-0006)
- **Finding:** the spec asked for Pareto(1.2) popularity normalized to mean 1. With α ≤ 2 the
  variance is infinite. Over 1,000 reqs the sample mean ranged 0.62–1.46 across 200 seeds (worst
  12.3), and one req reached 11,550× the mean. Views and applications scale with popularity, so
  event volumes would have been a lottery.
- **Decision:** truncate the raw draw at 200 and normalize by the truncated distribution's exact
  mean. The median req sits at 0.45 and the maximum at about 51× the mean. The sample mean then
  stays within 0.88–1.10, and the skew E8 needs is still there.
- **Q: "How did you find it?"** A: I measured before building. A 200-seed sweep of the sample mean
  showed the problem in seconds. It's the same reason join-skew benchmarks cap mega-keys: you want
  a reproducible skew, not a random extreme.

### Growth as a feedback controller (T1.4, ADR-0006)
- **Decision:** growth reqs come from a monthly headcount plan: open max(0, target −
  (headcount + open seats + scheduled backfills)). A fixed rate ("5% of headcount a year") would
  leave the company flat, because 30% of departures aren't backfilled and some reqs never fill. The
  controller also absorbs cancellations, no-starts and evergreen hires.
- **Q: "Why not a fixed rate?"** A: A fixed rate hits the target only if every other flow matches
  its expected value. A controller measures the gap and closes it. It's also what real companies
  do: headcount plans are set monthly.

### Proving subsystem isolation (T1.4)
- Adding a whole subsystem (requisitions) left every HRIS file byte-identical: tiny's 89 sha256
  hashes match `main`. That's the practical payoff of name-keyed random streams. A new feature
  can't silently reshuffle data it doesn't own.

### Validating simulation parameters at load time (T1.1)
- **Decision:** strict, frozen pydantic models over `base.yaml`: unknown keys rejected, shares must
  sum to 1, `p_advance + p_withdraw ≤ 1`, presets limited to `window` and `scale`.
- **Q: "Why so strict for a config file?"** A: A typo such as `attrition_anual` would otherwise be
  ignored, and the default would show up weeks later as a calibration miss. Failing at load time
  costs one line of error output.

## Phase 2 — Batch pipeline MVP

### A Spark foundation that checks parity instead of assuming it (T2.1, ADR-0018)
- **Decision:**
  - Jobs never build their own session. One factory applies the settings results depend on (UTC,
    ANSI, dynamic overwrite, snappy, microsecond Parquet timestamps) in both modes and verifies
    them at startup. Only sizing differs between local and EMR.
  - All IO goes through one lake layer that uses Hadoop's FileSystem, the same client Spark uses
    on `file:` and S3A. Lineage and state record lake-relative keys.
- **Q: "Why set ANSI explicitly when it's already Spark 4's default?"** A: The default is what you
  get when nobody overrides it. A setting in `spark-defaults`, a notebook habit or a platform
  default can turn it off, and then a bad cast becomes a silent null instead of an error. Making it
  explicit and verifying it at startup means a session without ANSI can't run a job.
- **Q: "Why go through Hadoop's FileSystem instead of boto3?"** A: Spark already reads and writes
  through it on both filesystems. Using it for listings and state files keeps one IO path and
  ships no extra dependency to EMR. With boto3 in the cloud and pathlib locally, CI would test
  code the cloud never runs.
- **Q: "How can a run be idempotent if every row has an `_ingested_at`?"** A: Split lineage in two.
  `_source_file` and `_arrival_ts` describe the input, so they're a function of it. `_run_id` and
  `_ingested_at` name the run, passed in once by the caller, never read from the clock mid-job.
  "Identical output" means identical apart from the run-scoped columns, and a fingerprint (schema
  plus rows as a multiset) checks exactly that.
- **Q: "What surprised you?"** A: A UTC session still collected New York times on my laptop,
  because PySpark converts timestamps in the Python process's zone. CI and EMR run in UTC, so it
  would only ever bite locally. The fix pins every process (driver, workers, JVM) and a test
  checks all three. Also, Spark URL-encodes the file paths it reports while Hadoop listings don't,
  so a manifest key and a row's `_source_file` could disagree. They're decoded and must match, or
  the read fails.

### One reason per bad line, with the rules read from the contracts (T2.2a, ADR-0019)
- **Decision:**
  - Silver parses each line once as VARIANT and checks every rule on every line.
  - An explicit precedence chooses one quarantine code. Types and enums come from the JSON Schema
    contracts, not from silver's code.
  - Naive start times are read in their zone with java.time's DST policy, and flagged.
- **Q: "Why check every rule instead of stopping at the first failure?"** A: Stop-at-first makes
  the order an accident of how the code is written, and a refactor can silently change which code a
  line gets. With every rule evaluated and a `PRECEDENCE` constant choosing, the order is one
  reviewable line. There's a test for each step of it: lines that break two rules, where the
  higher-ranked one must win.
- **Q: "How do you know silver quarantines the right lines?"** A: Three independent judges on a
  real tiny backfill:
  - the jsonschema classifier the generator's ground truth was checked with must agree on every
    one of 4,624 lines;
  - the counts per reason must equal the injected faults;
  - every column of every event must equal plain Python's reading of the raw line.

  None of them shares code with the Spark transform.
- **Q: "What happens on a DST gap or overlap?"** A: The spec's policy is "ambiguous → earlier
  offset, nonexistent → shift forward". That is exactly java.time's `atZone`, which Spark's
  `to_utc_timestamp` uses. I verified it on New York and London gaps and overlaps, and they are
  tests now. The trap was elsewhere: an unknown zone makes `to_utc_timestamp` throw, so zones are
  validated first, and a bad one quarantines a line instead of failing the run.

### A manifest as the commit point, so retries are safe (T2.2b, ADR-0019)
- **Decision:**
  - Each silver run commits one write-once record listing the objects it processed, after its
    table writes. An object is new until a record lists it.
  - Incremental runs list a trailing 48 h of hour prefixes, so late Firehose objects are still
    found.
  - Bulk mode rebuilds the source.
- **Q: "What happens if the job dies halfway?"** A: There's no record, so the retry processes the
  same objects again, and both merges make that harmless.
  - Silver anti-joins on `event_id`, so a row already written isn't added again.
  - Quarantine anti-joins on (file, line).
  - Touched partitions are a function of the batch, so gold still hears about them.

  A test deletes the record after a run, re-runs it, and requires identical content fingerprints.
- **Q: "Why not trust the hour folder to be complete?"** A: Firehose can deliver an object into an
  hour prefix after that hour has ended. If you process hour H once and move on, you miss it. The
  trailing window plus the manifest means each run looks back two days and processes only what it
  hasn't seen. A test drops a late object into a past hour, and the next run picks it up.
- **Q: "How do you know incremental equals a full rebuild?"** A: The test history runs three
  hourly batches, a late object and a crash, then rebuilds the same bronze in bulk in another lake.
  The two silver tables and the two quarantines must have equal content fingerprints, lineage
  included. That holds because objects are processed in the order they appear, so the first copy
  processed is the earliest one.

### A second source that cost only its differences (T2.3, ADR-0020)
- **Decision:** before adding the job board, the contract checks moved out of the scheduling
  transform into shared code, in a refactor commit whose proof was that scheduling's tests didn't
  change. The job board then added only its columns and its timestamp parsing.
- **Epoch milliseconds are guarded, not trusted.** The contracts' `minimum` (10^12) is the floor,
  so epoch seconds quarantine as `UNPARSEABLE_TIMESTAMP` instead of becoming 1970. Huge values are
  nulled before `timestamp_millis`, which has no `try_` form and would fail the whole job.
- **Q: "Why not just multiply seconds by 1000 when you see them?"** A: Because a producer sending
  the wrong unit has broken its contract, and the fix belongs with the producer. Repairing it
  silently hides the break and guesses at intent; quarantine makes it a count someone sees, with
  the raw line kept for a backfill once the producer is fixed.
- **Q: "How do you know job board silver is right?"** A: The generator records what silver must
  produce. On tiny, 178,017 lines become exactly the 175,202 events and 180 quarantined lines the
  ground truth expects. Each quarantined line also gets the same verdict from an independent
  jsonschema classifier, and every column equals what plain Python reads from the raw line.

### An incremental extract whose watermark can't lie (T2.4a, ADR-0021)
- **Concept:** a watermark extract pulls `updated_at > watermark`. Three things make that safe
  here:
  - **The watermark comes from the data:** the largest `updated_at` the extract pulled, never the
    clock.
  - **A 1 h overlap:** it catches rows a transaction still in flight committed with an older
    `updated_at`.
  - **One commit record per extract,** written last: the watermark only moves once the Parquet has
    landed and been counted.
- **Rejected alternative:** watermark = the time the extract ran. A transaction that started at
  09:59 and committed at 10:01 has `updated_at = 09:59`. An extract at 10:00 doesn't see it, and
  the next one asks for `> 10:00`, so the row is lost. The data's maximum plus an overlap is the
  standard fix, and the duplicates it causes are silver's job.
- **Also rejected:** one mutable watermarks file. It needs an overwrite (on S3, a torn write loses
  the watermark), and silver would still need to know which extract directories are complete.
  Write-once records answer both questions and keep the history S-ATS-04 checks.
- **Interview question:** "Your watermark extract runs every hour. What can it miss?"
  - **Strong answer:** three things.
    - **Late commits.** A row whose transaction commits after the extract with an older
      `updated_at`. The overlap window covers it, at the cost of re-reading some rows each run.
    - **Ids that commit out of order.** A `bigserial` id is taken at insert, not at commit, so a
      strict `change_id > max` can skip a slow transaction's row. I documented it rather than
      fixing it: the source has one writer. The fix is a small id overlap, since silver dedupes
      anyway.
    - **Hard deletes,** which no watermark can see. That needs CDC from the WAL.
  - **One more case:** I also detect a reloaded source by its table oids. After the tables are
    recreated, the old watermarks would make an incremental extract silently skip rows.

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
