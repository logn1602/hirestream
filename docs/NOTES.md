# NOTES — HireStream engineering log

Honest log of real problems, newest last. Not polished: what broke, why, and what changed.

Entry template:
```markdown
## YYYY-MM-DD — T<id>: <one-line problem>
- **Symptom:**
- **Root cause:**
- **Fix:**
- **Lesson:**
```

## 2026-09-23 — T0.4: the spec's Spark default was already stale
- **Symptom:** the spec named `emr-spark-8.0.0` (Spark 4.0.x) as the default. The EMR Serverless
  release list showed `emr-spark-8.1.0` (Spark 4.1.1, LTS), published 2026-09-08.
- **Root cause:** the spec was written before 8.1.0 shipped. Version tables in design docs go stale
  within months.
- **Fix:** ADR-0002 pins 8.1.0 after checking Spark, Java, Python, region, and ARM64 support in the
  AWS release guide. The pins moved into code (`hirestream.versions`) with a drift test.
- **Also found:** new accounts get a default EMR Serverless quota of 16 concurrent vCPUs, below the
  spec's planned 32-vCPU application cap (ADR-0003 G9, for T4.2). EMR 8.1.0 also changed Spark
  config from "job replaces application" to "merged" (for T4.3).
- **Lesson:** treat every version in the spec as a hypothesis. Check it at the task that uses it,
  and keep the checked value in code so a test can enforce it.

## 2026-09-24 — T1.1: numpy 2.5 dropped Python 3.11
- **Symptom:** PyPI's latest numpy (2.5.3) declares `requires-python >=3.12`. We are pinned to 3.11
  by ADR-0002 (EMR's default PySpark Python).
- **Root cause:** numpy follows the scientific-Python support schedule (SPEC 0) and drops old Python
  versions on a fixed calendar. EMR moves more slowly.
- **Fix:** `numpy>=2.4.6,<2.5`, with the reason commented in `pyproject.toml`. uv would have picked
  2.4.6 anyway; the explicit cap makes the constraint visible instead of accidental.
- **Lesson:** a runtime pin (Python 3.11) quietly caps every library. When ADR-0002 is superseded
  (for example, EMR defaulting to 3.12), revisit this cap.

## 2026-09-24 — T1.1: two conftest.py files broke mypy
- **Symptom:** the mypy pre-commit hook failed with `Duplicate module named "conftest"` once
  `tests/unit/generator/conftest.py` joined `tests/conftest.py`. The commit was blocked.
- **Root cause:** without `__init__.py`, mypy maps both files to the top-level module `conftest`.
- **Fix:** made `tests/` a package (`__init__.py` in `tests/`, `tests/unit/`, and
  `tests/unit/generator/`), so they become `tests.conftest` and `tests.unit.generator.conftest`.
- **Lesson:** decide the test-package layout before the second conftest appears. The hook caught it
  before it reached CI.

## 2026-09-24 — T1.1: CLI test passed locally, failed in CI
- **Symptom:** `test_bad_arguments_exit_2[--seed]` failed only on GitHub Actions. The output did
  contain `--seed`, but it was wrapped in `\x1b[...m` sequences.
- **Root cause:** rich (used by Typer for error boxes) turns colour on when it detects CI
  (`GITHUB_ACTIONS`/`FORCE_COLOR`). Colour codes split the substring the test searched for. A local
  terminal run under pytest has no TTY, so there was no colour and the test passed.
- **Fix:** the CLI tests strip ANSI codes and collapse rich's line wrapping before matching.
  Reproduced locally with `GITHUB_ACTIONS=true FORCE_COLOR=1 uv run pytest tests/unit/test_cli.py`.
- **Lesson:** assertions on human-facing output must normalise it. The branch ruleset blocked the
  merge until CI passed, which is exactly what T0.6 was for.

## 2026-09-24 — T1.2: Faker's en_IN names include a curly apostrophe
- **Symptom:** while checking which characters Faker emits, one en_IN surname came back as
  `D’Alia`, with U+2019 RIGHT SINGLE QUOTATION MARK rather than an ASCII apostrophe. Ruff's
  ambiguous-character rules (RUF001/RUF002) also flagged it when it went into a test and a
  docstring.
- **Root cause:** real name data contains typographic punctuation. A naive `first.last` email
  builder would have put a non-ASCII character into a work email address.
- **Fix:** email local parts are NFKD-folded to ASCII and stripped to `[a-z0-9]`, so this surname
  becomes `dalia`. Names keep their original form, since HRIS CSVs are UTF-8 by contract (§7.5).
  The test writes the character as `\u2019` so the source stays unambiguous.
- **Lesson:** look at what the data actually contains before writing normalisation. Silver email
  hashing (`lower(trim(email))`) is safe because addresses are already ASCII, but names must stay
  UTF-8 all the way through.

## 2026-09-24 — T1.3: dev showed fewer leave starts than the config implies
- **Symptom:** one dev run produced 44 leave starts. `leave_annual` 0.02 × 3,000 employees suggests
  about 60.
- **Root cause:** not a bug. Across 20 seeds the mean is 54 (sd 4). Leave only starts for *active*
  employees, and with no hires until T1.4/T1.6, active headcount falls by about 12% over the year
  (average ≈ 2,800). One run at 44 is just a low draw.
- **Fix:** none to the engine. The rate test uses bounds taken from the 20-seed means, not the
  naive rate × headcount.
- **Lesson:** check a suspicious rate against a seed sweep before touching code. Until hires exist,
  every workforce count at full is about 17% below "rate × starting headcount". T1.10's
  calibration has to use exposure (employee-days), not starting headcount.

## 2026-09-24 — T1.4: Pareto(1.2) popularity was unstable
- **Symptom:** while sizing the plan, 1M popularity draws normalized to mean 1 had a sample mean
  of 1.84 and a maximum of 969,000×.
- **Root cause:** Pareto with α ≤ 2 has infinite variance, and at α = 1.2 the mean converges very
  slowly. Across 200 seeds, 1,000 reqs averaged anywhere from 0.62 to 1.46 (worst 12.3).
- **Fix:** truncate the raw draw at 200 and normalize by the exact truncated mean (ADR-0006). The
  sample mean now stays within 0.88–1.10 and the maximum is about 51×. `pareto_alpha > 1` is
  validated.
- **Lesson:** measure a distribution's sample behaviour at realistic sizes before wiring it into a
  simulation. Interviewer popularity (T1.7, α = 1.5) has the same problem.

## 2026-09-24 — T1.4: the growth plan crashed once no team was left
- **Symptom:** the one-team edge test (the team dissolves under heavy attrition) raised inside
  `rng.integers(0)`.
- **Root cause:** the plan kept opening growth seats (headcount below target) and tried to copy an
  employee from an empty list of team members.
- **Fix:** no members means no growth seats. The plan is still recorded with `seats = 0`.
- **Lesson:** the degenerate-org fixture from T1.3 paid for itself again. Every new subsystem should
  run on it.

## 2026-09-24 — T1.5: the test suite slowed from ~70 s to 212 s
- **Symptom:** after the job board joined the day loop, `make test` took 212 s under coverage. HRIS
  and CLI tests took 14–36 s each.
- **Root cause:** every test that called `simulate()` now generated about 250k job-board events it
  never inspected, and coverage tracing slows the per-event Python loop about 3×.
- **Fix:** HRIS and CLI tests use a near-silent job-board config. HRIS output doesn't depend on the
  job board, and tiny's hashes match `main`. One CLI run stays on the default config to pin the
  summary numbers. The suite is now 147 s.
- **Lesson:** when a subsystem joins a shared loop, check the test-time cost for everyone else, not
  just for its own tests.

## 2026-09-24 — T1.5: a calibration check was flaky at reduced volume
- **Symptom:** a bot-share test failed at 17.2% (target 5–15%) on a run with base views cut to a
  third to keep collected events small.
- **Root cause:** the run had only about 31 bot sessions, each with a uniform 30–300 views, so the bot
  share of events had a standard deviation of about 2.5 points. That run was +1.8 sd high. The
  generator was right; the sample was too small.
- **Fix:** structural bot checks stay on the small run. The calibration band is checked on a
  default-volume tiny run through the counting sink (9.8%).
- **Lesson:** calibration bands need samples large enough for the band to mean something. Work out
  the variance before choosing the fixture.

## 2026-09-25 — T1.6a: evergreen reqs take most hires, so regular reqs rarely fill
- **Symptom:** at dev, 109 reqs filled against 173 expired, a `req_fill_rate` of about 39% (target
  80–92%). Headcount shrinks about 3% a year even though the growth plan targets +5%.
- **Root cause:** evergreen reqs (popularity × 25, 20–50 seats refilled every month) take about 71%
  of hires and 62% of applications. Regular reqs expire with a median of 9 applications, while a
  hire takes about 69. This is the dominance ADR-0006 and ADR-0007 predicted.
- **Fix:** none in T1.6a, because tuning parameters needs an ADR. It's T1.10's first target: base
  views, `evergreen_multiplier`, evergreen seats. Everything else is already in range: applications
  per hire, acceptance, internal fill rate, time to fill, HT2.
- **Lesson:** close the loop early. Calibration problems only show once every subsystem runs
  together.

## 2026-09-25 — T1.6a: two test assumptions the engine was right to break
- **Symptom:** with `no_start_probability = 0` there were still no-starts, and 48 internal hires
  came from only 41 distinct employees.
- **Root cause:** an internal hire can't start if the employee leaves or the team empties before the
  start date (`could_not_start`, by design). With internal browsing turned up to 0.3 a day, some
  employees are hired internally twice.
- **Fix:** the tests assert those facts instead: every no-start in that run is `could_not_start`,
  and transfer events equal internal hires.
- **Lesson:** when a test fails, check the assumption before the code. Here the engine was right.

## 2026-10-06 — T1.7a: HT1 came out at 4.46 because almost nobody was overloaded
- **Symptom:** at dev the overloaded / normal median feedback-latency ratio was 4.46, against a
  band of [1.6, 2.4] for a ×2.0 multiplier.
- **Root cause:** the multiplier worked; the comparison group was tiny.
  - **Load:** about 11k interviews a year spread over about 3,000 eligible employees is 0.07 per
    person-week.
  - **Overload:** only about 1% of feedback came from a week over the cap, from 5 interviewers.
  - **The ratio:** 86% of that feedback came from chronically slow interviewers (×3), so the
    ratio measured slowness, not overload.
  - **The spec:** its selection rule never did the arithmetic.
- **Fix (ADR-0010):**
  - Only a trained 10% of employees interview.
  - The over-cap penalty compounds with each interview past the cap; a flat ×0.3 let a few
    interviewers reach 79 a week.
  - Slowness is spread evenly over popularity.
  - HT1 is labelled by final weekly load, as the warehouse will compute it.
- **Lesson:** with a heavy-tailed population, check how many distinct units carry an effect before
  trusting a ratio of medians. Ten people make a sample of ten, not of 300 rows.

## 2026-10-06 — T1.7a: actions scheduled for "today" were silently dropped
- **Symptom:** the median latency of submitted feedback was 22 h, but the draws had a median of
  18 h.
- **Root cause:** `step` popped today's agenda list and then iterated over it. Anything scheduled
  for today while today ran went into a new list that nobody read. That included feedback arriving
  the same UTC day as its interview, a large share with an 18 h median. The lost short latencies
  biased the median upwards, and those stages waited for the 7-day cap.
- **Fix:** drain the agenda in a loop until today's entry is empty.
- **Lesson:** event-queue simulations need "schedule into the current tick" handled explicitly.
  The bug only showed when I measured what was emitted rather than what was drawn.

## 2026-10-06 — T1.7a: a reopened req crashed on a recruiter who had left
- **Symptom:** `KeyError` in `Requisitions._index` while I was instrumenting a run.
- **Root cause:** a filled req keeps its `recruiter_id`. If that recruiter leaves before a no-start
  reopens the seat, `reopen_seat` re-indexes a recruiter who is no longer in the load table. Seed
  1602 never took this path until the scheduler shifted hire timing.
- **Fix:** a reopened req with a stale recruiter gets the least-loaded available one (or waits
  unassigned). The fix has its own regression test and commit, and no previously completed run
  changes.
- **Lesson:** "closed" objects keep references to the world as it was. Re-validate them when they
  come back to life.

## 2026-10-06 — T1.7b: two test assumptions the new data broke
- **Symptom:** after panels changed the random draws, two T1.7a tests failed on tiny. One said a
  decision came before the feedback cap. The other said a reschedule's `previous_start` didn't
  match the interview's start.
- **Root cause:**
  - **The cap:** the test measured it from the stage's last interview of any kind, here a candidate
    no-show three days after the last completed interview. The engine measures it from the last
    completed interview, because a no-show has no feedback to wait for, and that is the rule
    ADR-0010 means.
  - **The starts:** the test compared them as strings. An interview booked before the bug window
    and moved during it has an aware `scheduled_start` and a naive `previous_start` for the same
    instant.
- **Fix:** the tests now measure the cap from completed interviews and compare starts as instants,
  reading naive ones in the interview's zone, as silver will.
- **Lesson:** tests on generated data pass on the draws they happen to see. A change of seed, or a
  new feature that shifts the draws, is a free fuzz run, so treat its failures as questions about
  the test as much as about the code.

## 2026-10-07 — T1.8a: orjson's bytes kept a 4 KiB buffer each
- **Symptom:** with bronze writing on, dev's peak memory went from 142 MB to 247 MB, though the
  delivery queue never held more than about 13k lines.
- **Root cause:** tracemalloc pointed at one line, `orjson.dumps(body)`: 5.7k live allocations of
  about 4 KiB each. orjson's result keeps its whole output buffer, 4,098 bytes for a 441-byte line.
  Lines wait about a day in the queue, so every waiting line cost 9× its size.
- **Fix:** copy each line to its own size (`memoryview(...).tobytes()`, about 1 µs). Peak memory
  fell to 181 MB.
- **Lesson:** `sys.getsizeof` reports the object's length, not the allocation behind it, so measure
  retained memory with tracemalloc. And speed-focused libraries make memory trade-offs you only see
  when results are kept.

## 2026-10-07 — T1.8a: flushing at the next midnight wrote 146 straggler parts
- **Symptom:** tiny's bronze had 146 hours with a second part, though every hour held far fewer
  lines than the roll limit.
- **Root cause:** I flushed day D's queue at D+1's UTC midnight, assuming the next day's events
  start then. Job-board sessions follow each visitor's local day, so Bengaluru (UTC+5:30) browses
  from 18:30 UTC the evening before, into hours already written.
- **Fix:** flush one day behind (after day D, write what arrived before D's midnight). No straggler
  parts remain, and memory still holds only about a day of lines.
- **Lesson:** in a day-stepped simulation with local time zones, "the day" is not a UTC day.
  Ask what the earliest timestamp of tomorrow's work can be.

## 2026-10-07 — T1.8a: timings swung 3× from run to run on WSL2
- **Symptom:** the same dev backfill took 1:04, then 1:41, then 2:17. Main took 26–47 s through
  the day.
- **Root cause:** bronze adds 16.5k hour folders and files per dev run. On this WSL2 virtual disk,
  `mkdir` ranged from 0.46 ms to 1.07 ms per call, and a plain loop creating 16.5k folders and
  files took 21 s, then 6 s, on back-to-back runs. The first chaos benchmark was also skewed by
  first-run garbage collection (48 µs per event, against 1.4 µs with GC off).
- **Fix:** measure main and the branch back to back, look at profiles rather than one wall time,
  and tune the CPU-side costs: `mkstemp`, per-line concatenation, and heap pushes. The filesystem
  part comes with SPEC's hourly layout. T1.11 measures `full`.
- **Lesson:** on a shared or virtualized disk, one wall-clock number is an anecdote. Profile, and
  compare against a baseline taken at the same moment.

## 2026-10-07 — T1.8b: a batch-limit test that tested the other limit
- **Symptom:** none. The test passed, which was the problem. I wrote 600 records of 10 KB "so the
  5 MiB cap splits them" and asserted two calls.
- **Root cause:** 500 records × 10 KB is 5.0 MB, under 5 MiB, so the 500-record count limit made
  the split. Deleting the byte check from the sink would have left the test green.
- **Fix:** 600 records of 20 KB (12 MB). The calls must be [261, 261, 78], fewer than 500 each, so
  only the byte limit can produce them.
- **Lesson:** a limit test should be impossible to pass through any other limit. Work out which
  constraint binds before writing the assertion.

## 2026-10-07 — T1.9: JSON Schema details that would have weakened the contracts
- **Symptom:** none in the end. A design review tried candidate contracts and test helpers against
  jsonschema before anything shipped, and found three traps.
- **Root cause:**
  - **`1.0` is an integer** in Draft 2020-12, so a producer emitting floats would pass, while
    Spark's `LongType` rejects them.
  - **`if/then/else` on `interview_type`** turned one upper-cased value into three errors (`enum`,
    plus `type` on two fields from the `else` branch). Silver needs one reason per bad line.
  - **Validating Python dicts isn't validating the wire.** A tuple fails as an array though orjson
    writes a JSON array; NaN passes as a number though orjson writes `null`.
- **Fix:**
  - tests use a strict-integer type checker
  - the conditional is two `if/then` rules with `required` inside each `if`
  - every event is validated after an orjson round trip
- **Also:** jsonschema runs about 3× slower under coverage tracing. The default suite validates
  every scheduling event and a sample of job-board events (marked `no_cover`). The literal
  every-event check is a slow test: 56 s for about 259k events.
- **Lesson:** a schema is code. Test it with mutations that must fail in exactly one way, and
  validate the bytes consumers will actually read.

## 2026-10-07 — T1.10a: sorted JSON keys scrambled the HT4 buckets
- **Symptom:** the first `ground_truth.json` listed the HT4 buckets as `31-45`, `46-60`, `<=30`,
  `>60`.
- **Root cause:** byte-stable output needs `json.dumps(sort_keys=True)`, which sorts keys by code
  point. Digits (0x33, 0x34) come before `<` (0x3C) and `>` (0x3E). A reader walking the object in
  order, or a "declines monotonically" check, would compare the wrong neighbours.
- **Fix:** the buckets are an ordered list of `{bucket, decided, acceptance}`. `monotonic` is
  computed from the raw rates in bucket order, and a test pins the order.
- **Lesson:** canonical JSON is for bytes. When order means something, use a list.

## 2026-10-07 — T1.10a: I blamed the measurement for a sampling fluke (HT2)
- **Symptom:** at tiny (seed 1602), the realized HT2 ratio was 2.11 against a configured 1.8, at
  the top of its [1.5, 2.1] band.
- **First theory (wrong):** censoring in the stage history. Rejections are decided a little faster
  than advances, and req closures cut pending decisions short.
- **Root cause:** sampling noise. Recording the ratio as drawn at entry gave 2.14, so the gap was
  already in the draws. On this seed, referral applications advanced at 0.504 (expected 0.45,
  +2.4σ on 494) and career-site ones at 0.236 (expected 0.25, −1.7σ on 2,711). Seeds 1–4 at tiny
  draw 1.73–1.85.
- **Fix:** nothing in the simulation. `ground_truth.json` keeps `drawn_ratio` beside the realized
  ratio, so a reader can tell the draws from the measurement.
- **Lesson:** before explaining a gap with a mechanism, measure the quantity at its source and
  across seeds.

## 2026-10-08 — T1.10b: regular reqs weren't starved of traffic, they were starved of hires
- **Symptom:** the req fill rate was 0.40 against 0.80–0.92 (dev, seed 1602).
- **First theory (wrong):** posting-age decay, with a 21-day half-life, cut traffic before reqs
  could fill. Without any decay, the fill rate reached only 0.65, and time to fill went to 84 days.
- **Second theory (half right):** not enough traffic. Less skew and a higher apply rate tripled
  the applications per regular req, and the fill rate still stopped at 0.64–0.74.
- **What gave it away:** I listed the expired reqs. Many had 50–90 applications and never hired.
- **Root causes:**
  - **The funnel's hit rate:** about 1 hire per 60 applications, so 60 applications still leave a
    ~37% chance of no hire.
  - **Evergreen soaks up gains:** at ×25 popularity, the evergreen reqs took most of any gain in
    conversion. Their hires went from 259 to 624 in one trial.
- **Fix:** ADR-0016.
  - a funnel that needs fewer applications
  - applications early in a posting's life
  - evergreen ×7
  - a faster pipeline with a tail
- **Also learned:** one seed can't rank configs. Changing one parameter reshuffles the whole
  random path, and on the same seed the fill rate moved ±0.04 between near-identical configs. I
  compared the final candidates over five seeds.
- **Lesson:** look at what the failing units actually received before tuning the inputs. The
  expired reqs' application counts pointed at the funnel, not the traffic.

## 2026-10-08 — T1.10b: a fill's same-day closures were dropped
- **Symptom:** after tuning, `test_closed_reqs_reject_early_applications` found an application
  still in `applied` on a req filled 10 days before the window closed.
- **Root cause:**
  - **The order:** `ATS.step` pops today's closures and then decides today's offers.
  - **The delay:** a fill schedules a closure for each early applicant with a delay of
    U{0..5} days.
  - **Together:** 1 closure in 6 went into today's list after it had been read. This is the same
    class of bug as T1.7a's scheduler agenda.
- **Effect** (old config, dev, seed 1602):
  - 18 applications kept advancing after their req filled, through 13 interview stages.
  - The rest were rejected later, as `not_selected` instead of `position_filled`: wrong status
    reasons for silver and gold to count.
- **Fix:** closures run after the offers. A regression test forces the delay to 0.
- **Lesson:** after fixing a "schedule into the current tick" bug, audit every queue keyed by day.
  I should have done that in T1.7a. Done now: the ATS's other queues use delays of at least a day,
  or are read after everything that writes to them.

## 2026-10-08 — T1.10b: HT4 failed on half the seeds whatever I tuned
- **Symptom:** HT4's strict "acceptance never rises" check failed on 2 of 4 seeds for every
  candidate config, the final one included.
- **Root cause:** statistical power, not the simulation.
  - **The gap is small:** decay starts at day 30, so the ≤ 30 and 31–45 buckets differ by only
    about 2–3 points.
  - **The sample is small:** at dev those buckets hold about 250 and 340 offers, a standard error
    of about 3 points.
  - **Speed-ups made it worse:** they emptied the > 60 bucket to 1–3 offers.
- **Fix:** no tuning. ADR-0016 §4 explains the dev miss (Shubh's call). The pipeline gets a
  realistic tail so full has enough slow offers, and T1.11 checks it there.
- **Lesson:** before tuning toward a check, work out whether the sample can pass it. A check that
  fails half the time on correct data is measuring noise.

## 2026-10-09 — T1.11: I blamed the reorg for a slowdown the host caused
- **Symptom:** full run 1 went from about 1.7 s per simulated day to 6–31 s for days 410–482.
  The slowdown began right after day 409, the day of the reorg.
- **First theory (wrong):** the reorg made some per-day work grow, such as interviewer pools by
  org, or reqs moving org.
- **The test:**
  - an identical second run, logging three clocks every day
  - cProfile on days 405–480 (after the reorg), against days 300–375 as a control
- **What it showed:**
  - **The slow days moved:** run 2 was slow on days 201–240 instead, which run 1 had done at
    normal speed.
  - **The work tracked seasonality:** the reorg window made 1.35× the control's calls per day.
    That's seasonality, February to April against November to early January, at a ratio of
    1.34.
  - **Same work, different times.**
- **Root cause:** the WSL2 VM doesn't always get the host's CPU. Its kernel has no steal
  accounting, so the lost time shows up as the process's own CPU time. One function cost 118 µs
  a call in one window and 20 µs in the other.
- **Fix:** nothing in the code (ADR-0017 §2).
- **Lesson:** on a VM, rank performance by work that doesn't depend on the machine (call counts,
  events), not by time. Run the same thing twice before believing a slowdown.

## 2026-10-09 — T1.11: three clocks, three runtimes
- **Symptom:** one full run measured 34 min wall, 1,417 s by the report, and 1,341 s of CPU. The
  next, identical, took 12 h 20 min wall and 2,535 s by the report.
- **Root cause:**
  - **Wall time** counts VM stalls and the laptop's overnight sleep: the VM's clock is resynced
    afterwards.
  - **`perf_counter`** is monotonic and excludes suspend.
  - **CPU time** includes whatever the host took.
- **Fix:** record the report's monotonic runtime beside the CPU time, name the machine, and judge
  output by byte-identical hashes instead (ADR-0017).
- **Later:** a third run on a quiet machine did the same work in 824 s by the report and 752 s of
  CPU, the ATS load included. Run 1's 1,341 "CPU seconds" were about 45% time the host took.
- **Lesson:** a timing without its clock and its machine is an anecdote.

## 2026-10-09 — T1.11: Docker Desktop was running, but WSL couldn't reach it
- **Symptom:** after Docker Desktop started, `docker` in WSL said it "could not be found in this
  WSL 2 distro". Then `/usr/bin/docker` appeared, but `Cannot connect to the Docker daemon at
  unix:///var/run/docker.sock`. Polling for five minutes didn't help.
- **What I ruled out:**
  - **Group membership:** I'm in the `docker` group.
  - **The engine:** Docker Desktop's backend log said `engine running` at 16:47 UTC.
- **Root cause:** a minute later the WSL integration agent for Ubuntu-24.04 died: `running echo
  $HOME in Ubuntu-24.04: … The pipe is being closed`. That agent is what serves the socket inside
  WSL, so the socket file existed with nothing behind it. Docker Desktop showed a dialog offering
  to restart it.
- **Fix:**
  - Shubh clicked "Restart the WSL integration", and `_ping` answered `OK`.
  - The shell had also cached the Windows `docker` shim, so `hash -r` was needed.
- **Lesson:** "can't connect" with the engine up means the bridge, not the daemon. The reason was
  one grep away in the backend log (now in the RUNBOOK).
