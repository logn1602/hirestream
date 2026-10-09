# ADR-0017: Full-preset acceptance: what was measured, and what wasn't changed

- **Status:** Accepted
- **Date:** 2026-10-09
- **Task:** T1.11

## Context
SPEC §6.11 asks full for:
- ≥ 10 M stream events
- calibration within target
- every event meeting its contract, checked on a sample
- ≤ 45 min and ≤ 4 GB peak memory on a 16 GB laptop, with the actuals recorded in the README

Earlier ADRs left T1.11 more:
- **ADR-0012:** the runtime levers (gzip level, parallel writes, GC)
- **ADR-0014:** the full-preset contract sample
- **ADR-0015:** runtime and peak RSS from the report
- **ADR-0016:** HT4 and E8's key shares at full
- **ADR-0009:** the ATS load time at full

**The machine:** WSL2 Ubuntu on a Windows laptop, with 8 vCPUs and a 3 GB VM. Its kernel reports
no steal time.

## Decision

### 1. Results: full, seed 1602

| | Measured | Target |
|---|--:|---|
| Stream events | 22,944,582 (job board 22.2 M, scheduling 0.77 M) | ≥ 10 M |
| Bronze lines | 23,288,857 in 26,314 hourly parts, 1.55 GB | |
| HRIS | 545 files, 14.3 M rows, 425 MB | |
| ATS | 546,487 applications, 10,322 offers, 7,465 hires | |
| ATS load into Postgres | 51 s for 2.39 M rows, 363 MB (§9) | |
| Runtime (the report) | 824 s on a quiet machine, the ATS load included; 1,417 s and 2,535 s on a busy one (§2) | ≤ 2,700 s |
| CPU time | 752 s quiet; 1,341 s busy | |
| Peak RSS | 1,276–1,294 MiB | ≤ 4 GB |
| Calibration | 18 of 18 | all |

### 2. Which clock is the runtime
- **The report's runtime:** `time.perf_counter()`, the process's monotonic clock (ADR-0015 §10).
  It doesn't count time the machine was suspended. This is the number recorded.
- **Three runs, identical work** (the same seed and bytes):

| Run | Conditions | The report | CPU | Wall |
|---|---|--:|--:|--:|
| 1 | `--skip-ats-db` | 1,417 s | 1,341 s | 34 min 10 s |
| 2 | `--skip-ats-db`, two profiled windows, three other jobs running | 2,535 s | 2,333 s | 12 h 20 min (the laptop slept) |
| 3 | with the ATS load, nothing else running | 824 s | 752 s | 13 min 22 s |

- **Wall time** counts VM stalls and suspend, because the VM resyncs its clock afterwards. Run 3's
  wall time even comes out shorter than its monotonic time: the clock was stepped back mid-run.
- **CPU time:** without steal accounting, the VM charges time the host took away to whatever was
  running. So CPU time also overstates the work. Run 3 shows by how much: run 1's 1,341 s of
  "CPU" did work that took run 3 about 735 s (752 s less the load's 18 s). Day by day, the same
  days cost 1.7 CPU-seconds in run 1 and 4–6 in run 2.
- **What actually ranks work:** counts that don't depend on the machine.
  - **The suspect:** run 1 slowed sharply after day 409, the day of the reorg.
  - **The test:** profiling run 2's days 405–480 against days 300–375 gave 1.35× the function
    calls per day, and 1.38× the job-board sessions. That's seasonality: February to April
    (×1.11 on average) against November to early January (×0.83), a ratio of 1.34.
  - **The verdict:** there's no algorithmic slowdown, and the slow stretches were the host.

### 3. No performance changes
- **Headroom:** full uses a third to a half of the 45-minute budget, and a third of the 4 GB.
- **Where the time goes** (dev profile, which scales about linearly to full):
  - building job-board events: about 17%
  - event ids and timestamps: about 11%
  - serialization: about 10%
  - gzip: about 10%
  - garbage collection: about 15%, timed at dev
  - folder creation: under 1% at full, since folders scale with hours, not events
- **ADR-0012's levers, measured but not taken:**
  - **gzip level:** stays at 1. Level 6 is 2.5× slower for files 17% smaller.
  - **GC tuning:** it would save at most its share, about 15%. A gain that size can't be
    verified on this machine, where a dev run took 73 s in one run and 502 s in one that shared
    the machine. The targets don't need it either.
  - **Parallel writes:** folder creation is under 1% at full.
- **Output first:** any later speed-up must leave dev's manifest hashes unchanged, so that it
  can't move a calibrated number.

### 4. Memory
- **Peak:** 1.3 GiB, inside this VM's 3 GB and SPEC's 4 GB.
- **Growth:** about 2 MiB per simulated day, from what the ATS keeps for its final state and the
  ground truth: applications, stage changes, offers and candidates.
- **Streams aren't kept:** the delivery queue holds about a day of lines.

### 5. Determinism at full
All three full runs (§2), with and without the ATS load, wrote identical hashes for all 26,859
files and the same ground truth. SPEC only requires this at tiny.

### 6. Contracts at full, read back from bronze
`tests/unit/generator/test_full_preset.py` (marker `full`) reads a full lake given by
`HIRESTREAM_FULL_LAKE`, and is skipped otherwise.
- **Scheduling:** all 780,880 lines. Each is valid or carries a fault that chaos or the timezone
  bug explains, and the counts per reason equal `expected_quarantine` exactly.
- **Job board:** every 50th line, 450,160 lines.
  - **Each line:** valid, or carrying exactly one chaos fault.
  - **Reason counts:** within 4 standard deviations of the ground truth's rates.
  - **Coverage:** every job-board contract is met at least once.
- **Cost:** 10 min 41 s.
- **Why bronze, rather than `ValidatingSink` inside the simulation** (ADR-0014's suggestion):
  - **The bytes:** it validates what consumers will read, chaos lines included.
  - **No second run:** it needs no second 25-minute simulation.

### 7. Calibration and hidden truths at full

| Metric | Full | Target |
|---|--:|---|
| req_fill_rate | 0.847 | 0.80 to 0.92 |
| median_time_to_fill_days | 54.0 | 35 to 60 |
| median_time_to_hire_days | 38.4 | 25 to 45 |
| applications_per_hire | 68.8 | 50 to 150 |
| offer_acceptance_rate | 0.799 | 0.70 to 0.85 |
| internal_fill_rate | 0.136 | 0.10 to 0.25 |
| feedback_within_48h_share | 0.751 | 0.72 to 0.90 |
| HT1 / HT2 / HT3 | 1.925 / 1.806 / 3.009 | §13.2 bands |
| HT4, decided offers @ acceptance | 3,408 @ 0.831 → 5,159 @ 0.798 → 1,257 @ 0.761 → 397 @ 0.657 | declines |

- **Everything else:** the channel mix and bot share pass too.
- **HT4:** declines, as ADR-0016 §4 predicted. The > 60-day bucket holds 397 offers, inside the
  150–400 forecast.

### 8. E8's hot keys at full
Job-board lines with a `req_id`, counted per req:

| | Full |
|---|--:|
| The 20 evergreen reqs' share | 31.7% |
| Each evergreen req | 0.86–4.97% (median 1.31%) |
| Top req / top 10 reqs | 4.97% / 21.2% |
| An evergreen req / the median regular req | 65–377× |
| The busiest regular req / the median | 25× |

- **Against ADR-0016's forecast:** it estimated about 1.4% per evergreen req. The median is 1.31%,
  but one evergreen req's Pareto draw puts it at 5%.
- **For E8:** a key holding 5% of the rows is a strong skew case.

### 9. The ATS load at full
ADR-0009 left the Postgres load time at full to T1.11. Measured in run 3, with only the `ats-db`
container running (no Metabase):

| Table | Rows | In Postgres |
|---|--:|--:|
| application_stage_changes | 1,369,622 | 177 MB |
| applications | 546,487 | 97 MB |
| candidates | 459,879 | 77 MB |
| offers | 10,322 | 2 MB |
| requisitions | 4,682 | 1 MB |
| **Total** | **2,390,992** | **363 MB** |

- **Time:** 51 s, as one transaction: drop, the DDL's keys and checks, then `COPY` per table.
- **Where it goes:** 18 s is the generator's own CPU (formatting and hashing the CSV); the rest
  is Postgres.
- **Memory:** flat on the generator's side (rows are formatted lazily). The Postgres container
  rose from 34 MiB to 274 MiB. Both together fit in the 3 GB VM.
- **The first attempt failed for another reason:** Docker Desktop was running, but its WSL
  integration had crashed. That's a RUNBOOK entry now.

## Alternatives considered
- **Optimize anyway** (GC thresholds or `gc.freeze`, faster id and timestamp formatting, gzip
  level, parallel writes). Rejected: the targets are met with 2–3× headroom, the gains can't
  be measured here, and each change risks the bytes.
- **Validate inside the simulation at full with `ValidatingSink`.** Rejected (§6).
- **Validate every job-board line at full.** Rejected: about 22.5 M validations, roughly two
  hours at the measured rate. A 1-in-50 sample still sees about 150 lines of each fault kind,
  enough to measure each rate to about ±8%.
- **Report wall time as the runtime.** Rejected: it counted a night's sleep.
- **Treat the slow days after the reorg as a bug.** Rejected after the test in §2: a second run
  moved the slow stretch to other days, and the call counts track seasonality.

## Consequences
- **Phase 1's exit criteria are met:**
  - determinism (the tiny test, and three full runs identical)
  - calibration at dev (ADR-0016) and at full
  - full ≥ 10 M events
- **Phase 2** gets a 23.3 M-line full lake to tune silver against.
- **T5's ground-truth check at full** can rely on HT4.
- **A later speed-up** must keep dev's manifest hashes, and should be judged by its effect on
  work, not one timing.
- **T2.4 (the ATS extract)** starts from a 2.39 M-row ats-db that loads in under a minute.

## References
- `docs/SPEC.md` §6.1, §6.9, §6.11, §13.2, §18.3 (E8)
- ADR-0009, ADR-0012, ADR-0014, ADR-0015, ADR-0016
