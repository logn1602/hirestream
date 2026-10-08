# ADR-0015: Ground truth, calibration, and the generation report

- **Status:** Accepted
- **Date:** 2026-10-07
- **Task:** T1.10a (measurement). T1.10b tunes parameters to the targets, with its own ADR.
- **Deviates from:** SPEC §13.2, in where its hidden-truth bands live: they move into the config
  (section 7 below). The ADR also pins what each calibration target and ground-truth count means,
  which §6.10 and the config's one-line comments leave open.

## Context
SPEC §6.10 asks every run for `ground_truth.json` (realized facts before chaos) and
`generation_report.md` (counts, calibration pass/warn against `calibration_targets`, runtime, peak
RSS). §6.11 wants calibration within target at dev and full, or an ADR that explains a miss.
§13.2 will compare the warehouse with the ground truth: ATS counts exactly, stream counts within
the injected loss, and HT1–HT4 within bands.

Left open:
- **Definitions.** Which reqs count toward the fill rate, what "time to hire" runs from and to, at
  what grain feedback is "within 48 h".
- **Months.** An offer accepted at 16:30 in Seattle on January 31 is February 1 in UTC. §13.2's
  exact match needs one answer.
- **What silver should quarantine.** The expected counts per reason have to follow ADR-0014 §5.
- **Determinism.** The report's runtime and memory differ between runs of the same seed.

Measuring dev (seed 1602) before writing any of this showed one target far off: the req fill rate,
0.40 against 0.80–0.92. Fixing it changes how the simulation behaves, and so every number it
produces. In the same PR as the measurement, nothing would stay still long enough to review the
measurement against.

## Decision

### 1. Two PRs
- **T1.10a (this ADR):** ground truth, calibration and the report. No simulated number moves:
  every existing pin stays as it was.
- **T1.10b:** tune parameters to the targets at dev, checked over five seeds, with an ADR for every
  parameter change (CLAUDE.md).

### 2. Files
Both go in `<lake>/_runs/<run_id>/`, beside `manifest.json`.
- **`ground_truth.json`:**
  - **Format:** `version: 1`, sorted keys, floats rounded to 6 digits, ASCII, a trailing newline.
  - **Written:** atomically, before the manifest.
  - **Hashed:** its sha256 is the manifest's `ground_truth_sha256`, which is part of
    `deterministic_view()`. Two runs of a seed must write the same bytes.
- **`generation_report.md`:**
  - **Written:** last, and never hashed.
  - **Varies:** only its title (run id), creation time, runtime and peak RSS. A test compares two
    same-seed reports with those lines removed.
- **CLI:** prints `calibration: N/M within target; warn: …` and the three paths.

### 3. Where the facts come from
- **Engine truth:** chaos, job board, scheduling and HRIS counters, recorded as things happen.
- **The ATS's final state:** the same rows the warehouse will extract.
- **Never bronze:** nothing reads it back. Reading it would grade the pipeline with its own input.

### 4. Months are UTC
- **Rule:** an ATS fact's month is the UTC month of its stored timestamp. Offers extended use
  `extended_ms`; accepted, declined and rescinded use `decided_ms`; hires use the offer's
  `start_date`.
- **Why:** the warehouse stores UTC (CLAUDE.md: "All timestamps are UTC after bronze"), so it can
  reproduce these counts exactly.

### 5. The ATS section
Counts by month × channel × internal, from the final state:
- **Offers:**
  - `offers_extended`: every offer, by extension month
  - `offers_accepted`, `offers_declined`, `offers_rescinded`: by decision month
  - `offers_pending`: still undecided when the window closed
- **Hires and starts:**
  - `hires`: applications whose final status is `hired`, by start month. A no-start's offer stays
    `accepted`, but its application doesn't stay `hired`.
  - `starts`: people who joined the workforce inside the window, as M08 counts them.
- **Applications:** by channel.

### 6. Calibration definitions
Measured the way SPEC §13's metrics will compute them. Every band is inclusive.

| Target | Definition |
|---|---|
| `req_fill_rate` | filled / (filled + expired) over non-evergreen reqs closed in the window. Cancelled reqs, dissolved teams' reqs and reqs still open are excluded: they aren't failures to fill. |
| `median_time_to_fill_days` | `closed_on − opened_on` for filled non-evergreen reqs. `closed_on` is the day of the final accepted offer (M02). |
| `median_time_to_hire_days` | offer `decided_ms` − application `applied_ms`, in days, for applications whose final status is `hired` (M01) |
| `applications_per_hire` | all applications / applications whose final status is `hired` |
| `offer_acceptance_rate` | accepted / (accepted + declined). Rescinded and pending offers aren't decisions (M07). |
| `internal_fill_rate` | internal starts / all starts inside the window (M08) |
| `application_channel_mix.*` | applications by channel / all applications |
| `clickstream_bot_event_share` | bot events / all job-board events, before chaos |
| `feedback_within_48h_share` | feedback submitted within 48 h of the interview's end / expected feedback (completed interviews × interviewers, `fct_interview`'s grain). Late and missing feedback both count against it. |
| `hidden_truth_bands.ht1`–`ht3` | the realized ratios (§7) |
| HT4 | acceptance never rises from one non-empty bucket to the next |
| event floor | events both stream producers emitted, before chaos, ≥ `scale.target_min_total_events` |

- **Nothing to measure:** a metric with nothing to measure (no decided offers, say) is `n/a`. That
  counts as a warning, not a pass.
- **A miss warns; it never fails the run.** Tiny is too small to calibrate, and §6.11 allows an
  ADR-explained miss. T1.10b and T1.11 hold dev and full to the targets.

### 7. Hidden truths: measured as the warehouse can measure them
- **HT1:** median feedback latency, overloaded / normal. Each submitted feedback is labelled by
  its interviewer's final load in the interview's week (ADR-0010).
- **HT2:** first-gate pass rate, referral / career_site.
  - **Decisions:** from the stage-change history, where a decision is advanced, `not_selected` or
    `candidate_withdrew` at `applied`.
  - **Closures** (req filled or cancelled, the applicant left) aren't screening decisions.
  - **`drawn_ratio`:** the ratio as drawn at entry, recorded alongside. It tells a reader whether a
    surprising realized ratio comes from the draws or from the measurement.
- **HT3:** internal applications per employee-day, long / short tenure in role. §13.2 says "per
  employee-month". The ratio of two rates doesn't depend on the time unit, so it's the same
  number.
- **HT4:** offer acceptance by days-to-offer bucket.
  - **Order:** an ordered list, because a JSON object's sorted keys would put `<=30` after `>60`.
  - **`monotonic`:** acceptance never rises from one non-empty bucket to the next.
- **Bands:** ht1 [1.6, 2.4], ht2 [1.5, 2.1], ht3 [2.4, 3.6], the §13.2 values. They move into
  `calibration_targets.hidden_truth_bands`, so the report, T5's `check-ground-truth` and the config
  hash all read one source.

### 8. Streams and expected quarantine
- **Per source:**
  - **Events:** emitted, by type
  - **Lines delivered:** duplicates by kind (regular, storm), malformed lines by kind
  - **`lost`:** every copy malformed
  - **`unusable`:** no copy silver can keep, because every copy is malformed or the event lost its
    `timezone`
  - **`expected_silver_events`:** events − unusable
  - **Lateness:** lag bands, the late burst
  - **Incident counts:** renamed lines (silent schema break), unresolvable-timezone lines
  - **Scheduling's `timezone_bug`:** naive starts and missing timezones, by event type
- **`expected_quarantine`:** lines per SPEC §9.1 reason, mapped as ADR-0014 §5 says:
  - `truncated_json` → `MALFORMED_JSON`
  - `missing_required_field` → `MISSING_REQUIRED_FIELD`
  - `invalid_enum` → `INVALID_ENUM`
  - a well-formed line whose start is naive and whose `timezone` is gone → `UNRESOLVABLE_TIMEZONE`

  The other three reasons are zero by construction, and listed so silver's tests can compare
  every reason.
- **Lines vs events:** the counts are lines, not events, because silver quarantines lines.
- **Checked against bronze:** a test classifies every bronze line with the contracts (ADR-0014
  §5's precedence) and must find exactly these counts.

### 9. HRIS
Missing days, renamed files, the duplicate row (day, employee), the partial file (day, rows kept,
rows dropped), changes deferred to a later file, and late exports.

### 10. Runtime and memory
- **Runtime:** `time.perf_counter()` from the start of `run_backfill` to the report.
- **Peak RSS:** `getrusage(RUSAGE_SELF).ru_maxrss`, the whole process's peak. That's KiB on Linux
  and bytes on macOS. In a test session it's the session's peak, so only CLI runs give
  per-run numbers.

## Results
- **Tiny, seed 1602:**
  - **Every bronze line classifies as predicted.** Job board: 88 `MALFORMED_JSON`, 82
    `MISSING_REQUIRED_FIELD` and 83 `INVALID_ENUM` in 258,604 lines. Scheduling: 2
    `MALFORMED_JSON` and 4 `UNRESOLVABLE_TIMEZONE` in 4,119 lines.
  - **Calibration:** 8 of 18 within target. Tiny is too small to judge: 2 starts and 34 decided
    offers.
  - **HT2:** realized 2.109, drawn 2.138, against a configured 1.8. That's noise, not bias: seeds
    1–4 at tiny draw 1.73–1.85. The realized ratio tracks the drawn one.
- **Dev, seed 1602:**
  - **Calibration:** 17 of 18 within target. The miss is the req fill rate, 0.396 against
    0.80–0.92. That's T1.10b's job.
  - **Hidden truths:**
    - HT1: 1.980
    - HT2: 1.798 realized, 1.795 drawn, against a configured 1.8
    - HT3: 3.089
    - HT4: declines, 0.871 → 0.807 → 0.750 → 0.333 across the buckets
  - **Volume:** 1,825,768 stream events against the 1M floor.
  - **Cost:** 94.6 s in the report (1 min 36 s wall clock with interpreter start), 177 MiB peak
    RSS.
  - **Headcount:** 3,000 → 2,877 (−4.1%) against a plan of +5% a year. There's no target for it;
    the report shows it for T1.10b, because reqs that expire unfilled shrink the workforce.

## Alternatives considered
- **Recount from bronze.** Rejected (§3). Ground truth has to come from before chaos, or it can't
  grade silver.
- **Months by the local business day.** Rejected. The warehouse would need every fact's timezone
  to reproduce them, and §13.2 wants an exact match.
- **Bands as constants, or only in the SPEC.** Rejected. Two copies drift. In the config they sit
  with the other targets and under the config hash.
- **Fail the run on a miss.** Rejected. Tiny can't calibrate, and §6.11 allows explained misses.
- **Hash the report.** Rejected. Runtime and memory would break determinism.
- **HT2 from the drawn outcomes only.** Rejected. The warehouse can't see draws, so the band is
  checked on what it can see. The drawn ratio is kept as a diagnostic.
- **Count cancellations as failures to fill.** Rejected. A business cancelling a req says nothing
  about recruiting, and the config's target excludes them.

## Consequences
- **T1.10b:** tunes against these numbers, over five seeds at dev.
- **T1.11:** reads full's runtime and peak RSS from the report.
- **T2.2/T2.3:** silver's quarantine tests can compare with `expected_quarantine`.
- **T5:** `metrics check-ground-truth` compares the warehouse with `ground_truth.json`, and reads
  the HT bands from the config.
- **Changes to the ground truth's shape** bump `version`.

## References
- `docs/SPEC.md` §6.10, §6.11, §9.1, §13
- ADR-0008, ADR-0010, ADR-0012, ADR-0014
