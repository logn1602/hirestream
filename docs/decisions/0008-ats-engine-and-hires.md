# ADR-0008: ATS engine: stages, offers, hires, and an ATS go-live

- **Status:** Accepted
- **Date:** 2026-09-25
- **Task:** T1.6 (engine in T1.6a; Postgres sink in T1.6b)
- **Deviates from:** `docs/SPEC.md` §6.6. It uses a stand-in for interview timing until T1.7 and
  fills in rules §6.6 leaves open.

## Context
SPEC §6.6 describes:
- **Intake:** direct-channel applications per req-day from expected external views, candidates
  created on first application with `reapply_probability`.
- **Stages:** the stage machine with HT2's first-gate channel multiplier; time in `applied` and
  `recruiter_screen` is lognormal, while time in `phone_screen` and `onsite` emerges from
  scheduling.
- **Offers:** HT4's acceptance decay, start dates, and no-starts 1–10 days after the start date.

§6.3 says new hires come only from ATS hires. The spec does not say:
- how interview stages behave before the scheduling engine (T1.7) exists
- what the ATS contains on `sim_start`
- how a hire becomes an employee, and what happens when an internal hire's old seat is left empty
- how offers interact with a req's seats
- what a no-start does to HRIS and to the req

## Decision

### 1. Two PRs
T1.6a builds the engine in memory (this ADR). T1.6b adds `sql/ats_source/`, the `PostgresSink`
(`COPY`), and integration tests.

### 2. Stages
- **Decision up front:** each stage's outcome is drawn when an application enters it, from
  `p_advance`/`p_withdraw` for external or internal applicants. At `applied` the advance
  probability is multiplied by `first_gate_channel_multiplier[channel]`, capped at 0.95 (HT2). The
  outcome takes effect when the stage's time is up.
- **How long a stage takes:**
  - `applied` and `recruiter_screen`: lognormal `stage_delay_days`, × `rejection_delay_multiplier`
    for a rejection, rounded to at least 1 day.
  - `phone_screen` and `onsite`: until T1.7, a stand-in behind an `InterviewTiming` interface:
    lognormal `lead_days` + one lognormal feedback latency (capped at `feedback_wait_cap_days`) +
    `interview_stage_decision_delay_days`. The scheduling engine replaces it with emergent timing,
    and interview recommendations arrive with it.
- **Timestamps:** stage changes are stamped in business hours in the req's city.
- **`changed_by`:** the recruiter for screens and offers, the hiring manager for interviews,
  `candidate` for submissions, withdrawals and decisions, and `system` for automatic rejections.

### 3. Intake
- **Job board:** each submission becomes an application with the event's timestamp.
- **Direct channels:** referral, sourced and agency applications per req-day are
  Poisson(`direct_apps_per_1000_external_views` × λ / 1000), with λ from the job board. They come
  from new candidates in the req's city, or an existing external candidate with
  `reapply_probability`.
- **Candidates:**
  - **Names:** Faker per country (from a seed derived from the `ats` stream).
  - **Emails:** unique `first.last@example.{com,net,org}` addresses.
  - **Phones:** only from ranges reserved for fiction (US 555-01xx, GB 07700 900xxx; elsewhere an
    unassignable leading 0), so no generated number can reach a real person.
  - **Internal candidates:** reuse the employee's name and work email.

### 4. Offers, seats, hires
- **Offers:** an offer goes out only while the req has a free seat (open seats minus undecided
  offers). Otherwise the application is rejected with `position_filled`. Acceptance is drawn at
  extension with HT4's probability; the decision comes after lognormal `decision_delay_days`.
- **Acceptance:** an accepted offer takes a seat (`record_accept`), and a req whose last seat is
  taken closes as filled. The application becomes `hired` and gets a start date after lognormal
  `start_delay_days`.
- **External hire on the start date:**
  - **Identity:** a new employee with the next `E` id, the candidate's name, a new work email, and
    `ats_candidate_id`.
  - **Attributes:** team, org, role family, level and city come from the req.
  - **Manager:** the req's hiring manager. Evergreen hires go to the least-loaded people manager in
    the team, so one manager doesn't collect hundreds of reports.
  - **HRIS:** hires are never exported late.
- **Internal hire on the start date:**
  - **The move:** the employee takes the req's team, org, role family and level, and reports to the
    hiring manager. Their own reports are handed on by the ADR-0005 succession rules.
  - **Backfill:** the seat they left opens a backfill with `backfill_probability`, like a
    departure.
  - **If they can't start:** if they left the company or the team emptied, the offer becomes a
    no-start with reason `could_not_start`.
- **No-start:** decided at acceptance with `no_start_probability`. The employee is never created (X-02
  excludes no-starts). The application flips to `no_start` 1–10 days after the start date, and the
  seat is given back (`reopen_seat`).
- **Closed reqs:** when a req is filled, cancelled or expires, its active applications before
  onsite are rejected (`position_filled` / `req_cancelled`) after U(`req_closed_rejection_delay_days`).
- **Leavers:** when an employee leaves, their active internal applications are withdrawn
  (`left_company`).

### 5. The ATS goes live empty
The ATS has no applications before `sim_start`. It starts with the go-live reqs (ADR-0006), and hires
begin about 6–8 weeks in, a ramp-up treated as the ATS going live on `sim_start`. A warm start (90 days
of pre-window history) was rejected for now; T1.10 may revisit it.

### 6. Ground truth
Counts are recorded as they happen (SPEC §6.10): applications by channel, first-gate outcomes by
channel (HT2), offers, acceptances and declines by month × channel × internal, hires by start month,
acceptance by days-to-offer bucket (HT4), and no-starts.

## Alternatives considered
- **One PR.** Rejected. It would be about 2,000 lines, too much to review well.
- **Wait for T1.7 before building the ATS.** Rejected. It would block HRIS hires, req filling, and
  every downstream task. The stand-in uses only existing parameters and sits behind an interface.
- **Warm-start the ATS.** Rejected for now. It adds about 400 lines, data before the window, and
  complexity for T2.4's watermark extraction.
- **Extend offers without checking seats.** Rejected. A req would accept more hires than it has
  seats, and the accumulating snapshot would show impossible fills.
- **Create the employee even for a no-start.** Rejected. X-02 excludes no-starts, which implies they
  never appear in HRIS.

## Consequences
- The simulation's loop now closes: hires fill reqs, join the workforce, and appear in HRIS the same
  day.
- Hires grow the population the workforce draws its daily hazards over, so every later workforce draw
  moves once the first hire starts. It stays deterministic, but workforce outcomes differ from a run
  without an ATS. The pinned CLI numbers changed deliberately.
- Measured, seed 1602:

| Preset | Applications | Offers | Hires (internal) | No-starts | Backfill wall time | Peak memory |
|---|---|---|---|---|---|---|
| tiny | 3,881 | 50 | 15 (0) | 0 | ~5 s | — |
| dev | 26,118 | 524 | 377 (82) | 14 | 37 s | 114 MB |

- **Calibration gaps for T1.10** (dev):
  - **In range:** applications per hire ≈ 69 (target 50–150), offer acceptance ≈ 0.82
    (0.70–0.85), internal fill rate ≈ 22% (10–25%), median time to fill 53 days (35–60), and HT2's
    ratio 1.8.
  - **Out of range:** `req_fill_rate` is ≈ 39% (target 80–92%). Evergreen reqs take about 71% of
    hires, and regular reqs expire with a median of 9 applications. Headcount also shrinks
    (≈ −3%/year against a +5% plan).
  - **What to tune:** base views, `evergreen_multiplier`, and evergreen seats. Tuning needs an ADR.

## References
- `docs/SPEC.md` §6.3, §6.4, §6.6, §7.4, §12.2 (X-02, X-03), §13.2 (HT2, HT4 bands)
