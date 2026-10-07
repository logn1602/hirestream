# ADR-0010: Scheduling engine: interviews, interviewer load, and feedback (HT1)

- **Status:** Accepted
- **Date:** 2026-10-06
- **Task:** T1.7 (engine in T1.7a; schema v2 panels and the timezone-bug build in T1.7b)
- **Deviates from:** `docs/SPEC.md` §6.7 in three places:
  - only trained employees interview
  - the over-cap weight compounds
  - chronic slowness is spread evenly over interviewer popularity

  §4 explains why. The ADR also fills in rules §6.7 leaves open.

## Context
SPEC §6.7 describes the scheduling service:
- **Interviews:** a phone screen is one interview; an onsite is a loop of 4–5 sessions, all on one
  day with `same_day_probability`.
- **Interviewer selection:** employees at or above the req's level, same org with
  `same_org_probability`, weighted by Pareto popularity; the weight is multiplied by
  `over_cap_weight_multiplier` once the interviewer passes `weekly_soft_cap`.
- **Disruptions:** reschedules, cancellations and no-shows.
- **Feedback latency:** lognormal × `overload_multiplier` when the interviewer is over the cap
  that week (HT1) × `chronic_slow_multiplier` for chronically slow interviewers.

The surrounding sections add:
- §6.6 makes time in interview stages emergent.
- §7.2 fixes the events.
- §10.4 defines `interviewer_weekly_load` (non-cancelled interviews that ISO week) and
  `is_overloaded` (load > soft cap).
- §13.2 expects the warehouse to recover HT1 as a median-latency ratio, overloaded / normal, in
  [1.6, 2.4].

The spec does not say:
- how the ATS and the scheduler hand a stage back and forth
- where and when interviews happen (time zones, business hours, loops spread over days)
- what replaces a cancelled interview, and what a no-show does to the stage
- when HT1's multiplier is decided, given that a week's load can still change afterwards
- how event timestamps relate to each other

Building it also showed that, with the selection rule as written, HT1 cannot be measured (§4).

## Decision

### 1. Two PRs
- **T1.7a:** the engine, interviewer selection, disruptions, feedback and HT1, v1 events, ATS
  integration, and truth.
- **T1.7b:** schema v2 (two-person panels with `interviewer_ids`, `interview_format`) and the
  producer `1.3.0` timezone bug.

### 2. Handoff with the ATS
- **Outcome first:** the ATS still draws each stage's outcome on entry (ADR-0008 §2). For
  `phone_screen` and `onsite` it calls `Scheduler.begin`, which books the interviews and returns
  the first one's date. Recommendations are drawn from `recommendation_given_decision` for that
  outcome.
- **Withdrawals:** the candidate withdraws U{1..d} days after entry, where d is the number of days
  until the first interview. Pending interviews are cancelled.
- **Decisions:** the scheduler calls `stage_ready` once every interview has happened and either all
  feedback is in or `feedback_wait_cap_days` have passed since the last one. The ATS decides
  `interview_stage_decision_delay_days` later (at least one day).
- **Closures:** when the ATS closes an application in an interview stage, its pending interviews
  are cancelled 5–60 minutes after the close, with these reasons:

  | Why the application closed | Cancel reason |
  |---|---|
  | withdrawal, or the internal candidate left the company | `candidate_withdrew` |
  | the req was filled | `position_filled` |
  | the req was cancelled or expired | `other` |
- **Stand-in kept:** ADR-0008's stand-in stays behind `InterviewTiming` for ATS tests that don't
  need interviews.

### 3. When and where
- **Phone screen:** on the business day at or after entry + lognormal `lead_days` (at least 1), in
  the interviewer's time zone.
- **Onsite loop:** 4–5 sessions (uniform), in the req's office time zone. With
  `same_day_probability` they run back to back on one day; otherwise one session per consecutive
  business day.
- **Start times:** quarter-hour slots inside `business_hours_local`, on weekdays only.
  `scheduled_start` is local ISO-8601 with its offset.
- **Booking time:** a business-hours moment on the booking day in the req's city, at least one
  hour before the start.

### 4. Interviewer selection (deviations A–C)

**The rule:**
- **Eligibility:** level ≥ the req's level + `min_level_offset`, active (not on leave or gone), and
  never the applicant.
- **Search order:** same org with `same_org_probability`, then any org.
- **Choice:** weighted by popularity (truncated Pareto, α 1.5, capped at 50 like ADR-0006's req
  popularity) with up to 64 accept/reject tries per search.
- **Loops:** a loop never uses the same interviewer twice, replacements included.
- **Pools:** rebuilt once per day from numpy arrays; a new hire can interview from their second
  day.

**A — a trained pool.** `trained_share: 0.10` of employees are trained interviewers, a stable
trait drawn with popularity. Only they are picked. If no trained candidate accepts, any eligible
employee steps in, and the req's hiring manager last.

**B — a compounding penalty.** A candidate whose load is at or past the cap is accepted with
probability `over_cap_weight_multiplier`^(load − cap + 1). That is ×0.3 at the cap, ×0.09 one past
it, and ×0.027 two past it. The multiplier is now validated to lie in (0, 1].

**C — slowness spread over popularity.** Among trained interviewers, chronic slowness is assigned
by systematic sampling down the popularity ranking from a random start. Each interviewer is still
slow with probability `chronic_slow_share`, but every popularity band holds its share of slow
people. Untrained employees still draw slowness independently.

**Why the spec's rule fails.** At dev, about 11k interviews a year spread over about 3,000 eligible
employees is 0.07 interviews per person-week. Under the spec's rule:
- Only about 1% of feedback came from an overloaded week (≥ 5 interviews), from 5 interviewers.
- The median ratio therefore measured whether those 5 happened to be chronically slow. It came out
  at 4.5–5.5.

**Why A.** Real companies concentrate interviewing on a trained subset. With 10% trained, 3–50% of
feedback comes from overloaded weeks, depending on application volume.

**Why B.** A flat ×0.3 then let a few very popular interviewers reach 34–79 interviews a week when
volume was high. Compounding keeps the weekly maximum near 10 and spreads overload over many
interviewers.

**Why C.** When volume is low, about 10 heavily loaded people carry all the overloaded feedback.
With independent draws, luck decided whether any of them were slow: 1–16% of overloaded feedback
came from slow interviewers across seeds, which moved HT1 between 1.59 and 2.11.

### 5. Disruptions
- **Planning:** each booking is planned once.
  - With 0.15 it is rescheduled; once it has been moved `max_times` times, it happens as booked
    instead.
  - Otherwise, with 0.05 it is cancelled.
  - Otherwise it happens as booked.

  The day of a reschedule or cancellation is uniform over the days before the interview.
- **Reschedules:**
  - **New date:** the business day lognormal `delay_days` (at least 1) after the later of today and
    the old date, in a new slot.
  - **Who and why:** `initiated_by` follows the config; the reason follows the initiator (candidate
    → `candidate_conflict`, interviewer → `interviewer_conflict`, coordinator → `other`).
  - **After a move:** each reschedule is planned again.
- **Random cancellation** (`other`): a replacement is booked with a new id and a new interviewer,
  on the same loop and session.
- **Interviewer unavailable:** if the interviewer left or went on leave after booking, the interview
  is cancelled two hours before its start (`interviewer_unavailable`) and replaced.
- **No-shows:**
  - **Rates:** candidate 0.03, interviewer 0.01.
  - **Recording:** logged `no_show_recorded_after_minutes` (15) after the start.
  - **What follows:** rebooked with `no_show_reschedule_probability` as a coordinator reschedule
    (within `max_times`); otherwise the stage goes ahead without it.
- **Completion:** the actual start is the scheduled start + U{0..10} minutes
  (`completion_jitter_minutes`); the actual end adds the duration.

### 6. Feedback and HT1
- **Latency:** lognormal(18 h, σ 1.0) × 2.0 if the interviewer's load that week is over the cap when
  the interview completes × 3.0 if chronically slow.
- **Missing and revised feedback:** 2% is never submitted. 3% is revised later: one more latency
  after submission, with a fresh recommendation.
- **Load:** the interviewer's non-cancelled interviews in the ISO week of the scheduled start's UTC
  date, which is the warehouse's `interviewer_weekly_load`. A reschedule moves the interview's count
  to its new week.
- **Labels:** the multiplier uses the load at completion. Interviews booked later that week, or
  cancellations, can still change it; this affects about 1–3% of feedback. Truth labels feedback by
  the final weekly load, as the warehouse does, so the generator's HT1 is the number the warehouse
  should recover.
- **Truth:** latencies of feedback actually submitted inside the window. `within_48h` = completed
  interviews with feedback within 48 h ÷ completed interviews.

### 7. Events
- **Envelope:** §7.2 v1, producer `1.2.4`, partitioned by `interview_id`. `event_ts` is the moment
  of the action; `sent_ts` is 0–5 s later.
- **Ordering:**
  - each interview's events are strictly ordered, at least 1 s apart, across time zones
  - a replacement is booked 5–60 minutes after the cancellation it replaces
  - a closure cancellation never comes before the ATS close
- **Ids:** `I` + 9 digits, `L` + 8 digits, `F` + 9 digits.
- **Same-day actions:** each day's agenda is drained, so an action scheduled for today while today
  runs (feedback minutes after an interview) still happens today.

## Results
- **Default seed (1602, dev):**
  - HT1 = 1.97, from 1,176 overloaded feedbacks across 44 interviewers
  - 76% of completed interviews get feedback within 48 h (target 0.72–0.90)
  - 76% of interviewers come from the req's org
  - the maximum weekly load is 9
- **Seeds 1–8 (dev):** HT1 runs from 1.70 to 2.22, inside [1.6, 2.4] every time. The overloaded
  share follows application volume (276 to 12,825 feedbacks from 16 to 151 interviewers). The
  maximum weekly load is 6–11.
- **Progression:** independent slowness draws gave 1.59–2.11 on the same seeds (seed 3 out of
  band); the spec's rule gave 4.5–5.5.
- **Within 48 h:** the share is 0.68–0.78. The busiest seeds sit under 0.72, which is the volume
  variance left for T1.10.

## Alternatives considered
- **Keep "any eligible employee" and get overload from more volume.** Rejected. Volume comes from
  reqs and the job board (T1.10), and overload would still sit on a handful of people.
- **Lower `weekly_soft_cap` to 2.** Rejected. It is unrealistic, changes a spec value, and leaves
  the concentration problem.
- **Remove the popularity cap.** Rejected. Even more of the load lands on a few superstars.
- **A hard weekly cap.** Rejected. The spec wants overload to still happen.
- **Decide latency at the end of each week, from the final load.** Rejected. Feedback hours after
  an interview could no longer be emitted in order.
- **Freeze an interviewer's week once their feedback is drawn.** Rejected. It is contrived, and
  the mismatch it removes is only 1–3%.
- **Independent slowness draws.** Rejected. They swing HT1 across seeds (§4 C).

## Consequences
- **M04 (§13):** interviewer loads look realistic: weekly p90 of 3–6 interviews and a maximum
  around 10.
- **tiny:** about 4× busier per interviewer than dev. About two-thirds of feedback is overloaded,
  57% of completed interviews get feedback within 48 h, and fewer than 70% of interviewers come
  from the req's org. Calibration is checked at dev, in a slow test, and in T1.10.
- **Volume variance:** application volume still varies about 4× across seeds. That is T1.10's
  problem; the overloaded share and `within_48h` move with it.
- **T1.10:** builds ground truth from `Scheduler.summary()` and `truth`.
- **T1.7b:** adds panels, `interview_format`, and the timezone bug on top of these events.
- **Runtime:** dev goes from about 22 s to about 25 s (measured back to back on main and this
  branch). Peak memory is about 135 MB, against about 150 MB on main.

## References
- `docs/SPEC.md` §6.6, §6.7, §7.2, §10.4 (`fct_interview`), §13 (M04, M05), §13.2 (HT1)
- ADR-0006 (popularity caps), ADR-0008 (the ATS and its interview-stage stand-in)
