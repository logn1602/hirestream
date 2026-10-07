# ADR-0011: Scheduling schema v2 and the timezone-bug build

- **Status:** Accepted
- **Date:** 2026-10-06
- **Task:** T1.7 (T1.7b)
- **Deviates from:** nothing in the spec. It fills in what §6.7, §6.8 and §7.2 leave open and adds
  one parameter, `scheduling.onsite.in_person_share_v2`.

## Context
The spec sets these points:
- **Panels (§6.7):** from schema v2, an onsite session is a two-person panel with
  `panel_session_probability_v2`, listed in an `interviewer_ids` array.
- **What v2 changes (§7.2):** only `interview_scheduled` changes. `interviewer_id` becomes
  `interviewer_ids`, and the event gains `interview_format` (`virtual`, `in_person`).
- **The bug (§6.8):** for 14 days, producer `1.3.0` writes `scheduled_start`, `previous_start` and
  `new_start` as naive local time. 2% of those events also lack `payload.timezone`, so they are
  unresolvable.
- **Silver (§9.1):** it unifies v1 and v2, reads naive starts in `payload.timezone` (setting
  `start_tz_inferred`), and quarantines starts with no timezone as `UNRESOLVABLE_TIMEZONE`.
- **`fct_interview` (§10.4):** its grain splits a panel into two rows, each with its own feedback.

The spec does not say:
- what a panel means for load, feedback and disruptions
- where the format comes from
- which events the bug touches, and which version numbers come before and after it
- when an event counts as coming from a given build
- how the bug's random draws stay out of the simulated reality

## Decision

### 1. When v2 and the buggy build apply
By each event's own `event_ts` (UTC date), so "every event from the switch date on is v2" holds
exactly:

| From (UTC date of `event_ts`) | `schema_version` | `producer_version` |
|---|---|---|
| `sim_start` | 1 | `1.2.4` |
| `chaos.scheduling_tz_bug` start (0.30), for 14 days | 1 | `1.3.0` (the buggy build) |
| the day after the bug | 1 | `1.3.1` (the hotfix) |
| `chaos.schema_v2.scheduling` (0.50) | 2 | `2.0.0` (a major bump: a scalar became an array) |

The bug is a v1 build. A config whose bug window reaches the v2 switch fails when the scheduler
starts, with an error that names both dates.

### 2. Panels
- **Who gets one:** from v2, each onsite session is a two-person panel with 0.25. Phone screens
  never are.
- **The second panelist:** chosen by the same rules as the first (ADR-0010 §4), excluding the first
  panelist and the rest of the loop. If the only person left is already seated (the hiring-manager
  fallback), the session stays single.
- **Load:** the session counts toward both panelists' weekly loads.
- **Feedback:** each panelist writes their own, with their own latency (their own overload and
  slowness), their own recommendation from the stage's outcome, and their own `feedback_id` and
  chance of revision. `feedback_submitted.interviewer_id` stays a scalar: one event per panelist.
  The stage waits for both.
- **Disruptions:** handled per session. If either panelist has left or is on leave, the session is
  cancelled (`interviewer_unavailable`) and replaced, and the replacement draws its own panel. A
  no-show applies to the session.
- **The 48-hour share:** taken over completed interviews × their interviewers, which is
  `fct_interview`'s rows.

### 3. Format
- **Phone screens:** always `virtual`.
- **Onsite loops:** each loop draws its format once, `in_person` with `in_person_share_v2` (0.40).
  Every session and replacement in the loop shares it.
- **When it shows:** the format is drawn for every loop but emitted only from v2, the same way the
  job board handles `device_type` (ADR-0007).
- **Time zone:** in-person and virtual loops alike stay in the req's office time zone.

### 4. The timezone bug
- **Affected events:** `interview_scheduled` and `interview_rescheduled` whose `event_ts` falls in
  the window.
- **What changes:** their starts are written as local wall-clock time without the offset (for
  example `2025-04-22T10:30:00`). The wall time is correct; only the offset is missing, so silver
  can rebuild the instant from `timezone`. Interviews are in business hours, nowhere near a DST
  transition, so the rebuild is never ambiguous.
- **Dropped timezones:** `missing_timezone_share` (2%) of affected events also drop `timezone`, so
  they are unresolvable.
- **Other events in the window:** they carry producer `1.3.0`, but their payloads don't change.
  `actual_start`, `actual_end`, `event_ts` and `sent_ts` stay UTC `Z`.
- **Truth:** `naive_starts` and `missing_timezone`, by event type, for T1.10's ground truth (expected
  `start_tz_inferred` rows and `UNRESOLVABLE_TIMEZONE` quarantines).

### 5. The bug changes bytes, never reality
- **Its own stream:** the 2% draws come from a new `scheduling_chaos` stream. Streams are keyed by
  name, and the eight existing golden values did not move.
- **No other randomness:** formatting is a pure function of the event's time.
- **The proof:** a test moves the window and raises `missing_timezone_share` to 0.5. Every
  interview, stage change and workforce event stays identical, and so do the events once their
  starts are converted back to UTC.
- **Panels and formats are different:** they are reality and use the `scheduling` stream.

## Results
- **Default seed (1602, dev):**
  - 11,987 interviews, of which 996 are panels
  - HT1 1.98, from 1,517 overloaded feedbacks
  - 75% of expected feedback within 48 h
  - timezone bug: 547 naive starts, 10 of them without a timezone
- **Seeds 1–8 (dev):** HT1 runs from 1.81 to 1.98, tighter than ADR-0010's 1.70–2.22, because panels
  give HT1 more interviewer-weeks. Each seed has 334–1,530 naive starts and 6–30 unresolvable
  events (about 2%).
- **tiny:** 151 panels, 261 naive starts, 4 without a timezone. All four producer versions appear.
- **Runtime:** dev takes 28 s on this branch against 26–31 s on main, run back to back: no
  measurable cost.

## Alternatives considered
- **Switch versions by simulation day, as the job board does.** Rejected. Scheduling events are
  stamped in local business hours and can fall on the next UTC day, so the switch wouldn't be exact.
  Job-board events never leave their UTC day.
- **Roll back to `1.2.4` after the bug.** Rejected. Fixes ship forward. Either way, silver has to
  detect naive starts by their format, not by version.
- **Inject the bug in T1.8's chaos layer.** Rejected in the T1.7 plan. The producer had the bug,
  and a chaos layer would have to re-parse payloads to fake it.
- **Draw the 2% from the `scheduling` stream.** Rejected. Moving the bug would then shift every
  later interview.
- **One feedback per panel.** Rejected. §10.4's grain wants each panelist's own feedback.
- **Replace only the unavailable panelist.** Rejected for now: more state for a rare case.
- **A format per session.** Rejected. A candidate comes onsite for a loop, not for one session.

## Consequences
- **Silver (Phase 2) receives:**
  - scalar `interviewer_id` before the switch and `interviewer_ids` arrays after it
  - naive starts from `1.3.0`, which become `start_tz_inferred`
  - dropped timezones, which become `UNRESOLVABLE_TIMEZONE`
  - four producer versions
- **T1.9's contracts:** they need v1 and v2 schemas for `interview_scheduled`. The buggy build's
  starts break v1's "with offset" rule, so T1.9 decides whether the contracts describe the bug or
  validation leaves those events out.
- **T1.10's ground truth:** it gets the bug counters, and the 48-hour share on `fct_interview`'s
  grain.
- **Load:** panels add interviewer load in the second half of the window. The results above show
  HT1 staying in band.

## References
- `docs/SPEC.md` §6.7, §6.8, §7.1, §7.2, §9.1, §10.4
- ADR-0007 (the job board's v2 `device_type`), ADR-0010 (the scheduling engine)
