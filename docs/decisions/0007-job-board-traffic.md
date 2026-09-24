# ADR-0007: Job-board traffic: sessions, sinks, and candidate ids

- **Status:** Accepted
- **Date:** 2026-09-24
- **Task:** T1.5
- **Deviates from:** `docs/SPEC.md` §6.5. It adds `jobboard.session.*` and
  `jobboard.bots.seconds_between_views`, and fills in rules the spec leaves open.

## Context
SPEC §6.5 defines expected views per open req-day and says views group into sessions with
optional search, home page, saves, and applications. It covers returning visitors, internal
browsing with the HT3 multipliers, bots, and schema v2's `device_type`. §7.1 and §7.3 fix the
envelope and payloads. The spec does not say:

- **how sessions start:** referrers, the width of the two daily peaks
- **how they unfold:** pauses between actions, which page events accompany a view, where a
  clicked result sits in the results
- **how bots pace themselves:** gold's bot rule looks for more than 60 views in 10 minutes
- **where events go before T1.8** builds delivery and file sinks
- **who assigns `candidate_id` and `application_id`:** `apply_submit` must carry both when it is
  emitted, but candidates belong to the ATS (T1.6)

The code may not hard-code distributions (§6), so each of these needs a parameter or a rule.

## Decision

### 1. New parameters
```yaml
jobboard:
  bots:
    seconds_between_views: [1.0, 8.0]             # uniform
  session:
    referrer_mix: { direct: 0.35, search_engine: 0.40, social: 0.10, email: 0.15 }
    seconds_between_events: { median: 25, sigma: 0.9 }   # lognormal pause before an action
    diurnal_spread_hours: 2.5
    results_position_geometric_p: 0.3
    filter_location_share: 0.35
    filter_role_family_share: 0.50
```
`p_employee_browses_per_day` × `browse_multiplier` and internal `p_apply_start_given_view` ×
`apply_multiplier` must each stay ≤ 1 (validated).

### 2. Volume and allocation (external)
- **Expected views:** λ(req, day) = `base_daily_views_per_open_req` × popularity ×
  `seasonality_by_month` × `day_of_week` × 0.5^(age / `posting_age_half_life_days`). Evergreen
  reqs don't decay, and internal-only reqs get no external views.
- **Sessions:** human sessions ~ Poisson(Σλ / E[size]). Each session has min(Geometric(p), cap)
  views.
- **Which reqs are viewed:** views are assigned to reqs in proportion to λ, so each req averages
  exactly its λ.
- **Bot sessions:** ~ Poisson(human sessions × share / (1 − share)), so bots make up
  `session_share` of sessions.
- **Hook for T1.6:** each day's λ per req is exposed for referral, sourced and agency
  applications (§6.6).

### 3. A session
- **Visitor:** with `returning_visitor_share` the session reuses an earlier visitor, who keeps their
  city and device. Otherwise it's a new visitor whose city is drawn by the location weights and
  device by `device_mix_v2`. The device is always known internally but only emitted from v2.
- **Start time:** one of the `diurnal_peaks_local_hour` peaks, plus Normal(0,
  `diurnal_spread_hours`), wrapped to the day in the visitor's timezone and converted with that
  city's UTC offset at local midnight. Sessions right after a DST switch can be an hour off.
- **Events:**
  - A searching session (`p_search_before_view`) begins with `page_view(home)`, `job_search`,
    `page_view(search_results)`. It uses the first viewed req's base title as query text (plus the
    city when filtering by location), counts matching open reqs as `results_count`, and places
    each click at min(Geometric(`results_position_geometric_p`), results).
  - Every view emits `page_view(job_detail)` followed by `job_view`.
  - A view may add `job_save`. It may add `page_view(apply_form)` + `apply_start`, then
    `apply_submit` + `page_view(confirmation)`.
  - Sessions that don't search land directly on a job page, with no position in results.
- **Timing:** each user action waits a lognormal `seconds_between_events`. Events that fire on the
  same page share its timestamp.
- **Bots:** a pool of `distinct_visitor_pool` visitors, `known_bot_user_agent_share` of whom send
  known bot user agents. A bot session views U(`job_views_per_session`) reqs chosen uniformly (a
  crawl), starts uniformly across the UTC day, waits U(`seconds_between_views`) between views,
  never searches or applies, and has referrer `direct`. Bots with more than 60 views in 10
  minutes break gold's rule. Spoofed bots with 30–60 views should mostly go undetected.
- **Internal sessions (HT3):**
  - Each active employee browses with p × `browse_multiplier` if ≥
    `tenure_in_role_threshold_days` in role (the engine's role clock, ADR-0005), and applies at
    `p_apply_start_given_view` × `apply_multiplier`. The expected long/short application ratio is
    3.0.
  - Employees see every open req except their own team's, weighted like external views, and use
    the external session-size parameters.
  - Referrer is `internal_portal`, the context carries `employee_id`, and each employee has one
    stable visitor id.

### 4. Envelope
- **Event ids:** UUIDv4 built from the `jobboard` stream.
- **Times:** `event_ts` is epoch milliseconds. `sent_ts` = `event_ts` + U(0, 5 s), as an ISO-8601 UTC
  string ending in `Z`.
- **Versions:** `schema_version` changes from 1 to 2, and `producer_version` from `3.2.0` to
  `3.3.0`, from the resolved `chaos.schema_v2.jobboard` date. v2 adds `context.device_type`.
- **Ids:**
  - Visitor and session ids are counters mapped through a salted bijection (`v-…`, `s-…`), so they
    look random but can never collide.
  - User agents come from built-in lists per device, plus known bot user agents.
- **Ordering:** each day's events are emitted in `event_ts` order.

### 5. Sinks before T1.8
Events flow through an `EventSink` interface. T1.5 ships a counting sink, which the CLI uses, and a
collecting sink for tests. T1.8 puts the delivery queue, chaos and file and Kinesis sinks behind the
same interface. `silent_schema_break` and `duplicate_storm` belong to T1.8's chaos layer.

### 6. Candidate and application ids
A shared registry assigns `A` + 9-digit application ids and `C` + 8-digit candidate ids:
- An external visitor who applied before keeps their candidate.
- A new visitor reuses a random existing external candidate with `reapply_probability`.
- Internal applicants get one candidate per employee.

Each `apply_submit` becomes a `Submission` (application, candidate, req, channel, time) for the ATS
(T1.6). T1.6 adds names, emails and phones to candidates and uses the same registry for referral,
sourced and agency applications.

## Alternatives considered
- **Write JSONL files now.** Rejected. It would take over T1.8's arrival-hour bucketing, file rolling
  and delivery queue, and then get rewritten.
- **Only `job_view` per view (no page views).** Rejected. §7.3 lists `page_view` with `job_detail`,
  and web trackers emit both. The bot share of events is about 12.6% with both (inside the
  5–15% target) and about 6.7% without.
- **Filters that constrain every view in a session.** Rejected. It would need per-session req
  sampling. Instead filters come from the first viewed req, so the first click always matches.
- **Hard-coded pauses, referrers and peak widths.** Rejected by SPEC §6.

## Consequences
- Expected cost at steady state after T1.6 (seed-independent estimate): about 6.9 events per human
  session and about 330 per bot session. That's roughly 27M events at `full`, above the 10M target,
  and T1.11 measures it.
- Until T1.6 fills reqs, open reqs pile up (ADR-0006), so T1.5 alone overstates traffic at `full`.
  Tests run at tiny and dev.
- Evergreen reqs drive about two thirds of views (popularity × 25). T1.10 calibrates base views
  against `applications_per_hire` with that in mind.
- The job board draws only from the `jobboard` stream; workforce, requisitions and HRIS output are
  unchanged.

## References
- `docs/SPEC.md` §6.2, §6.5, §7.1, §7.3, §10.4 (bot rules), §13 (M10), §13.2 (HT3 band)
- `config/generator/base.yaml` → `calibration_targets.clickstream_bot_event_share`
