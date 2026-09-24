# ADR-0006: Requisitions: growth plan, go-live pipeline, capped popularity

- **Status:** Accepted
- **Date:** 2026-09-24
- **Task:** T1.4
- **Deviates from:** `docs/SPEC.md` §6.4. It adds `requisitions.initial_pipeline_days` and
  `requisitions.popularity.truncate_at`, and fills in rules the spec leaves open.

## Context
SPEC §6.4 says reqs open by backfill, by growth "to reach `growth_annual`", and as evergreen seats.
They move from open, optionally through on hold, to filled or cancelled, and their popularity is
Pareto(`pareto_alpha`) normalized to mean 1. Several things are unspecified or unsafe:

- **Growth has two readings.** "5% of headcount a year as new seats" leaves the company roughly
  flat, because 30% of departures aren't backfilled and not every req fills. "Net growth of 5%"
  is what ADR-0004's steady-state start assumed.
- **Go-live would be empty.** Nothing says what is open on day 1. With no reqs, the job board
  (T1.5) and hiring would take weeks to ramp up.
- **Pareto(1.2) has infinite variance.** Normalized by its theoretical mean, the average
  popularity over 1,000 reqs swung between 0.62 and 1.46 across 200 seeds (worst seed 12.3), and
  one req reached 11,550× the mean. Views and applications scale with popularity, so event
  volumes and calibration would depend on luck.

## Decision

### 1. Growth follows a monthly headcount plan
On `sim_start` and on the 1st of every month:
- target = starting headcount × (1 + `growth_annual`)^(years since `sim_start`)
- growth seats = max(0, round(target at the next plan day − (current headcount + unfilled seats
  on open reqs + backfill seats already scheduled)))

The seats are split into reqs using `headcount_distribution`. Their open days are spread
uniformly across the month. Each growth req copies a random current employee's team, role
family, level and location, so reqs mirror the company's make-up. The plan is recorded so
calibration (T1.10) can check realised net growth.

It corrects itself for unbackfilled departures, cancellations, expired reqs, no-starts and evergreen
hires. When headcount is above target, no growth reqs open, but backfills still do.

### 2. The ATS goes live with a pipeline
- **Evergreen reqs** already exist on `sim_start`: max(1, round(`per_1000_headcount` × headcount /
  1000)) of them, opened on a random day in the prior `max_open_days`.
- **Other open seats on day 1** = the steady opening rate × `initial_pipeline_days` (45). The rate is
  headcount × (effective attrition + `growth_annual`) / 365, where effective attrition applies the
  first-year multiplier to the world's actual first-year share. A share of `backfill_probability` of
  the attrition part is backfill reqs (1 seat); the rest are growth reqs.
- **Their open dates** are spread over the 45 days before `sim_start`, and their lifecycles are
  sampled the same way as any other req. A req whose sampled cancellation falls before go-live is
  dropped, and one mid-hold on `sim_start` starts on hold.
- Measured on `sim_start` (seed 1602):

| Preset | Evergreen reqs | Other open reqs | Their seats |
|---|---|---|---|
| full | 20 | 507 | 549 |
| dev | 2 | 61 | 67 |
| tiny | 1 | 6 | 6 |
- **In-progress applications** for these reqs are T1.6's decision.

### 3. Popularity is a capped Pareto
The raw draw is Pareto(`pareto_alpha` = 1.2, x_m = 1) truncated at `truncate_at` = 200, divided by
the truncated distribution's exact mean. The median req is at 0.45 and the most popular at most
50.9× the mean. Over 1,000 reqs the average stays within 0.88–1.10 across 200 seeds, while the skew
§18/E8 needs remains. Evergreen reqs multiply their draw by `evergreen_multiplier` (25).
`pareto_alpha > 1` and `truncate_at > 1` are validated.

### 4. Backfills
Every termination except an org leader's opens a backfill with probability `backfill_probability`,
after U(`backfill_open_delay_days`) days. The org leader's role is filled by succession
(ADR-0005). The backfill copies the leaver's team, role family, level and location, with 1 seat.
The hiring manager is the leaver's manager if they are still employed and in that team; otherwise
the team manager. If the team has emptied by then, no req opens.

### 5. Other rules (no new parameters)
- **Evergreen attributes:** team weighted by headcount. Role family and level are drawn from
  `evergreen.role_families` and `evergreen.levels`, weighted by the org-wide shares. Seats are drawn
  from `headcount_range` and refilled to the full count on the 1st of every month. Evergreen reqs
  never hold, cancel or close.
- **Lifecycle:** at open, a hold is drawn with `on_hold_probability` and a random cancellation with
  `cancel_probability`. Each gets a day spread uniformly over `max_open_days`. A hold lasts
  U(`on_hold_days`). A req expires (status `cancelled`, close reason `expired`) at open date +
  `max_open_days`. Close reasons are `filled`, `cancelled`, `expired` and `team_dissolved`, so
  T1.10 can compute `req_fill_rate` without business cancellations.
- **Filling (the API for T1.6):**
  - `record_accept` takes one seat, and the req is filled when its last seat is taken.
  - `reopen_seat` puts a seat back after a no-start, reopening a filled req.
  - Evergreen reqs take seats until the monthly refill.
- **Keeping reqs current:**
  - If a hiring manager leaves or changes team, the team's current manager takes over. If the team
    has emptied, the req is cancelled as `team_dissolved`.
  - If a recruiter leaves, their reqs move to the least-loaded available recruiter. Ties are broken
    at random.
  - After a reorg, a req's org follows its team.
- **Identity:**
  - IDs are `R` + 6 digits in open order (go-live reqs sorted by open date).
  - Titles come from a built-in word list per role family plus a level prefix
    (Associate / — / Senior / Staff / Principal / Senior Principal).
  - Dates are kept per day. T1.6 adds times of day when it writes the ATS tables.
- **Random stream:** the existing `requisitions` stream. Workforce and HRIS output are unchanged.

## Alternatives considered
- **Growth as a fixed rate of new seats.** Rejected. Net growth ends near 0%, which contradicts §6.4
  "to reach" and ADR-0004's starting state.
- **An empty go-live.** Rejected. The job board would have only evergreen reqs for weeks, and every
  metric would show a start-up ramp. It needs no new parameter, but the ramp costs more.
- **An untruncated Pareto, or a switch to lognormal.** Rejected. The first is unstable, as measured.
  The second changes the distribution family the spec asks for; truncation keeps it and bounds the
  variance.
- **Backfilling org leaders.** Rejected. It would hire a second person into a one-person leadership
  team, although succession has already filled the role.

## Consequences
- Until T1.6 records hires, no req fills. Reqs only expire or are cancelled, and the growth plan
  keeps reopening seats as headcount falls, so T1.4 summaries overstate open reqs.
- Realised net growth is emergent, and T1.10 should report it next to the plan.
- The engine is cheap: `full` (546 days) runs in about 3 s. It consumes only the `requisitions`
  stream, and tiny's 89 HRIS file hashes are identical before and after it was added.
- The plan reacts with a month's lag. A burst of departures shows up as growth reqs the following
  month.
- When a req closes, T1.6 rejects its active applications that haven't reached onsite, with reason
  `position_filled` or `req_cancelled`.
- Heavy-tailed interviewer popularity (T1.7, Pareto 1.5) has the same infinite-variance problem;
  `sample_truncated_pareto` is reusable there.

## References
- `docs/SPEC.md` §6.4, §7.4 (`requisitions` table), §13 (M02), §18 (E8)
- `config/generator/base.yaml` → `calibration_targets.req_fill_rate`, `median_time_to_fill_days`
