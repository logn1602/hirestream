# ADR-0004: Initial workforce: steady-state start and org-structure rules

- **Status:** Accepted
- **Date:** 2026-09-24
- **Task:** T1.2
- **Deviates from:** `docs/SPEC.md` §6.3. It adds `meta.company_founded` and fills in rules §6.3 leaves open.

## Context
The world builder creates Halcyon's workforce as it stands on `sim_start`. §6.3 fixes the structure:
every team has an L6+ manager, every org has an L8 leader, levels, role families and locations follow
the configured shares, and spans come from `span_of_control`. It gives no distributions for how long
initial employees have been at the company or in their role, or whether any are on leave. The code
may not hard-code distributions (§6), and new parameters or distributions need an ADR (`CLAUDE.md`).

These fields matter downstream:
- the first-year attrition multiplier (tenure < 365 days)
- promotion eligibility (`promotion_min_days_in_level`)
- HT3 (tenure in role ≥ 548 days)
- the HRIS `hire_date` and `job_effective_date` columns

## Decision

### 1. Start from the steady state of the configured dynamics
The initial workforce is drawn from the distribution the configured workforce rates would produce
after running for a long time. The simulation then has no warm-up transient: on day 1, first-year
attrition, promotions and leaves already run at their long-run levels.

- **Tenure τ:** the density is ∝ e^{−gτ}·S(τ). Hiring cohorts shrink with the growth rate
  g = `growth_annual`. The survival S(τ) uses the attrition rate λ = `attrition_annual`, multiplied by
  `attrition_first_year_multiplier` in the first 365 days. That is piecewise exponential, with rate
  g + λ·m before day 365 and g + λ after, truncated at `meta.company_founded`.
- **Time in role:** min(A, τ). A is the backward-recurrence time of role changes, with rate
  c₀ = `lateral_move_annual` + `location_change_annual` before `promotion_min_days_in_level` and
  c₀ + `promotion_annual` after. The top level (L8) can't be promoted, so its rate is c₀
  throughout. This is exact for memoryless changes and approximate given the promotion floor.
  `job_effective_date` = `sim_start` − 1 − time in role.
- **On leave at start:** Bernoulli(`leave_annual` × mean(`leave_duration_days`) / 365). The duration is
  uniform over `leave_duration_days` and the elapsed part is uniform within it, capped by tenure.
  The return date is kept for T1.3.

| At `full` (prototype) | Value |
|---|---|
| Employees in their first year | 16% |
| Median tenure (90th percentile) | 4.0 years (12.8) |
| Median time in role | 2.3 years |
| At or beyond HT3's 548 days in role | 64% |
| On leave on `sim_start` | ≈ 100 (dev ≈ 12, tiny ≈ 1) |

### 2. One new parameter
`meta.company_founded: 2003-01-01`, the earliest possible hire date. It caps tenure and must be
before every preset's `sim_start` (validated).

### 3. Org structure
- **Orgs:** the first `org_count` of `org_model.orgs`.
- **Teams:** each org gets a number of teams drawn uniformly from `teams_per_org`. Names are unique
  across the company, such as "Payments Platform", built from two fixed word lists in code.
- **Team sizes:** headcount minus the org leaders is split across teams with an equal-probability
  multinomial, so org sizes differ through their team counts. No new parameter.
- **Leadership team:** each org leader is the only member of a "<Org> Leadership" team, so every
  HRIS row has a team. It is flagged, and the reorg (T1.3) skips it. Org leaders have no manager.
- **Management tree:** team managers report to their org leader. Within a team of B people below
  the manager, a node with budget b ≤ hi has b IC reports. Otherwise:
  1. It takes k ∈ [lo, min(hi, b − lo)] reports, of which j are managers.
  2. j is chosen so the remaining b − k people split evenly, with lo..hi each where possible.

  Every manager except org leaders then has **lo..hi direct reports**, which is 5–9 with the current
  config. Org leaders manage `teams_per_org` team managers, 4–10. The config must satisfy
  hi ≥ 2·lo − 1, which guarantees the split exists (validated), and each team needs at least lo + 1
  people (checked at build time).

### 4. Levels
- **Counts:** exact counts from the shares, using largest-remainder rounding.
- **Org leaders:** the top level, L8.
- **Managers of managers:** L7 if available, otherwise L8, then L6.
- **First-line managers:** L6 if available, otherwise L7, then L8.
- **Individual contributors:** the remaining levels, shuffled.
- **Failure:** the build fails if there are not enough L6+ levels for all managers.

### 5. Role families, locations, identity
- **Role families and locations:** exact counts from the shares, assigned at random, independent
  of team, org and level.
- **IDs:** `E` + 6 digits, issued in hire-date order with random tie-breaks. New hires (T1.3)
  continue the sequence.
- **Names:** first and last name only, from a Faker instance per country
  (`meta.faker_locale_by_country`), seeded from the `world` stream. No titles, no gender, no
  demographic attributes.
- **Emails:** `first.last@<internal_email_domain>`, folded to ASCII (`D’Alia` → `dalia`), with
  `2`, `3`, … appended in ID order when names repeat.
- **Other §7.5 fields:** `ats_candidate_id` and `termination_date` are empty for initial employees,
  who were hired before the ATS window.

## Alternatives considered
- **Explicit tenure and time-in-role distributions in the config.** Rejected. It adds parameters to
  tune, and they can drift out of step with the workforce rates. Any mismatch shows up as a
  warm-up trend in the first months of every metric.
- **Hire dates uniform since founding.** Rejected. It gives a flat tenure distribution with only about
  4% first-year employees, so first-year attrition would climb through the whole simulation.
- **Managers drawn at random from all L6+ levels.** Rejected. About 60% of department heads would be
  L6 with L7 managers below them.
- **A primary role family per team.** Rejected for now. It would need a new parameter, and no metric
  depends on it.
- **Generating people terminated in the 90 days before `sim_start`.** Rejected for now. It affects
  no metric (see Consequences).

## Consequences
- The simulation has no warm-up transient, so the WBR 6-12 charts won't show a trend that comes only
  from how the world was initialised.
- Tuning attrition, growth or promotion in T1.10 also reshapes the initial workforce. This coupling is
  intended; remember it when reading calibration diffs.
- Accepted simplifications:
  - Role family and location don't depend on team.
  - An IC's level doesn't depend on their manager's level, so an L8 principal can report to an L6
    manager.
  - Level doesn't depend on tenure.
- The first HRIS snapshots contain terminated rows only from in-window terminations. They reach
  steady state after `terminated_retention_days`, and no metric depends on it.
- A golden fingerprint of the tiny world (seed 1602) fails on any change to the world builder,
  Faker, or numpy. Update it deliberately, in the PR that changes the data.

## References
- `docs/SPEC.md` §6.3 (world and workforce), §7.5 (HRIS columns), §10.3 (SCD2 first sighting uses `hire_date`)
