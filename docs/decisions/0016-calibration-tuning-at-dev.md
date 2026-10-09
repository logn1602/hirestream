# ADR-0016: Calibration tuning at dev, and HT4 as an explained miss

- **Status:** Accepted
- **Date:** 2026-10-08
- **Task:** T1.10b
- **Deviates from:** ADR-0006 §3's values: `pareto_alpha` 1.2 → 2.0 and `evergreen_multiplier`
  25 → 7. It also adds `hirestream generate calibrate` to SPEC §5.3's CLI. The other changes are
  config values that no spec section or earlier ADR fixes.

## Context
T1.10a measured dev at seed 1602: 17 of 18 within target, with the req fill rate at 0.396
against 0.80–0.92 (ADR-0015). Seeds 1603 and 1604 passed 14 of 18. Measured on the old config at
dev, seed 1602:

- **Regular reqs got too few applications.**
  - **Supply:** the median expired req received 9 applications in its life.
  - **Demand:** an external hire took about 60. The funnel passes 0.25 × 0.50 × 0.40 × 0.35 =
    1.75% of career-site applications to an offer, and about 80% of offers are accepted.
- **Popularity decided who filled.** Pareto(1.2), normalized to mean 1, puts the median req at
  0.45× the mean. Reqs below the median filled 11–17% of the time, the top decile 83%.
- **Evergreen took the hires.**
  - **Share:** the two evergreen reqs had 61% of the applications and 259 of the 379 hires.
  - **They absorb any conversion gain.** Whatever makes an application convert better reaches
    them too. At ×25 with the higher apply rate (a trial config), evergreen hires reached 624 of
    812, headcount grew 7% against a 5% plan, and feedback within 48 h fell to 0.717 under the
    interview load.
- **Decay wasn't the cause.** With no posting-age decay at all, the fill rate rose only to 0.650,
  and median time to fill went to 84.5 days.
- **Internal-only reqs filled 17%.** They are 10% of reqs, only employees can apply, and few do.
- **Time to fill had no room.** Median time to hire was 42 days, so a req had to receive its
  eventual hire within about 18 days to fill in under 60.

The targets pull against each other:
- **Fill rate against time to fill.**
  - **When a req fills:** if its eventual hire applies before the req expires, which leaves 180
    days minus a ~40-day pipeline.
  - **Time to fill:** that wait plus the pipeline.
  - **What both need:** eventual hires arriving at about 0.05 a day for a typical req when it's
    posted. The old config gave the median req 0.007: 0.45 applications a day, at 1 hire in 61.
- **Fill rate against applications per hire.**
  - **More traffic alone** would take about 6× the views on regular reqs.
  - **Waste:** every application a req receives while its hire is in the pipeline is wasted, so
    applications per hire would pass 150.
  - **Volume:** full would produce 60 M+ events, beyond T1.11's 45-minute budget.

## Decision

### 1. A five-seed sweep
- **Command:** `hirestream generate calibrate --preset P [--seed N ...]`, or `make calibrate
  PRESET=dev [SEEDS="1 2"]`.
- **What it does:** backfills each seed into a throwaway lake, one at a time, so a dev run's
  memory is freed before the next. It keeps only the calibration checks and never touches
  `data/lake` or ats-db.
- **Output:** a line per seed, then a table with every target per seed, its min and max, and how
  many seeds missed.
- **Seeds:** `meta.seed` and the next four (1602–1606) unless `--seed` is given.

### 2. Parameters
Grouped by the problem each one solves. Measured values are dev, seed 1602, unless a range is
given.

**More applications, early in a posting's life, for regular reqs**

| Parameter | Was | Now | Why |
|---|--:|--:|---|
| `requisitions.popularity.pareto_alpha` | 1.2 | 2.0 | Median req 0.45 → 0.71× the mean, bottom decile 0.28 → 0.53×. The top 1% still get ≥ 5×. |
| `jobboard.external.base_daily_views_per_open_req` | 19 | 30 | Early applications, which is what time to fill needs |
| `jobboard.external.p_apply_start_given_view` | 0.06 | 0.10 | 3.9% → 6.5% of job views end in an application |
| `ats.direct_apps_per_1000_external_views` | 6.7 / 5.6 / 1.7 | 11.2 / 9.3 / 2.8 | × 5/3 with the apply rate, so the channel mix holds |
| `jobboard.posting_age_half_life_days` | 21 | 35 | A posting keeps half its views for 5 weeks, not 3. Evergreen reqs never decay, so only regular reqs gain. |
| `jobboard.internal.p_apply_start_given_view` | 0.10 | 0.15 | Applicants for internal-only reqs. The internal share would otherwise fall to its 3% floor as external volume rises. |

**A funnel that needs fewer applications per hire**

| Parameter | Was | Now | Why |
|---|--:|--:|---|
| `ats.external.p_advance.phone_screen` | 0.40 | 0.50 | With onsite, 1.75% → 2.81% of career-site applications reach an offer. Across channels, about 1 hire in 38 applications instead of 61. |
| `ats.external.p_advance.onsite` | 0.35 | 0.45 | As above |

The first gate and its channel multipliers (HT2) don't change. Neither do the internal gates.

**Evergreen hiring stays where it was**

| Parameter | Was | Now | Why |
|---|--:|--:|---|
| `requisitions.popularity.evergreen_multiplier` | 25 | 7 | A unit of popularity now yields about 4× the hires: 1.58× the views, 1.67× the applications per view, 1.6× the hires per application. At ×7 evergreen hiring stays near its old level, 168–350 a year at dev against 114–259. ADR-0008 had named it as the knob to tune. |

**A faster pipeline with a realistic right tail**

| Parameter | Was | Now | Why |
|---|--:|--:|---|
| `ats.stage_delay_days.applied` | {5, 0.6} | {3, 1.1} | Shorter median, longer tail: p99 20 → 39 days, so some applications sit for weeks |
| `ats.stage_delay_days.recruiter_screen` | {6, 0.5} | {4, 1.0} | As above: p99 19 → 41 days |
| `ats.interview_stage_decision_delay_days.sigma` | 0.5 | 0.8 | The p90 wait after the last feedback goes from 2.8 to 4.2 days |
| `scheduling.onsite.lead_days.median` | 7 | 6 | One day of room for time to fill |

- **Time to hire:** the median drops from 42 to 37–38.5 days.
- **Why the tail matters:** speeding the pipeline up without it left HT4's > 60-day bucket with
  1–3 offers at dev. With it, that bucket holds 12–31 (the old config had 6–9). Days to offer
  have p90/p50 1.41–1.52.

**Unchanged on purpose:**
- the hidden-truth effect sizes (HT1–HT4)
- offer acceptance and its decay
- the first-gate rates
- chaos
- every target and band

### 3. Results: dev, five seeds
`make calibrate PRESET=dev` with this config:

| Metric | Target | Min | Max | Seeds within |
|---|---|--:|--:|--:|
| req_fill_rate | 0.80 to 0.92 | 0.859 | 0.887 | 5 of 5 |
| median_time_to_fill_days | 35 to 60 | 53.0 | 54.5 | 5 of 5 |
| median_time_to_hire_days | 25 to 45 | 37.0 | 38.5 | 5 of 5 |
| applications_per_hire | 50 to 150 | 68.2 | 78.7 | 5 of 5 |
| offer_acceptance_rate | 0.70 to 0.85 | 0.779 | 0.826 | 5 of 5 |
| internal_fill_rate | 0.10 to 0.25 | 0.143 | 0.165 | 5 of 5 |
| channel_mix.agency | 0.01 to 0.05 | 0.029 | 0.030 | 5 of 5 |
| channel_mix.career_site | 0.60 to 0.78 | 0.686 | 0.691 | 5 of 5 |
| channel_mix.internal | 0.03 to 0.10 | 0.059 | 0.071 | 5 of 5 |
| channel_mix.referral | 0.08 to 0.16 | 0.115 | 0.121 | 5 of 5 |
| channel_mix.sourced | 0.06 to 0.14 | 0.098 | 0.099 | 5 of 5 |
| clickstream_bot_event_share | 0.05 to 0.15 | 0.105 | 0.122 | 5 of 5 |
| feedback_within_48h_share | 0.72 to 0.90 | 0.750 | 0.755 | 5 of 5 |
| ht1_ratio | 1.6 to 2.4 | 1.887 | 2.127 | 5 of 5 |
| ht2_ratio | 1.5 to 2.1 | 1.781 | 1.839 | 5 of 5 |
| ht3_ratio | 2.4 to 3.6 | 2.883 | 3.224 | 5 of 5 |
| ht4_monotonic | declines | | | 4 of 5 (§4) |
| stream_events | ≥ 1,000,000 | 1,530,801 | 1,825,809 | 5 of 5 |

Across the five seeds:
- **Fill rate:** public reqs fill 0.89–0.94 and internal-only reqs 0.29–0.44 (0.17 before).
- **Evergreen:** takes 168–350 of 505–631 hires, 33–55%; the old config gave 44–68%.
- **Headcount:** grows 0.9–2.7% over the dev year, where it shrank 4–7%. The growth plan targets
  +5%, but it counts open seats as headcount, so actual headcount lags the plan by the seats
  still in the pipeline (ADR-0006 §1).
- **Runtime:** 93–98 s per seed when nothing else is running.
- **Expired reqs:** have a median of 48 applications now (filled reqs: 64). They are unlucky, not starved.

### 4. HT4 at dev is an explained miss
- **The check:** ADR-0015 §6's strict one, acceptance never rises from one non-empty bucket to the
  next. It stays as written, and so does SPEC §13.2.
- **The miss:** seed 1606. Acceptance goes from 0.836 (238 offers, ≤ 30 days) to 0.848 (330 offers,
  31–45 days), a rise of 1.2 points. The difference has a standard error of 3.1 points.
- **Why tuning can't remove it:**
  - **The effect is small near the threshold.** Acceptance falls 0.4 points per day after day 30
    (the hidden truth's stated effect), so the 31–45 bucket sits only about 2–3 points below
    ≤ 30. Across the five seeds the gap measured −1.2 to +3.9 points.
  - **The sample is small.** Dev decides about 650 offers a year, so that gap's standard error is
    about 3 points.
  - **The slower buckets don't help.** Their gaps are larger, but they hold only 61–95 and 12–31
    offers.
  - **Odds per seed:** each of the three comparisons holds 75–85% of the time, and all three
    together about half the time. Four of these five seeds held.
- **What would remove it:**
  - **A bigger effect:** the hidden truth fixes 0.4 points a day.
  - **Wider buckets:** M07 fixes them.
  - **A check that tolerates noise:** see Alternatives.
- **At full:** full decides about 12× the offers, so the first gap's standard error is about 0.9
  points, and a chance rise has odds of about 0.3%. The > 60-day bucket would hold roughly
  150–400 offers. T1.11 checks HT4 at full.
- **Buckets at dev**, as decided offers @ acceptance:

| Seed | ≤ 30 | 31–45 | 46–60 | > 60 | Declines |
|---|--:|--:|--:|--:|---|
| 1602 | 282 @ 0.837 | 392 @ 0.798 | 95 @ 0.789 | 31 @ 0.710 | yes |
| 1603 | 260 @ 0.842 | 339 @ 0.808 | 75 @ 0.800 | 22 @ 0.545 | yes |
| 1604 | 240 @ 0.825 | 335 @ 0.794 | 61 @ 0.623 | 29 @ 0.552 | yes |
| 1605 | 252 @ 0.806 | 332 @ 0.798 | 87 @ 0.770 | 12 @ 0.667 | yes |
| 1606 | 238 @ 0.836 | 330 @ 0.848 | 73 @ 0.726 | 20 @ 0.700 | no |

### 5. E8 keeps its hot keys
Job-board events per req, counted from bronze (dev, seed 1602):

| | Old config | This config |
|---|--:|--:|
| Evergreen reqs' share of events | 52.4% | 28.4% |
| Top req's share | 35.4% | 17.2% |
| Top 10 reqs' share | 61.7% | 36.8% |
| An evergreen req / the median regular req | 231–482× | 70–108× |
| The busiest regular req / the median | 39× | 17× |

- **At full:** 20 evergreen reqs would share that 28%, about 1.4% of events each instead of about
  2.6%.
- **What E8 may need:** a lower `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`, or
  more shuffle partitions, for AQE to flag the skew.
- **T1.11 measures it.**

## Alternatives considered
- **More traffic alone.** Rejected (Context). It needs about 6× the views on regular reqs,
  applications per hire would pass 150, and full would produce 60 M+ events.
- **Sourcing that ignores popularity.** In this version, recruiters source harder for starving
  reqs. It's realistic, but it changes SPEC §6.6. Rescuing the expired reqs would also take
  sourced applications to about 30% of the mix, against a 14% ceiling.
- **Keep evergreen ×25.** This is the plan T1.10b started with, for E8. In a trial config,
  evergreen hires went from 259 to 624, headcount overshot its plan, and the feedback SLA fell
  below its floor. That was before the funnel change, which would add more. Fewer
  evergreen seats would cap the hires, but then the evergreen applications are wasted and
  applications per hire passes 150.
- **Only slow the decay.** A 60-day half-life gave a fill rate of 0.498 and a time to fill of 71
  days. Late fills take longer.
- **A noise-aware HT4 check.** A rise counts only beyond 2 standard errors, and the ≤ 30 bucket
  must beat the last one. It passes at dev, but at dev it can barely tell a decline from no
  effect at all, and it changes ADR-0015 §6 and SPEC §13.2. Shubh chose the explained miss.
- **HT4 as a slope with a band,** like HT1–HT3. At dev the slope's standard error is about half
  the effect, so a band check would miss as often.
- **Widen the bands.** Rejected: it hides the problem.

## Consequences
- **Every simulated number changes.**
  - **Tiny:** 180 k stream events (was 259 k, since tiny's one evergreen req is now ×7), 8 hires
    (was 2), 12 of 18 within target.
  - **Dev:** 1.53–1.83 M stream events.
- **A bug fixed on the way.** A fill's same-day closures were dropped (NOTES). The numbers here
  include the fix.
- **T1.11 checks at full:**
  - calibration
  - HT4's buckets
  - the evergreen key shares
  - runtime: about 20 M events, dev × 12.5
- **HT3 sits near its ceiling on some seeds.** It spans 2.88–3.22 over these seeds, but one
  earlier candidate reached 3.64. That's noise around 3.0, not a bias.

## References
- `docs/SPEC.md` §5.3, §6.4–§6.7, §6.11, §13 (M02, M07), §13.2
- ADR-0006, ADR-0008, ADR-0010, ADR-0015
