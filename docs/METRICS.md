# METRICS — HireStream

**Source of truth** for metric definitions (SPEC §13). SQL lives in `sql/metrics/`. Weeks are ISO
weeks (Monday start, UTC). Filled in T2.13; SPEC §13 is the starting draft.

For each metric: definition, numerator / denominator, grain, cuts, exclusions, SQL file, and
ground-truth check (if any).

## M01 Time to hire
## M02 Time to fill
## M03 Stage conversion
## M04 Interviewer load
## M05 Feedback SLA
## M06 Interview disruption
## M07 Offer acceptance
## M08 Internal fill rate
## M09 Internal mobility rate
## M10 Job board funnel
## M11 Pipeline health

## WBR layout
<!-- 6-12 charts, KPI tiles, week-over-week and year-over-year (SPEC §13.1). -->

## Ground-truth verification
<!-- Exact vs banded checks, HT1–HT4 bands (SPEC §13.2). -->
Each run's `_runs/<run_id>/ground_truth.json` holds the expected values. The HT1–HT3 bands are
`calibration_targets.hidden_truth_bands` in `config/generator/base.yaml`. The generator's
calibration (ADR-0015) measures M01, M02, M05, M07 and M08 with the same definitions, so a change
to one of them here must change ADR-0015's too.
