# TUNING — HireStream

Performance methodology and experiment write-ups (SPEC §18). Harness in T5.1; experiments T5.2–T5.4.
v1.0.0 needs at least four completed experiments with measured numbers.

## Methodology
<!-- Harness command, warm-up + 5 measured runs, result cache off, dataset size, hardware. -->

## Benchmark suite
<!-- Q1–Q5 (sql/tuning/suite.yaml). -->

## Baselines
| Target | Query | Median | p90 | Notes |
|---|---|---|---|---|

## Experiments
Each: hypothesis → one change → median and p90 before/after → plan evidence → keep or revert.

| ID | Experiment | Target | Result | Kept? |
|---|---|---|---|---|
| E1 | Sort key on `fct_jobboard_event` | Redshift Q2, Q5 | | |
| E2 | DISTKEY + DISTSTYLE ALL | Redshift Q2 | | |
| E3 | Materialized view vs `agg_` table | Redshift Q2 | | |
| E4 | Point-in-time keys in Spark vs warehouse range join | Q3 | | |
| E5 | `fct_headcount_monthly` vs range join | Q4 | | |
| E6 | Postgres B-tree vs BRIN vs partitioning | Postgres Q2, Q5 | | |
| E7 | Small-file compaction | Spark | | |
| E8 | Skewed join: AQE off / on / salting | Spark | | |
| E9 | Partition pruning on silver reads | Spark | | |
