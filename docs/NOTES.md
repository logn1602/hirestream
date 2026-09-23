# NOTES — HireStream engineering log

Honest log: date, problem, symptom, root cause, fix, lesson. T0.5 adds the full skeleton.

## 2026-09-23 — T0.4: the spec's Spark default was already stale
- **Symptom:** the spec named `emr-spark-8.0.0` (Spark 4.0.x) as the default. The EMR Serverless
  release list showed `emr-spark-8.1.0` (Spark 4.1.1, LTS), published 2026-09-08.
- **Root cause:** the spec was written before 8.1.0 shipped. Version tables in design docs go stale
  within months.
- **Fix:** ADR-0002 pins 8.1.0 after checking Spark, Java, Python, region, and ARM64 support in the
  AWS release guide. The pins moved into code (`hirestream.versions`) with a drift test.
- **Also found:** new accounts get a default EMR Serverless quota of 16 concurrent vCPUs, below the
  spec's planned 32-vCPU application cap (ADR-0003 G9, for T4.2). EMR 8.1.0 also changed Spark
  config from "job replaces application" to "merged" (for T4.3).
- **Lesson:** treat every version in the spec as a hypothesis. Check it at the task that uses it,
  and keep the checked value in code so a test can enforce it.
