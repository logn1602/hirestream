# PROGRESS — HireStream

**Current phase:** 1 · **Next task:** T1.3 · **Last updated:** 2026-09-24 (T1.2)

How this works: Claude Code takes the first unchecked task (or the one Shubh names), follows `CLAUDE.md`, and ticks the box inside that task's own PR. Branches are `<type>/<task-id>-<slug>`. `§N` refers to `docs/SPEC.md`.

## Phase 0 — Bootstrap (local, free)
- [x] **T0.1 Repo bootstrap** (§4) — Commit the kit as-is plus LICENSE (MIT), a README stub, `pyproject.toml` (uv, Python 3.11), `src/hirestream/__init__.py`, `tests/`, and a Makefile skeleton. Create `logn1602/hirestream` (public) with `gh repo create logn1602/hirestream --public --source . --remote origin --push` **after approval**. The only task allowed to commit to `main` directly. Commit: `chore(repo): bootstrap repository`.
- [x] **T0.2 Dev tooling + CI** (§17, §20) — ruff, mypy, pytest + coverage (report only for now), pre-commit (ruff, ruff-format, mypy, gitleaks, check-yaml, end-of-file-fixer, trailing-whitespace, check-added-large-files ≤ 1 MB), Make targets, `ci.yml` with lint, test, secrets. Branch `ci/t0.2-tooling`.
- [x] **T0.3 Local stack** (§2.1, §5) — `docker/docker-compose.yml` with `ats-db`, `warehouse-db` (Postgres 16), `metabase`; healthchecks; `.env.example`; `make up` / `make down`. Branch `build/t0.3-local-stack`.
- [x] **T0.4 ADRs** (§3) — ADR-0001 record architecture decisions; ADR-0002 EMR Serverless release + Spark, Python, Java pins (verify `emr-spark-8.0.0` vs `emr-7.13.0` in current AWS docs; pin local pyspark exactly); ADR-0003 local/cloud parity gaps. Branch `docs/t0.4-adrs`.
- [x] **T0.5 Doc skeletons** (§21) — DESIGN, DATA_MODEL, METRICS, DQ, TUNING, COST, RUNBOOK, NOTES, TALKING_POINTS, `coe/TEMPLATE.md`, `decisions/TEMPLATE.md`. Branch `docs/t0.5-doc-skeletons`.
- [x] **T0.6 Protect main** (§17) — Propose the ruleset; apply it via `gh api` only after approval (or give Shubh the UI steps).

Exit: CI green on main; `make up` healthy; ADR-0002 merged.

## Phase 1 — Generator (§6, §7)
- [x] **T1.1** Config models, preset loading, fraction → date calendar, SeedSequence plumbing, `hirestream generate` skeleton, run manifest.
- [x] **T1.2** World builder: orgs, teams, locations, managers, initial employees (seeded Faker, `.example` domains).
- [ ] **T1.3** Workforce dynamics + HRIS snapshot sink + HRIS chaos.
- [ ] **T1.4** Requisition lifecycle, popularity, evergreen seats.
- [ ] **T1.5** Job-board traffic: external, internal (HT3), bots, seasonality, diurnal curves — vectorized.
- [ ] **T1.6** ATS engine + source DDL (`sql/ats_source/`) + PostgresSink: stages, channels (HT2), offers (HT4), req-closure rejections, no-starts, reapplies.
- [ ] **T1.7** Scheduling engine: phone screens, loops, interviewer selection with load, reschedules / cancels / no-shows, feedback latency (HT1), v2 panels.
- [ ] **T1.8** Chaos layer + delivery queue + FileSink + KinesisSink (moto tests, including partial failures).
- [ ] **T1.9** JSON Schema contracts for every event type and version + contract tests.
- [ ] **T1.10** Ground truth + generation report + calibration; tune parameters to targets at `dev` (ADR for any parameter change).
- [ ] **T1.11** Performance pass at `full`: ≥ 10 M stream events, memory-bounded; record runtime and peak RSS; check calibration at `full`.

Exit: determinism test green; calibration in range at dev and full; `full` ≥ 10 M events.

## Phase 2 — Batch pipeline MVP, local (§8–§14) → v0.1.0
- [ ] **T2.1** Spark foundation: session factory (local / EMR), lake IO, common utilities, shared test fixture.
- [ ] **T2.2** Bronze manifest + `silver.scheduling_events` (timezone resolution, v1/v2, dedupe, quarantine, touched partitions).
- [ ] **T2.3** `silver.jobboard_events`.
- [ ] **T2.4** ATS incremental extract (watermarks, overlap, change_id) + ATS silver tables.
- [ ] **T2.5** `silver.hris_snapshots` (alias map, dedupe, typing).
- [ ] **T2.6** `dim_date`, `dim_source_channel`, `dim_requisition`, `dim_candidate`.
- [ ] **T2.7** `dim_employee` SCD2 (full + incremental, equality test) + `fct_employee_movement`.
- [ ] **T2.8** `fct_application_pipeline` (accumulating snapshot, touched-application recompute, MERGE semantics).
- [ ] **T2.9** `fct_interview` (grain, point-in-time interviewer key, weekly load, SLA flags).
- [ ] **T2.10** `fct_jobboard_event` + bot rules + `agg_jobboard_req_daily`.
- [ ] **T2.11** DQ framework + Phase 2 catalog (§12.2) + `ops.dq_results` + circuit breaker + alert sinks.
- [ ] **T2.12** Postgres warehouse DDL + loader (staging, MERGE / delete-insert, verification, ANALYZE) + ops tables.
- [ ] **T2.13** Metrics SQL M01–M11 + `metrics` views + ground-truth checks (§13.2).
- [ ] **T2.14** Metabase bootstrap: WBR and Pipeline Health dashboards; screenshots in `docs/img/`.
- [ ] **T2.15** `make e2e` (tiny) in CI + `make pipeline PRESET=dev`; enforce the 85% coverage gate; add `e2e-tiny` to required checks (ask first); README quickstart.

Exit: dev end-to-end clean; CI e2e green; ground truth reconciles. Ask Shubh before tagging **v0.1.0**.

## Phase 3 — Orchestration and incremental processing (§15) → v0.2.0
- [ ] **T3.1** Airflow in compose (JRE 17 + pinned pyspark + wheel); thin DAG helpers.
- [ ] **T3.2** `hirestream_daily` DAG with gates + idempotency test (rerun → identical outputs).
- [ ] **T3.3** `hirestream_backfill` + bulk-vs-incremental equality test at `tiny`.
- [ ] **T3.4** Generator live-tail + `hirestream_live_tail` DAG.
- [ ] **T3.5** Drill `late_burst`: corrections reach gold and the warehouse; RUNBOOK entry.
- [ ] **T3.6** Drill `hris_partial_file`: the circuit breaker blocks publishing; RUNBOOK entry.

Exit: 14 consecutive simulated days through Airflow with no manual steps.

## Phase 4 — AWS (§16, §17) → v0.3.0 · costs money: every cloud step needs explicit approval
- [ ] **T4.1** CDK app + six stacks; `cdk synth` job in CI (no credentials, no lookups); add `infra` to required checks (ask first).
- [ ] **T4.2** Cost guardrails (budget, Redshift usage limit, EMR caps ≤ account vCPU quota (ADR-0003 G9) + auto-stop, lifecycle rules, tags) + COST.md estimate — before any deploy.
- [ ] **T4.3** Spark packaging (`requirements-spark.txt`, zip) + `hirestream cloud submit` for EMR Serverless.
- [ ] **T4.4** Redshift baseline DDL + Data API loader (`hirestream cloud load`) + verification.
- [ ] **T4.5** `deploy.yml` / `destroy.yml` (OIDC, environment approval) + RUNBOOK cloud section.
- [ ] **T4.6** Cloud demo run with Shubh present: deploy → full backfill to S3 → EMR silver/gold → Redshift → DQ → metrics sanity → live-tail through Kinesis / Firehose → micro-batch → destroy → verify nothing tagged remains → log actual cost the next day. Check every parity gap G1–G12 in ADR-0003.

Exit: reproducible cloud run from a clean deploy; costs logged; everything destroyed.

## Phase 5 — Tuning and COE (§18, §19) → v0.4.0
- [ ] **T5.1** Benchmark harness + suite + results → TUNING.md.
- [ ] **T5.2** Postgres baseline + E4, E5, E6.
- [ ] **T5.3** Redshift baseline + E1–E3 in one bounded cloud session (estimate first).
- [ ] **T5.4** Spark E7–E9.
- [ ] **T5.5** COE-001 `silent_schema_break`: impact → write-up → action items (S-JB-03, drift check, contract test) → backfill → verify.

Exit: ≥ 4 measured experiments; COE-001 closed with merged action items.

## Phase 6 — Polish (§21, §23) → v1.0.0
- [ ] **T6.1** README final (real numbers, diagram, screenshots, quickstart, limitations, next steps).
- [ ] **T6.2** DESIGN.md final — alternatives: Kinesis vs MSK vs self-managed Kafka; micro-batch vs Structured Streaming; Redshift vs Athena + Iceberg; CDK vs Terraform; custom DQ vs Deequ / Glue Data Quality; Airflow vs Step Functions; snapshot-based SCD2 vs CDC.
- [ ] **T6.3** TALKING_POINTS.md final + Leadership Principles story map.
- [ ] **T6.4** Optional stretch (at most two, ask first).
- [ ] **T6.5** Release v1.0.0 with notes; propose a Featured Work row for the `logn1602/logn1602` profile README (edit that repo only after approval).

## Session log
| Date | Task | PR | Outcome |
|---|---|---|---|
| 2026-09-22 | T0.1 | direct to main (bootstrap) | Kit committed with LICENSE, README stub, uv/Python 3.11 skeleton, Makefile skeleton |
| 2026-09-22 | T0.2 | ci/t0.2-tooling | ruff, strict mypy, pytest + coverage (report only), pre-commit, `ci.yml` (lint, test, secrets) |
| 2026-09-23 | T0.3 | build/t0.3-local-stack | Compose stack (Postgres 16.15 ×2 on 15432/15433, Metabase v0.63.18.1 on 3000), healthchecks, `make up/down/ps`, CI `stack` job |
| 2026-09-23 | T0.4 | [#3](https://github.com/logn1602/hirestream/pull/3) | ADR-0001 (ADR format), ADR-0002 (`emr-spark-8.1.0`, Spark 4.1.1, JDK 17, Python 3.11, ARM64; `pyspark==4.1.1` pinned + drift test), ADR-0003 (parity gaps G1–G12) |
| 2026-09-23 | T0.5 | [#4](https://github.com/logn1602/hirestream/pull/4) | Skeletons for DESIGN, DATA_MODEL, METRICS, DQ, TUNING, COST, RUNBOOK; NOTES and TALKING_POINTS given full structure; ADR and COE templates; `docs/img/` |
| 2026-09-23 | T0.6 | [#5](https://github.com/logn1602/hirestream/pull/5) | Ruleset `protect-main` active on main (PR required, 0 approvals, merge-only; lint, test, secrets, stack required; no force push, deletion, or bypass), versioned in `.github/rulesets/main.json` |
| 2026-09-24 | T1.1 | [#6](https://github.com/logn1602/hirestream/pull/6) | Strict config models + preset loading, fraction → date calendar, name-keyed SeedSequence streams (golden-value test), atomic run manifest, `hirestream generate backfill` + `make generate`; 65 tests, 98% coverage |
| 2026-09-24 | T1.2 | [#7](https://github.com/logn1602/hirestream/pull/7) | World builder (ADR-0004): steady-state tenure/time-in-role/leave, 5–9 span management tree, exact level/role/location quotas, hire-ordered ids, seeded Faker names + `.example` emails; tiny/dev/full build in 0.1/0.3/1.9 s; golden world fingerprint |
