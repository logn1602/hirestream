# CLAUDE.md — HireStream

## Project
HireStream is Shubh Dave's portfolio data-engineering project (GitHub: **logn1602**, repo `logn1602/hirestream`). It simulates the recruiting and internal-mobility systems of **Halcyon**, a fictional 25,000-person company, and builds the data platform behind them: multi-source event ingestion, a medallion lake (bronze/silver/gold), a Kimball warehouse, data-quality gates, WBR-style metrics, query tuning, and an AWS deployment via CDK.

Sources of truth, in order:
1. `docs/SPEC.md` — requirements and design. Read the sections a task references (§N) before touching code.
2. `docs/PROGRESS.md` — task checklist, current state, session log. Update it in every task branch.
3. `config/generator/base.yaml` — simulation parameters. Changing a probability, distribution, or calibration target requires an ADR.

If the spec is wrong, ambiguous, or out of date (library versions, AWS APIs), stop and propose an ADR in `docs/decisions/` instead of silently deviating. Verify versions against official docs at build time rather than trusting memory.

## Environment
- WSL2 Ubuntu. The repo lives at `~/code/hirestream` on the Linux filesystem. Use bash, never Windows paths. LF line endings only.
- Python 3.11 managed by `uv`. Java 17. Docker Desktop with WSL integration.
- PySpark is pinned to the exact Spark version of the chosen EMR Serverless release: `emr-spark-8.1.0` → `pyspark==4.1.1` (ADR-0002, `hirestream.versions`).
- Local services run via `docker compose` (see `docker/`).

## Commands
Created in T0.2; keep this list current as targets are added.
- `make setup` — `uv sync` + `pre-commit install`
- `make fmt` / `make lint` / `make test` / `make e2e`
- `uv run pre-commit run --all-files` — every hook on the whole tree (first run builds hook envs)
- `make up` / `make down` / `make ps` — local stack (needs `.env`: `cp .env.example .env`); ats-db `localhost:15432`, warehouse-db `localhost:15433`, Metabase `localhost:3000`
- `make generate PRESET=tiny|dev|full [SEED=N]` — `hirestream generate backfill`; output under `data/lake/` (manifest in `_runs/<run_id>/`)
- `make pipeline PRESET=dev`
- `make cloud-*` — billable; see guardrails below

## Session protocol (every task)
1. Verify identity: `git var GIT_AUTHOR_IDENT` must show Shubh Dave with his GitHub email, and `gh auth status` must show `logn1602`. If not, stop and tell Shubh.
2. `git switch main && git pull --ff-only` (skip the pull while no remote exists, i.e. T0.1). The working tree must be clean.
3. Take the next unchecked task in `docs/PROGRESS.md` (or the one Shubh names). Read its SPEC sections, related ADRs, and the code it touches.
4. Post a plan: goal, files, tests, commands, risks, branch name. Wait for approval if the task touches infra or cloud, adds a dependency, or will change more than ~300 lines. Otherwise proceed.
5. Branch: `<type>/<task-id>-<slug>` (e.g. `feat/t1.3-hris-dynamics`).
6. Implement in small commits with tests alongside the code. `make lint test` passes before every commit.
7. Update docs in the same branch: PROGRESS.md always; DESIGN, DATA_MODEL, METRICS, DQ, TUNING, COST, RUNBOOK, NOTES, TALKING_POINTS, and ADRs as relevant.
8. Push, open a PR that follows `.github/pull_request_template.md`, and wait for CI (`gh pr checks --watch`).
9. Report back, ending with a "What to understand" section. **Never merge until Shubh says "merge".**

Slash commands in `.claude/commands/` wrap this loop: `/next-task`, `/ship`, `/merge`.

## Git rules
- Every commit is authored by the configured git identity (Shubh Dave / logn1602). Never change git config, never pass `--author`, never commit as anyone else.
- Conventional Commits: `type(scope): imperative summary`, subject ≤ 72 characters, body explains *why*, footer `Refs: T<id>`.
  - types: feat, fix, test, docs, refactor, perf, ci, build, chore
  - scopes: repo, generator, ingest, bronze, silver, gold, dq, warehouse, metrics, dashboards, airflow, infra, ci, docs
- One task → one branch → one PR. Commits are atomic and each one passes lint and tests.
- Never: commit to `main` after the T0.1 bootstrap, force-push, rewrite pushed history, use `--no-verify`, `git add -A` without reviewing `git status`, or commit `data/`, `.env`, credentials, account IDs, or any file over 1 MB.
- Tags only at phase milestones and only when Shubh asks: annotated `vX.Y.Z`.

## Cloud and cost guardrails (hard rules)
- Every billable operation goes through `hirestream cloud …` or `make cloud-*`. Before running one (or any `cdk deploy`, `cdk destroy`, or `aws` command that creates or changes resources), state what it will create, the expected cost, and the teardown step, then wait for explicit approval.
- Never create IAM access keys. Use the AWS SSO profile `hirestream`. Never print, log, or commit credentials or account IDs.
- After every cloud session, list resources tagged `project=hirestream` and remind Shubh to run `make cloud-destroy`.

## Code standards
- `src/` layout, type hints everywhere, mypy clean, ruff (line length 100).
- Spark code in `src/hirestream/transforms` and `src/hirestream/dq` imports only the stdlib, pyspark, and pure-Python packages listed in `requirements-spark.txt`.
- Spark 4 runs with ANSI mode on: parse untrusted input with `try_*` functions (`try_cast`, `try_to_timestamp`) and detect malformed JSON explicitly. Bad rows go to quarantine with a reason code; nothing is silently dropped.
- Deterministic and idempotent: seeded RNG; partition overwrite or MERGE on keys; re-running a job on the same inputs produces identical outputs.
- All timestamps are UTC after bronze. Durations are precomputed numeric columns in gold.
- HR data is treated as if it were real: no demographic or protected attributes anywhere; emails are salted-hashed in silver; gold contains no names or emails.
- Airflow DAGs stay thin; all logic lives in the package and is runnable from the CLI.
- Tests: pytest; PySpark tests use the shared session fixture; slow tests are marked `slow`; coverage ≥ 85% on `src/hirestream` (infra excluded), enforced in CI from T2.15.

## Explain as you go
Shubh has to defend every decision in interviews. End each task report with **What to understand**: the key concept, the alternative you rejected and why, and one likely interview question with a strong answer. Add durable items to `docs/TALKING_POINTS.md`. Log real problems you hit (symptom → root cause → fix) in `docs/NOTES.md` — honest, not polished.
