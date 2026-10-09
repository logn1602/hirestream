# RUNBOOK — HireStream

Alert → diagnosis → fix. Every DQ alert links to an anchor here. Anchors use the check ID
(e.g. `#s-jb-01`).

## Local stack
<!-- make up / down / ps, ports, resetting volumes, common failures (T0.3). -->
- **Symptom:** in WSL, `docker` says it "could not be found in this WSL 2 distro", or `Cannot
  connect to the Docker daemon at unix:///var/run/docker.sock`, while Docker Desktop is running.
  - **Cause:** Docker Desktop's integration agent for the distro isn't running. Either it hasn't
    started yet, or it crashed: on 2026-10-09 it died with `running echo $HOME in Ubuntu-24.04:
    … The pipe is being closed`. The engine was fine; nothing served the socket inside WSL.
  - **Check:** `curl -s --unix-socket /var/run/docker.sock http://localhost/_ping` prints `OK`
    once it works.
  - **Logs:** the reason is in
    `/mnt/c/Users/<you>/AppData/Local/Docker/log/host/com.docker.backend.exe.log` (grep
    `wslintegration`).
  - **Fix:** in Docker Desktop, click **Restart the WSL integration**, or toggle the distro under
    Settings → Resources → WSL integration. Then run `hash -r`, in case the shell cached the
    Windows `docker` shim.
- **Saving memory:** the VM has 3 GB. For a full backfill with the ATS load, start only the
  database: `docker compose -f docker/docker-compose.yml --env-file .env up -d --wait ats-db`.
  Metabase isn't needed.

## Generating source data
`make generate PRESET=tiny|dev|full [SEED=N]` runs `hirestream generate backfill --overwrite`. It
**replaces** the generated source data under `data/lake/` and writes a manifest to
`data/lake/_runs/<run_id>/manifest.json` (ADR-0005 §7).

| Preset | HRIS files | Stream parts | On disk | Time (WSL2) |
|---|---|---|---|---|
| tiny | 89 | ≈ 3,600 | ≈ 40 MB | ≈ 10 s |
| dev | 364 | ≈ 17,000 | ≈ 260 MB | 1–2 min |
| full | 545 | ≈ 26,300 | ≈ 2.0 GB, plus 363 MB in ats-db | 14–35 min (the ATS load ≈ 1 min), peak ≈ 1.3 GiB (T1.11) |

Use tiny or dev for day-to-day work. On WSL2, creating one folder per hour per stream source is
slow and erratic on its virtual disk (NOTES, 2026-10-07).

- **Full's time varies.** The run is CPU-bound, but this WSL2 VM doesn't always get the CPU: the
  same days took 1.7 s each in one run and 4–6 s in the next (NOTES, 2026-10-08).
  - **The report's runtime:** the process's monotonic clock.
  - **`/usr/bin/time` wall time:** can be longer, when the VM stalls and its clock is resynced.
  - **To compare two full runs:** use CPU time (`/usr/bin/time -v`), and expect even that to
    include time the host took away.
- **Full's memory:** grows with what the ATS keeps (about 2 MiB per simulated day) to about
  1.3 GiB. Leave about 2 GB free for the VM.

- **Symptom:** `Invalid value for --lake-root: … already holds generated data; pass --overwrite`.
  **Cause:** `hirestream generate backfill` was run directly against a lake that already holds a
  run. Backfill never mixes two runs. **Fix:** add `--overwrite` to replace the data, or point
  `--lake-root` (or `HIRESTREAM_LAKE_ROOT`) somewhere empty.
- **Check determinism:** run the same preset and seed into two lake roots and compare the
  manifests' file hashes. `RunManifest.deterministic_view()` ignores run id, time and git state.

### Bronze stream files
Backfill writes job-board and scheduling events, after chaos, as Firehose-style parts by arrival
hour (UTC): `bronze/{jobboard,scheduling}/yyyy=…/mm=…/dd=…/hh=…/part-<n>-<uuid>.jsonl.gz`
(ADR-0012). The CLI's `bronze:` line counts lines, duplicates, malformed lines and late arrivals.

- **Look at one:** `zcat data/lake/bronze/jobboard/yyyy=2025/mm=03/dd=01/hh=14/part-*.jsonl.gz | head -3`.
  A file's mtime is its latest arrival.
- **Expect dirt:** about 1.5% duplicates (same `event_id`), 0.1% malformed lines, and 3% arriving an
  hour to a week late, sometimes in the next hours' folders. That's chaos (§6.8), not a bug; silver
  quarantines and dedupes.
- **Incidents:** `--incident duplicate_storm` (job board), `silent_schema_break` (job board) or
  `late_burst` (scheduling); the `bronze:` line then adds their counts.
- **Check determinism:** every part is in the manifest with its sha256. The same seed writes the same
  bytes to the same paths.

### The ATS source database (ats-db)
Backfill loads the ATS's final state into the `ats-db` container at the end of the run (ADR-0009).
`make generate` needs the stack up (`make up`) and `.env` in place.

- **Look at it:** `docker compose -f docker/docker-compose.yml --env-file .env exec ats-db psql -U ats -d ats`,
  then e.g. `SELECT status, count(*) FROM applications GROUP BY 1;`
- **Symptom:** `no ats-db configured`. **Fix:** `cp .env.example .env` (fill in the values) and use
  `make generate`, or pass `--skip-ats-db` for a run without the database.
- **Symptom:** `cannot reach the ats-db at 127.0.0.1:15432/ats`. **Fix:** `make up`, then
  `make ps` until ats-db is healthy. The check runs before anything is deleted or simulated, so
  nothing was lost.
- **Symptom:** `the ats-db … already holds generated data; pass --overwrite`. **Fix:** add
  `--overwrite` (`make generate` already does). The replacement is one transaction: if a row is
  rejected, the old data stays.
- **Check determinism:** the manifest's `ats_tables` has `{rows, sha256}` per table. Two runs with
  the same seed must match.

### Ground truth and the generation report
Every run also writes `ground_truth.json` and `generation_report.md` beside its manifest in
`data/lake/_runs/<run_id>/` (ADR-0015). The CLI prints all three paths and a `calibration:` line.

- **Read the report first:**
  - calibration pass/warn for every target
  - the hidden-truth ratios against their bands
  - counts per source and event type
  - the chaos injected
  - runtime and peak RSS
- **A `warn` is not a failure.** Tiny is too small to calibrate. Dev and full should be within
  target (T1.10b, T1.11), or an ADR explains the miss.
- **Ground truth grades the pipeline.** It holds:
  - ATS counts by UTC month × channel × internal
  - stream events per source
  - the lines silver should quarantine, per reason
  - the HRIS faults

  **Look at it:** `uv run python -m json.tool data/lake/_runs/<run_id>/ground_truth.json | less`.
- **Check determinism:** its sha256 is the manifest's `ground_truth_sha256`, so the same seed
  writes the same bytes. The report isn't hashed: its runtime and memory change from run to run.
- **Measuring memory:** peak RSS is the whole process's peak. Use a CLI run for a preset's number,
  not a test session.

### Checking contracts at full
After a full backfill, run `HIRESTREAM_FULL_LAKE=<lake root> uv run pytest -m full` (ADR-0017 §6).
- **What it checks:**
  - every scheduling line classifies exactly as the ground truth's `expected_quarantine`
  - every 50th job-board line is valid, or carries exactly the fault chaos or the timezone bug
    explains
- **Cost:** about 11 minutes, at about 120 MiB.
- **Needs:** a lake holding exactly one run. Without the variable, the tests are skipped.

### Checking calibration over seeds
`make calibrate PRESET=dev` runs `hirestream generate calibrate` (ADR-0016).
- **What it does:** backfills `meta.seed` and the next four seeds, each into a throwaway lake, one
  at a time. It prints a line per seed, then a table of every target with its min and max and how
  many seeds missed.
- **Safe to run anytime:** it never touches `data/lake` or ats-db, and it needs neither `.env` nor
  `make up`.
- **Other seeds:** `make calibrate PRESET=dev SEEDS="7 8 9"`.
- **Cost:** about 95 s per dev seed, so 8 minutes for five. Peak memory is one run's.
- **When to run it:** after any change to `config/generator/base.yaml`. Paste its table into the
  ADR that justifies the change.
- **Expect HT4 to warn on some dev seeds.** Dev has too few offers to resolve the first bucket gap
  (ADR-0016 §4). Any other warn is a real miss.

## Branch protection
`main` is protected by the repository ruleset `protect-main`, versioned in
`.github/rulesets/main.json`: pull request required (0 approvals, merge commits only), required
checks `lint`, `test`, `secrets`, `stack` from GitHub Actions, no force pushes, no deletion, no
bypass actors.

- **Show what applies to main:** `gh api repos/logn1602/hirestream/rules/branches/main`
- **Find the ruleset id:** `gh api repos/logn1602/hirestream/rulesets --jq '.[] | "\(.id) \(.name)"'`
- **Reapply after editing the file:** `gh api -X PUT repos/logn1602/hirestream/rulesets/<id> --input .github/rulesets/main.json`
- **Recreate from scratch:** `gh api -X POST repos/logn1602/hirestream/rulesets --input .github/rulesets/main.json`
- **Emergency disable** (admin only; re-enable straight after):
  `gh api -X PUT repos/logn1602/hirestream/rulesets/<id> -f enforcement=disabled`

**Symptom:** every PR shows "Expected — Waiting for status to be reported" on a check and can't merge.
**Cause:** a CI job was renamed or removed, so the required check name never reports.
**Fix:** rename a job in `ci.yml` and the context in `main.json` in the same PR, then reapply the
ruleset once it merges. Adding a required check (e.g. `e2e-tiny` in T2.15, `infra` in T4.1) follows
the same path and needs Shubh's approval.

## Pipeline runs
<!-- Rerunning a day, backfill, reading ops.pipeline_runs (T3.2, T3.3). -->

## DQ alerts
<!-- One subsection per check ID (T2.11). -->

## Drills
### `late_burst` (T3.5)
### `hris_partial_file` (T3.6)

## Cloud
<!-- Deploy, submit, load, destroy, verify nothing tagged project=hirestream remains (T4.5, T4.6). -->
