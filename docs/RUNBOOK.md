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

### Silver from stream bronze (T2.2b, T2.3; ADR-0019, ADR-0020)
Every command below takes `--source scheduling` or `--source jobboard`; the examples use
scheduling. Each source has its own table, quarantine and manifest, so they run independently.

- **Rebuild a source from all of its bronze.** Do this on first use, and after `make generate`
  replaces bronze:

  ```bash
  uv run hirestream run silver --source scheduling --mode bulk
  ```

  It deletes `silver/scheduling_events`, `quarantine/scheduling` and
  `state/bronze_manifest/scheduling` (for the job board: `silver/jobboard_events`,
  `quarantine/jobboard`, `state/bronze_manifest/jobboard`), then processes everything. Bronze is
  never touched. Tiny takes about 45 s for scheduling and about 3 min for the job board (178k
  lines in 2,225 objects).
- **Process what is new:**

  ```bash
  uv run hirestream run silver --source scheduling --as-of 2025-03-02T00:00:00Z
  ```

  It lists the 48 h of hour prefixes before `--as-of` (`listing_window_hours` in
  `config/pipeline/`) and skips objects a manifest record lists. Without `--as-of` it uses the
  current hour, which finds nothing in a lake of historical data.
- **What a run prints:** objects, lines, `written`, `duplicates` and `quarantined` by reason. `lines`
  always equals written + duplicates + quarantined; that is X-01's reconciliation.
- **Retrying:** run the same command with the same `--run-id`.
  - **The run had finished:** it says "already finished: nothing to do".
  - **It crashed before its commit:** the retry processes the same objects again. Rows and
    quarantine lines already written are not added twice.
- **`BronzeChangedError`:** an object a manifest record lists now has a different size or mtime.
  Bronze is append-only, so something rewrote it, almost always a regenerated lake. Rebuild with
  `--mode bulk`.
- **`unexpected bronze key`:** a file under `bronze/<source>/` is outside the
  `yyyy=/mm=/dd=/hh=/<object>` layout. Move it out. Names starting with `_` or `.` are ignored.
- **State on disk:**
  - `state/bronze_manifest/<source>/<run_id>.json` is the commit record: objects, counts and
    touched partitions.
  - `state/touched_partitions/<run_id>/<source>.json` is what gold reads.
  - Both are write-once.

### ATS extract (T2.4a; ADR-0021)
Needs the ats-db (`make up`) and `.env`.

- **Extract what changed:**

  ```bash
  uv run --env-file .env hirestream ingest ats
  ```

  It reads all five tables in one snapshot and lands each table's new rows in
  `bronze/ats/<table>/extract_ts=YYYYMMDDTHHMMSSZ/`. Each line it prints shows a table's rows, what
  they were pulled after (`updated_at` minus the 1 h overlap, or `change_id`), and the new
  watermark. The first extract into an empty lake pulls everything (`mode=full`).
- **After `make generate`:** an incremental extract refuses with "the ats-db tables were recreated
  … run `hirestream ingest ats --full`". The generator replaced the database, so the old
  watermarks no longer apply. Run:

  ```bash
  uv run --env-file .env hirestream ingest ats --full
  ```

  That starts a new baseline. Earlier extracts stay in bronze, which is append-only, and ATS silver
  (T2.4b) ignores everything before the latest full extract.
- **The rows repeat on every run:** that is the overlap. Rows updated in the hour before each
  watermark are pulled again on purpose, and silver drops the duplicates.
- **"extracts are at least a second apart":** two extracts started in the same second. Run it
  again.
- **A crash mid-extract:** nothing moved. The watermark record is written last, so a retry pulls
  from the same watermarks. Directories without a record are orphans, and nothing reads them.
- **State on disk:** `state/ats_watermarks/<extract_ts>.json`, write-once, one per extract. The
  latest one holds the current watermarks.
- **Speed:** tiny's 17.7k rows take about 15 s, mostly Spark starting up. Full's 2.39 M rows take about 45 s, and
  an incremental extract with nothing new about 20 s.

### ATS silver (T2.4b; ADR-0022)
Needs at least one committed extract (`ingest ats`) and the PII salt (below).

- **Build or bring up to date:**

  ```bash
  uv run --env-file .env hirestream run silver --source ats
  ```

  It rebuilds `silver/ats_candidates`, `ats_requisitions`, `ats_applications`, `ats_offers` and
  `ats_stage_changes` from the current baseline: the latest full extract and every incremental
  after it. When no extract has arrived since the last run, it says "nothing new: tables
  unchanged" and rewrites nothing. `--mode bulk` always rebuilds. `--as-of` is recorded but
  doesn't change what is read.
- **What a run prints:** the baseline and how many of its extracts are new, then per table the
  rows written, the rows read and how many were superseded (overlap copies and older versions).
  `read` always equals written + superseded.
- **After `make generate` and `ingest ats --full`:** the next silver run sees a new baseline,
  rebuilds from that extract alone, and reports "a new baseline". Rows the old source had and the
  new one doesn't are gone from silver.
- **"bronze holds N … rows for the baseline, but its extract records say M":** a committed
  extract directory was changed. Bronze is append-only, so find what wrote into it. To recover,
  take a new baseline with `ingest ats --full`.
- **"no committed ATS extract":** run `ingest ats` first.
- **Retrying:** the same command with the same `--run-id`. A finished run says "already finished:
  nothing to do". A crashed one rebuilds again, with the same result.
- **State on disk:** `state/bronze_manifest/ats/<run_id>.json` (the commit record) and
  `state/touched_partitions/<run_id>/ats.json` (what gold reads), both write-once.
- **Speed:** at full (2.39 M rows), a rebuild takes about 3 minutes on this VM, most of it
  writing stage changes' 548 `change_date` partitions. A run with nothing new takes about
  20 s, mostly Spark starting.

### PII salt (ADR-0022)
`HIRESTREAM_PII_SALT` keys every `email_hash`. Set it once per lake and never change it.

- **First time:**

  ```bash
  python -c "import secrets; print(secrets.token_hex(32))"
  ```

  Put the output in `.env` as `HIRESTREAM_PII_SALT=…`, and keep a copy in your password manager:
  losing it is the same as changing it. Never pass it as a command-line argument, which `ps` and
  Airflow logs would show.
- **Refused before Spark starts:** unset, the `.env.example` placeholder `change-me` (public on
  GitHub), or under 32 characters.
- **"not the salt this lake's email hashes were made with":** the salt in the environment isn't
  the one recorded in `state/pii/salt.json` (its fingerprint, never the salt itself). A different
  salt would give every person a new hash and silently break joins. Restore the original salt.
- **Rotating on purpose** (the salt leaked): hashes are one-way, so every hash has to be rebuilt
  from the raw emails, which only bronze has. In S3, bronze expires after 30 days, so rotate
  while it still holds what silver was built from.
  1. Set the new salt in `.env`.
  2. Delete `state/pii/salt.json`.
  3. Rebuild every hashed silver table with `--mode bulk` (`--source ats`; `hris` from T2.5),
     then gold.

## DQ alerts
<!-- One subsection per check ID (T2.11). -->

## Drills
### `late_burst` (T3.5)
### `hris_partial_file` (T3.6)

## Cloud
<!-- Deploy, submit, load, destroy, verify nothing tagged project=hirestream remains (T4.5, T4.6). -->
