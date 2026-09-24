# RUNBOOK — HireStream

Alert → diagnosis → fix. Every DQ alert links to an anchor here. Anchors use the check ID
(e.g. `#s-jb-01`).

## Local stack
<!-- make up / down / ps, ports, resetting volumes, common failures (T0.3). -->

## Generating source data
`make generate PRESET=tiny|dev|full [SEED=N]` runs `hirestream generate backfill --overwrite`. It
**replaces** the generated source data under `data/lake/` and writes a manifest to
`data/lake/_runs/<run_id>/manifest.json` (ADR-0005 §7).

| Preset | HRIS files | Size | Time |
|---|---|---|---|
| tiny | 89 | ≈ 1 MB | < 1 s |
| dev | 364 | ≈ 33 MB | ≈ 5 s |
| full | 545 | ≈ 380 MB | ≈ 1.5 min |

- **Symptom:** `Invalid value for --lake-root: … already holds generated data; pass --overwrite`.
  **Cause:** `hirestream generate backfill` was run directly against a lake that already holds a
  run. Backfill never mixes two runs. **Fix:** add `--overwrite` to replace the data, or point
  `--lake-root` (or `HIRESTREAM_LAKE_ROOT`) somewhere empty.
- **Check determinism:** run the same preset and seed into two lake roots and compare the
  manifests' file hashes. `RunManifest.deterministic_view()` ignores run id, time and git state.

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
