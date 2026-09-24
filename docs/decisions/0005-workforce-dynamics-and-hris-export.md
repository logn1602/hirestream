# ADR-0005: Workforce dynamics and HRIS export semantics

- **Status:** Accepted
- **Date:** 2026-09-24
- **Task:** T1.3
- **Deviates from:** `docs/SPEC.md` §6.1 (adds an `hris_chaos` stream). It also fills in rules
  that §6.3, §6.8, §6.9 and §7.5 leave open.

## Context
SPEC §6.3 lists the daily hazards (annual rate / 365), the reorg, retention of terminated
employees, and "every job change sets `job_effective_date`". §6.8 lists the HRIS chaos:
- a missing day
- `manager_id` exported as `mgr_id` for a day
- 10% of changes exported with a past effective date
- a duplicated row

§7.5 fixes the columns. The spec does not say:
- what happens when two hazards hit one employee on one day
- which employees each hazard applies to
- who takes over the reports of a manager who leaves
- which changes count as "job changes"
- what a snapshot contains, and when
- exactly how the chaos behaves

The SCD2 build (§10.3) needs every change to carry its true effective date, even across missing
and late exports.

## Decision

### 1. The day
Each simulated day runs in this order:
1. Returns from leave.
2. The reorg, on its day.
3. The daily hazards.

The HRIS export then writes the **end-of-day** state. Later subsystems (T1.4+) join this loop in
`generator/simulation.py`.

### 2. Hazards
One uniform draw per employee per hazard per day, taken from the `workforce` stream. **At most one
hazard fires per employee per day.** They are checked in this order: termination, leave start,
promotion, lateral move, manager change, location change.

| Hazard | Applies to |
|---|---|
| Termination (× first-year multiplier) | Active employees and those on leave |
| Leave start (duration uniform over `leave_duration_days`) | Active |
| Promotion (one level up) | Active, below the top level, ≥ `promotion_min_days_in_level` in level |
| Lateral move, manager change | Active ICs who are neither an org leader nor a team manager |
| Location change (new city by weight, excluding the current one) | Active |

- A **lateral move** goes to a random other team with at least one member. The mover reports to one
  of that team's people managers.
- A **manager change** picks another people manager in the same team. If there isn't one, nothing
  happens.
- The **reorg** moves `teams_moved` random teams to a random other org. Every member gets an org
  change, and the team manager now reports to the new org leader.

Restricting lateral moves and manager changes to ICs means neither ever cascades through the tree.

### 3. When someone with reports leaves
This covers attrition, and later internal transfers of managers (T1.6).
- **A manager below team level:** their reports move to the departing manager's own manager.
- **A team manager:** their highest-level report takes over the team, promoted to L6 if below it.
  That person now reports to the org leader, and their former peers report to them.
- **An org leader:** the highest-level team manager becomes org leader, promoted to the top level if
  needed, and moves to the leadership team. The other team managers report to them. Their old team
  gets a new manager by the team-manager rule.
- **Ties:** among heirs, active employees beat those on leave, then higher levels win, then a
  random draw decides.
- **Nobody left:** an org leader with no reports stays (the attrition draw is ignored). A team whose
  last member leaves has `manager_id = None`.

These rules keep every people manager at L6+, every org leader at the top level, and every
reporting line pointing at someone still employed. Spans may drift outside 5–9 over time.

### 4. What counts as a change
**Every change to an HRIS field** sets `job_effective_date` to the day it takes effect. The fields
are org, team, level, manager, location, and status (leave start and end, termination). The SCD2
build can then date each change correctly, even when the export is late or a day is missing.

Time in level (promotion eligibility) and time in role (HT3: reset by promotions and lateral moves,
not by manager or location changes) are clocks kept inside the engine. Initial values follow
ADR-0004. The T1.2 world is unchanged.

### 5. The HRIS snapshot
- **Location:** `bronze/hris/snapshot_date=YYYY-MM-DD/employees_YYYYMMDD.csv.gz`.
- **Contents:** end-of-day state, with rows for active employees, those on leave, and employees
  terminated in the last `terminated_retention_days` (each appears in exactly that many snapshots,
  starting on the termination day).
- **Format:** rows in `employee_id` order. A header in the exact §7.5 column order, UTF-8, `,`
  separators, minimal quoting, LF line endings, empty strings for nulls. Gzip with `mtime=0` and no
  embedded file name, so the same data always compresses to the same bytes.
- **Manifest:** every file is recorded with its sha256, size in bytes, and number of data rows.

### 6. HRIS chaos
Draws come from a new, name-keyed `hris_chaos` stream, so stream chaos (T1.8) can never reshuffle it.
- **Late exports:** each change to an existing employee has a `retro_effective_share` chance of first
  appearing k ~ U(`retro_effective_days`) days later, carrying its true `job_effective_date`.
  If that employee changes again before then, their full current state is exported at once. Hires
  (T1.6) are never late.
- **Missing snapshot:** no file that day. The simulation still advances and late changes still
  come due.
- **Column rename:** the header uses `mgr_id` instead of `manager_id` on the configured days.
- **Duplicate row:** one uniformly chosen row is repeated immediately after itself.
- **`hris_partial_file` incident** (only with `--incident`): that day's file keeps the first
  `keep_fraction` of rows.

### 7. Re-running into the same lake
`generate backfill` refuses to write if `bronze/hris` already holds files. `--overwrite` deletes
that folder first, so two runs are never silently mixed. `make generate` passes `--overwrite`,
because its purpose is to regenerate the sources.

## Alternatives considered
- **Several hazards per employee per day.** Rejected. It needs an ordering anyway, and it allows
  "promoted and terminated on the same day", which no HRIS would export.
- **Position-based org with vacancies refilled by backfill hires.** Rejected for now. It needs the
  requisitions (T1.4) and ATS (T1.6), and the skip-level and heir rules already keep every
  invariant. Backfill hires will join their hiring manager (T1.6).
- **A random heir.** Rejected. An L3 could inherit a 400-person team.
- **`job_effective_date` only on role changes.** Rejected. Manager, location and status changes
  could then not be dated across a missing day or a late export. HT3 uses the engine's role clock
  instead.
- **pandas or pyarrow for the CSV.** Rejected. The generator would take on a dependency, while cached
  per-employee CSV lines (rebuilt only when that employee changes) are fast enough.
- **Always overwrite silently, or never allow overwriting.** Rejected. The first silently destroys
  or mixes runs; the second adds friction for no safety.

## Consequences
- Until hires arrive (T1.4 and T1.6), headcount shrinks by roughly the attrition rate, and managers
  above departing managers absorb their reports. This is expected; backfills will refill teams.
- Late exports, the missing day, the rename and the duplicate row give silver and the SCD2 build
  (T2.5 and T2.7) real work, and S-HR-01/03/05 something to detect. `hris_partial_file` trips
  S-HR-02 and S-HR-04, as drilled in T3.6.
- Files are byte-identical between runs on one machine. A different zlib build may compress
  differently; the uncompressed content is the contract.
- The in-memory event log (kind and cause per change) is the source for workforce ground truth
  (T1.10).
- Measured cost, seed 1602 (§6.11 budget: 45 minutes and 4 GB at `full`):

| Preset | Files | Rows | On disk | Wall time | Peak memory |
|---|---|---|---|---|---|
| tiny | 89 | 26,701 | 0.9 MB | 0.3 s | — |
| dev | 364 | 1,051,521 | 32.5 MB | 5.4 s | — |
| full | 545 | 12,766,493 | 378 MB | 84 s (whole backfill) | 114 MB |

## References
- `docs/SPEC.md` §6.3, §6.8, §6.9, §7.5, §10.3 (SCD2), §12.2 (S-HR checks), §19 (drills)
