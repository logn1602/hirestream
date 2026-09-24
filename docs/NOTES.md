# NOTES — HireStream engineering log

Honest log of real problems, newest last. Not polished: what broke, why, and what changed.

Entry template:
```markdown
## YYYY-MM-DD — T<id>: <one-line problem>
- **Symptom:**
- **Root cause:**
- **Fix:**
- **Lesson:**
```

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

## 2026-09-24 — T1.1: numpy 2.5 dropped Python 3.11
- **Symptom:** PyPI's latest numpy (2.5.3) declares `requires-python >=3.12`. We are pinned to 3.11
  by ADR-0002 (EMR's default PySpark Python).
- **Root cause:** numpy follows the scientific-Python support schedule (SPEC 0) and drops old Python
  versions on a fixed calendar. EMR moves more slowly.
- **Fix:** `numpy>=2.4.6,<2.5`, with the reason commented in `pyproject.toml`. uv would have picked
  2.4.6 anyway; the explicit cap makes the constraint visible instead of accidental.
- **Lesson:** a runtime pin (Python 3.11) quietly caps every library. When ADR-0002 is superseded
  (for example, EMR defaulting to 3.12), revisit this cap.

## 2026-09-24 — T1.1: two conftest.py files broke mypy
- **Symptom:** the mypy pre-commit hook failed with `Duplicate module named "conftest"` once
  `tests/unit/generator/conftest.py` joined `tests/conftest.py`. The commit was blocked.
- **Root cause:** without `__init__.py`, mypy maps both files to the top-level module `conftest`.
- **Fix:** made `tests/` a package (`__init__.py` in `tests/`, `tests/unit/`, and
  `tests/unit/generator/`), so they become `tests.conftest` and `tests.unit.generator.conftest`.
- **Lesson:** decide the test-package layout before the second conftest appears. The hook caught it
  before it reached CI.

## 2026-09-24 — T1.1: CLI test passed locally, failed in CI
- **Symptom:** `test_bad_arguments_exit_2[--seed]` failed only on GitHub Actions. The output did
  contain `--seed`, but it was wrapped in `\x1b[...m` sequences.
- **Root cause:** rich (used by Typer for error boxes) turns colour on when it detects CI
  (`GITHUB_ACTIONS`/`FORCE_COLOR`). Colour codes split the substring the test searched for. A local
  terminal run under pytest has no TTY, so there was no colour and the test passed.
- **Fix:** the CLI tests strip ANSI codes and collapse rich's line wrapping before matching.
  Reproduced locally with `GITHUB_ACTIONS=true FORCE_COLOR=1 uv run pytest tests/unit/test_cli.py`.
- **Lesson:** assertions on human-facing output must normalise it. The branch ruleset blocked the
  merge until CI passed, which is exactly what T0.6 was for.

## 2026-09-24 — T1.2: Faker's en_IN names include a curly apostrophe
- **Symptom:** while checking which characters Faker emits, one en_IN surname came back as
  `D’Alia`, with U+2019 RIGHT SINGLE QUOTATION MARK rather than an ASCII apostrophe. Ruff's
  ambiguous-character rules (RUF001/RUF002) also flagged it when it went into a test and a
  docstring.
- **Root cause:** real name data contains typographic punctuation. A naive `first.last` email
  builder would have put a non-ASCII character into a work email address.
- **Fix:** email local parts are NFKD-folded to ASCII and stripped to `[a-z0-9]`, so this surname
  becomes `dalia`. Names keep their original form, since HRIS CSVs are UTF-8 by contract (§7.5).
  The test writes the character as `\u2019` so the source stays unambiguous.
- **Lesson:** look at what the data actually contains before writing normalisation. Silver email
  hashing (`lower(trim(email))`) is safe because addresses are already ASCII, but names must stay
  UTF-8 all the way through.

## 2026-09-24 — T1.3: dev showed fewer leave starts than the config implies
- **Symptom:** one dev run produced 44 leave starts. `leave_annual` 0.02 × 3,000 employees suggests
  about 60.
- **Root cause:** not a bug. Across 20 seeds the mean is 54 (sd 4). Leave only starts for *active*
  employees, and with no hires until T1.4/T1.6, active headcount falls by about 12% over the year
  (average ≈ 2,800). One run at 44 is just a low draw.
- **Fix:** none to the engine. The rate test uses bounds taken from the 20-seed means, not the
  naive rate × headcount.
- **Lesson:** check a suspicious rate against a seed sweep before touching code. Until hires exist,
  every workforce count at full is about 17% below "rate × starting headcount". T1.10's
  calibration has to use exposure (employee-days), not starting headcount.
