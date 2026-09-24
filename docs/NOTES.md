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
