# ADR-0002: EMR Serverless release and runtime pins

- **Status:** Accepted
- **Date:** 2026-09-23
- **Task:** T0.4
- **Deviates from:** `docs/SPEC.md` §3 (default was `emr-spark-8.0.0`, fallback `emr-7.13.0`)

## Context
Spark jobs run on `local[*]` for development and CI, and on EMR Serverless in the cloud (§2.1). The
jobs are identical in both modes. That only holds if the local Spark, Java, and Python match what
EMR runs. The spec proposed `emr-spark-8.0.0` (Spark 4.0.x) and asked T0.4 to check it against
current AWS docs. The spec was written before `emr-spark-8.1.0` existed.

What the AWS docs said on 2026-09-23:

| Release label | Spark | Java (default first) | Python (default first) | Released | Standard support until |
|---|---|---|---|---|---|
| `emr-spark-8.1.0` (LTS) | 4.1.1-amzn-0 | 17, 21 | 3.11, 3.13 | 2026-09-08 | 2029-09-07 |
| `emr-spark-8.0.0` | 4.0.2-amzn-0 | 17, 21 | 3.11, 3.13 | 2026-05-21 | 2028-05-20 |
| `emr-7.14.0` | 3.5.8 | 17 | 3.11 | — | — |

- Both 8.x releases are listed for EMR Serverless and are available in us-east-1. Only the two
  Middle East regions are excluded.
- Both run ANSI SQL mode by default, use Scala 2.13, and dropped EMRFS in favour of S3A.
- Both support ARM64 (Graviton) through the application's `architecture` setting.
- `pyspark==4.1.1` is on PyPI and needs Python ≥ 3.10.

## Decision
| Pin | Value | Where it is enforced |
|---|---|---|
| EMR Serverless release | `emr-spark-8.1.0` | `hirestream.versions.EMR_RELEASE_LABEL` |
| Spark / local pyspark | `4.1.1` (exact) | `pyproject.toml` group `spark`; `hirestream.versions.SPARK_VERSION` |
| Java | 17 (Amazon Corretto on EMR, Temurin in CI) | `hirestream.versions.JAVA_MAJOR`; `ci.yml` |
| Python | 3.11 | `requires-python = ">=3.11,<3.12"` |
| Architecture | ARM64 | CDK `HirestreamCompute` (T4.1) |

`tests/unit/test_versions.py` fails if the installed pyspark, the pin in `pyproject.toml`, the JVM
major version, or ANSI mode differ from these values.

The pyspark dependency lives in a `spark` dependency group that `uv sync` installs by default. It is
not a runtime dependency of the wheel, because EMR provides its own pyspark and the job zip (T4.3)
must not bundle a second copy.

## Alternatives considered
- **`emr-spark-8.0.0` (spec default).** Rejected. It has the same Java and Python, but an older
  Spark and support ending 2028. It stays the fallback: switching is a change to `EMR_RELEASE_LABEL`,
  `SPARK_VERSION`, and the pyspark pin (4.0.2).
- **`emr-7.14.0` (Spark 3.5.8).** Rejected. ANSI mode is off by default in Spark 3.5, so local and
  cloud would have to set it explicitly to agree. It also uses Scala 2.12 and the older EMRFS
  path, and it is the older release line.
- **Java 21.** Rejected for now. EMR's default is 17, and CI and the Airflow image (T3.1) already
  assume it. Choosing 21 would mean passing `JAVA_HOME` overrides to every job for no benefit to
  this project.
- **Python 3.13.** Rejected. It is not EMR's default and would narrow library wheel availability.
- **x86_64.** Rejected. EMR Serverless charges less per vCPU-hour on ARM64. The job ships only
  pure-Python dependencies (`requirements-spark.txt`), so there is nothing to compile for ARM.

## Consequences
- Upstream Spark 4.1.1 locally vs Amazon's `4.1.1-amzn-0` on EMR is a parity gap. ADR-0003 records it.
- New in 8.1.0: EMR Serverless *merges* application-level and job-level Spark config. In 8.0.0 the
  job-level config replaced it. `hirestream cloud submit` (T4.3) must not assume either behaviour
  implicitly. It passes the full job config.
- 8.1.0 was two weeks old when this ADR was written. If a blocking issue appears, fall back to
  8.0.0 by writing a new ADR that supersedes this one.
- Spark 4.1 rules to follow: ANSI mode is on, so untrusted input is parsed with `try_*` functions
  (already in `CLAUDE.md`).
- The `spark` group adds about 400 MB to every `uv sync`, including CI.

## References
- [EMR Serverless release versions](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/release-versions.html)
- [emr-spark-8.1.0 (Release Guide)](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark810-release.html)
- [emr-spark-8.1.0 (EMR Serverless)](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/release-version-emr-spark-8.1.0.html)
- [emr-spark-8.0.0 (Release Guide)](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark800-release.html)
- [EMR Serverless 7.14.0](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/release-version-7140.html)
- [Using Java 17 with EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/using-java-runtime.html)
- [EMR Serverless architecture options](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/architecture.html)
- [pyspark on PyPI](https://pypi.org/project/pyspark/4.1.1/)
