"""The pipeline's SparkSession, locally or on EMR Serverless (ADR-0018).

Every setting a job's results depend on is in `PIPELINE_CONFS`, applied and then verified in both
modes, so "jobs are identical in both modes" (ADR-0003) is checked at startup, not assumed. Only
sizing differs between modes. Settings that configure a JVM only work before it starts:
`LAUNCH_CONFS` go on the builder locally and on `spark-submit` for EMR Serverless (T4.3), and
`verify_session` refuses a session that is missing them.
"""

from __future__ import annotations

import os
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Final, Literal

from pyspark.sql import SparkSession

SparkMode = Literal["local", "emr"]
SPARK_MODES: Final = ("local", "emr")
UTC: Final = "UTC"

PIPELINE_CONFS: Final[Mapping[str, str]] = MappingProxyType(
    {
        # All timestamps are UTC after bronze: parsing, date extraction and display.
        "spark.sql.session.timeZone": UTC,
        # Spark 4's default (ADR-0002), set explicitly so that no environment can turn it off.
        "spark.sql.ansi.enabled": "true",
        # An overwrite replaces only the partitions the written frame contains (SPEC §9.1).
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.sql.parquet.compression.codec": "snappy",
        # Spark's default is INT96, which Parquet deprecated and pyarrow reads as nanoseconds.
        # Microseconds are Spark's own precision and load cleanly into Postgres and Redshift (G6).
        "spark.sql.parquet.outputTimestampType": "TIMESTAMP_MICROS",
    }
)

_UTC_JAVA_OPTIONS = f"-Duser.timezone={UTC}"
LAUNCH_CONFS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "spark.driver.extraJavaOptions": _UTC_JAVA_OPTIONS,
        "spark.executor.extraJavaOptions": _UTC_JAVA_OPTIONS,
        # Python workers turn TIMESTAMP values into datetimes in their own process's zone.
        "spark.executorEnv.TZ": UTC,
    }
)
_SHUFFLE_PARTITIONS = "spark.sql.shuffle.partitions"
_RESERVED = frozenset({*PIPELINE_CONFS, *LAUNCH_CONFS, _SHUFFLE_PARTITIONS})


class SparkSessionError(RuntimeError):
    """The session is missing a setting the pipeline depends on."""


@dataclass(frozen=True)
class SparkSettings:
    """How to start Spark. `master`, `driver_memory` and `ui_enabled` apply in local mode only:
    EMR Serverless provides the master, and driver sizing is a submit setting there (T4.3)."""

    mode: SparkMode = "local"
    app_name: str = "hirestream"
    master: str = "local[*]"
    shuffle_partitions: int = 8
    driver_memory: str | None = None
    ui_enabled: bool = False
    extra_confs: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.mode not in SPARK_MODES:
            raise ValueError(f"mode must be one of {SPARK_MODES}, got {self.mode!r}")
        if self.shuffle_partitions < 1:
            raise ValueError(f"shuffle_partitions must be positive, got {self.shuffle_partitions}")
        if clash := sorted(_RESERVED.intersection(self.extra_confs)):
            raise ValueError(f"extra_confs may not override pipeline settings: {clash}")


def session_confs(settings: SparkSettings) -> dict[str, str]:
    """Runtime settings, set on the builder in both modes."""
    return {
        **PIPELINE_CONFS,
        _SHUFFLE_PARTITIONS: str(settings.shuffle_partitions),
        **settings.extra_confs,
    }


def launch_confs(settings: SparkSettings) -> dict[str, str]:
    """JVM settings: on the builder locally, on `spark-submit` for EMR Serverless (T4.3)."""
    confs = dict(LAUNCH_CONFS)
    if settings.mode == "local":
        confs["spark.ui.enabled"] = "true" if settings.ui_enabled else "false"
        if settings.driver_memory is not None:
            confs["spark.driver.memory"] = settings.driver_memory
    return confs


def build_spark_session(settings: SparkSettings) -> SparkSession:
    """Start the pipeline's session, or join the running one, and verify it.

    Jobs never build their own session: they take this one as an argument.
    """
    pin_utc_process_timezone()
    confs = session_confs(settings)
    builder = SparkSession.builder.appName(settings.app_name)
    if settings.mode == "local":
        # Workers must run the driver's interpreter; `python3` on PATH can be another version.
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        builder = builder.master(settings.master)
        confs = {**launch_confs(settings), **confs}
    for key, value in confs.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    verify_session(spark)
    return spark


def pin_utc_process_timezone() -> None:
    """Make this Python process's local timezone UTC.

    PySpark turns TIMESTAMP values into naive datetimes in the Python process's zone, whatever the
    session zone is. On a laptop in New York, 2026-01-01T00:00Z collects as 2025-12-31 19:00,
    while CI and EMR, which run in UTC, see 00:00. Pinning the zone removes that difference.
    """
    os.environ["TZ"] = UTC
    time.tzset()


def verify_session(spark: SparkSession) -> None:
    """Raise SparkSessionError unless the session has every pipeline setting and a UTC JVM."""
    problems = [
        f"{key}={actual!r} (want {expected!r})"
        for key, expected in PIPELINE_CONFS.items()
        if (actual := spark.conf.get(key)) != expected
    ]
    zone = jvm_default_timezone(spark)
    if zone != UTC:
        problems.append(
            f"JVM default timezone {zone!r} (want {UTC!r}; pass launch_confs() at launch)"
        )
    if problems:
        raise SparkSessionError(
            "Spark session is not set up for the pipeline: " + "; ".join(problems)
        )


def jvm(spark: SparkSession) -> Any:
    """The session's py4j view of the JVM, for Hadoop and java.* calls (classic Spark only)."""
    view = spark.sparkContext._jvm
    if view is None:
        raise SparkSessionError("the SparkContext has no JVM (it was stopped)")
    return view


def jvm_default_timezone(spark: SparkSession) -> str:
    return str(jvm(spark).java.util.TimeZone.getDefault().getID())
