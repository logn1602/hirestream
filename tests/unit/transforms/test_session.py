import os
import time
from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

from hirestream.transforms.common.session import (
    LAUNCH_CONFS,
    PIPELINE_CONFS,
    SparkSessionError,
    SparkSettings,
    build_spark_session,
    jvm,
    jvm_default_timezone,
    launch_confs,
    pin_utc_process_timezone,
    session_confs,
    verify_session,
)
from tests.conftest import TEST_SPARK


def test_session_has_every_pipeline_setting(spark: SparkSession) -> None:
    for key, value in PIPELINE_CONFS.items():
        assert spark.conf.get(key) == value
    assert spark.conf.get("spark.sql.shuffle.partitions") == "2"


def test_driver_jvm_and_python_workers_agree_on_utc(spark: SparkSession) -> None:
    assert jvm_default_timezone(spark) == "UTC"
    df = spark.sql("SELECT timestamp'2026-01-01 00:00:00' AS ts")
    assert df.first() == (datetime(2026, 1, 1, 0, 0),)  # driver: collected in UTC
    hour = F.udf(lambda ts: ts.hour, IntegerType())  # a Python worker converts it
    assert df.select(hour("ts")).first() == (0,)


def test_settings_are_identical_in_both_modes_except_sizing() -> None:
    local, emr = SparkSettings(mode="local"), SparkSettings(mode="emr")
    assert session_confs(local) == session_confs(emr)
    assert launch_confs(local) == {**LAUNCH_CONFS, "spark.ui.enabled": "false"}
    assert launch_confs(emr) == dict(LAUNCH_CONFS)  # master and sizing come from EMR / submit
    sized = SparkSettings(shuffle_partitions=3, driver_memory="2g", ui_enabled=True)
    assert session_confs(sized)["spark.sql.shuffle.partitions"] == "3"
    assert launch_confs(sized)["spark.driver.memory"] == "2g"
    assert launch_confs(sized)["spark.ui.enabled"] == "true"


@pytest.mark.parametrize(
    "key", ["spark.sql.ansi.enabled", "spark.sql.shuffle.partitions", *LAUNCH_CONFS]
)
def test_extra_confs_cannot_override_pipeline_settings(key: str) -> None:
    with pytest.raises(ValueError, match="may not override"):
        SparkSettings(extra_confs={key: "x"})


def test_extra_confs_reach_the_builder() -> None:
    settings = SparkSettings(extra_confs={"spark.sql.adaptive.enabled": "false"})
    assert session_confs(settings)["spark.sql.adaptive.enabled"] == "false"


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [({"mode": "yarn"}, "mode must be"), ({"shuffle_partitions": 0}, "must be positive")],
)
def test_settings_reject_bad_values(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        SparkSettings(**kwargs)  # type: ignore[arg-type]


def test_verify_rejects_a_session_that_drifted(spark: SparkSession) -> None:
    spark.conf.set("spark.sql.ansi.enabled", "false")
    try:
        with pytest.raises(SparkSessionError, match=r"spark\.sql\.ansi\.enabled='false'"):
            verify_session(spark)
    finally:
        spark.conf.set("spark.sql.ansi.enabled", "true")
    verify_session(spark)


def test_verify_rejects_a_jvm_outside_utc(spark: SparkSession) -> None:
    # What a cluster would look like if spark-submit lost launch_confs() (T4.3).
    zone = jvm(spark).java.util.TimeZone
    zone.setDefault(zone.getTimeZone("America/New_York"))
    try:
        with pytest.raises(SparkSessionError, match="JVM default timezone 'America/New_York'"):
            verify_session(spark)
    finally:
        zone.setDefault(zone.getTimeZone("UTC"))
    verify_session(spark)


def test_emr_mode_joins_the_running_session(spark: SparkSession) -> None:
    # On EMR the session is created by spark-submit; the factory only applies and verifies.
    emr = SparkSettings(mode="emr", shuffle_partitions=TEST_SPARK.shuffle_partitions)
    assert build_spark_session(emr) is spark


def test_pin_utc_process_timezone() -> None:
    os.environ["TZ"] = "America/New_York"
    time.tzset()
    try:
        assert time.localtime(0).tm_hour == 19  # 1970-01-01T00:00Z in New York
    finally:
        pin_utc_process_timezone()
    assert time.localtime(0).tm_hour == 0
    assert time.tzname == ("UTC", "UTC")
