import tomllib
from importlib.metadata import version
from pathlib import Path

from pyspark.sql import SparkSession

from hirestream.transforms.common.session import jvm
from hirestream.versions import JAVA_MAJOR, SPARK_VERSION

PYPROJECT = Path(__file__).parents[2] / "pyproject.toml"


def test_installed_pyspark_matches_emr_spark_version() -> None:
    assert version("pyspark") == SPARK_VERSION


def test_pyproject_pins_pyspark_exactly() -> None:
    groups = tomllib.loads(PYPROJECT.read_text())["dependency-groups"]
    assert groups["spark"] == [f"pyspark=={SPARK_VERSION}"]


def test_local_spark_runs_on_pinned_java(spark: SparkSession) -> None:
    assert spark.version == SPARK_VERSION
    # ADR-0002 relies on Spark 4 defaulting to ANSI mode; the pipeline also sets it explicitly,
    # so check the built-in default rather than the session's value.
    ansi = jvm(spark).org.apache.spark.sql.internal.SQLConf.ANSI_ENABLED()
    assert ansi.defaultValueString() == "true"
    java = jvm(spark).java.lang.System.getProperty("java.version")
    assert int(java.split(".")[0]) == JAVA_MAJOR
    assert spark.range(3).count() == 3
