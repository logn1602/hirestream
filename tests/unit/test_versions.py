import tomllib
from importlib.metadata import version
from pathlib import Path

from pyspark.sql import SparkSession

from hirestream.versions import JAVA_MAJOR, SPARK_VERSION

PYPROJECT = Path(__file__).parents[2] / "pyproject.toml"


def test_installed_pyspark_matches_emr_spark_version() -> None:
    assert version("pyspark") == SPARK_VERSION


def test_pyproject_pins_pyspark_exactly() -> None:
    groups = tomllib.loads(PYPROJECT.read_text())["dependency-groups"]
    assert groups["spark"] == [f"pyspark=={SPARK_VERSION}"]


def test_local_spark_runs_on_pinned_java() -> None:
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("versions-smoke")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        assert spark.version == SPARK_VERSION
        assert spark.conf.get("spark.sql.ansi.enabled") == "true"
        java = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")  # type: ignore[union-attr]
        assert int(java.split(".")[0]) == JAVA_MAJOR
        assert spark.range(3).count() == 3
    finally:
        spark.stop()
