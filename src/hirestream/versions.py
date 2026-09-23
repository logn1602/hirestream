"""Runtime pins shared by local Spark, the CDK app, and `hirestream cloud submit` (ADR-0002)."""

from typing import Final

EMR_RELEASE_LABEL: Final = "emr-spark-8.1.0"
"""EMR Serverless release label. Changing it requires a new ADR superseding ADR-0002."""

SPARK_VERSION: Final = "4.1.1"
"""Upstream Spark version of EMR_RELEASE_LABEL (EMR ships 4.1.1-amzn-0); pyspark is pinned to it."""

JAVA_MAJOR: Final = 17
"""JDK major version: the EMR default (Amazon Corretto 17), and the one used locally and in CI."""
