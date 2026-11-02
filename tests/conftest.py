"""Shared pytest fixtures: event contracts (ADR-0014) and one Spark session (ADR-0018)."""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from hirestream.transforms.common.session import (
    SparkSettings,
    build_spark_session,
    pin_utc_process_timezone,
)
from tests.contract_checks import Contracts

# Every test runs in UTC, as CI and EMR do. PySpark collects timestamps in the Python process's
# zone, so on a laptop outside UTC a Spark test would otherwise see different values than CI.
pin_utc_process_timezone()

TEST_SPARK = SparkSettings(app_name="hirestream-tests", master="local[2]", shuffle_partitions=2)


@pytest.fixture(scope="session")
def contracts() -> Contracts:
    """Every event contract, compiled once (ADR-0014)."""
    return Contracts()


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One local session for the whole run, built by the pipeline's own factory (~10 s to start).

    Tests share it, so they must not stop it, and must restore any runtime setting they change.
    """
    session = build_spark_session(TEST_SPARK)
    yield session
    session.stop()
