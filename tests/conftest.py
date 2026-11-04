"""Shared pytest fixtures: the generator's config and one tiny backfill, event contracts
(ADR-0014), and one Spark session (ADR-0018)."""

from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from hirestream.generator.config import load_config
from hirestream.generator.run import BackfillResult, run_backfill
from hirestream.transforms.common.lake import Lake, LakePaths
from hirestream.transforms.common.session import (
    SparkSettings,
    build_spark_session,
    pin_utc_process_timezone,
)
from tests.contract_checks import Contracts
from tests.pg import throwaway_db

# Every test runs in UTC, as CI and EMR do. PySpark collects timestamps in the Python process's
# zone, so on a laptop outside UTC a Spark test would otherwise see different values than CI.
pin_utc_process_timezone()

TEST_SPARK = SparkSettings(app_name="hirestream-tests", master="local[2]", shuffle_partitions=2)
BASE_CONFIG = Path(__file__).parents[1] / "config" / "generator" / "base.yaml"


@pytest.fixture(scope="session")
def base_config_path() -> Path:
    return BASE_CONFIG


@pytest.fixture(scope="session")
def backfill(tmp_path_factory: pytest.TempPathFactory, base_config_path: Path) -> BackfillResult:
    """One tiny backfill (no ats-db), shared: ground truth, calibration and the report read it,
    and silver is reconciled against its bronze. Tests must not write into its lake."""
    lake = tmp_path_factory.mktemp("lake")
    return run_backfill(load_config(base_config_path, "tiny"), lake, run_id="gt")


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


@pytest.fixture
def lake(spark: SparkSession, tmp_path: Path) -> Lake:
    """An empty lake under the test's temp directory."""
    return Lake(spark, LakePaths.from_root(tmp_path / "lake"))


@pytest.fixture
def fresh_db() -> Iterator[str]:
    """A throwaway database on the test Postgres server, dropped afterwards (tests marked
    `integration` only: see tests/pg.py)."""
    with throwaway_db() as dsn:
        yield dsn
