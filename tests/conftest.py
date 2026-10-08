"""Shared pytest fixtures. The Spark session fixture arrives in T2.1."""

import pytest

from tests.contract_checks import Contracts


@pytest.fixture(scope="session")
def contracts() -> Contracts:
    """Every event contract, compiled once (ADR-0014)."""
    return Contracts()
