"""The test Postgres server (`HIRESTREAM_TEST_ATS_DSN`, ADR-0009 §6): tests that need it are
marked `integration` and skipped when the variable is unset. `fresh_db` is in conftest."""

import os
import secrets
from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
import pytest
from psycopg.conninfo import make_conninfo

TEST_DSN = os.environ.get("HIRESTREAM_TEST_ATS_DSN")
integration = pytest.mark.skipif(TEST_DSN is None, reason="HIRESTREAM_TEST_ATS_DSN is not set")


@contextmanager
def throwaway_db() -> Iterator[str]:
    """A new database on the test server, dropped afterwards, even if connections remain."""
    assert TEST_DSN is not None
    name = f"hs_test_{secrets.token_hex(4)}"
    with psycopg.connect(TEST_DSN, autocommit=True) as conn:
        conn.execute(f"CREATE DATABASE {name}")
    try:
        yield make_conninfo(TEST_DSN, dbname=name)
    finally:
        with psycopg.connect(TEST_DSN, autocommit=True) as conn:
            conn.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
