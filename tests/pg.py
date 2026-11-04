"""The test Postgres server (`HIRESTREAM_TEST_ATS_DSN`, ADR-0009 §6): tests that need it are
marked `integration` and skipped when the variable is unset. `fresh_db` is in conftest."""

import os

import pytest

TEST_DSN = os.environ.get("HIRESTREAM_TEST_ATS_DSN")
integration = pytest.mark.skipif(TEST_DSN is None, reason="HIRESTREAM_TEST_ATS_DSN is not set")
