"""Where the simulated vendor ATS lives: resolve the ats-db connection string (ADR-0008)."""

from __future__ import annotations

import os
from collections.abc import Mapping

from psycopg.conninfo import conninfo_to_dict, make_conninfo


class AtsDbError(RuntimeError):
    """The ats-db can't be reached or isn't configured."""


def resolve_ats_dsn(env: Mapping[str, str] | None = None) -> str | None:
    """`HIRESTREAM_ATS_DSN`, else a DSN built from the `ATS_DB_*` keys in `.env`; None if unset."""
    env = os.environ if env is None else env
    if dsn := env.get("HIRESTREAM_ATS_DSN"):
        return dsn
    name, user, password = (
        env.get("ATS_DB_NAME"),
        env.get("ATS_DB_USER"),
        env.get("ATS_DB_PASSWORD"),
    )
    if not (name and user and password):
        return None
    return make_conninfo(
        host=env.get("ATS_DB_HOST", "127.0.0.1"),
        port=env.get("ATS_DB_PORT", "15432"),
        dbname=name,
        user=user,
        password=password,
    )


def describe(dsn: str) -> str:
    """host:port/dbname for messages. Never includes the user or password."""
    parts = conninfo_to_dict(dsn)
    return (
        f"{parts.get('host', 'localhost')}:{parts.get('port', '5432')}/{parts.get('dbname', '?')}"
    )
