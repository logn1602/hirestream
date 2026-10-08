"""Event contracts: a JSON Schema per stream event type and version (SPEC §7, ADR-0014).

The files live at the repository root, `contracts/<folder>/<event_type>.v<N>.json`. This module
only finds and reads them, with the standard library alone, so silver's Spark jobs can use it too
(they can't import jsonschema). Validation lives in the tests.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

CONTRACTS_DIR = Path(__file__).resolve().parents[2] / "contracts"
SOURCE_DIRS = {"scheduling-service": "scheduling", "jobboard-web": "jobboard"}  # = bronze folders
_FILE = re.compile(r"^(?P<event_type>[a-z][a-z_]*)[.]v(?P<version>[1-9][0-9]*)[.]json$")


def contract_path(source: str, event_type: str, version: int) -> Path:
    return _root() / SOURCE_DIRS[source] / f"{event_type}.v{version}.json"


def load_contract(source: str, event_type: str, version: int) -> dict[str, Any]:
    contract: dict[str, Any] = json.loads(
        contract_path(source, event_type, version).read_text(encoding="utf-8")
    )
    return contract


def list_contracts() -> list[tuple[str, str, int]]:
    """Every (source, event_type, version) on disk. A file that isn't a contract is an error."""
    found = []
    for source, folder in SOURCE_DIRS.items():
        for path in sorted((_root() / folder).iterdir()):
            match = _FILE.match(path.name)
            if match is None:
                raise ValueError(f"{path} is not named <event_type>.v<N>.json")
            found.append((source, match["event_type"], int(match["version"])))
    return found


def _root() -> Path:
    if not CONTRACTS_DIR.is_dir():
        raise FileNotFoundError(
            f"no contracts at {CONTRACTS_DIR}: they ship with the repository (ADR-0014)"
        )
    return CONTRACTS_DIR
