from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

from hirestream.generator.config import load_config
from hirestream.generator.run import BackfillResult, run_backfill

BASE_CONFIG = Path(__file__).parents[3] / "config" / "generator" / "base.yaml"


@pytest.fixture(scope="session")
def base_config_path() -> Path:
    return BASE_CONFIG


@pytest.fixture(scope="session")
def backfill(tmp_path_factory: pytest.TempPathFactory, base_config_path: Path) -> BackfillResult:
    """One tiny backfill (no ats-db) shared by the run-output tests: ground truth, calibration and
    the report all read it."""
    lake = tmp_path_factory.mktemp("lake")
    return run_backfill(load_config(base_config_path, "tiny"), lake, run_id="gt")


@pytest.fixture
def raw_config() -> dict[str, Any]:
    data: dict[str, Any] = yaml.safe_load(BASE_CONFIG.read_text())
    return data


@pytest.fixture
def write_config(tmp_path: Path) -> Callable[[dict[str, Any]], Path]:
    def _write(data: dict[str, Any]) -> Path:
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump(data, sort_keys=False))
        return path

    return _write
