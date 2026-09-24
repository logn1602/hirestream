from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

BASE_CONFIG = Path(__file__).parents[3] / "config" / "generator" / "base.yaml"


@pytest.fixture(scope="session")
def base_config_path() -> Path:
    return BASE_CONFIG


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
