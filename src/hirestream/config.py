"""Pipeline configuration: `config/pipeline/{local,cloud}.yaml` plus overrides (SPEC §5).

`HIRESTREAM_ENV` picks the file, and `HIRESTREAM_LAKE_ROOT` replaces its lake root. These are
the only switches between modes (ADR-0003). This module runs on the CLI side (pydantic), and it
hands Spark code plain values: `SparkSettings` and `LakePaths`.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal, get_args

import yaml
from pydantic import BaseModel, ConfigDict, Field, PositiveInt, ValidationError, model_validator

from hirestream.transforms.common.lake import LakePaths
from hirestream.transforms.common.pii import PiiKey, PiiKeyError
from hirestream.transforms.common.session import SparkSettings

Environment = Literal["local", "cloud"]
ENVIRONMENTS: tuple[str, ...] = get_args(Environment)
DEFAULT_CONFIG_DIR = Path("config/pipeline")
PII_SALT_ENV = "HIRESTREAM_PII_SALT"
NEW_SALT = 'python -c "import secrets; print(secrets.token_hex(32))"'


class PipelineConfigError(ValueError):
    """The pipeline configuration is missing, malformed, or inconsistent."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SparkConfig(_Strict):
    master: str = "local[*]"
    shuffle_partitions: PositiveInt = 8
    driver_memory: str | None = None
    ui_enabled: bool = False
    conf: dict[str, str] = Field(default_factory=dict)


class PipelineConfig(_Strict):
    env: Environment
    lake_root: str
    reprocess_days: PositiveInt = 7  # gold's trailing safety net (SPEC §10.5)
    listing_window_hours: PositiveInt = 48  # hour prefixes an incremental silver run lists (§8)
    dq_checks: Path
    spark: SparkConfig = SparkConfig()

    @model_validator(mode="after")
    def _spark_settings_are_valid(self) -> PipelineConfig:
        self.spark_settings()  # e.g. spark.conf may not override a pipeline setting
        LakePaths.from_root(self.lake_root)
        return self

    def spark_settings(self, app_name: str = "hirestream") -> SparkSettings:
        s = self.spark
        return SparkSettings(
            mode="local" if self.env == "local" else "emr",
            app_name=app_name,
            master=s.master,
            shuffle_partitions=s.shuffle_partitions,
            driver_memory=s.driver_memory,
            ui_enabled=s.ui_enabled,
            extra_confs=dict(s.conf),
        )

    def lake_paths(self) -> LakePaths:
        return LakePaths.from_root(self.lake_root)


def load_pipeline_config(
    env: str | None = None,
    *,
    config_dir: Path = DEFAULT_CONFIG_DIR,
    environ: Mapping[str, str] | None = None,
) -> PipelineConfig:
    environ = os.environ if environ is None else environ
    env = env or environ.get("HIRESTREAM_ENV") or "local"
    if env not in ENVIRONMENTS:
        raise PipelineConfigError(f"HIRESTREAM_ENV must be one of {ENVIRONMENTS}, got {env!r}")
    path = config_dir / f"{env}.yaml"
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise PipelineConfigError(f"cannot read {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise PipelineConfigError(f"{path} must be a mapping")
    if "env" in raw:
        raise PipelineConfigError(f"{path} must not set env: the file name and HIRESTREAM_ENV do")
    data: dict[str, Any] = {**raw, "env": env}
    if override := environ.get("HIRESTREAM_LAKE_ROOT"):
        data["lake_root"] = override
    if data.get("lake_root") is None:
        raise PipelineConfigError(f"{path} has no lake_root: set HIRESTREAM_LAKE_ROOT")
    try:
        return PipelineConfig.model_validate(data)
    except ValidationError as exc:
        raise PipelineConfigError(f"{path}: {exc}") from exc


def load_pii_key(environ: Mapping[str, str] | None = None) -> PiiKey:
    """The email-hashing key from `HIRESTREAM_PII_SALT` (SPEC §5.1, ADR-0022). It is a secret, so
    it lives in the environment, not in config files, and no message ever includes it."""
    environ = os.environ if environ is None else environ
    salt = environ.get(PII_SALT_ENV)
    if not salt:
        raise PipelineConfigError(
            f"{PII_SALT_ENV} is not set: generate one with `{NEW_SALT}`, put it in .env, keep a "
            "copy in your password manager, and never change it (RUNBOOK: PII salt)"
        )
    try:
        return PiiKey(salt)
    except PiiKeyError as exc:
        raise PipelineConfigError(f"{PII_SALT_ENV}: {exc}; generate one with `{NEW_SALT}`") from exc
