from pathlib import Path

import pytest

from hirestream.config import PipelineConfigError, load_pipeline_config

REPO_CONFIG = Path(__file__).parents[2] / "config" / "pipeline"


def write(directory: Path, env: str, text: str) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{env}.yaml").write_text(text)
    return directory


def test_local_config_runs_spark_locally_on_the_local_lake(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    config = load_pipeline_config(config_dir=REPO_CONFIG, environ={})
    assert config.env == "local"
    assert (config.reprocess_days, config.listing_window_hours) == (7, 48)
    assert config.lake_paths().root == f"file:{tmp_path}/data/lake"
    settings = config.spark_settings(app_name="silver-scheduling")
    assert (settings.mode, settings.master, settings.app_name) == (
        "local",
        "local[*]",
        "silver-scheduling",
    )


def test_cloud_config_needs_its_lake_root_from_the_environment() -> None:
    with pytest.raises(PipelineConfigError, match="set HIRESTREAM_LAKE_ROOT"):
        load_pipeline_config("cloud", config_dir=REPO_CONFIG, environ={})
    environ = {"HIRESTREAM_ENV": "cloud", "HIRESTREAM_LAKE_ROOT": "s3a://lake-bucket"}
    config = load_pipeline_config(config_dir=REPO_CONFIG, environ=environ)
    assert config.env == "cloud"
    assert config.spark_settings().mode == "emr"
    assert config.lake_paths().root == "s3a://lake-bucket"


def test_lake_root_override_applies_locally_too(tmp_path: Path) -> None:
    environ = {"HIRESTREAM_LAKE_ROOT": str(tmp_path / "elsewhere")}
    config = load_pipeline_config(config_dir=REPO_CONFIG, environ=environ)
    assert config.lake_paths().root == f"file:{tmp_path}/elsewhere"


def test_unknown_environment_is_rejected() -> None:
    with pytest.raises(PipelineConfigError, match="HIRESTREAM_ENV must be one of"):
        load_pipeline_config(config_dir=REPO_CONFIG, environ={"HIRESTREAM_ENV": "staging"})


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("lake_root: /lake\ndq_checks: x.yaml\nspark:\n  shufle_partitions: 4\n", "shufle"),
        (
            "lake_root: /lake\ndq_checks: x.yaml\nspark:\n  conf:\n"
            "    spark.sql.ansi.enabled: 'false'\n",
            "may not override",
        ),
        ("lake_root: /lake\ndq_checks: x.yaml\nenv: cloud\n", "must not set env"),
        ("lake_root: s3a://\ndq_checks: x.yaml\n", "names no bucket"),
        ("- a list\n", "must be a mapping"),
    ],
)
def test_bad_files_are_rejected(tmp_path: Path, text: str, message: str) -> None:
    config_dir = write(tmp_path, "local", text)
    with pytest.raises(PipelineConfigError, match=message):
        load_pipeline_config("local", config_dir=config_dir, environ={})


def test_missing_file_is_a_config_error(tmp_path: Path) -> None:
    with pytest.raises(PipelineConfigError, match="cannot read"):
        load_pipeline_config("local", config_dir=tmp_path, environ={})
