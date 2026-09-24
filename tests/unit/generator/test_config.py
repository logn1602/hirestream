from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from hirestream.generator.config import (
    INCIDENT_NAMES,
    config_hash,
    load_config,
    preset_names,
)

WriteConfig = Callable[[dict[str, Any]], Path]


@pytest.mark.parametrize("preset", ["tiny", "dev", "full"])
def test_every_preset_loads(base_config_path: Path, preset: str) -> None:
    cfg = load_config(base_config_path, preset)
    assert cfg.preset == preset
    assert cfg.scale.org_count <= len(cfg.org_model.orgs)


def test_preset_overrides_window_and_scale_only(base_config_path: Path) -> None:
    tiny = load_config(base_config_path, "tiny")
    base = load_config(base_config_path, None)
    assert tiny.window.n_days == 90
    assert tiny.scale.initial_headcount == 300
    assert base.preset == "base"
    assert tiny.model_dump(exclude={"preset", "window", "scale"}) == base.model_dump(
        exclude={"preset", "window", "scale"}
    )


def test_preset_names(base_config_path: Path) -> None:
    assert preset_names(base_config_path) == ["dev", "full", "tiny"]


def test_unknown_preset_is_rejected(base_config_path: Path) -> None:
    with pytest.raises(KeyError, match="unknown preset 'huge'"):
        load_config(base_config_path, "huge")


def test_preset_may_not_override_other_sections(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    raw_config["presets"]["tiny"]["workforce"] = {"attrition_annual": 0.5}
    with pytest.raises(ValidationError, match="workforce"):
        load_config(write_config(raw_config), "tiny")


def test_unknown_key_is_rejected(raw_config: dict[str, Any], write_config: WriteConfig) -> None:
    raw_config["workforce"]["attrition_anual"] = 0.12  # typo must not be silently ignored
    with pytest.raises(ValidationError, match="attrition_anual"):
        load_config(write_config(raw_config), "tiny")


@pytest.mark.parametrize(
    ("section", "key", "value", "message"),
    [
        (("workforce",), "attrition_annual", 1.2, "less than or equal to 1"),
        (("org_model",), "levels", {"L3": 0.5, "L4": 0.4}, "shares must sum to 1"),
        (("org_model",), "span_of_control", [9, 5], "lower bound 9 exceeds"),
        (("chaos", "schema_v2"), "jobboard_at", 1.5, "less than or equal to 1"),
        (("window",), "sim_end", "2024-12-31", "sim_end is before sim_start"),
        (("ats", "external", "p_advance"), "applied", 0.99, "above 1"),
        (("ats",), "first_gate_channel_multiplier", {"referral": 1.8}, "keys must be"),
        (("org_model", "locations", 0), "tz", "Mars/Olympus", "unknown IANA timezone"),
        (("org_model",), "span_of_control", [6, 8], "needs lo >= 1"),
        (("meta",), "company_founded", "2025-01-01", "must be before sim_start"),
        (("requisitions", "popularity"), "pareto_alpha", 1.0, "greater than 1"),
        (("requisitions", "popularity"), "truncate_at", 1.0, "greater than 1"),
    ],
)
def test_invalid_values_are_rejected(
    raw_config: dict[str, Any],
    write_config: WriteConfig,
    section: tuple[str | int, ...],
    key: str,
    value: object,
    message: str,
) -> None:
    node: Any = raw_config
    for part in section:
        node = node[part]
    node[key] = value
    with pytest.raises(ValidationError, match=message):
        load_config(write_config(raw_config), None)


def test_org_count_cannot_exceed_named_orgs(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    raw_config["presets"]["tiny"]["scale"]["org_count"] = 9
    with pytest.raises(ValidationError, match="exceeds the 8 orgs"):
        load_config(write_config(raw_config), "tiny")


def test_every_location_country_has_a_faker_locale(
    raw_config: dict[str, Any], write_config: WriteConfig
) -> None:
    del raw_config["meta"]["faker_locale_by_country"]["IN"]
    with pytest.raises(ValidationError, match=r"no Faker locale for countries \['IN'\]"):
        load_config(write_config(raw_config), None)


def test_config_hash_is_stable_and_preset_sensitive(base_config_path: Path) -> None:
    tiny = config_hash(load_config(base_config_path, "tiny"))
    assert tiny == config_hash(load_config(base_config_path, "tiny"))
    assert tiny != config_hash(load_config(base_config_path, "dev"))
    assert len(tiny) == 64


def test_config_hash_changes_with_any_parameter(
    raw_config: dict[str, Any], write_config: WriteConfig, base_config_path: Path
) -> None:
    raw_config["scheduling"]["feedback"]["overload_multiplier"] = 2.1
    changed = config_hash(load_config(write_config(raw_config), "tiny"))
    assert changed != config_hash(load_config(base_config_path, "tiny"))


def test_incident_names_match_spec() -> None:
    assert INCIDENT_NAMES == (
        "duplicate_storm",
        "silent_schema_break",
        "late_burst",
        "hris_partial_file",
    )


def test_config_is_immutable(base_config_path: Path) -> None:
    cfg = load_config(base_config_path, "tiny")
    with pytest.raises(ValidationError, match="frozen"):
        cfg.meta.seed = 1  # type: ignore[misc]
