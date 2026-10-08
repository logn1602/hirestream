"""The contract files themselves (SPEC §7, ADR-0014): dialect, consistency, examples, mutations.

Real producer events are validated in the producer tests (test_scheduling, test_jobboard, and the
slow test_producer_contracts); chaos lines in test_chaos.
"""

import ast
import copy
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

import hirestream.contracts as contracts_module
from hirestream.contracts import CONTRACTS_DIR, SOURCE_DIRS, contract_path, list_contracts
from hirestream.generator import chaos, sinks
from tests.contract_checks import Contracts, Signature

# SPEC §7.2 and §7.3: every event type, in schema versions 1 and 2.
EXPECTED = {
    "scheduling-service": {
        "interview_scheduled", "interview_rescheduled", "interview_cancelled",
        "interview_completed", "interview_no_show", "feedback_submitted", "feedback_updated",
    },
    "jobboard-web": {
        "page_view", "job_search", "job_view", "job_save", "apply_start", "apply_submit",
    },
}  # fmt: skip
DIALECT = {
    "$schema", "$id", "title", "description", "$comment", "examples", "type", "properties",
    "required", "additionalProperties", "enum", "const", "pattern", "minimum", "minLength",
    "items", "minItems", "uniqueItems", "allOf", "if", "then",
}  # fmt: skip
REQ_BEARING = ("page_view", "job_view", "job_save", "apply_start", "apply_submit")


def _subschemas(node: Any, path: str = "") -> Iterator[tuple[str, dict[str, Any]]]:
    """Every schema object in a contract (not the `examples`)."""
    if isinstance(node, dict):
        yield path, node
        for key, value in node.items():
            if key == "examples":
                continue
            if key == "properties":
                for name, prop in value.items():
                    yield from _subschemas(prop, f"{path}/properties/{name}")
            elif isinstance(value, (dict, list)) and key not in ("enum", "required", "const"):
                yield from _subschemas(value, f"{path}/{key}")
    elif isinstance(node, list):
        for i, item in enumerate(node):
            yield from _subschemas(item, f"{path}/{i}")


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    keys = [k for k, _ in pairs]
    assert len(keys) == len(set(keys)), f"duplicate keys {keys}"
    return dict(pairs)


def test_the_expected_contracts_exist_and_nothing_else() -> None:
    expected = {(s, t, v) for s, types in EXPECTED.items() for t in types for v in (1, 2)}
    assert set(list_contracts()) == expected
    assert (CONTRACTS_DIR / "README.md").is_file()


@pytest.mark.parametrize("key", list_contracts())
def test_files_follow_the_dialect(key: tuple[str, str, int]) -> None:
    path = contract_path(*key)
    text = path.read_text(encoding="utf-8")
    assert text.isascii()
    schema = json.loads(text, object_pairs_hook=_no_duplicate_keys)
    assert text == json.dumps(schema, indent=2) + "\n"  # canonical formatting
    Draft202012Validator.check_schema(schema)
    for where, node in _subschemas(schema):
        assert set(node) <= DIALECT, (where, set(node) - DIALECT)
        assert "$ref" not in node and "format" not in node and "else" not in node
        if node.get("type") == "object":
            assert node["required"] == list(node["properties"]), where  # closed and complete
            assert node["additionalProperties"] is False, where
        if "pattern" in node:
            pattern = node["pattern"]
            assert pattern.startswith("^") and pattern.endswith("$"), where
            assert "\\" not in pattern and "(?" not in pattern, where  # portable to Java/Spark
        for value in node.get("enum", []):
            assert value == value.lower(), (where, value)  # chaos upper-cases to break an enum


@pytest.mark.parametrize("key", list_contracts())
def test_names_ids_and_consts_agree(contracts: Contracts, key: tuple[str, str, int]) -> None:
    source, event_type, version = key
    schema = contracts.schemas[key]
    assert schema["$id"] == f"urn:hirestream:contract:{SOURCE_DIRS[source]}:{event_type}:v{version}"
    props = schema["properties"]
    assert props["event_type"]["const"] == event_type
    assert props["source"]["const"] == source
    assert props["schema_version"] == {"type": "integer", "const": version}


def _envelope(schema: dict[str, Any]) -> dict[str, Any]:
    envelope = {k: v for k, v in schema.items() if k not in ("$id", "title", "description")}
    envelope.pop("examples")
    props = dict(envelope["properties"])
    props.pop("payload")
    props.pop("event_type")
    return {**envelope, "properties": props}


def test_envelopes_are_identical_within_a_source_and_version(contracts: Contracts) -> None:
    for source, types in EXPECTED.items():
        for version in (1, 2):
            envelopes = [_envelope(contracts.schemas[(source, t, version)]) for t in sorted(types)]
            assert all(e == envelopes[0] for e in envelopes), (source, version)


def test_v2_changes_exactly_what_spec_says(contracts: Contracts) -> None:
    for (source, event_type, version), v1 in contracts.schemas.items():
        if version != 1:
            continue
        v2 = contracts.schemas[(source, event_type, 2)]
        p1, p2 = v1["properties"], v2["properties"]
        if source == "jobboard-web":  # v2 adds context.device_type (SPEC §7.1)
            ctx1, ctx2 = p1["context"]["properties"], p2["context"]["properties"]
            assert set(ctx2) - set(ctx1) == {"device_type"} and set(ctx1) <= set(ctx2)
            assert p1["payload"] == p2["payload"]
        elif event_type == "interview_scheduled":  # interviewer_id -> interviewer_ids, + format
            k1, k2 = set(p1["payload"]["properties"]), set(p2["payload"]["properties"])
            assert k1 - k2 == {"interviewer_id"}
            assert k2 - k1 == {"interviewer_ids", "interview_format"}
        else:
            assert p1["payload"] == p2["payload"]


@pytest.mark.parametrize("key", list_contracts())
def test_examples_meet_their_contract(contracts: Contracts, key: tuple[str, str, int]) -> None:
    examples = contracts.schemas[key]["examples"]
    assert examples
    for example in examples:
        assert contracts.signature(key[0], example) == frozenset()
    if key[1] == "interview_scheduled":
        types = {e["payload"]["interview_type"] for e in examples}
        assert types == {"phone_screen", "onsite"}  # both conditional branches


def test_chaos_can_break_what_the_contracts_require(contracts: Contracts) -> None:
    for (source, _, _), schema in contracts.schemas.items():
        for path in chaos.REQUIRED[source]:
            node = schema
            for part in path[:-1]:
                node = node["properties"][part]
            assert path[-1] in node["required"], (source, path)
        props = schema["properties"]
        if source == "jobboard-web":
            assert "enum" in props["context"]["properties"]["referrer_type"]
        for key in chaos.SCHEDULING_ENUMS:
            if source == "scheduling-service" and key in props["payload"]["properties"]:
                assert "enum" in props["payload"]["properties"][key]


def test_source_names_match_the_rest_of_the_generator() -> None:
    assert SOURCE_DIRS == sinks.SOURCE_DIRS
    assert set(SOURCE_DIRS) == set(chaos.SOURCES.values())


def test_the_loader_needs_only_the_standard_library() -> None:
    """Silver's Spark jobs will import it; they may not import jsonschema (CLAUDE.md)."""
    tree = ast.parse(Path(contracts_module.__file__).read_text())
    imported = {
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in (
            node.names if isinstance(node, ast.Import) else [ast.alias(node.module or "")]
        )
    }
    assert imported <= set(sys.stdlib_module_names) | {"__future__"}


def test_a_stray_file_is_an_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    for folder in SOURCE_DIRS.values():
        (tmp_path / folder).mkdir()
    (tmp_path / "jobboard" / "notes.txt").write_text("hi")
    monkeypatch.setattr(contracts_module, "CONTRACTS_DIR", tmp_path)
    with pytest.raises(ValueError, match="not named"):
        list_contracts()
    monkeypatch.setattr(contracts_module, "CONTRACTS_DIR", tmp_path / "missing")
    with pytest.raises(FileNotFoundError, match="ship with the repository"):
        list_contracts()


# ------------------------------------------------------------------ mutations: one fault each


def _example(contracts: Contracts, source: str, event_type: str, version: int) -> dict[str, Any]:
    return copy.deepcopy(contracts.schemas[(source, event_type, version)]["examples"][0])


def _phone(contracts: Contracts, version: int) -> dict[str, Any]:
    examples = contracts.schemas[("scheduling-service", "interview_scheduled", version)]["examples"]
    return copy.deepcopy(
        next(e for e in examples if e["payload"]["interview_type"] == "phone_screen")
    )


S, J = "scheduling-service", "jobboard-web"


def _mutations(contracts: Contracts) -> Iterator[tuple[str, str, dict[str, Any], Signature]]:
    e = _example(contracts, J, "job_view", 2)
    e["payload"]["requisition_id"] = "R000001"
    yield "unknown field", J, e, frozenset({("additionalProperties", "/payload/requisition_id")})
    e = _example(contracts, S, "interview_completed", 1)
    e["schema_version"] = 3
    yield "unknown version", S, e, frozenset({("unknown_schema_version", "/schema_version")})
    e = _example(contracts, J, "job_search", 1)
    e["payload"]["results_count"] = "7"
    yield "wrong type", J, e, frozenset({("type", "/payload/results_count")})
    e = _example(contracts, J, "job_search", 1)
    e["payload"]["results_count"] = 7.0
    yield "a float is not an integer", J, e, frozenset({("type", "/payload/results_count")})
    e = _example(contracts, J, "apply_submit", 1)
    e["payload"]["candidate_id"] = None
    yield "null where not allowed", J, e, frozenset({("type", "/payload/candidate_id")})
    e = _example(contracts, S, "interview_no_show", 2)
    e["payload"]["no_show_party"] = "Candidate"
    yield "bad enum", S, e, frozenset({("enum", "/payload/no_show_party")})
    e = _example(contracts, S, "interview_completed", 2)
    e["event_type"] = "INTERVIEW_COMPLETED"
    yield "unknown event type", S, e, frozenset({("enum", "/event_type")})
    e = _example(contracts, J, "page_view", 1)
    del e["event_id"]
    yield "missing envelope field", J, e, frozenset({("required", "/event_id")})
    e = _example(contracts, J, "page_view", 2)
    del e["context"]["device_type"]
    yield "v2 context without device_type", J, e, frozenset({("required", "/context/device_type")})
    e = _phone(contracts, 1)
    e["payload"]["interviewer_ids"] = [e["payload"].pop("interviewer_id")]
    yield "v1 with a v2 field", S, e, frozenset({
        ("additionalProperties", "/payload/interviewer_ids"),
        ("required", "/payload/interviewer_id"),
    })  # fmt: skip
    e = _phone(contracts, 1)
    e["payload"]["loop_id"] = "L00000001"
    yield "a phone screen in a loop", S, e, frozenset({("type", "/payload/loop_id")})
    e = _phone(contracts, 2)
    e["payload"]["interviewer_ids"] *= 2
    yield "the same panelist twice", S, e, frozenset({("uniqueItems", "/payload/interviewer_ids")})
    e = _phone(contracts, 1)
    e["payload"]["interview_type"] = "PHONE_SCREEN"
    yield "an upper-cased type is one error", S, e, frozenset({("enum", "/payload/interview_type")})
    e = _phone(contracts, 1)
    e["payload"]["scheduled_start"] = e["payload"]["scheduled_start"][:19]  # the 1.3.0 bug
    yield "a naive start", S, e, frozenset({("pattern", "/payload/scheduled_start")})
    e = _example(contracts, S, "interview_rescheduled", 1)
    del e["payload"]["timezone"]
    yield "no timezone", S, e, frozenset({("required", "/payload/timezone")})
    e = _example(contracts, J, "job_view", 1)
    e["event_ts"] = e["event_ts"] // 1000
    yield "seconds instead of milliseconds", J, e, frozenset({("minimum", "/event_ts")})


def test_each_fault_is_reported_exactly(contracts: Contracts) -> None:
    for name, source, event, expected in _mutations(contracts):
        assert contracts.signature(source, event) == expected, name


@pytest.mark.parametrize("event_type", REQ_BEARING)
def test_the_silent_schema_break_is_visible(contracts: Contracts, event_type: str) -> None:
    """COE-001's rename breaks the contract; catching it in production is T5.5's work."""
    for version in (1, 2):
        event = _example(contracts, J, event_type, version)
        payload = event["payload"]
        event["payload"] = {
            ("requisition_id" if k == "req_id" else k): v for k, v in payload.items()
        }
        assert contracts.signature(J, event) == frozenset({
            ("additionalProperties", "/payload/requisition_id"),
            ("required", "/payload/req_id"),
        })  # fmt: skip
