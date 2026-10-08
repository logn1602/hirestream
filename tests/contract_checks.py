"""Validating events against their contracts, for tests (SPEC §7, ADR-0014).

A violation signature is a set of (keyword, JSON pointer) pairs. `required` points at the missing
key and `additionalProperties` at the unexpected one, so lines with the same faults compare equal.
Events are validated as they travel: `orjson.loads(orjson.dumps(body))`, and integers are strict
(JSON `1.0` is not an integer here, as it isn't for Spark's LongType).
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import orjson
from jsonschema import Draft202012Validator, validators
from jsonschema.exceptions import ValidationError
from jsonschema.protocols import Validator

from hirestream.contracts import list_contracts, load_contract
from hirestream.generator.events import StreamEvent

Signature = frozenset[tuple[str, str]]
VALID: Signature = frozenset()
STARTS = ("scheduled_start", "previous_start", "new_start")


def _is_integer(checker: object, instance: object) -> bool:
    return isinstance(instance, int) and not isinstance(instance, bool)


StrictValidator = validators.extend(  # type: ignore[no-untyped-call]
    Draft202012Validator,
    type_checker=Draft202012Validator.TYPE_CHECKER.redefine("integer", _is_integer),
)


class Contracts:
    """Every contract on disk, ready to validate decoded events."""

    def __init__(self) -> None:
        self.schemas = {key: load_contract(*key) for key in list_contracts()}
        self._validators: dict[tuple[str, str, int], Validator] = {
            key: StrictValidator(schema) for key, schema in self.schemas.items()
        }
        self.event_types = {
            source: {t for s, t, _ in self.schemas if s == source} for source, _, _ in self.schemas
        }

    def signature(self, source: str, body: object) -> Signature:
        """The violations of a decoded event. The contract is chosen by the event's own
        `event_type` and `schema_version`, never by date.
        """
        if not isinstance(body, dict):
            return frozenset({("type", "")})
        if "event_type" not in body:
            return frozenset({("required", "/event_type")})
        if body["event_type"] not in self.event_types[source]:
            return frozenset({("enum", "/event_type")})  # e.g. an upper-cased event_type
        version = body.get("schema_version")
        if not isinstance(version, int) or isinstance(version, bool):
            return frozenset({("unknown_schema_version", "/schema_version")})
        validator = self._validators.get((source, str(body["event_type"]), version))
        if validator is None:
            return frozenset({("unknown_schema_version", "/schema_version")})
        if validator.is_valid(body):
            return VALID
        return frozenset(pair for error in validator.iter_errors(body) for pair in _pairs(error))

    def check(self, event: StreamEvent) -> Signature:
        """An event as it would arrive: serialized and parsed back."""
        return self.signature(event.source, wire(event.body))


def wire(body: Mapping[str, Any]) -> Any:
    return orjson.loads(orjson.dumps(body))


def quarantine_reason(contracts: Contracts, source: str, line: bytes) -> str | None:
    """The SPEC §9.1 reason silver should quarantine a bronze line for, or None to keep it,
    by ADR-0014 §5's precedence. A naive start that still has its timezone is repaired, and
    unknown fields are drift: both are kept.
    """
    try:
        body = json.loads(line)
    except ValueError:
        return "MALFORMED_JSON"
    signature = contracts.signature(source, body)
    if any(kw == "required" and pointer != "/payload/timezone" for kw, pointer in signature):
        return "MISSING_REQUIRED_FIELD"
    if any(kw in ("enum", "const") for kw, _ in signature):
        return "INVALID_ENUM"
    if any(kw == "unknown_schema_version" for kw, _ in signature):
        return "UNKNOWN_SCHEMA_VERSION"
    if ("required", "/payload/timezone") in signature:
        return "UNRESOLVABLE_TIMEZONE"
    return None


def naive_start_violations(body: Mapping[str, Any]) -> Signature:
    """What the timezone-bug build is expected to break: every start field's offset, and
    `timezone` when it was dropped (ADR-0011, ADR-0014).
    """
    payload = body["payload"]
    found = {("pattern", f"/payload/{key}") for key in STARTS if key in payload}
    if "timezone" not in payload:
        found.add(("required", "/payload/timezone"))
    return frozenset(found)


class ValidatingSink:
    """An event sink that keeps only counts: (source, event_type, version, producer, signature)."""

    def __init__(self, contracts: Contracts) -> None:
        self._contracts = contracts
        self.counts: Counter[tuple[str, str, int, str, Signature]] = Counter()

    def write(self, events: Sequence[StreamEvent]) -> None:
        for event in events:
            body = wire(event.body)
            signature = self._contracts.signature(event.source, body)
            key = (
                event.source,
                event.event_type,
                body["schema_version"],
                body["producer_version"],
                signature,
            )
            self.counts[key] += 1


def _pairs(error: ValidationError) -> Iterator[tuple[str, str]]:
    pointer = "".join(f"/{part}" for part in error.absolute_path)
    required, schema = error.validator_value, error.schema
    if error.validator == "required" and isinstance(error.instance, dict):
        assert isinstance(required, list)
        for key in sorted(set(required) - set(error.instance)):
            yield ("required", f"{pointer}/{key}")
    elif error.validator == "additionalProperties" and isinstance(error.instance, dict):
        allowed = set(schema.get("properties", {})) if isinstance(schema, Mapping) else set()
        for key in sorted(set(error.instance) - allowed):
            yield ("additionalProperties", f"{pointer}/{key}")
    else:
        yield (str(error.validator), pointer)
