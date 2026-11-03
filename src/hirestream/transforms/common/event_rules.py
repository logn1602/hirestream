"""What silver checks a stream event against, read from its JSON Schema contracts (ADR-0019).

SPEC §7 makes the contracts "the single source ... for silver parsing expectations". This module
turns a source's contracts into plain rules: for each (event_type, schema_version), every field's
path, Spark type, nullability, allowed values and minimum. Silver never restates an enum, a type or
a bound. The contracts are read with the standard library through `hirestream.contracts`
(ADR-0014).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pyspark.sql import Column
from pyspark.sql import functions as F

from hirestream.contracts import list_contracts, load_contract

_SPARK_TYPES = {"string": "string", "integer": "bigint", "boolean": "boolean", "number": "double"}


@dataclass(frozen=True)
class FieldRule:
    path: str  # dotted, e.g. "event_id" or "payload.reason"
    spark_type: str  # what try_variant_get extracts: "string", "bigint", "array<string>", ...
    nullable: bool  # the contract allows JSON null
    allowed: frozenset[str] | None  # an enum or const, as text; None if any value goes
    minimum: int | float | None = None  # a number's `minimum`; None if it has none


@dataclass(frozen=True)
class EventRules:
    event_type: str
    version: int
    fields: Mapping[str, FieldRule]

    @property
    def key(self) -> str:
        return contract_key(self.event_type, self.version)


def contract_key(event_type: str, version: int) -> str:
    """How rules are keyed in Spark too: see `contract_key_column`."""
    return f"{event_type}:v{version}"


@dataclass(frozen=True)
class SourceRules:
    source: str  # e.g. "scheduling-service"
    events: Mapping[str, EventRules]  # by contract key

    @classmethod
    def load(cls, source: str) -> SourceRules:
        """Every contract of `source` on disk."""
        schemas = {
            (event_type, version): load_contract(source, event_type, version)
            for s, event_type, version in list_contracts()
            if s == source
        }
        if not schemas:
            raise ValueError(f"no contracts for source {source!r}")
        return cls.from_schemas(source, schemas)

    @classmethod
    def from_schemas(
        cls, source: str, schemas: Mapping[tuple[str, int], Mapping[str, Any]]
    ) -> SourceRules:
        events = {}
        for (event_type, version), schema in sorted(schemas.items()):
            rules = EventRules(event_type, version, dict(_flatten(schema)))
            events[rules.key] = rules
        found = cls(source, events)
        for path in found.paths:
            found.field(path)  # every field must have one Spark type and minimum
        return found

    @property
    def event_types(self) -> frozenset[str]:
        return frozenset(rules.event_type for rules in self.events.values())

    @property
    def paths(self) -> frozenset[str]:
        return frozenset(path for rules in self.events.values() for path in rules.fields)

    def keys_with(self, path: str) -> tuple[str, ...]:
        """The contract keys whose events carry `path`."""
        return tuple(key for key, rules in self.events.items() if path in rules.fields)

    def field(self, path: str) -> FieldRule:
        """`path`'s rule, which must have the same Spark type and minimum in every contract that
        has it. Nullability and allowed values may differ: see `allowed`."""
        rules = [r.fields[path] for r in self.events.values() if path in r.fields]
        if not rules:
            raise KeyError(f"no {self.source} contract has {path!r}")
        if len({rule.spark_type for rule in rules}) > 1:
            raise ValueError(f"{path!r} has different types across {self.source} contracts")
        if len({rule.minimum for rule in rules}) > 1:
            raise ValueError(f"{path!r} has different minimums across {self.source} contracts")
        return rules[0]

    def allowed(self, path: str) -> dict[str, frozenset[str]]:
        """Allowed values per contract key, for the contracts that restrict `path`."""
        return {
            key: rule.allowed
            for key, events in self.events.items()
            if (rule := events.fields.get(path)) is not None and rule.allowed is not None
        }


def contract_key_column(event_type: Column, version: Column) -> Column:
    """The contract key of each row; null if either part is."""
    return F.concat(event_type, F.lit(":v"), version.cast("string"))


def extract(parsed: Column, rule: FieldRule) -> Column:
    """`rule`'s field from a VARIANT, cast to its Spark type; null if absent, JSON null, or not
    castable. Casts follow Spark's ANSI rules, so the string "45" reads as 45 for a bigint."""
    return F.try_variant_get(parsed, f"$.{rule.path}", rule.spark_type)


def _flatten(schema: Mapping[str, Any], prefix: str = "") -> dict[str, FieldRule]:
    found: dict[str, FieldRule] = {}
    for name, node in schema.get("properties", {}).items():
        path = f"{prefix}{name}"
        types = node["type"] if isinstance(node["type"], list) else [node["type"]]
        kinds = [t for t in types if t != "null"]
        if kinds == ["object"]:
            found |= _flatten(node, f"{path}.")
            continue
        found[path] = FieldRule(
            path=path,
            spark_type=_spark_type(node, kinds),
            nullable="null" in types,
            allowed=_allowed(node),
            minimum=node.get("minimum"),
        )
    return found


def _spark_type(node: Mapping[str, Any], kinds: list[str]) -> str:
    if len(kinds) != 1:
        raise ValueError(f"expected one JSON type besides null, got {kinds}")
    if kinds[0] == "array":
        items = node["items"]
        return f"array<{_spark_type(items, [items['type']])}>"
    return _SPARK_TYPES[kinds[0]]


def _allowed(node: Mapping[str, Any]) -> frozenset[str] | None:
    if "const" in node:
        return frozenset({str(node["const"])})
    if "enum" in node:
        return frozenset(str(value) for value in node["enum"])
    return None
