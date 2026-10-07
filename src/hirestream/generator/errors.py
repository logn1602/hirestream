"""Errors shared by the generator's entry points."""

from __future__ import annotations


class OutputExistsError(RuntimeError):
    """Generated source data already exists and overwriting was not requested (ADR-0005 §7)."""


class StreamDeliveryError(RuntimeError):
    """A stream sink gave up on records after its retries (ADR-0013)."""
