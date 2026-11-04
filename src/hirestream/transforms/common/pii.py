"""Email pseudonymization for silver (SPEC §9.1, ADR-0022).

`email_hash` is HMAC-SHA256 of the normalized email, keyed by the pipeline's secret salt
(`HIRESTREAM_PII_SALT`), in lowercase hex. It lets silver and gold link one person across the ATS
and HRIS without keeping the address. Anyone who holds the salt can hash a known email and find
that person, so a hash is still personal data: pseudonymized, not anonymized.

- **Why a keyed hash:** emails are guessable. A plain `sha256(email)` is reversed by hashing a
  list of likely addresses and looking for matches. Without the key, a guess can't be checked.
- **Why HMAC rather than SPEC's `sha2(concat(salt, email))`:** HMAC is the standard keyed hash
  (RFC 2104). Spark has none built in, so `hmac_sha256` builds it from `sha2`, `concat` and
  `unhex`, with the key's pads computed here.
- **Normalization:** strip leading and trailing whitespace, then lowercase. Spark's `trim` strips
  only spaces, so tabs, newlines and Unicode spaces (a pasted non-breaking space) are stripped by
  a regular expression. An email that is empty after that has no hash, so two blank emails never
  look like one person. This is the only implementation; HRIS silver uses it too.
- **One salt per lake, forever:** a different salt doesn't fail, it gives every person a new hash
  and silently breaks joins with what is already hashed. `register` records the salt's fingerprint
  on the first run and refuses any other salt after that.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass, field
from typing import Final

from pyspark.sql import Column
from pyspark.sql import functions as F

from hirestream.transforms.common.lake import Lake

PLACEHOLDER: Final = "change-me"  # .env.example's value, public on GitHub: never a key
MIN_SALT_LENGTH: Final = 32  # a tripwire for obvious mistakes, not a measure of randomness
REGISTRY: Final = "state/pii/salt.json"
REGISTRY_VERSION: Final = 1
_BLOCK: Final = 64  # SHA-256's block size in bytes (RFC 2104's B)
_FINGERPRINT_LABEL: Final = b"hirestream/pii-salt-fingerprint/v1"
# Java regex: \s is [ \t\n\x0B\f\r], \p{Z} every Unicode space and line or paragraph separator.
_EDGE_WHITESPACE: Final = r"^[\s\p{Z}]+|[\s\p{Z}]+$"


class PiiKeyError(ValueError):
    """The salt is unusable, or isn't the one this lake's hashes were made with."""


@dataclass(frozen=True)
class PiiKey:
    """The secret that keys every email hash. It never appears in a repr, message or record:
    records carry its `fingerprint`, which identifies the salt without revealing it."""

    salt: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self.salt:
            raise PiiKeyError("the PII salt is empty")
        if self.salt == PLACEHOLDER:
            raise PiiKeyError(
                f"the PII salt is .env.example's placeholder {PLACEHOLDER!r}, which is public"
            )
        if len(self.salt) < MIN_SALT_LENGTH:
            raise PiiKeyError(
                f"the PII salt is {len(self.salt)} characters; use at least {MIN_SALT_LENGTH}"
            )

    @property
    def fingerprint(self) -> str:
        """16 hex characters that name this salt. Knowing it doesn't help an attacker: checking
        a guessed salt against it is no easier than against any known (email, hash) pair."""
        return hmac.new(self._key, _FINGERPRINT_LABEL, hashlib.sha256).hexdigest()[:16]

    def email_hash(self, email: Column) -> Column:
        """The email's hash, or null when the email is null or blank."""
        normalized = F.lower(F.regexp_replace(email, _EDGE_WHITESPACE, ""))
        return F.when(normalized != "", hmac_sha256(self._key, F.encode(normalized, "UTF-8")))

    @property
    def _key(self) -> bytes:
        return self.salt.encode("utf-8")


def hmac_sha256(key: bytes, message: Column) -> Column:
    """HMAC-SHA256 of a binary column in lowercase hex (RFC 2104), from Spark's built-ins:
    `sha2(K ⊕ opad || unhex(sha2(K ⊕ ipad || m)))`. The pads are literals in the query plan, and
    they are as secret as the key: anyone who has them can compute the HMAC."""
    if len(key) > _BLOCK:
        key = hashlib.sha256(key).digest()
    block = key.ljust(_BLOCK, b"\0")
    inner = bytes(b ^ 0x36 for b in block)
    outer = bytes(b ^ 0x5C for b in block)
    inner_hash = F.unhex(F.sha2(F.concat(F.lit(inner), message), 256))
    return F.sha2(F.concat(F.lit(outer), inner_hash), 256)


def register(lake: Lake, key: PiiKey) -> None:
    """Check `key` against the salt this lake's hashes were made with. The first run that hashes
    records the salt's fingerprint; every later run must present the same salt."""
    entry = {"version": REGISTRY_VERSION, "fingerprint": key.fingerprint}
    try:
        lake.write_once_json(REGISTRY, entry)
    except FileExistsError:
        found = lake.read_json(REGISTRY).get("fingerprint")
        raise PiiKeyError(
            f"HIRESTREAM_PII_SALT is not the salt this lake's email hashes were made with "
            f"(fingerprint {key.fingerprint}; the lake's is {found}). A new salt would give "
            "every person a new hash and break joins: restore the original, or rotate on "
            "purpose (RUNBOOK: PII salt)"
        ) from None
