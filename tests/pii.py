"""A test salt and a plain-Python reference for `email_hash` (ADR-0022), for checking Spark's
results row by row. The pipeline has one implementation, in `transforms/common/pii.py`; this one
exists only to test it, and the pinned values in test_pii.py keep the two from drifting together.
"""

import hashlib
import hmac
import unicodedata

from hirestream.transforms.common.pii import PiiKey

TEST_SALT = "hirestream-test-salt-0123456789abcdef"  # public, like the synthetic data it hashes
TEST_KEY = PiiKey(TEST_SALT)


def _edge(char: str) -> bool:
    """What Java's `[\\s\\p{Z}]` matches: ASCII whitespace, and Unicode separators."""
    return char in " \t\n\x0b\x0c\r" or unicodedata.category(char).startswith("Z")


def expected_email_hash(email: str | None, salt: str = TEST_SALT) -> str | None:
    if email is None:
        return None
    start, end = 0, len(email)
    while start < end and _edge(email[start]):
        start += 1
    while end > start and _edge(email[end - 1]):
        end -= 1
    normalized = email[start:end].lower()
    if not normalized:
        return None
    return hmac.new(salt.encode(), normalized.encode(), hashlib.sha256).hexdigest()
