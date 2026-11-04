"""Email hashing (ADR-0022): HMAC-SHA256 from Spark's built-ins, one normalization, and the
salt's rules and registry. The pinned hex values are the definition: if one changes, every
email_hash in every lake changes with it."""

import hashlib
import hmac

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from hirestream.transforms.common.lake import Lake
from hirestream.transforms.common.pii import REGISTRY, PiiKey, PiiKeyError, hmac_sha256, register
from tests.pii import TEST_KEY, TEST_SALT, expected_email_hash

ANN = "88d70cb5413cf0dba233b8be28881c5a8628fef5a046af11a64012052f010243"  # 'ann@x.example'


def _hmac(spark: SparkSession, key: bytes, messages: list[bytes]) -> list[str]:
    df = spark.createDataFrame([(i, m) for i, m in enumerate(messages)], "i int, m binary")
    rows = df.select("i", hmac_sha256(key, F.col("m")).alias("h")).orderBy("i").collect()
    return [r.h for r in rows]


def test_hmac_matches_rfc_4231(spark: SparkSession) -> None:
    # Test case 2 (a short key) and test case 6 (a key longer than the 64-byte block).
    assert _hmac(spark, b"Jefe", [b"what do ya want for nothing?"]) == [
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    ]
    assert _hmac(
        spark, b"\xaa" * 131, [b"Test Using Larger Than Block-Size Key - Hash Key First"]
    ) == ["60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"]


@pytest.mark.parametrize("length", [1, 63, 64, 65, 200])
def test_hmac_matches_python_for_every_key_length(spark: SparkSession, length: int) -> None:
    key = bytes(range(256))[:length]
    messages = [b"", b"a", "é@x.example".encode(), b"x" * 1000]
    assert _hmac(spark, key, messages) == [
        hmac.new(key, m, hashlib.sha256).hexdigest() for m in messages
    ]


EMAILS = {
    "plain": ("ann@x.example", ANN),
    "case and spaces": (" Ann@X.Example ", ANN),
    "tab and newline": ("\tann@x.example\n", ANN),  # Spark's trim would keep both
    "unicode spaces": ("\u00a0ANN@x.example\u3000", ANN),  # a pasted non-breaking space
    "inner space kept": (
        "a b@x.example",
        "2a56baa3f07e5312489e468cc623b80142837936f83359afc099be29b9431505",
    ),
    "empty": ("", None),
    "blank": (" \t\u00a0", None),
    "null": (None, None),
}


def test_email_hash_normalizes_then_hashes(spark: SparkSession) -> None:
    df = spark.createDataFrame([(n, e) for n, (e, _) in EMAILS.items()], "name string, e string")
    got = {
        r.name: r.h for r in df.select("name", TEST_KEY.email_hash(F.col("e")).alias("h")).collect()
    }
    assert got == {name: expected for name, (_, expected) in EMAILS.items()}
    # The plain-Python reference the silver tests use agrees with every pinned value.
    assert {n: expected_email_hash(e) for n, (e, _) in EMAILS.items()} == got


def test_the_salt_keys_the_hash(spark: SparkSession) -> None:
    other = PiiKey("another-salt-of-at-least-thirty-two-chars")
    df = spark.createDataFrame([("ann@x.example",)], "e string")
    (row,) = df.select(other.email_hash(F.col("e")).alias("h")).collect()
    assert row.h != ANN and row.h == expected_email_hash("ann@x.example", other.salt)


@pytest.mark.parametrize(
    ("salt", "message"),
    [
        ("", "empty"),
        ("change-me", "placeholder"),
        ("x" * 31, "31 characters; use at least 32"),
    ],
)
def test_unusable_salts_are_refused(salt: str, message: str) -> None:
    with pytest.raises(PiiKeyError, match=message):
        PiiKey(salt)


def test_the_salt_never_shows() -> None:
    assert TEST_SALT not in repr(TEST_KEY) and TEST_SALT not in str(TEST_KEY)
    assert TEST_KEY.fingerprint == "4beff6148ce5f6c7"
    assert TEST_SALT not in TEST_KEY.fingerprint


def test_register_accepts_the_same_salt_and_refuses_another(lake: Lake) -> None:
    register(lake, TEST_KEY)
    register(lake, PiiKey(TEST_SALT))  # the same salt again: a no-op
    assert lake.read_json(REGISTRY) == {"version": 1, "fingerprint": "4beff6148ce5f6c7"}
    other = PiiKey("another-salt-of-at-least-thirty-two-chars")
    with pytest.raises(PiiKeyError, match="not the salt this lake's email hashes were made with"):
        register(lake, other)
    assert lake.read_json(REGISTRY)["fingerprint"] == TEST_KEY.fingerprint  # unchanged
