"""Lake paths and IO with one code path for a local directory and for S3 (ADR-0018, ADR-0003 G2).

Every path is a URI under one root: `file:/abs/path` locally, `s3a://bucket[/prefix]` in the
cloud. A *key* is a path relative to that root (`bronze/scheduling/yyyy=2026/.../part-00000...`).
Lineage and state record keys, never URIs, so outputs don't depend on where the lake lives.

Filesystem calls go through Hadoop's FileSystem API in the Spark JVM, the client Spark itself
reads and writes with, so listings, existence checks and state files behave the same on a laptop
and on EMR, with no extra dependency to ship.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from hirestream.transforms.common.lineage import ARRIVAL_TS, SOURCE_FILE
from hirestream.transforms.common.session import jvm

LAYERS: Final = ("bronze", "silver", "gold", "quarantine", "state")
LINE_SCHEMA: Final = StructType(
    [
        StructField("value", StringType()),
        StructField(SOURCE_FILE, StringType()),
        StructField(ARRIVAL_TS, TimestampType()),
    ]
)


@dataclass(frozen=True)
class LakePaths:
    """Path arithmetic under a lake root. Pure: no Spark and no IO."""

    root: str

    @classmethod
    def from_root(cls, root: str | os.PathLike[str]) -> LakePaths:
        """Accepts a local path (relative paths resolve against the working directory), a
        `file:` URI, or an object-store URI such as `s3a://bucket/prefix`."""
        text = os.fspath(root)
        if text.startswith("file:"):
            text = text.removeprefix("file:")
            text = text.removeprefix("//") if text.startswith("///") else text
        elif "://" in text:
            scheme, rest = text.split("://", 1)
            if not scheme or not rest.strip("/"):
                raise ValueError(f"lake root {text!r} names no bucket")
            return cls(f"{scheme}://{rest.rstrip('/')}")
        return cls("file:" + os.path.abspath(text))

    def uri(self, key: str) -> str:
        return f"{self.root}/{_checked(key)}"

    def key(self, uri: str) -> str:
        """The key of a URI under this root, as Hadoop spells it (decoded)."""
        prefix = f"{self.root}/"
        if not uri.startswith(prefix) or uri == prefix:
            raise ValueError(f"{uri!r} is not under the lake root {self.root!r}")
        return uri.removeprefix(prefix)

    def layer(self, layer: str, *parts: str) -> str:
        """The key of `parts` inside a layer, e.g. layer("silver", "scheduling_events")."""
        if layer not in LAYERS:
            raise ValueError(f"unknown layer {layer!r}; choose from {LAYERS}")
        return _checked("/".join((layer, *parts)))

    def bronze(self, source: str) -> str:
        return self.layer("bronze", source)

    def silver(self, table: str) -> str:
        return self.layer("silver", table)

    def gold(self, table: str) -> str:
        return self.layer("gold", table)

    def quarantine(self, source: str) -> str:
        return self.layer("quarantine", source)

    def state(self, *parts: str) -> str:
        return self.layer("state", *parts)


@dataclass(frozen=True, order=True)
class FileInfo:
    key: str
    size: int
    mtime_ms: int  # file mtime locally, LastModified on S3: the object's arrival (ADR-0012)


class Lake:
    """The lake seen from a Spark job: listings, small state files, and table reads and writes."""

    def __init__(self, spark: SparkSession, paths: LakePaths) -> None:
        self.spark = spark
        self.paths = paths
        self._jvm = jvm(spark)
        self._hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    # -- filesystem ---------------------------------------------------------------------------

    def exists(self, key: str) -> bool:
        fs, path = self._resolve(key)
        return bool(fs.exists(path))

    def list_files(self, prefix: str) -> list[FileInfo]:
        """Every visible file at or under `prefix`, sorted by key; empty if there is none.

        Hidden as in Spark's own file listing: any name below `prefix` that starts with "." or
        with "_" (unless it contains "=", like a partition directory). So temp files, `_SUCCESS`,
        checksum files and `_runs/` are never mistaken for inputs.
        """
        fs, path = self._resolve(prefix)
        if not fs.exists(path):
            return []
        found = []
        files = fs.listFiles(path, True)
        while files.hasNext():
            status = files.next()
            key = self.paths.key(status.getPath().toString())
            below = key.removeprefix(prefix).lstrip("/")
            if not any(_hidden(name) for name in below.split("/") if name):
                found.append(FileInfo(key, int(status.getLen()), int(status.getModificationTime())))
        return sorted(found)

    def read_text(self, key: str) -> str:
        fs, path = self._resolve(key)
        stream = fs.open(path)
        try:
            data: bytes = self._jvm.org.apache.commons.io.IOUtils.toByteArray(stream)
        finally:
            stream.close()
        return data.decode("utf-8")

    def write_new_text(self, key: str, text: str) -> None:
        """Write a small file that must not exist yet (state is append-only, like bronze).

        The bytes go to a hidden temp file beside the target, which is then renamed into place,
        so a reader sees either nothing or the whole file. Raises FileExistsError if the key
        exists, before or during the write.
        """
        fs, path = self._resolve(key)
        if fs.exists(path):
            raise FileExistsError(key)
        temp = self._jvm.org.apache.hadoop.fs.Path(
            path.getParent(), f".{path.getName()}.{uuid.uuid4().hex}.tmp"
        )
        try:
            out = fs.create(temp, False)
            try:
                out.write(bytearray(text.encode("utf-8")))
            finally:
                out.close()
            if not fs.rename(temp, path):  # Hadoop's rename never replaces an existing file
                raise FileExistsError(key)
        finally:
            if fs.exists(temp):
                fs.delete(temp, False)

    def read_json(self, key: str) -> Any:
        return json.loads(self.read_text(key))

    def write_new_json(self, key: str, value: Any) -> None:
        self.write_new_text(key, json.dumps(value, indent=2, sort_keys=True) + "\n")

    # -- tables -------------------------------------------------------------------------------

    def read_lines(self, keys: Sequence[str]) -> DataFrame:
        """The raw lines of the given files, one row per line (gzip is decompressed by its
        extension), with each file's key and arrival time. Lines are read as text so that one bad
        line can be quarantined on its own (SPEC §9.1)."""
        if not keys:
            return self.spark.createDataFrame([], LINE_SCHEMA)
        prefix = f"{self.paths.root}/"
        # `_metadata.file_path` is URL-encoded (a space reads %20); Hadoop listings, and so the
        # manifest's keys, are not. Decode it, keeping '+' literal as a URI path does.
        uri = F.url_decode(F.replace(F.col("_metadata.file_path"), F.lit("+"), F.lit("%2B")))
        key = F.when(uri.startswith(prefix), F.substr(uri, F.lit(len(prefix) + 1))).otherwise(
            F.raise_error(F.concat(F.lit(f"file outside the lake root {prefix}: "), uri))
        )
        return self.spark.read.text([self.paths.uri(k) for k in keys]).select(
            F.col("value"),
            key.alias(SOURCE_FILE),
            F.col("_metadata.file_modification_time").alias(ARRIVAL_TS),
        )

    def read_parquet(self, key: str, schema: StructType) -> DataFrame:
        """A Parquet table read with `schema`, or an empty frame if nothing was written yet."""
        if not self.exists(key):
            return self.spark.createDataFrame([], schema)
        return self.spark.read.schema(schema).parquet(self.paths.uri(key))

    def write_partitioned(
        self,
        df: DataFrame,
        key: str,
        partition_by: Sequence[str],
        sort_by: Sequence[str] = (),
    ) -> None:
        """Replace the partitions `df` contains and leave every other partition alone.

        One file per partition, rows sorted by `sort_by`, so a re-run rewrites the same layout.
        """
        if not partition_by:
            raise ValueError("partition_by is empty: an overwrite would replace the whole table")
        (
            df.repartition(*partition_by)
            .sortWithinPartitions(*partition_by, *sort_by)
            .write.option("partitionOverwriteMode", "dynamic")  # per write, not only per session
            .mode("overwrite")
            .partitionBy(*partition_by)
            .parquet(self.paths.uri(key))
        )

    def _resolve(self, key: str) -> tuple[Any, Any]:
        path = self._jvm.org.apache.hadoop.fs.Path(self.paths.uri(key))
        return path.getFileSystem(self._hadoop_conf), path


def _checked(key: str) -> str:
    names = key.split("/")
    if not key or any(name in ("", ".", "..") for name in names):
        raise ValueError(f"invalid lake key {key!r}")
    return key


def _hidden(name: str) -> bool:
    return (name.startswith("_") and "=" not in name) or name.startswith(".")
