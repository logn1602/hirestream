"""Spark jobs: silver, gold and their shared plumbing (SPEC §9, §10).

Code here runs on EMR Serverless, so it imports only the standard library, pyspark, and the
packages in `requirements-spark.txt` (tests/unit/test_spark_imports.py, ADR-0018).
"""
