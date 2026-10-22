"""Verify Bronze ingestion in a disposable schema using copies of case inputs."""

from copy import deepcopy
from pathlib import Path
from time import perf_counter
from uuid import uuid4
import csv
import io

from pyspark.sql import types as T

from transformations.bronze.ingestion import (
    load_bronze_config, ingest_sources, inspect_new_rows,
)
from utils.execution import start_execution


def run_bronze_scenarios(spark, dbutils):
    """Check first load, new files, no-op reruns and quality review signals.

    Production files are read only. All writes use a generated test schema,
    which is removed in finally. Original checkpoints are never used.
    """

    root = Path(__file__).resolve().parents[1]
    config = deepcopy(load_bronze_config(root / "configs/bronze"))
    schema = f"workspace.beverage_bronze_test_{uuid4().hex}"
    state = f"{schema}.state"
    landing = f"/Volumes/{schema.replace('.', '/')}/inputs"
    config["state_volume"] = state
    fixtures, checks = {}, []

    def versions():
        return {s["name"]: spark.sql(
            f"DESCRIBE HISTORY {s['target']} LIMIT 1"
        ).first()["version"] for s in config["sources"]}

    def process():
        started = perf_counter()
        result = ingest_sources(spark, dbutils, config, start_execution())
        elapsed = perf_counter() - started
        displayed = []
        result = inspect_new_rows(spark, config, result, show=lambda df: displayed.extend(df.collect()))
        return result, displayed, elapsed

    spark.sql(f"CREATE SCHEMA {schema}")
    try:
        spark.sql(f"CREATE VOLUME {schema}.inputs")
        spark.sql(f"CREATE VOLUME {state}")
        for source in config["sources"]:
            original_name = next(iter(source["reference_counts"]))
            original = str(Path(source["source_path"]) / original_name)
            assert dbutils.fs.cp(original, f"{landing}/{original_name}")
            encoding = "utf-16" if source["name"] == "sales" else "utf-8-sig"
            rows = list(csv.reader(io.StringIO(Path(original).read_bytes().decode(encoding)),
                                   delimiter=source["reader_options"]["sep"]))
            fixtures[source["name"]] = rows
            source.update(source_path=landing, target=f"{schema}.{source['target'].split('.')[-1]}")

        first, displayed, first_seconds = process()
        assert [x["rows_added"] for x in first["sources"]] == [31, 16151]
        assert not first["review_required"]
        assert len(displayed) == 2 and all(x["file_count"] == 1 for x in displayed)
        for source, item in zip(config["sources"], first["sources"]):
            assert item["reference_checks"][0]["matches"]
            fields = [c["target"] for c in source["columns"]]
            raw = fixtures[source["name"]][1:]
            expected = spark.createDataFrame(raw, T.StructType([
                T.StructField(name, T.StringType()) for name in fields
            ]))
            actual = spark.table(source["target"]).select(*fields)
            assert expected.exceptAll(actual).count() == actual.exceptAll(expected).count() == 0
        checks.append("first_load_matches_original_occurrences_and_reference_counts")

        before = versions()
        empty, displayed, no_new_seconds = process()
        assert empty["status"] == "NO_NEW_DATA" and not displayed
        assert versions() == before
        checks.append("no_new_files_do_not_write_or_run_row_checks")

        for source in config["sources"]:
            rows = fixtures[source["name"]]
            output = io.StringIO(newline="")
            writer = csv.writer(output, delimiter=source["reader_options"]["sep"])
            # Two identical occurrences must both be retained in the new file.
            writer.writerows([rows[0], rows[1], rows[1]])
            encoding = "utf-16" if source["name"] == "sales" else "utf-8-sig"
            filename = source["file_pattern"].replace("*", "test_increment")
            Path(landing, filename).write_bytes(output.getvalue().encode(encoding))
        incremental, displayed, _ = process()
        assert [x["rows_added"] for x in incremental["sources"]] == [2, 2]
        assert not incremental["review_required"]
        assert all(x["reference_checks"] == [] and x["rescued_rows"] == 0 for x in incremental["sources"])
        assert all(x["file_count"] == 1 and x["row_count"] == 2 for x in displayed)
        for source in config["sources"]:
            assert spark.table(source["target"]).count() == len(fixtures[source["name"]]) - 1 + 2
        checks.append("new_files_append_only_new_occurrences_and_keep_original_checkpoints")
        before = versions()
        assert process()[0]["status"] == "NO_NEW_DATA" and versions() == before
        checks.append("incremental_replay_does_not_duplicate_rows")

        # Exercise both review signals without deliberately failing a stream.
        quality_table = f"{schema}.quality_fixture"
        fields = "_source_file_path string, _source_file_name string, _rescued_data string, _ingestion_run_id string"
        spark.createDataFrame([
            ("/fixture/known.csv", "known.csv", None, "quality-test"),
            ("/fixture/known.csv", "known.csv", '{"extra":"value"}', "quality-test"),
            ("/fixture/new.csv", "new.csv", None, "quality-test"),
            ("/fixture/old.csv", "old.csv", '{"old":"ignored"}', "previous-run"),
        ], fields).write.saveAsTable(quality_table)
        review = inspect_new_rows(spark, {"sources": [{"name": "fixture", "reference_counts": {"known.csv": 3}}]},
                                  {"run_id": "quality-test", "status": "BRONZE_UPDATED", "sources": [
                                      {"source": "fixture", "target": quality_table, "rows_added": 3}
                                  ]})
        assert review["status"] == "BRONZE_UPDATED_REVIEW_REQUIRED"
        assert review["sources"][0]["rescued_rows"] == 1
        assert review["sources"][0]["reference_checks"] == [
            {"file": "known.csv", "expected_rows": 3, "actual_rows": 2, "matches": False}
        ]
        checks.append("rescued_data_and_reference_mismatches_still_request_review")
        return {"status": "PASS", "checks": checks, "first_load_seconds": first_seconds,
                "no_new_seconds": no_new_seconds, "fixture_schema": schema}
    finally:
        assert schema.startswith("workspace.beverage_bronze_test_")
        spark.sql(f"DROP SCHEMA {schema} CASCADE")
