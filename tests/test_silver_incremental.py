"""Exercise incremental key validation using an isolated, temporary UC schema.

Call run_incremental_scenarios(spark, dbutils) from a Databricks notebook. The
function creates its own fixtures and removes only its uniquely named schema.
"""

from copy import deepcopy
from datetime import datetime
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

from transformations.silver.configuration import load_silver_config
from transformations.silver.key_validation import (
    KEY_RULES,
    make_validation_writer,
    make_batch_writer,
)
from transformations.silver.processing import process_sources, prepare_source, expected_input_types
from transformations.silver.tables.channel_group import transform_channel_group
from transformations.silver.tables.sales import transform_sales
from utils.execution import start_execution
from utils.metadata import apply_table_descriptions
from utils.streaming import latest_version


def make_frame(spark, contract, changes):
    """Create small Bronze-shaped fixtures with real Spark data types."""

    row = {
        "sale_date": "1/8/2006", "brand_flavor_code": "003440", "brand_name": " BRAND  A ",
        "region_name": " REGION ", "channel_group": " SUPERS ",
        "trade_channel_description": " Supermarket ",
        "package_category_code": "N56P", "package_category_description": "500ML 6P",
        "package_name": ".5L NRP 6P*", "sales_amount": " 12.34 ",
        "year": "2006", "month": "1", "period": "2",
        "trade_group_description": "GROCERY", "trade_type_description": "MIX",
        "_rescued_data": None, "_source_file_path": "/Volumes/a/b/c/sample.csv",
        "_source_file_name": "sample.csv", "_source_file_size": 200,
        "_source_file_modified_at": datetime(2026, 1, 1), "_ingested_at": datetime(2026, 1, 2),
        "_ingestion_run_id": "bronze-test",
    }
    types = {"string": StringType(), "bigint": LongType(), "timestamp": TimestampType()}
    schema = StructType([StructField(n, types[t], True) for n, t in expected_input_types(contract).items()])
    return spark.createDataFrame([{n: {**row, **change}[n] for n in schema.names} for change in changes], schema)



def run_incremental_scenarios(spark, dbutils, include_expected_stream_failure=False):
    """Verify new input, idempotent replay, pending writes and rejected conflicts.

    The optional negative streaming scenario must run in a separate job:
    Databricks marks a job failed when its stream fails, even if Python catches
    the expected exception and all data assertions pass.
    """

    root = Path(__file__).resolve().parents[1]
    config = deepcopy(load_silver_config(root / "configs/silver"))
    schema = f"workspace.beverage_validation_test_{uuid4().hex}"
    volume = f"{schema}.state"
    config["state_volume"] = volume
    for name, source in config["contracts"].items():
        source.update(
            source_table=f"{schema}.bronze_{name}", target=f"{schema}.{name}",
            validation_table=f"{schema}.approved_{name}", state_volume=volume,
            checkpoint=f"/Volumes/{schema.replace('.', '/')}/state/{name}/checkpoint",
        )
    config["sources"] = list(config["contracts"].values())
    def frame(name, changes):
        return make_frame(spark, config["contracts"][name], changes)
    checks = []

    def process():
        return process_sources(spark, dbutils, config, start_execution())

    def append(name, changes):
        frame(name, changes).write.format("delta").mode("append").saveAsTable(
            config["contracts"][name]["source_table"]
        )

    def count(name):
        return spark.table(config["contracts"][name]["target"]).count()

    def registry_count(name):
        return spark.table(config["contracts"][name]["validation_table"]).count()

    def reject_old_sales(frame):
        prepared = transform_sales(frame)
        return prepared.withColumn(
            "_quality_errors", F.when(
                F.col("_source_file_path").endswith("initial.csv"),
                F.array(F.lit("historical_source_rows_were_reprocessed")),
            ).otherwise(F.col("_quality_errors")),
        )

    spark.sql(f"CREATE SCHEMA {schema}")
    try:
        spark.sql(f"CREATE VOLUME {volume}")
        append("channel_group", [{}])
        append("sales", [{"_source_file_path": "/fixture/initial.csv"}] * 2)
        first = process()
        assert count("sales") == 2 and registry_count("sales") == 1
        assert count("channel_group") == 1 and registry_count("channel_group") == 1
        assert first["sources"][1]["rows_added"] == 2
        checks.append("initial_load_preserves_duplicate_occurrences")

        append("sales", [
            {"_source_file_path": "/fixture/increment.csv"},
            {"brand_flavor_code": "NEW", "brand_name": "NEW BRAND", "_source_file_path": "/fixture/increment.csv"},
        ])
        # Fail if the incremental batch reprocesses initial source rows.
        with patch.dict("transformations.silver.processing.TRANSFORMATIONS", {"sales": reject_old_sales}), patch(
            "transformations.silver.processing.sales.validate_products", side_effect=AssertionError("Full historical product scan")
        ), patch("transformations.silver.tables.channel_group.validate_channels", side_effect=AssertionError("Full historical channel scan")):
            incremental = process()
        assert count("sales") == 4 and registry_count("sales") == 2
        assert incremental["sources"][1]["rows_added"] == 2
        checks.append("new_batches_do_not_reprocess_historical_rows")

        before_versions = {name: latest_version(spark, c["target"])
                           for name, c in config["contracts"].items()}
        with patch("transformations.silver.processing.make_batch_writer",
                   side_effect=AssertionError("No-change execution started a stream")):
            no_new = process()
        assert no_new["status"] == "NO_NEW_DATA"
        assert all(item["batches_committed"] == 0 and item["query_id"] is None
                   for item in no_new["sources"])
        assert before_versions == {name: latest_version(spark, c["target"])
                                   for name, c in config["contracts"].items()}
        checks.append("consumed_versions_skip_streams_without_target_writes")

        append("sales", [{"brand_flavor_code": "PENDING", "_source_file_path": "/fixture/pending.csv"}])
        with patch("transformations.silver.processing.process_source", side_effect=RuntimeError("simulated data sink failure")):
            try:
                process()
            except RuntimeError as exc:
                assert "simulated data sink failure" in str(exc)
            else:
                raise AssertionError("Expected simulated sink failure")
        assert registry_count("sales") == 2 and count("sales") == 4
        resumed = process()
        assert count("sales") == 5 and resumed["sources"][1]["rows_added"] == 1
        assert process()["status"] == "NO_NEW_DATA"
        checks.append("pending_input_is_written_once_after_processing_failure")

        for name, transform in (("sales", transform_sales), ("channel_group", transform_channel_group)):
            keys, attributes = KEY_RULES[name]
            writer = make_validation_writer(config["contracts"][name]["validation_table"], keys, attributes)
            expected = registry_count(name)
            same = transform(frame(name, [{}]))
            writer(same, 99)
            writer(same, 99)
            assert registry_count(name) == expected
            writer(same.limit(0), 100)
            field = "brand_name" if name == "sales" else "trade_group_description"
            for changes, expected_error in (([{}, {field: "CONFLICT"}], "within incoming batch"), ([{field: "CONFLICT"}], "against approved keys")):
                try:
                    writer(transform(frame(name, changes)), 101)
                except ValueError as exc:
                    assert expected_error in str(exc)
                else:
                    raise AssertionError("Conflicting attributes were accepted")
                assert registry_count(name) == expected
        checks.append("registry_replay_and_empty_batches_are_idempotent")
        checks.append("same_batch_and_historical_conflicts_are_rejected_for_both_tables")

        # Exercise failures on both sides of the target commit, using a separate
        # target so direct callback batch IDs cannot affect pipeline checkpoints.
        replay = deepcopy(config["contracts"]["sales"])
        replay.update(target=f"{schema}.replay_sales", validation_table=f"{schema}.replay_keys",
                      checkpoint=f"/Volumes/{schema.replace('.', '/')}/state/replay/checkpoint")
        app_id = prepare_source(spark, dbutils, replay)["beverage.appId"]
        # Resume initialization before a checkpoint exists: metadata-only CREATE.
        assert prepare_source(spark, dbutils, replay)["beverage.appId"] == app_id
        sample = transform_sales(frame("sales", [{}] * 2))
        writer_type = type(sample.write)
        original_save = writer_type.saveAsTable

        def fail_before_write(writer, name, *args, **kwargs):
            if name == replay["target"]:
                raise RuntimeError("simulated failure before target commit")
            return original_save(writer, name, *args, **kwargs)

        with patch.object(writer_type, "saveAsTable", fail_before_write):
            try:
                make_batch_writer(replay, app_id, "before-failure")(sample, 0)
            except RuntimeError as exc:
                assert "before target commit" in str(exc)
            else:
                raise AssertionError("Expected failure before the data commit")
        assert spark.table(replay["validation_table"]).count() == 1
        assert spark.table(replay["target"]).count() == 0
        make_batch_writer(replay, app_id, "retry")(sample, 0)
        make_batch_writer(replay, app_id, "retry-again")(sample, 0)
        assert spark.table(replay["target"]).count() == 2

        def fail_after_write(writer, name, *args, **kwargs):
            result = original_save(writer, name, *args, **kwargs)
            if name == replay["target"]:
                raise RuntimeError("simulated failure after target commit")
            return result

        with patch.object(writer_type, "saveAsTable", fail_after_write):
            try:
                make_batch_writer(replay, app_id, "after-failure")(sample, 1)
            except RuntimeError as exc:
                assert "after target commit" in str(exc)
            else:
                raise AssertionError("Expected failure after the data commit")
        assert spark.table(replay["target"]).count() == 4
        make_batch_writer(replay, app_id, "replay-after-commit")(sample, 1)
        assert spark.table(replay["target"]).count() == 4
        checks.append("callback_retries_before_and_after_commit_preserve_occurrences")

        bad = transform_sales(frame("sales", [{"sale_date": "invalid"}]))
        try:
            make_batch_writer(replay, app_id, "invalid")(bad, 2)
        except ValueError as exc:
            assert "Invalid Silver input" in str(exc)
        else:
            raise AssertionError("Invalid data was accepted")
        assert spark.table(replay["target"]).count() == 4
        assert spark.table(replay["validation_table"]).count() == 1
        checks.append("invalid_batch_does_not_reach_target_or_registry")

        # Comments exist at CREATE time and updating several fields is one DDL.
        comments = {f.name: f.metadata.get("comment") for f in spark.table(replay["target"]).schema.fields}
        assert all(comments.values())
        changed = {"brand_name": "Brand's description.", "package_name": "Package description."}
        version_before = spark.sql(f"DESCRIBE HISTORY {replay['target']} LIMIT 1").first()["version"]
        result = apply_table_descriptions(spark, replay["target"], replay["description"], changed)
        version_after = spark.sql(f"DESCRIBE HISTORY {replay['target']} LIMIT 1").first()["version"]
        assert version_after == version_before + 1 and len(result["columns_updated"]) == 2
        fields = {f.name: f.metadata.get("comment") for f in spark.table(replay["target"]).schema.fields}
        assert all(fields[k] == v for k, v in changed.items())
        assert not apply_table_descriptions(spark, replay["target"], replay["description"], changed)["columns_updated"]
        checks.append("descriptions_created_together_and_updated_in_one_column_statement")

        # Existing checkpoints without the new marker resume once, without replay.
        target = config["contracts"]["sales"]["target"]
        bronze = config["contracts"]["sales"]["source_table"]
        spark.sql(f"ALTER TABLE {target} UNSET TBLPROPERTIES ('beverage.sourceVersion')")
        adopted = process()
        assert count("sales") == 5 and adopted["sources"][1]["rows_added"] == 0
        assert adopted["sources"][1]["query_id"] is not None
        checks.append("existing_checkpoint_without_marker_is_adopted_without_replay")

        # A committed stream with a failed marker update must resume safely.
        old_marker = spark.sql(f"DESCRIBE DETAIL {target}").first()["properties"]["beverage.sourceVersion"]
        append("sales", [{"_source_file_path": "/fixture/marker_failure.csv"}])
        original_sql = spark.sql
        def fail_marker(statement, *args, **kwargs):
            if statement.startswith(f"ALTER TABLE {target} SET TBLPROPERTIES"):
                raise RuntimeError("simulated failure before version marker")
            return original_sql(statement, *args, **kwargs)
        with patch.object(spark, "sql", side_effect=fail_marker):
            try:
                process()
            except RuntimeError as exc:
                assert "before version marker" in str(exc)
            else:
                raise AssertionError("Expected marker failure")
        assert count("sales") == 6
        assert spark.sql(f"DESCRIBE DETAIL {target}").first()["properties"]["beverage.sourceVersion"] == old_marker
        retry = process()
        assert count("sales") == 6 and retry["sources"][1]["rows_added"] == 0
        assert retry["sources"][1]["query_id"] is not None
        checks.append("failed_marker_update_retries_checkpoint_without_duplicate_rows")

        # Source advances after the pre-stream version was captured.
        append("sales", [{"_source_file_path": "/fixture/before_capture.csv"}])
        captured = []
        def advance_source(session, table):
            version = latest_version(session, table)
            if table == bronze and not captured:
                captured.append(version)
                append("sales", [{"_source_file_path": "/fixture/after_capture.csv"}])
            return version
        with patch("transformations.silver.processing.latest_version", side_effect=advance_source):
            process()
        assert spark.sql(f"DESCRIBE DETAIL {target}").first()["properties"]["beverage.sourceVersion"] == str(captured[0])
        process()
        assert count("sales") == 8
        with patch("transformations.silver.processing.make_batch_writer",
                   side_effect=AssertionError("Caught-up execution started a stream")):
            assert process()["status"] == "NO_NEW_DATA"
        checks.append("concurrent_source_append_is_consumed_without_advancing_marker_past_capture")

        # A current marker does not bypass missing checkpoint protection.
        with patch("transformations.silver.processing.checkpoint_has_metadata", return_value=False):
            try:
                process()
            except RuntimeError as exc:
                assert "Checkpoint is missing" in str(exc)
            else:
                raise AssertionError("Current marker bypassed checkpoint validation")
        checks.append("current_version_does_not_bypass_checkpoint_validation")

        if include_expected_stream_failure:
            append("sales", [{"brand_name": "INVALID HISTORICAL CHANGE"}])
            try:
                process()
            except Exception as exc:
                assert "Conflicting attributes against approved keys" in str(exc), str(exc)
            else:
                raise AssertionError("Conflicting streaming input was accepted")
            assert count("sales") == 8 and registry_count("sales") == 3
            checks.append("invalid_streaming_batch_does_not_reach_silver")
        return {"status": "PASS", "checks": checks, "fixture_schema": schema}
    finally:
        # This name is generated locally above, never supplied by a user/config.
        assert schema.startswith("workspace.beverage_validation_test_")
        spark.sql(f"DROP SCHEMA {schema} CASCADE")
