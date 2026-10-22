"""Validate Gold with real case data and isolated incremental scenarios."""

from copy import deepcopy
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from pyspark.sql import functions as F

from transformations.gold.configuration import load_gold_config
from transformations.gold.processing import make_table_writer, process_sources, summary_cursor
from utils.execution import start_execution
from utils.streaming import latest_version

DIMENSIONS = ("dim_calendar", "dim_product", "dim_region", "dim_channel")

SUMMARIES = (
    "agg_sales_region_trade_group", "agg_sales_brand_month", "agg_sales_region_brand",
)

def assert_same(left, right, label):
    """Compare row multiplicity as well as values; repeated sales are retained."""
    right = right.select(*left.columns)
    assert left.exceptAll(right).limit(1).count() == 0, label + ": unexpected rows"
    assert right.exceptAll(left).limit(1).count() == 0, label + ": missing rows"

def validate_gold(spark, tables):
    """Reconcile a Gold model independently against its complete Silver sources.

    This explicit audit scans all rows; it is not part of normal ingestion.
    It verifies schema comments, unique dimension keys, references, record
    multiplicity and all three business aggregations.
    """
    fact = spark.table(tables["fact_sales"]["target"])
    silver = spark.table(tables["fact_sales"]["source_table"])
    channels = spark.table(tables["dim_channel"]["source_table"])
    dimensions = {n: spark.table(tables[n]["target"]) for n in DIMENSIONS}
    key_names = ["date_key", "product_key", "region_key", "channel_key"]
    checks = []
    for name, key in zip(DIMENSIONS, key_names):
        frame = dimensions[name]
        assert frame.filter(F.col(key).isNull()).count() == 0, name + ": null key"
        assert frame.groupBy(key).count().filter("count > 1").count() == 0, name + ": repeated key"
        assert fact.join(frame.select(key), key, "left_anti").count() == 0, name + ": orphan fact"
        checks.append(name + ": unique keys and complete relationships")
    for name, contract in tables.items():
        frame = spark.table(contract["target"])
        assert dict(frame.dtypes) == {c["name"]: c["type"] for c in contract["columns"]}, name
        comments = {f.name: f.metadata.get("comment") for f in frame.schema.fields}
        assert comments == {c["name"]: c["description"] for c in contract["columns"]}, name + ": comments"
        assert spark.catalog.getTable(contract["target"]).description == contract["description"], name
    restored = fact
    for name, key in zip(DIMENSIONS, key_names):
        restored = restored.join(dimensions[name], key)
    # Channel display spelling belongs to the channel dimension; match the
    # normalized business key and retain every original sales occurrence.
    fields = ["brand_flavor_code", "brand_name", "region_name", "channel_group", "trade_channel_key",
              "package_category_code", "package_category_description", "package_name", "sales_amount",
              "year", "month", "period", "_source_file_path", "_silver_run_id"]
    assert_same(restored.select(F.col("calendar_date").alias("sale_date"), *fields),
                silver.select("sale_date", *fields), "Fact occurrence reconciliation")
    checks.append("All sales occurrences and amounts reconcile with Silver")
    channel_columns = ["trade_channel_key", "trade_channel_description", "trade_group_description", "trade_type_description"]
    assert_same(dimensions["dim_channel"].select(*channel_columns),
                channels.select(*channel_columns).distinct(), "All source channels retained")
    expected = {
        "agg_sales_region_trade_group": silver.join(
            channels.select("trade_channel_key", "trade_group_description").distinct(), "trade_channel_key"
        ).groupBy("region_name", "trade_group_description").agg(F.sum("sales_amount").alias("sales_amount")),
        "agg_sales_brand_month": silver.groupBy("brand_name", "year", "month").agg(F.sum("sales_amount").alias("sales_amount")),
        "agg_sales_region_brand": silver.groupBy("region_name", "brand_name").agg(F.sum("sales_amount").alias("sales_amount")),
    }
    for name, frame in expected.items():
        assert_same(spark.table(tables[name]["target"]), frame, name)
        checks.append(name + ": matches independent Silver aggregation")
    return checks


def run_incremental_scenarios(spark, dbutils):
    """Test a full sample, duplicate replay, late data and independently pending summaries.

    All writes are isolated in a uniquely named schema. Its source copies,
    Gold tables and checkpoint volume are removed in finally. Production
    Bronze, Silver and Gold are never written by this test.
    """
    root = Path(__file__).resolve().parents[1]
    config = deepcopy(load_gold_config(root / "configs/gold"))
    schema = f"workspace.beverage_gold_test_{uuid4().hex}"
    tables = config["tables"]
    source = tables["fact_sales"]["source_table"]
    channel_source = tables["dim_channel"]["source_table"]
    for name, contract in tables.items():
        contract["target"] = f"{schema}.{name}"
    for name, contract in tables.items():
        if contract["kind"] != "summary":
            contract.update(
                source_table=f"{schema}.silver_channels" if name == "dim_channel" else f"{schema}.silver_sales",
                state_volume=f"{schema}.processing_state",
                checkpoint=f"/Volumes/{schema.replace('.', '/')}/processing_state/{name}/checkpoint",
            )
    fact_config = tables["fact_sales"]
    checks = []
    spark.sql(f"CREATE SCHEMA {schema}")
    try:
        spark.table(source).write.saveAsTable(fact_config["source_table"])
        spark.table(channel_source).write.saveAsTable(tables["dim_channel"]["source_table"])
        # A standalone fact cannot silently build its missing prerequisites.
        try:
            process_sources(spark, dbutils, {**config, "selected": ["fact_sales"]}, start_execution())
        except ValueError as error:
            assert "Load dim_" in str(error)
        else:
            raise AssertionError("Missing dimensions must be reported before a fact write")
        assert not spark.catalog.tableExists(tables["fact_sales"]["target"])
        two = process_sources(spark, dbutils, {**config, "selected": ["dim_product", "dim_region"]}, start_execution())
        assert {r["table"] for r in two["sources"]} == {"dim_product", "dim_region"}
        assert all(not spark.catalog.tableExists(tables[n]["target"]) for n in ("dim_calendar", "dim_channel", "fact_sales"))
        remaining = process_sources(spark, dbutils, {**config, "selected": ["dim_calendar", "dim_channel"]}, start_execution())
        dim_versions = {n: latest_version(spark, tables[n]["target"]) for n in DIMENSIONS}
        first = process_sources(spark, dbutils, {**config, "selected": ["fact_sales"]}, start_execution())
        assert first["sources"][0]["rows"] == 16151
        assert dim_versions == {n: latest_version(spark, tables[n]["target"]) for n in DIMENSIONS}
        summaries = process_sources(spark, dbutils, {**config, "selected": list(SUMMARIES)}, start_execution())
        checks.append("Two dimensions, remaining dimensions, fact and summaries run independently")
        checks.extend(validate_gold(spark, tables))
        total = spark.table(tables["fact_sales"]["target"]).agg(F.sum("sales_amount")).first()[0]
        assert total == Decimal("4348925.16")
        assert spark.table(tables["dim_product"]["target"]).count() == 11
        assert spark.table(tables["dim_region"]["target"]).count() == 7
        assert spark.table(tables["dim_channel"]["target"]).count() == 31
        assert spark.table(tables["dim_calendar"]["target"]).count() == 365
        checks.append("Original case: 16151 sales, exact total, 11 standardized products, 7 regions, 31 channels and 365 days")
        versions = {n: latest_version(spark, tables[n]["target"]) for n in tables}
        no_new = process_sources(spark, dbutils, config, start_execution())
        assert not no_new["has_new_data"], repr(no_new)
        assert versions == {n: latest_version(spark, tables[n]["target"]) for n in versions}
        checks.append("No-new-data run preserves fact and summary versions")
        properties = spark.sql(f"DESCRIBE DETAIL {tables['fact_sales']['target']}").first()["properties"]
        make_table_writer(tables, "fact_sales", properties["beverage.appId"], "replay_test")(
            spark.table(fact_config["source_table"]), 0)
        assert spark.table(tables["fact_sales"]["target"]).count() == 16151
        checks.append("Retry after committed fact batch does not duplicate occurrences")
        # Two identical new occurrences are intentional and must both survive.
        sample = spark.table(fact_config["source_table"]).limit(1)
        new = (sample.withColumn("sale_date", F.lit("2008-02-29").cast("date"))
               .withColumn("year", F.lit(2008)).withColumn("month", F.lit(2))
               .withColumn("brand_flavor_code", F.lit("TEST_NEW"))
               .withColumn("brand_name", F.lit("TEST NEW BRAND"))
               .withColumn("region_name", F.lit("TEST NEW REGION"))
               .withColumn("sales_amount", F.lit("12.34").cast("decimal(28,2)"))
               .withColumn("_source_file_path", F.lit("/test/new.csv")))
        late = sample.withColumn("sales_amount", F.lit("-0.25").cast("decimal(28,2)"))
        added = new.unionByName(new).unionByName(late)
        added.write.mode("append").saveAsTable(fact_config["source_table"])
        model_only = {**config, "selected": ["fact_sales"]}
        # Test the callback directly so an intentional failure does not mark a stream as failed.
        try:
            make_table_writer(tables, "fact_sales", properties["beverage.appId"], "missing_test")(added, 1)
        except ValueError as error:
            assert "Missing dimension members" in str(error)
        else:
            raise AssertionError("Fact must not accept orphan keys")
        assert spark.table(tables["fact_sales"]["target"]).count() == 16151
        process_sources(spark, dbutils, {**config, "selected": ["dim_calendar", "dim_product", "dim_region"]}, start_execution())
        dim_versions = {n: latest_version(spark, tables[n]["target"]) for n in DIMENSIONS}
        increment = process_sources(spark, dbutils, model_only, start_execution())
        assert increment["sources"][0]["rows"] == 3
        assert dim_versions == {n: latest_version(spark, tables[n]["target"]) for n in DIMENSIONS}
        assert spark.table(tables["fact_sales"]["target"]).count() == 16154
        assert spark.table(tables["dim_calendar"]["target"]).filter("calendar_date = DATE '2008-02-29'").count() == 1
        assert all(latest_version(spark, tables[n]["target"]) == versions[n] for n in SUMMARIES)
        checks.append("Increment preserves duplicate rows, late negative amount and leap-day member")
        one_summary = {**config, "selected": [SUMMARIES[0]]}
        process_sources(spark, dbutils, one_summary, start_execution())
        assert all(latest_version(spark, tables[n]["target"]) == versions[n] for n in SUMMARIES[1:])
        # All tables must finish pending summaries even though the fact has no new rows.
        resumed = process_sources(spark, dbutils, config, start_execution())
        resumed_status = {r["table"]: r["status"] for r in resumed["sources"]}
        assert resumed_status["fact_sales"] == resumed_status[SUMMARIES[0]] == "NO_NEW_DATA"
        checks.extend(validate_gold(spark, tables))
        checks.append("Independent summary selection and pending-summary recovery reconcile")
        before_cursor = summary_cursor(spark, tables[SUMMARIES[0]]["target"])
        process_sources(spark, dbutils, one_summary, start_execution())
        assert summary_cursor(spark, tables[SUMMARIES[0]]["target"]) == before_cursor
        checks.append("Committed summary cursor prevents incremental amounts from being added twice")
        return {"status": "PASSED", "checks": checks, "first_load": [two, remaining, first, summaries], "no_new": no_new,
                "incremental": increment, "resumed": resumed}
    finally:
        # schema is constructed solely from this test's fixed prefix and UUID.
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
