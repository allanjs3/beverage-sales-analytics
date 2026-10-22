# Bronze ingestion

`01_ingest_bronze` uses Auto Loader and a Delta streaming sink to ingest Landing files. `AvailableNow` processes available deliveries and ends the run.

## Inputs

| Contract | Filename pattern | Target |
|---|---|---|
| [sales.json](../../configs/bronze/sales.json) | `abi_bus_case1_beverage_sales_*.csv` | `workspace.beverage_sales_bronze.sales` |
| [channel_group.json](../../configs/bronze/channel_group.json) | `abi_bus_case1_beverage_channel_group_*.csv` | `workspace.beverage_sales_bronze.channel_group` |

Landing: `/Volumes/workspace/beverage_sales_landing/source_files/`. Sales use tabs and UTF-16 with a BOM; channels use commas and UTF-8 with a BOM.

Each delivery must be complete and have a new name matching its pattern. Files are immutable after ingestion. Renaming and resending content creates another delivery; file tracking does not deduplicate sales.

## Reading and metadata

Business fields remain strings. Mappings and descriptions come from JSON. Bronze does not trim values, convert dates or amounts, aggregate or deduplicate records.

The reader uses an explicit schema, `failOnNewColumns` and `FAILFAST`. `_rescued_data` retains fields rescued during parsing. Sales use `multiLine=True` for UTF-16 decoding; parsing cannot split one file across tasks, although separate files can run in parallel.

| Technical column | Meaning |
|---|---|
| `_source_file_path` | Full source file path |
| `_source_file_name` | Source filename |
| `_source_file_size` | Source file size in bytes |
| `_source_file_modified_at` | File modification time |
| `_ingested_at` | Ingestion timestamp |
| `_ingestion_run_id` | Ingestion execution identifier |

## Incremental processing

Auto Loader discovers files by directory listing. `includeExistingFiles=True` includes files present at initial load; `allowOverwrites=False` enforces immutable deliveries.

State resides in `workspace.beverage_sales_bronze.ingestion_state`:

```text
sales/checkpoint/
sales/schema/
channels/checkpoint/
channels/schema/
```

`channels` is the stable source identifier for `channel_group`. The streaming sink and checkpoint coordinate committed batches. A missing target or checkpoint in an existing load requires investigation. Each checkpoint has one writer.

## Execution and output

Set **Project Root** and **Bronze Tables** using the [pipeline selection pattern](../README.md). Implementation: [ingestion.py](../../transformations/bronze/ingestion.py).

Delta commit metrics determine added rows. Only new records are inspected for rescued fields and optional per-file `reference_counts`. Checks run after writing and do not roll back records.

| Status | Meaning |
|---|---|
| `NO_NEW_DATA` | No selected source added rows |
| `BRONZE_UPDATED` | Rows added without review signals |
| `BRONZE_UPDATED_REVIEW_REQUIRED` | Rows added with rescued fields or a reference-count mismatch |

An empty file can produce `NO_NEW_DATA`. Initialization, discovery and metadata checks still run. Catalog comments are updated only when different; comment changes do not set `has_new_data`.

A compatible CSV source needs a new [configuration](../../configs/README.md); another format requires implementation changes. See [tests](../../tests/README.md) and [orchestration](../../jobs/README.md).
