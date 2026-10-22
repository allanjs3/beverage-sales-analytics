# Silver processing

`01_process_silver` converts Bronze records into typed, standardized data. Each table uses one `AvailableNow` stream, one checkpoint and batch validation before writing.

## Tables and dependencies

| Contract | Source | Target |
|---|---|---|
| [channel_group.json](../../configs/silver/channel_group.json) | `beverage_sales_bronze.channel_group` | `beverage_sales_silver.channel_group` |
| [sales.json](../../configs/silver/sales.json) | `beverage_sales_bronze.sales` | `beverage_sales_silver.sales` |

Locations use catalog `workspace`. Transformations, keys and dependencies are registered in [tables/__init__.py](../../transformations/silver/tables/__init__.py).

Set **Project Root** and **Silver Tables** as described in [pipelines](../README.md). Selecting all tables processes channels before sales. Selecting only sales uses channels already loaded; include channel processing when new classifications are needed.

## Field rules

| Field or group | Transformation |
|---|---|
| `sale_date` | Parse `M/d/yyyy` into `DATE` |
| `sales_amount` | Convert to `DECIMAL(28,2)`; reject excess precision without silent rounding |
| `year`, `month` | Derive from the date and compare with source values |
| `period` | Convert source `PERIOD` to integer; fiscal meaning is unspecified |
| Codes | Preserve strings and leading zeros |
| Text | Trim outer whitespace |
| `trade_channel_key` | Trim and uppercase the channel name |
| `package_name` | Uppercase, prefix initial decimals with zero, remove trailing `S`, `F` and `*` markers |

Package normalization removes isolated terminal `S` and `F` tokens and terminal asterisks. `.5L NRP 6P*` becomes `0.5L NRP 6P`; `12Z NRP 8P F` becomes `12Z NRP 8P`. An initial `,5L` becomes `0.5L`. Internal letters and components such as `8P` remain.

Removing these markers is a project assumption because the source does not explain them. `0.591L` and `20Z` remain distinct. See `normalize_package_name` in [sales.py](../../transformations/silver/tables/sales.py).

Negative amounts, zero amounts and repeated attribute combinations remain. Normalization does not deduplicate sales. `channel_group` and `trade_group_description` are different classifications; currency is unspecified.

## Batch checks and controls

Before writing, the pipeline checks conversions, required fields, date consistency, rescued input, lineage and channel references.

| Control table | Purpose |
|---|---|
| `workspace.beverage_sales_control.silver_sales_keys` | Keep consistent brand and category attributes for each brand/flavor, category and standardized package key |
| `workspace.beverage_sales_control.silver_channel_group_keys` | Keep consistent trade group and type for each channel key |

Each registry is configured in its Silver table JSON. Incoming distinct mappings are compared with approved keys and inserted by `MERGE`. Sales occurrences are appended in full; regular batch validation does not rescan all Bronze sales.

Invalid batches stop the load. Previously committed batches remain.

## State and recovery

Checkpoints reside in `workspace.beverage_sales_silver.processing_state`, under `<table>/checkpoint`.

Each batch validates, updates its registry, then appends Silver records. These writes are separate transactions. Registry `MERGE` is idempotent, and Silver uses `txnAppId` and `txnVersion` to avoid replaying a committed append.

Before starting a stream, Silver compares the Bronze version with `beverage.sourceVersion` on its own target. A matching version returns `NO_NEW_DATA` without starting a stream or writing a progress marker. Existing source, target and checkpoint checks still apply.

The marker is saved only after the stream and commit checks succeed, using the source version captured before processing. Failed runs resume through the checkpoint. A source append during execution can require an extra check on the next run; it is not skipped. Existing targets without the property resume once through their current checkpoint to establish the marker.

Keep the target, registry and checkpoint together. Recreated sources, missing state or changed rule semantics require planned reprocessing. `RULES_VERSION` tracks semantic changes, not comment edits.

## Metadata and results

Silver retains `_source_file_path`, `_silver_run_id` and `_processed_at`. `_quality_errors` is temporary. Negative and zero counts can appear in execution metrics without adding a warning column to the table.

Descriptions come from configuration. The notebook reports added rows and returns `SILVER_UPDATED` or `NO_NEW_DATA`.

[Configurations](../../configs/README.md) · [Tests](../../tests/README.md) · [Job](../../jobs/README.md)
