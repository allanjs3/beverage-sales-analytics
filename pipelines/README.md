# Pipeline notebooks

Each notebook loads configuration, resolves selected tables, calls the layer implementation and displays results.

| Entry point | Input | Output |
|---|---|---|
| [Bronze](bronze/README.md): `bronze/01_ingest_bronze` | Landing CSVs | Bronze tables |
| [Silver](silver/README.md): `silver/01_process_silver` | Bronze | Silver tables and key registries |
| [Gold](gold/README.md): `gold/01_process_gold` | Silver and Gold | Dimensions, fact and aggregates |

## Parameters

| Parameter | Label | Purpose |
|---|---|---|
| `project_root` | `Project Root` | Absolute workspace project path |
| `table_name` | `Bronze Tables`, `Silver Tables`, `Gold Tables` | One table, multiple tables or `All tables` |

The first three code cells prepare imports and widgets. Rerun them after changing Project Root or available configurations, select tables, then run the remaining cells. Remove the `Select a table` placeholder; use `All tables` alone.

Each Job task supplies its table parameter. Manual widgets support multiple values; programmatic selection accepts comma-separated names.

## Dependencies and output

Selected tables are ordered by dependencies. Unselected dependencies are not automatically loaded and must already be available. The [Job](../jobs/README.md) coordinates dependencies across layers.

Notebooks show summaries and return JSON for automation. `NO_NEW_DATA` indicates no added rows. Gold distinguishes new dimension/fact rows from aggregate groups written. Presentation uses metrics already produced by the load.

Configuration, reading and validation errors stop execution. Targets do not share one transaction; committed batches remain and retries use each target's state.

[Project overview](../README.md) · [Transformations](../transformations/README.md)
