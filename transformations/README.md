# Processing implementations

[Pipeline notebooks](../pipelines/README.md) call these modules with Spark, Databricks utilities, configuration and execution context.

| Module | Purpose |
|---|---|
| [bronze/ingestion.py](bronze/ingestion.py) | Load source contracts, run Auto Loader and inspect new records |
| [silver/configuration.py](silver/configuration.py) | Validate contracts and resolve selected tables |
| [silver/processing.py](silver/processing.py) | Prepare targets and coordinate Silver streams |
| [silver/key_validation.py](silver/key_validation.py) | Validate batches, maintain key registries and append records with replay protection |
| [silver/rules.py](silver/rules.py) | Shared Silver expressions and lineage rules |
| [silver/tables/__init__.py](silver/tables/__init__.py) | Register Silver transformations, keys, attributes and dependencies |
| [silver/tables/sales.py](silver/tables/sales.py) | Type and standardize sales; check channel relationships |
| [silver/tables/channel_group.py](silver/tables/channel_group.py) | Standardize channels and check classifications |
| [gold/configuration.py](gold/configuration.py) | Load Gold contracts and resolve matching modules |
| [gold/processing.py](gold/processing.py) | Coordinate dimensions, fact and aggregate progress |
| `gold/tables/` | One projection or aggregation module per Gold table |

## Organization

Bronze shares CSV ingestion driven by [configuration](../configs/README.md). Silver has one table module and one `TABLES` entry per transformation. Gold discovers the module matching each contract and calls its `transform` function.

Layer-specific rules remain in the layer. Functions independent of business meaning belong in [utils](../utils/README.md).

```text
Layer notebook
  -> load and validate configuration
  -> select and order tables
  -> transform records
  -> validate and write
  -> return execution results
```

Transformations operate on DataFrames. Processing modules create tables, write records and update metadata. Importing a module does not start a load.

Add contracts, transformations, dependencies and relevant [tests](../tests/README.md) when extending the pipeline. Update the [Job](../jobs/README.md) for new tasks. Changes affecting historical data need a reprocessing strategy; code edits do not replay committed batches.
