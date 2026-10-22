# Table configurations

Each JSON describes one table's locations, catalog descriptions and quality rules. Business transformations remain in Python modules.

| Directory | Contracts |
|---|---|
| `bronze/` | [sales.json](bronze/sales.json), [channel_group.json](bronze/channel_group.json) |
| `silver/` | [sales.json](silver/sales.json), [channel_group.json](silver/channel_group.json) |
| `gold/` | One JSON per dimension, fact or aggregate |

The filename without `.json` is the table selector option. Discovery reads JSONs directly inside each layer directory.

## Bronze settings

| Setting | Purpose |
|---|---|
| `schema_version` | Configuration contract version |
| `state_volume` | Auto Loader checkpoint and schema volume |
| `name` | Stable source identifier used in state paths |
| `source_path`, `file_pattern` | Input directory and filename pattern |
| `format`, `reader_options` | Format, encoding, delimiter and multiline settings |
| `target` | Delta target table |
| `description` | Table description |
| `columns` | Ordered `source`, `target` and `description` mappings |
| `reference_counts` | Optional expected counts for named files |

The filename matches the target table name. Source identifiers can differ: `channel_group.json` uses `name = channels`. Changing that identifier changes its state path. Selected sources share a contract version and state volume; file patterns must not overlap.

## Silver settings

| Setting | Purpose |
|---|---|
| `schema_version`, `name` | Contract version and registered table name |
| `source_table`, `target` | Bronze source and Silver target |
| `state_volume` | Checkpoint volume |
| `validation_table` | Approved key registry in `beverage_sales_control` |
| `validation_description` | English description of that control table |
| `description`, `columns` | Table and output field descriptions |

Each column declares `target` and `description`. Types, transformations, keys and dependencies belong to the [Silver registry](../transformations/silver/tables/__init__.py). Filename, `name` and target table name must match. Source, target and key registry must be distinct.

## Gold settings

| Setting | Purpose |
|---|---|
| `name`, `target` | Contract identity and destination |
| `kind` | `dimension`, `fact` or `summary` |
| `depends_on` | Dependencies among Gold tables |
| `description`, `columns` | Table description; columns with `name`, `type`, `description` |
| `source_table`, `state_volume` | Source and state for dimensions and fact |
| `key` | Dimension key |
| `source_name` | Fact contract consumed by an aggregate |

A contract resolves the matching Python module: [dim_product.json](gold/dim_product.json) uses [dim_product.py](../transformations/gold/tables/dim_product.py). Dependencies order selected tables without adding unselected ones.

## Quality and metadata

The optional `quality` block defines checks. Each entry has an identifier, supported `type`, English `description` and rule settings. See [quality configuration](../data_quality/README.md).

Results store the rule description in `rules` and the table's top-level description in `description_table`, captured at validation time. Catalog comments use the same table and column definitions.

## Adding a table

1. Add its JSON with descriptions and quality rules.
2. For Silver, implement and register the transformation, keys and dependencies. For Gold, add the matching transformation module.
3. Check the configuration with its layer loader and exercise relevant [tests](../tests/README.md).
4. Add a task, dependencies and quality write indicator to the [Job](../jobs/README.md) when needed.

Changing locations or state settings does not migrate existing data or checkpoints. Comment changes do not require rebuilding business data.
