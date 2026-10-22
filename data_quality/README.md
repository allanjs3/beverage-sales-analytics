# Data quality

[01_validate_tables](01_validate_tables.ipynb) checks loaded tables using the `quality` block in each [table configuration](../configs/README.md). Reusable implementations are in [checks.py](checks.py).

## Rules

| Group | Check 1 | Check 2 |
|---|---|---|
| Bronze | No rescued source fields | Source file path is populated |
| Silver | Essential fields are populated | Sales reference an existing channel |
| Dimensions and fact | Dimension keys are populated and unique | Fact keys reference existing dimension members |
| Aggregates | Group keys are populated and unique | Group totals match the sales fact |

These eight rule definitions produce 18 results: four Bronze, three Silver, five dimensions/fact and six aggregates. Negative and zero sales amounts are allowed.

## Configuration

The rule identifier becomes `check_name`; its English `description` becomes `rules`. The table's top-level `description` becomes `description_table`.

```json
"quality": {
  "gold_dimension_key_valid": {
    "type": "unique",
    "columns": ["product_key"],
    "description": "Checks that product_key is populated and unique."
  }
}
```

| Type | Check | Settings |
|---|---|---|
| `null` | Fields contain only nulls | `columns` |
| `required` | No nulls or blank strings | `columns` |
| `unique` | Complete, unique key | `columns`; composite keys supported |
| `references` | Keys exist in another table | `references` with `table` and `columns` |
| `aggregate` | Grouped sums match the configured source | `source`, `joins`, `group_by`, `measure` |

References use `layer/filename`, without `.json`, such as `silver/channel_group`. The referenced contract supplies the physical target. For different referenced column names, supply `reference_columns` in the same order as `columns`.

Aggregate rules use `source` for the fact contract, `joins` for dimension contracts and join columns, and `group_by` and `measure` for the sum. See [agg_sales_brand_month.json](../configs/gold/agg_sales_brand_month.json).

Add a `quality` block to include a table. Supported rule types require no Python changes. Tables without this block are skipped; unsupported rules produce `ERROR`. Quality configuration does not create a load task: configure the [Job](../jobs/README.md) separately.

## Execution

The final Job task checks complete tables when any selected load writes data. If none writes data, it returns `SKIPPED` without reading tables or storing results. Excluded tasks do not participate in this decision.

For a manual check or retry after correcting a failure, open the notebook, set **Project Root**, keep **Execution Mode = Manual** and run all cells. This always validates without rerunning the loading pipeline. Avoid concurrent writes during validation.

**Upstream Results** and **Run ID** are supplied by the Job. Manual execution generates its own identifier. Direct table edits require a manual check because automatic execution uses load-task indicators.

## Results

`workspace.beverage_sales_quality.validation_tables` stores one row per table and rule for each actual validation.

| Column | Type | Meaning |
|---|---|---|
| `run_id` | STRING | Job or manual validation identifier |
| `validated_at` | TIMESTAMP | Check completion time |
| `table_name` | STRING | Fully qualified table checked |
| `check_name` | STRING | Rule identifier |
| `status` | STRING | `PASS`, `FAIL` or `ERROR` |
| `message` | STRING | Failure or execution error; null for passed checks |
| `rules` | STRING | English explanation of the rule |
| `description_table` | STRING | English description of the checked table |

`FAIL` means the data did not satisfy a rule. `ERROR` means the check could not run. Results are saved before the task signals failure. Quality checks do not change business data.

The writer maintains table and column comments. Rule and table descriptions are captured with each result; configuration edits affect future validations, not previous results.
