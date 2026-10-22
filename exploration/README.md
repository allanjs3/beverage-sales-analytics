# Source exploration

These notebooks investigate source structure, grain, relationships and quality assumptions. They read Landing without writing tables or processing state.

| Order | Notebook | Purpose |
|---|---|---|
| 00 | `00_data_profiling` | Inspect structure, completeness, dates, amounts and repeated values |
| 01 | `01_relationships_and_grain` | Examine sales-to-channel joins and reporting grain |
| 02 | `02_modeling_and_business_rules` | Compare product keys, monetary precision, periods, coverage and ties |

The notebooks are independent. Their order follows the investigation from profiling to modeling.

## Inputs and execution

Review source paths in the opening cells and run each notebook in cell order. Sales use tabs, UTF-16 with a BOM and multiline reading. Channels use commas and UTF-8 with a BOM. Fields start as strings; exploratory conversions identify invalid values.

Multiline UTF-16 parsing does not split one file across tasks. Outputs include counts, distributions, differences and sample values.

## Interpretation

- Compare counts and sums before and after joins to detect lost or multiplied sales.
- Check that each channel key has one classification.
- Evaluate product keys using category and package presentation as well as brand/flavor.
- Distinguish missing observations, zero amounts and negative amounts.
- Do not infer currency or fiscal meaning for `PERIOD` without a source definition.
- Define tie handling and missing combinations before building rankings.

The source has no unique transaction identifier. Equal attributes alone do not establish duplicate sales.

Exploration shows original values; [Silver](../pipelines/silver/README.md) applies explicit normalization. The [Gold model](../pipelines/gold/README.md), [quality checks](../data_quality/README.md) and [business queries](../case_answers/README.md) use standardized data.
