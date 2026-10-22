# Business questions

[01_business_questions.ipynb](01_business_questions.ipynb) reads Gold aggregates and displays answers to the case questions. It does not load data or participate in the processing Job.

## Execution

Load [Gold](../pipelines/gold/README.md), including all three aggregates. Open the notebook, set **Project Root** and run cells in order. Targets come from `configs/gold`. Refresh pending loads first; this notebook does not update aggregates.

## Questions and interpretation

| Item | Question | Table | Selection rule |
|---|---|---|---|
| 4.1 | What are the top three trade groups in each region? | `agg_sales_region_trade_group` | Descending sales, then group name; up to three groups per region |
| 4.2 | How much did each brand sell per month? | `agg_sales_brand_month` | Observed brand, year and month combinations |
| 4.3 | Which brand had the lowest sales in each region? | `agg_sales_region_brand` | Lowest observed total over the available period; include all tied brands |

Trade group means channel classification `TRADE_GROUP_DESC`, not sales field `CHNL_GROUP`. Unobserved combinations are not filled with zeros when choosing the lowest-selling brand. Negative and zero amounts contribute to totals. Currency is unspecified. Monthly queries retain the year to avoid mixing months from different years.

## Case coverage

Sales and channel classification are related in Silver and represented by the Gold channel dimension. The [dimensional model](../pipelines/gold/README.md) provides dimensions, a fact and summary tables. This notebook answers business questions from that model.

## Further development

- Define refresh frequency, scheduling and alerts for the [Job](../jobs/README.md).
- Confirm currency, reporting-period meaning, business calendar and sales-correction rules.
- Define dimensional change handling and whether SCD history is needed.
- Measure production volume, distribution and latency before selecting input formats, clustering, partitioning and compute capacity.
- Align data, checkpoint and CDF retention with recovery windows.
- Automate tests and deployment from Git with environment-specific settings.
- Add dashboards for agreed analytical needs.
- For Azure deployment, provision ADLS storage, identity, permissions and infrastructure.

[Project overview](../README.md)
