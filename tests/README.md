# Incremental processing tests

These functions exercise initial loading, new inputs, replay protection and recovery. They create isolated temporary schemas and remove them when finished. Production destinations and checkpoints are not modified.

| File | Scenarios |
|---|---|
| [test_bronze_incremental.py](test_bronze_incremental.py) | Initial ingestion, new files, repeated runs and quality review signals |
| [test_silver_incremental.py](test_silver_incremental.py) | Batch validation, conflicting keys, invalid records and recovery around writes |
| [test_gold_incremental.py](test_gold_incremental.py) | Table selection, references, new sales and pending aggregate recovery |

## On-demand execution

Run in a Databricks notebook with project access and permission to create and remove temporary schemas:

```python
import sys
from pathlib import Path

project_root = Path("/Workspace/Users/<user>/beverage_sales")
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "tests"))

from test_bronze_incremental import run_bronze_scenarios
from test_silver_incremental import run_incremental_scenarios as test_silver
from test_gold_incremental import run_incremental_scenarios as test_gold

bronze_result = run_bronze_scenarios(spark, dbutils)
silver_result = test_silver(spark, dbutils)
gold_result = test_gold(spark, dbutils)
```

Functions can run independently. Bronze and Gold read sample files or tables as reference inputs; Silver creates small fixtures. Silver's `include_expected_stream_failure` is disabled by default because it intentionally triggers a streaming failure.

These functions are called explicitly, not by `unittest discover` or the loading Job. To check loaded business tables, use [data_quality](../data_quality/README.md).
