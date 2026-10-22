# Shared utilities

These modules provide reusable functions without product, channel or sales-specific business rules. Runtime dependencies are passed explicitly.

| Module | Purpose |
|---|---|
| [config.py](config.py) | Read UTF-8 JSON, discover contracts and reject duplicate keys |
| [selection.py](selection.py) | Parse table selections and order dependencies |
| [widgets.py](widgets.py) | Build widget choices from contracts and check selections |
| [validation.py](validation.py) | Check configuration fields, uniqueness and identifiers |
| [metadata.py](metadata.py) | Create described tables and update changed catalog comments |
| [streaming.py](streaming.py) | Check target/checkpoint consistency and read Delta versions and metrics |
| [execution.py](execution.py) | Generate execution identifiers, UTC timestamps and JSON |
| [presentation.py](presentation.py) | Format execution metrics as HTML |

`select_tables` accepts a name, comma-separated names, a list or all tables. `order_tables` orders selected dependencies without adding other tables.

Metadata functions apply configured descriptions. Presentation functions format existing metrics without querying tables. Configuration validation is separate from business record checks in [transformations](../transformations/README.md).

Use this directory for functions shared across layers. Keep table rules in their layer and business queries in [case_answers](../case_answers/README.md).
