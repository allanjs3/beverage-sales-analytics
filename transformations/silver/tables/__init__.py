"""Single registry of Silver transformations, schemas and key contracts."""

from transformations.silver.tables import channel_group, sales


TABLES = {
    "channel_group": {
        "transform": channel_group.transform_channel_group,
        "input_columns": channel_group.INPUT_COLUMNS,
        "columns": channel_group.OUTPUT_COLUMNS,
        "keys": ("trade_channel_key",),
        "attributes": ("trade_group_description", "trade_type_description"),
        "depends_on": (),
    },
    "sales": {
        "transform": sales.transform_sales,
        "input_columns": sales.INPUT_COLUMNS,
        "columns": sales.OUTPUT_COLUMNS,
        "keys": ("brand_flavor_code", "package_category_code", "package_name"),
        "attributes": ("brand_name", "package_category_description"),
        "depends_on": ("channel_group",),
    },
}
