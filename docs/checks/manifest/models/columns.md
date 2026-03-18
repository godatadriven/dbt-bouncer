# Columns

!!! note

    The below checks require `manifest.json` to be present.

These checks validate column-level properties on your dbt models. Use them to ensure every column has a description, a data type, the required meta keys, and relationship tests where applicable. You can also enforce model-level constraints such as primary keys or unique constraints. Thorough column documentation and testing make your data contracts explicit and help downstream consumers understand the data they depend on.

::: manifest.models.columns
