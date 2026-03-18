# Naming

!!! note

    The below checks require `manifest.json` to be present.

These checks enforce naming conventions for your dbt models. Use them to ensure every model name matches a required regex pattern, such as a prefix that indicates the model's layer (e.g. `stg_`, `int_`, `fct_`, `dim_`). Consistent naming conventions make your project self-documenting and help team members instantly identify a model's purpose and position in the DAG.

::: manifest.models.naming
