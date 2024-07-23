{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Enter this block when run on stg or prd (except for CICD runs).
        We want the same dataset and table names to be used across all environments.
        For example, `marts.dim_customer` should exist in stg and prd, i.e. there should be no references to the project in the dataset name.
        This will allow other tooling (BI, CICD scripts, etc.) to work across all environments without the need for differing logic per environment.
    #}
    {% if env_var("DBT_CICD_RUN", "false") == "true" %} {{ env_var("DBT_DATASET") }}

    {% elif target.name in ["stg", "prd"] and env_var(
        "DBT_CICD_RUN", "false"
    ) == "false" %}

        {{ node.config.schema }}

    {% else %} {{ default__generate_schema_name(custom_schema_name, node) }}

    {%- endif -%}

{%- endmacro %}
