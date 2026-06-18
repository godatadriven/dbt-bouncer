{% macro apply_grants(relation, grant_config, should_revoke) %}
    {#- Under dbt 2.0 / Fusion the bundled apply_grants is incompatible with the duckdb adapter,
        so skip grant application. On dbt 1.x, defer to the built-in implementation unchanged. -#}
    {% if dbt_version_at_least("2.0") %} {{ return("") }} {% endif %}
    {{
        return(
            adapter.dispatch("apply_grants", "dbt")(
                relation, grant_config, should_revoke
            )
        )
    }}
{% endmacro %}
