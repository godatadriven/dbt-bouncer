{% macro dbt_version_at_least(target) %}
    {#- Return true if the running dbt engine version is >= `target` (e.g. '1.12', '2.0').
        Encodes major*100 + minor as an int so the comparison works identically in both the
        Python (dbt-core 1.x) and Rust (dbt 2.0 / Fusion) Jinja engines. -#}
    {% set p = dbt_version.split(".") %}
    {% set t = target.split(".") %}
    {{
        return(
            ((p[0] | int) * 100 + (p[1] | int)) >= ((t[0] | int) * 100 + (t[1] | int))
        )
    }}
{% endmacro %}
