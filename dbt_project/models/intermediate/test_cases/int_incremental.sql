{{
    config(
        incremental_strategy="delete+insert",
        materialized="incremental",
        unique_key="id",
    )
}}

select 1 as id
where 1 = 1 {% if is_incremental() %} and 2 = 2 {% endif %}
