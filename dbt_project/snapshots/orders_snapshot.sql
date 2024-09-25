{% snapshot orders_snapshot %}

    {{
        config(
            target_database=(
                "dbt" if target.type == "duckdb" else "padraic-slattery-sndbx-o"
            ),
            target_schema="dbt_pslattery",
            unique_key="id",
            strategy="timestamp",
            updated_at="order_date",
        )
    }}

    select *
    from {{ ref("raw_orders") }}

{% endsnapshot %}
