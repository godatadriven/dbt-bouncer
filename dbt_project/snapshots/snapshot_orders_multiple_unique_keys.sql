{% snapshot snapshot_orders_multiple_unique_keys %}

    {{
        config(
            target_database=(
                "dbt" if target.type == "duckdb" else "padraic-slattery-sndbx-o"
            ),
            target_schema="dbt_pslattery",
            unique_key=["id", "order_date"],
            strategy="timestamp",
            tags=["payment"],
            updated_at="order_date",
        )
    }}

    select *
    from {{ ref("raw_orders") }}

{% endsnapshot %}
