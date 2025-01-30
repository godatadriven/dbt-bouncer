{% snapshot snapshot_orders %}

    {{
        config(
            target_database=(
                "dbt" if target.type == "duckdb" else "padraic-slattery-sndbx-o"
            ),
            target_schema="dbt_pslattery",
            unique_key="id",
            strategy="timestamp",
            tags=["payment"],
            updated_at="order_date",
        )
    }}

    select *
    from {{ ref("raw_orders") }}

{% endsnapshot %}
