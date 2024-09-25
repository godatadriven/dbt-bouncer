{% snapshot orders_snapshot %}

    {{
        config(
            target_database="dbt",
            target_schema="snapshots",
            unique_key="id",
            strategy="timestamp",
            updated_at="order_date",
        )
    }}

    select *
    from {{ ref("raw_orders") }}

{% endsnapshot %}
