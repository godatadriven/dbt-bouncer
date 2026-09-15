{{
    config(
        materialized="table",
    )
}}


-- `dbt_utils.date_spine` emits adapter-correct SQL. The previous `range()` call was a
-- DuckDB table function and it failed on the BigQuery probe. `end_date` is exclusive,
-- which matches the bounds of the `range()` call it replaces. The outer cast keeps
-- `date_day` a `date`, because `date_spine` returns a timestamp.
with

    spine as (
        {{
            dbt_utils.date_spine(
                datepart="day",
                start_date="cast('2009-01-01' as date)",
                end_date="cast('2013-12-31' as date)",
            )
        }}
    )

select cast(date_day as date) as date_day
from spine
