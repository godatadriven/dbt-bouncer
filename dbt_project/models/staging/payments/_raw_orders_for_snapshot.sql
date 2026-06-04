{{ config(materialized="ephemeral") }}

-- Wrapper around raw_orders that casts order_date to TIMESTAMP so it can
-- be used as the timestamp column in snapshots (Fusion requires the
-- snapshot updated_at column to be a timestamp, not a date).
select id, user_id, cast(order_date as timestamp) as order_date, status
from {{ ref("raw_orders") }}
