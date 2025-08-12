{{ config(materialized="table") }}

with
    customers as (select * from {{ ref("stg_customers") }}),

    orders as (select * from {{ ref("stg_orders") }}),

    payments as (select * from {{ ref("stg_payments") }}),

    customer_orders as (

        select
            customer_id,

            min(order_date) as first_order_date,
            max(order_date) as most_recent_order_date,
            count(order_id) as number_of_orders
        from orders

        group by customer_id

    ),

    customer_payments as (

        select orders.customer_id, sum(amount) as total_amount

        from payments

        left join orders on payments.order_id = orders.order_id

        group by orders.customer_id

    ),

    final as (

        select
            customers.customer_id,
            customers.first_name,
            customers.last_name,
            'New column' as new_column,
            customer_orders.first_order_date,
            customer_orders.most_recent_order_date,
            customer_orders.number_of_orders,
            if(
                customer_orders.number_of_orders > 5, true, false
            ) as is_recurring_customer,
            coalesce(customer_payments.total_amount, 0) as customer_lifetime_value

        from customers

        left join customer_orders on customers.customer_id = customer_orders.customer_id

        left join
            customer_payments on customers.customer_id = customer_payments.customer_id

    )

select *
from final
