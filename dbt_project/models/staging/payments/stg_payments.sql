with
    source as (

        {#-
    Normally we would select from the table here, but we are using seeds to load
    our data in this project
    #}
        select * from {{ ref("raw_payments") }}

    ),

    renamed as (

        select
            id as payment_id,
            order_id,
            payment_method,

            -- `amount` is currently stored in cents, so we convert it to dollars
            {{ cents_to_dollars("amount") }} as amount

        from source

    )

select *
from renamed
