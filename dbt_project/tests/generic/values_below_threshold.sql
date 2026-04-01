{% test values_below_threshold(model, column_name, threshold) %}

    with
        validation as (

            select count(*) as above_threshold_cnt

            from {{ model }}
            where {{ column_name }} > {{ threshold }}

        )

    select *
    from validation
    where above_threshold_cnt > 0

{% endtest %}
