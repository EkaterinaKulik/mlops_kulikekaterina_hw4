with base as (

    select
        day_of_week,
        transaction_hour,
        amount,
        is_fraud
    from {{ ref('stg_transactions') }}

),

aggregated as (

    select
        day_of_week,
        transaction_hour,

        count(*) as transactions_cnt,
        sum(is_fraud) as fraud_cnt,

        sum(is_fraud) * 100.0 / nullif(count(*), 0) as fraud_rate,

        sum(amount) as amount_sum,
        avg(amount) as amount_avg

    from base
    group by
        day_of_week,
        transaction_hour

)

select
    day_of_week,
    transaction_hour,
    transactions_cnt,
    fraud_cnt,
    fraud_rate,
    amount_sum,
    amount_avg
from aggregated
order by
    day_of_week,
    transaction_hour
