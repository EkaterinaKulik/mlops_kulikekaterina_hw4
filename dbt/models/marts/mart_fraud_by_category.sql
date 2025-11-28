with base as (

    select
        category_id,
        amount,
        is_fraud
    from {{ ref('stg_transactions') }}

),

aggregated as (

    select
        category_id,

        count(*) as transactions_cnt,

        sum(is_fraud) as fraud_cnt,

        sum(is_fraud) * 100.0 / nullif(count(*), 0) as fraud_rate,

        sum(amount) as amount_sum,
        sum(
            case
                when is_fraud = 1 then amount
                else 0
            end
        ) as fraud_amount_sum

    from base
    group by
        category_id

)

select
    category_id,
    transactions_cnt,
    fraud_cnt,
    fraud_rate,
    amount_sum,
    fraud_amount_sum
from aggregated
