with base as (

    select
        merchant_id,
        amount,
        is_fraud
    from {{ ref('stg_transactions') }}
    where merchant_id is not null

),

aggregated as (

    select
        merchant_id,

        count(*) as transactions_cnt,
        sum(amount) as amount_sum,

        sum(is_fraud) as fraud_cnt,
        sum(is_fraud) * 100.0 / nullif(count(*), 0) as fraud_rate

    from base
    group by
        merchant_id

),

scored as (

    select
        merchant_id,
        transactions_cnt,
        amount_sum,
        fraud_cnt,
        fraud_rate,

        case
            when fraud_rate >= 5 or fraud_cnt >= 3 then 1
            else 0
        end as is_suspicious

    from aggregated

)

select
    merchant_id,
    transactions_cnt,
    amount_sum,
    fraud_cnt,
    fraud_rate,
    is_suspicious
from scored
