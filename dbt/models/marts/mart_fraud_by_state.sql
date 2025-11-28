with base as (

    select
        us_state,
        customer_name,
        merchant_id,
        amount,
        is_fraud
    from {{ ref('stg_transactions') }}
    where us_state is not null

),

aggregated as (

    select
        us_state,

        count(*) as transactions_cnt,

        sum(is_fraud) as fraud_cnt,

        sum(is_fraud) * 100.0 / nullif(count(*), 0) as fraud_rate,

        uniqExact(customer_name) as unique_customers_cnt,
        uniqExact(merchant_id) as unique_merchants_cnt,

        sum(amount) as amount_sum,
        sum(
            case
                when is_fraud = 1 then amount
                else 0
            end
        ) as fraud_amount_sum

    from base
    group by
        us_state

)

select
    us_state,
    transactions_cnt,
    fraud_cnt,
    fraud_rate,
    unique_customers_cnt,
    unique_merchants_cnt,
    amount_sum,
    fraud_amount_sum
from aggregated
