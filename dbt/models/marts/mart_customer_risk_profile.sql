with base as (

    select
        customer_name,
        us_state,
        amount,
        is_fraud,
        transaction_date
    from {{ ref('stg_transactions') }}
    where customer_name is not null

),

aggregated as (

    select
        customer_name,
        any(us_state) as us_state,

        count(*) as transactions_cnt,
        sum(is_fraud) as fraud_transactions_cnt,

        sum(is_fraud) * 100.0 / nullif(count(*), 0) as fraud_rate,

        avg(amount) as avg_amount,
        max(transaction_date) as last_transaction_date

    from base
    group by
        customer_name

),

segmented as (

    select
        customer_name,
        us_state,
        transactions_cnt,
        fraud_transactions_cnt,
        fraud_rate,
        avg_amount,
        last_transaction_date,

        case
            when fraud_rate >= 5 then 'HIGH'
            when fraud_rate >= 1 then 'MEDIUM'
            else 'LOW'
        end as risk_segment

    from aggregated

)

select
    customer_name,
    us_state,
    transactions_cnt,
    fraud_transactions_cnt,
    fraud_rate,
    avg_amount,
    last_transaction_date,
    risk_segment
from segmented
