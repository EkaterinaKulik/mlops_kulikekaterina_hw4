with base as (

    select
        transaction_date,
        us_state,
        amount,
        amount_bucket
    from {{ ref('stg_transactions') }}

),

aggregated as (

    select
        transaction_date,
        us_state,

        count(*) as transactions_cnt,

        sum(amount) as amount_sum,
        avg(amount) as amount_avg,

        quantileExact(0.95)(amount) as amount_p95,

        sum(
            case
                when amount_bucket in ('HIGH', 'VERY_HIGH') then 1
                else 0
            end
        ) * 1.0 / count(*) as big_txn_share

    from base
    group by
        transaction_date,
        us_state

)

select
    transaction_date,
    us_state,
    transactions_cnt,
    amount_sum,
    amount_avg,
    amount_p95,
    big_txn_share
from aggregated
