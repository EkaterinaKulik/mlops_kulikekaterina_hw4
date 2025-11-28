with raw as (

    select
        transaction_time,
        merch,
        cat_id,
        amount,
        name_1,
        name_2,
        gender,
        us_state,
        lat,
        lon,
        merchant_lat,
        merchant_lon,
        target
    from {{ source('transactions_db', 'transactions') }}

),

normalized as (

    select
        transaction_time,
        toDate(transaction_time) as transaction_date,
        toHour(transaction_time) as transaction_hour,
        toDayOfWeek(transaction_time) as day_of_week,

        merch as merchant_id,
        cat_id as category_id,

        name_1,
        name_2,
        concat(name_1, ' ', name_2) as customer_name,
        lower(gender) as gender_normalized,
        us_state,

        lat as customer_lat,
        lon as customer_lon,
        merchant_lat,
        merchant_lon,

        amount,
        {{ amount_bucket('amount') }} as amount_bucket,

        target as is_fraud

    from raw

)

select
    transaction_time,
    transaction_date,
    transaction_hour,
    day_of_week,

    merchant_id,
    category_id,

    name_1,
    name_2,
    customer_name,
    gender_normalized,
    us_state,

    customer_lat,
    customer_lon,
    merchant_lat,
    merchant_lon,

    amount,
    amount_bucket,
    is_fraud

from normalized
