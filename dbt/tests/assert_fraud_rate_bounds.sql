select
    *
from {{ ref('mart_fraud_by_category') }}
where fraud_rate > 100
   or fraud_rate < 0
