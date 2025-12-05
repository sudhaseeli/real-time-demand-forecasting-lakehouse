select
  cast(order_id as string) as order_id,
  cast(product_id as string) as product_id,
  cast(store_id as string) as store_id,
  cast(quantity as integer) as quantity,
  cast(price as numeric(10,2)) as price,
  cast(order_timestamp as timestamp) as order_timestamp,
  quantity * price as total_amount
from {{ source('bronze', 'orders_silver') }}
