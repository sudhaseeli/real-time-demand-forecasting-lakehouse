select
  f.product_id,
  f.forecast_date,
  f.forecast_quantity
from {{ ref('stg_demand_forecast') }} f
