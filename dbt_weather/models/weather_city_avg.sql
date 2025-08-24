{{ config(materialized='table') }}

select
  city,
  avg(temp_c) as avg_temp_c,
  count(*)    as readings
from {{ source('main', 'weather_now') }}
group by 1
order by 1
