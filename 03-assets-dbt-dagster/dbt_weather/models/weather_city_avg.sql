{{ config(materialized='table') }}

select
  city,
  temp_c,
  cloudcover,
  humidity,
  (humidity * cloudcover) / 10000.0 AS rain_score,
  ((humidity * cloudcover) / 10000.0 > 0.5) AS predict_rain

from {{ source('main', 'weather_staging') }}
