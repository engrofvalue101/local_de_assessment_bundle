WITH src AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'stores') }}
),
typed AS (
  SELECT
    {{ safe_cast('store_id', 'bigint') }} AS store_id,
    {{ trim_string('store_code') }} AS store_code,
    {{ trim_string('name') }} AS name,
    {{ trim_string('channel') }} AS channel,
    {{ trim_string('region') }} AS region,
    {{ trim_string('state') }} AS state,
    {{ safe_cast('latitude', 'double') }} AS latitude,
    {{ safe_cast('longitude', 'double') }} AS longitude,
    {{ safe_cast('open_dt', 'date') }} AS open_dt,
    {{ safe_cast('close_dt', 'date') }} AS close_dt
  FROM src
)
SELECT * FROM typed