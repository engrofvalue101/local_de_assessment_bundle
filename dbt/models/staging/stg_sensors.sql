WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'sensors') }}
),
cleaned AS (
  SELECT
    {{ convert_to_utc('sensor_ts') }} AS sensor_ts,
    {{ safe_cast('store_id', 'bigint') }} AS store_id,
    {{ trim_string('shelf_id') }} AS shelf_id,
    {{ safe_cast('temperature_c', 'decimal(5,2)') }} AS temperature_c,
    {{ safe_cast('humidity_pct', 'decimal(5,2)') }} AS humidity_pct,
    {{ safe_cast('battery_mv', 'integer') }} AS battery_mv,
    ingestion_ts
  FROM source
)
SELECT * FROM cleaned