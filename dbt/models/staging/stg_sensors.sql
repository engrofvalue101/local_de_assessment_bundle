-- models/staging/stg_sensors.sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'sensors') }}
),

renamed  AS (
  SELECT
    -- Timestamp
    {{ convert_to_utc('sensor_ts') }} AS sensor_ts,

    -- IDs
    {{ safe_cast('store_id', 'bigint') }} AS store_id,
    {{ trim_string('shelf_id') }} AS shelf_id,

    -- Sensor readings
    {{ safe_cast('temperature_c', 'decimal(5,2)') }} AS temperature_c,
    {{ safe_cast('humidity_pct', 'decimal(5,2)') }} AS humidity_pct,
    {{ safe_cast('battery_mv', 'integer') }} AS battery_mv,

    -- Audit columns
    {{ convert_to_utc('ingestion_ts') }} AS ingestion_ts,
    src_filename,
    src_row_hash

  FROM source

)

SELECT * FROM renamed 