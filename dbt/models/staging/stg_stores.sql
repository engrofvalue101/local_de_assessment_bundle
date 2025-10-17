-- models/staging/stg_stores.sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'stores') }}
),

renamed AS (
  SELECT
    -- IDs
    {{ safe_cast('store_id', 'bigint') }} AS store_id,
    {{ trim_string('store_code') }} AS store_code,

    -- Store info
    {{ trim_string('name') }} AS store_name,
    {{ trim_string('channel') }} AS channel,
    {{ trim_string('region') }} AS region,
    {{ trim_string('state') }} AS state,
    {{ safe_cast('latitude', 'double') }} AS latitude,
    {{ safe_cast('longitude', 'double') }} AS longitude,
    {{ safe_cast('open_dt', 'date') }} AS open_dt,
    {{ safe_cast('close_dt', 'date') }} AS close_dt,

    -- Audit columns
    {{ convert_to_utc('ingestion_ts') }} AS ingestion_ts,
    src_filename,
    src_row_hash

  FROM source

)

SELECT * FROM renamed