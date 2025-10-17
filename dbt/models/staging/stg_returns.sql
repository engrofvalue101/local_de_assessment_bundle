-- models/staging/stg_returns.sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'returns') }}
),

renamed  AS (
  SELECT
    -- IDs
    {{ safe_cast('return_id', 'bigint') }} AS return_id,
    {{ safe_cast('order_id', 'bigint') }} AS order_id,
    {{ safe_cast('product_id', 'bigint') }} AS product_id,

    -- Return info
    {{ convert_to_utc('return_ts') }} AS return_ts,
    {{ safe_cast('qty', 'integer') }} AS qty,
    {{ trim_string('reason') }} AS reason,

    -- Audit columns
    {{ convert_to_utc('ingestion_ts') }} AS ingestion_ts,
    src_filename,
    src_row_hash
    
  FROM source

)

SELECT * FROM renamed 