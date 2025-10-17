{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'products') }}
),

renamed  AS (
  SELECT
    -- IDs
    {{ safe_cast('product_id', 'bigint') }} AS product_id,
    {{ trim_string('sku') }} AS sku,

    -- Product info
    {{ trim_string('name') }} AS product_name,
    {{ trim_string('category') }} AS category,
    {{ trim_string('subcategory') }} AS subcategory,
    {{ safe_cast('current_price', 'decimal(12,4)') }} AS current_price,
    {{ trim_string('currency') }} AS currency,
    
    -- FLags
    {{ safe_cast(handle_null('is_discontinued', 'false'), 'boolean') }} AS is_discontinued,

    -- Dates
    {{ safe_cast('introduced_dt', 'date') }} AS introduced_dt,
    {{ safe_cast('discontinued_dt', 'date') }} AS discontinued_dt,

    -- Audit columns
    ingestion_ts,
    src_filename,
    src_row_hash
    
  FROM source

)

SELECT * FROM renamed 