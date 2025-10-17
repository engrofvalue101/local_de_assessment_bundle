-- models/staging/stg_orders_lines.sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'order_lines') }}
),

renamed AS (
  SELECT
    -- IDs
    {{ safe_cast('order_id', 'bigint') }} AS order_id,
    {{ safe_cast('line_number', 'integer') }} AS line_number,
    {{ safe_cast('product_id', 'bigint') }} AS product_id,

    -- Line details
    {{ safe_cast('qty', 'integer') }} AS qty,
    {{ safe_cast('unit_price', 'decimal(12,4)') }} AS unit_price,
    {{ safe_cast('line_discount_pct', 'decimal(5,4)') }} AS line_discount_pct,
    {{ safe_cast('tax_pct', 'decimal(5,4)') }} AS tax_pct,

    -- Audit columns
    {{ convert_to_utc('ingestion_ts') }} AS ingestion_ts,
    src_filename,
    src_row_hash

  FROM source
  
)

SELECT * FROM renamed