WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'order_lines') }}
),
cleaned AS (
  SELECT
    {{ safe_cast('order_id', 'bigint') }} AS order_id,
    {{ safe_cast('line_number', 'integer') }} AS line_number,
    {{ safe_cast('product_id', 'bigint') }} AS product_id,
    {{ safe_cast('qty', 'integer') }} AS qty,
    {{ safe_cast('unit_price', 'decimal(12,4)') }} AS unit_price,
    {{ safe_cast('line_discount_pct', 'decimal(5,4)') }} AS line_discount_pct,
    {{ safe_cast('tax_pct', 'decimal(5,4)') }} AS tax_pct,
    ingestion_ts
  FROM source
)
SELECT * FROM cleaned