WITH src AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'products') }}
),
typed AS (
  SELECT
    {{ safe_cast('product_id', 'bigint') }} AS product_id,
    {{ trim_string('sku') }} AS sku,
    {{ trim_string('name') }} AS product_name,
    {{ trim_string('category') }} AS category,
    {{ trim_string('subcategory') }} AS subcategory,
    {{ safe_cast('current_price', 'decimal(12,4)') }} AS current_price,
    {{ trim_string('currency') }} AS currency,
    {{ handle_null('is_discontinued', 'false') }} AS is_discontinued,
    {{ safe_cast('introduced_dt', 'date') }} AS introduced_dt,
    {{ safe_cast('discontinued_dt', 'date') }} AS discontinued_dt
  FROM src
)
SELECT * FROM typed