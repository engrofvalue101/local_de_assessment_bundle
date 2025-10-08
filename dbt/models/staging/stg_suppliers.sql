WITH src AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'suppliers') }}
),
typed AS (
  SELECT
    {{ safe_cast('supplier_id', 'bigint') }} AS product_id,
    {{ trim_string('supplier_code') }} AS sku,
    {{ trim_string('name') }} AS supplier_name,
    {{ trim_string('country_code') }} AS country_code,
    {{ safe_cast('lead_time_days', 'integer') }} AS lead_time_days,
    {{ handle_null('preferred', 'false') }} AS preferred
  FROM src
)
SELECT * FROM typed