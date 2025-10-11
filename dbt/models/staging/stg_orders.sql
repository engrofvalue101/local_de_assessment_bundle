WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'orders') }}
),
cleaned AS (
  SELECT
    {{ safe_cast('order_id', 'bigint') }} AS order_id,
    {{ convert_to_utc('order_ts') }} AS order_ts,
    {{ safe_cast('order_dt_local', 'date') }} AS order_dt_local,
    {{ safe_cast('customer_id', 'bigint') }} AS customer_id,
    {{ safe_cast('store_id', 'bigint') }} AS store_id,
    {{ trim_string('channel') }} AS channel,
    {{ trim_string('payment_method') }} AS payment_method,
    {{ trim_string('coupon_code') }} AS coupon_code,
    {{ safe_cast('shipping_fee', 'decimal(12,2)') }} AS shipping_fee,
    {{ trim_string('currency') }} AS currency,
    ingestion_ts
  FROM source
)
SELECT * FROM cleaned