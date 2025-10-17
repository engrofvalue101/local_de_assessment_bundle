-- models/staging/stg_orders.sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'orders') }}
),

renamed  AS (
  SELECT
    -- IDs
    {{ safe_cast('order_id', 'bigint') }} AS order_id,
    {{ safe_cast('customer_id', 'bigint') }} AS customer_id,
    {{ safe_cast('store_id', 'bigint') }} AS store_id,

    -- Order info
    {{ convert_to_utc('order_ts') }} AS order_ts,
    {{ safe_cast('order_dt_local', 'date') }} AS order_dt_local,
    {{ trim_string('channel') }} AS channel,
    {{ trim_string('payment_method') }} AS payment_method,
    {{ trim_string('coupon_code') }} AS coupon_code,
    {{ safe_cast('shipping_fee', 'decimal(12,2)') }} AS shipping_fee,
    {{ trim_string('currency') }} AS currency,

    -- Audit columns
    {{ convert_to_utc('ingestion_ts') }} AS ingestion_ts,
    src_filename,
    src_row_hash

  FROM source

)

SELECT * FROM renamed