WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'shipments') }}
),
cleaned AS (
  SELECT
    {{ safe_cast('shipment_id', 'bigint') }} AS shipment_id,
    {{ safe_cast('order_id', 'bigint') }} AS order_id,
    {{ trim_string('carrier') }} AS carrier,
    {{ convert_to_utc('shipped_at') }} AS shipped_at,
    {{ convert_to_utc('delivered_at') }} AS delivered_at,
    {{ safe_cast('ship_cost', 'decimal(12,4)') }} AS ship_cost,
    ingestion_ts
  FROM source
)
SELECT * FROM cleaned