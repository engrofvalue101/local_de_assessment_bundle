-- models/staging/stg_shipments.sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'shipments') }}
),

renamed AS (
  SELECT
    -- IDs
    {{ safe_cast('shipment_id', 'bigint') }} AS shipment_id,
    {{ safe_cast('order_id', 'bigint') }} AS order_id,

    -- Shipment details
    {{ trim_string('carrier') }} AS carrier,
    {{ convert_to_utc('shipped_at') }} AS shipped_at,
    {{ convert_to_utc('delivered_at') }} AS delivered_at,
    {{ safe_cast('ship_cost', 'decimal(12,4)') }} AS ship_cost,

    -- Audit columns
    {{ convert_to_utc('ingestion_ts') }} AS ingestion_ts,
    src_filename,
    src_row_hash

  FROM source

)

SELECT * FROM renamed 