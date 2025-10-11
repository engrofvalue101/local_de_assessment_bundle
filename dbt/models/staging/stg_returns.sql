WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'returns') }}
),

cleaned AS (
  SELECT
    {{ safe_cast('return_id', 'bigint') }} AS return_id,
    {{ safe_cast('order_id', 'bigint') }} AS order_id,
    {{ safe_cast('product_id', 'bigint') }} AS product_id,
    {{ convert_to_utc('return_ts') }} AS return_ts,
    {{ safe_cast('qty', 'integer') }} AS qty,
    {{ trim_string('reason') }} AS reason,
    ingestion_ts    
  FROM source
)

SELECT * FROM cleaned